// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::collections::HashMap;
use std::{fs, mem};
use std::ops::{Bound, Deref};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(())?;
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        if !path.exists() {
            fs::create_dir_all(path)?
        }

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };
        let mut sstable_iters = vec![];

        // check the current memtable
        if let Some(val) = snapshot.memtable.get(_key) {
            return if !val.is_empty() {
                Ok(Some(val))
            } else {
                Ok(None)
            };
        }

        // check the frozen memtables from the newest to the oldest
        for imm_table in &snapshot.imm_memtables {
            if let Some(val) = imm_table.get(_key) {
                return if !val.is_empty() {
                    Ok(Some(val))
                } else {
                    Ok(None)
                };
            }
        }

        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if !key_within(table.first_key().as_key_slice(), table.last_key().as_key_slice(), _key) {
                continue
            }
            let mut iter: SsTableIterator;
            iter = SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(_key))?;

            if iter.is_valid() {
                sstable_iters.push(Box::new(iter));
            }
        }

        let miter = MergeIterator::create(sstable_iters);
        if miter.is_valid() && miter.key() == KeySlice::from_slice(_key) {
            if !miter.value().is_empty() {
                return Ok(Some(Bytes::copy_from_slice(miter.value())));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        let guard = self.state.read();
        // I don't think we can drop the guard here, because otherwise another thread can freeze the memtable
        guard.memtable.put(_key, _value)?;
        let approximate_size = guard.memtable.approximate_size();
        // it's crucial to drop the guard here, otherwise it may try waiting for the state lock
        // that's currently grabbed by another thread that's waiting to grab the write lock
        // (and it can't if the read guard isn't dropped here)
        drop(guard);

        self.try_freeze_memtable(approximate_size)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let guard = self.state.read();
        guard.memtable.put(_key, &[])?;

        let approximate_size = guard.memtable.approximate_size();
        drop(guard);

        self.try_freeze_memtable(approximate_size)
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let memtable = Arc::new(MemTable::create(self.next_sst_id()));

        let rguard = self.state.read();

        // make a snapshot of the current state
        // as_ref extracts the inner mem Arc wraps, otherwise you are just cloning Arc itself
        // clone() can be done because LsmStorageState has Clone() trait derived

        // doing Arc.field = new_value is generally unacceptable.
        // this often cannot be done because it's unsafe due to Arc's nature of being shared by many.
        // For this reason, you shouldn't wrap snapshot inside Arc too early if you want to change the fields within
        let mut snapshot = rguard.as_ref().clone();

        // The following line is result-wise equivalent to
        //
        // let old_memtable = snapshot.memtable.clone();
        // snapshot.memtable = memtable;
        //
        // They work with different principles
        let old_memtable = std::mem::replace(&mut snapshot.memtable, memtable);
        // freeze the old memtable, this won't be truly frozen until the state is replaced by the modified snapshot
        // before that the old_mentable can still have kv pairs put into it by other threads
        // they may also try freezing the memtable, but will be blocked by the state_lock
        snapshot.imm_memtables.insert(0, old_memtable);

        drop(rguard);

        let mut guard = self.state.write();
        // change what guard points directly
        *guard = Arc::new(snapshot);

        drop(guard);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // acquire the state lock first in case one memtable gets popped twice
        let lock = self.state_lock.lock();
        // copy on write, and drop read lock immediately
        let mut snapshot = {
            let guard = self.state.read();
            guard.as_ref().clone()
        };
        
        if snapshot.imm_memtables.is_empty() {
            return Ok(())
        }
        
        let memtable = snapshot.imm_memtables.pop().unwrap();
        // flush the memtable to disk
        let mut builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut builder)?;
        let next_id = self.next_sst_id();
        let table = builder.build(next_id, Some(self.block_cache.clone()), self.path_of_sst(next_id))?;
        
        // bookkeeping
        snapshot.l0_sstables.insert(0, next_id);
        snapshot.sstables.insert(next_id, Arc::new(table));
        
        // write the copy back
        let mut wguard = self.state.write();
        // this probably will work as well mem::replace(&mut *wguard, Arc::new(snapshot));
        // the wguard and self.state sharing the same pointer that points to the same address,
        // or you can directly change the changed fields I think
        *wguard = Arc::new(snapshot);
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let mut memtable_iters = vec![];
        let mut sstable_iters = vec![];

        let mut snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        }; // the lock is dropped here, 
        // it's safe since even there are insert into the memtable skiplist
        // because recall that crossbeam skip list only need an immutable reference
        // this is achieved because the skip list has lock free atomic APIs, you are only seeing a snapshot

        memtable_iters.push(Box::new(snapshot.memtable.scan(_lower, _upper)));

        for iter in &snapshot.imm_memtables {
            memtable_iters.push(Box::new(iter.scan(_lower, _upper)));
        }

        for table_id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[table_id].clone();
            if !range_overlap(table.first_key().as_key_slice(), table.last_key().as_key_slice(), _lower, _upper) {
                continue
            }
            let mut iter: SsTableIterator;
            match _lower {
                Bound::Unbounded => {
                    iter = SsTableIterator::create_and_seek_to_first(table.clone())?
                }
                Bound::Included(bound) => {
                    iter =
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(bound))?
                }
                Bound::Excluded(bound) => {
                    iter = SsTableIterator::create_and_seek_to_key(
                        table,
                        KeySlice::from_slice(bound),
                    )?;
                    // TODO: why is this enough, do we guarantee the first keys in blocks are ordered?
                    while iter.is_valid() && iter.key() == KeySlice::from_slice(bound) {
                        iter.next()?;
                    }
                }
            }
            if iter.is_valid() {
                sstable_iters.push(Box::new(iter));
            }
        }

        //TODO: level iters

        let miter1 = MergeIterator::create(memtable_iters);
        let miter2 = MergeIterator::create(sstable_iters);
        let two_merge_iter = TwoMergeIterator::create(miter1, miter2)?;
        let iter = FusedIterator::new(LsmIterator::new(
            two_merge_iter,
            _upper.map(|bound| Bytes::copy_from_slice(bound)),
        )?);

        Ok(iter)
    }

    fn try_freeze_memtable(&self, size: usize) -> Result<()> {
        if size > self.options.target_sst_size {
            let lock = self.state_lock.lock();

            let rguard = self.state.read();
            let cur_size = rguard.memtable.approximate_size();
            drop(rguard);

            if cur_size.ge(&self.options.target_sst_size) {
                self.force_freeze_memtable(&lock)?;
            }
        }
        Ok(())
    }
}

fn range_overlap(table_first: KeySlice, table_last: KeySlice, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> bool {
    match lower {
        Bound::Unbounded => {
            match upper {
                Bound::Unbounded => {true}
                Bound::Included(bound_upper) => {
                    KeySlice::from_slice(bound_upper) >= table_first
                }
                Bound::Excluded(bound_upper) => {
                    KeySlice::from_slice(bound_upper) > table_first
                }
            }
        }
        Bound::Included(bound_lower) => {
            match upper {
                Bound::Unbounded => {
                    KeySlice::from_slice(bound_lower) <= table_last
                }
                Bound::Included(bound_upper) => {
                    KeySlice::from_slice(bound_lower) <= table_last && KeySlice::from_slice(bound_upper) >= table_first
                }
                Bound::Excluded(bound_upper) => {
                    KeySlice::from_slice(bound_lower) <= table_last && KeySlice::from_slice(bound_upper) > table_first
                }
            }
        }
        Bound::Excluded(bound_lower) => {
            match upper {
                Bound::Unbounded => {
                    KeySlice::from_slice(bound_lower) < table_last
                }
                Bound::Included(bound_upper) => {
                    KeySlice::from_slice(bound_lower) < table_last && KeySlice::from_slice(bound_upper) >= table_first
                }
                
                Bound::Excluded(bound_upper) => {
                    KeySlice::from_slice(bound_lower) < table_last && KeySlice::from_slice(bound_upper) > table_first
                }
            }
        }
    }
}

fn key_within(table_first: KeySlice, table_last: KeySlice, key: &[u8]) -> bool {
    let key_slice = KeySlice::from_slice(key);
    key_slice >= table_first && key_slice <= table_last
}