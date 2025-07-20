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

use std::io::Bytes;
use std::path::Path;
use std::sync::Arc;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::{Key, KeyBytes, KeyVec};
use crate::{block::BlockBuilder, key::KeySlice, lsm_storage::BlockCache};
use anyhow::Result;
use bytes::BufMut;
use rustyline::Event::KeySeq;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
        }
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if !self.builder.add(key, value) {
            self.finish_block();
            // try adding the kv again
            let _ = self.builder.add(key, value);
        }

        if self.first_key.is_empty() {
            self.first_key.extend_from_slice(key.raw_ref());
        }
        self.last_key = Vec::from(key.raw_ref())
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.data.len()
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut data = Vec::new();
        let mut meta_data = Vec::new();

        self.finish_block();

        let data_len = self.data.len() as u32;
        data.extend(self.data);

        BlockMeta::encode_block_meta(&self.meta[..], &mut meta_data);
        data.extend(meta_data);
        data.put_u32_ne(data_len);

        let sstable = SsTable {
            file: FileObject::create(path.as_ref(), data)?,
            block_meta_offset: data_len as usize,
            id,
            block_cache,
            first_key: self.meta.first().unwrap().first_key.clone(),
            last_key: self.meta.last().unwrap().last_key.clone(),
            block_meta: self.meta,
            bloom: None,
            max_ts: 0,
        };

        Ok(sstable)
    }

    fn finish_block(&mut self) {
        // split a new block
        let new_builder = BlockBuilder::new(self.block_size);
        let old_builder = std::mem::replace(&mut self.builder, new_builder);

        let block = old_builder.build();
        // record meta data
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: Key::from_vec(self.first_key.clone()).into_key_bytes(),
            last_key: Key::from_vec(self.last_key.clone()).into_key_bytes(),
        });
        self.data.extend(block.encode());

        // reset first key and last key
        self.first_key = Vec::new();
        self.last_key = Vec::new();
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
