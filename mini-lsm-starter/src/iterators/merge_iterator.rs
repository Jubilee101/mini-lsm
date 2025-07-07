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

use crate::key::{Key, KeySlice};
use anyhow::{Result, anyhow};
use log::error;
use std::cmp::{self, Ordering};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::process::id;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut heap = BinaryHeap::new();

        if iters.is_empty() {
            let mut miter = MergeIterator {
                iters: heap,
                current: None,
            };
            return miter;
        }

        for (idx, iter) in iters.into_iter().enumerate() {
            if !iter.is_valid() {
                continue;
            }
            heap.push(HeapWrapper(idx, iter));
        }

        let current = heap.pop();
        MergeIterator {
            iters: heap,
            current: current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        // could be cases where current becomes invalid, and we can't find any iter to substitute
        self.current
            .as_ref()
            .map(|cur| cur.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        // assume next won't be called before is_valid
        let current = self.current.as_mut().unwrap();
        // wrapper's lifetime is too short. We cannot keep anything inside beyond that life time
        // that's why we keep current
        // we'll try to advance the iters so that they all skip the current key, then do the same with current
        // be mindful about none and in valid iters
        while let Some(mut wrapper) = self.iters.peek_mut() {
            if wrapper.1.key() != current.1.key() {
                break;
            }
            if let e @ Err(_) = wrapper.1.next() {
                PeekMut::pop(wrapper);
                return e;
            }
            if !wrapper.1.is_valid() {
                PeekMut::pop(wrapper);
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            if let Some(wrapper) = self.iters.pop() {
                *current = wrapper;
            }
            return Ok(());
        }

        if let Some(mut wrapper) = self.iters.peek_mut() {
            if &mut *wrapper > current {
                std::mem::swap(&mut *wrapper, current)
            }
        }

        Ok(())
    }
}
