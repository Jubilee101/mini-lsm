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

use std::ops::Deref;
use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);

        iter.seek_to_first();

        if !iter.key.is_empty() {
            iter.first_key = iter.key.clone();
        }

        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut iter = Self::new(block);

        iter.seek_to_key(key);

        if !iter.key.is_empty() {
            iter.first_key = iter.key.clone();
        }

        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let num_entries = self.block.offsets.len();
        if num_entries == 0 {
            return;
        }

        self.idx = 0;
        let key_size = u16::from_ne_bytes([self.block.data[0], self.block.data[1]]);
        let value_offset = 2 + key_size as usize;
        let value_size = u16::from_ne_bytes([
            self.block.data[value_offset],
            self.block.data[value_offset + 1],
        ]);

        self.key = KeyVec::from_vec(Vec::from(&self.block.data[2..2 + key_size as usize]));
        self.value_range = (value_offset + 2, value_offset + 2 + value_size as usize);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        if self.idx + 1 >= self.block.offsets.len() {
            self.key = KeyVec::new();
            return;
        }

        let offset = self.block.offsets[self.idx + 1];
        self.key = KeyVec::from_vec(Vec::from(self.get_key_at(offset as usize).into_inner()));

        let value_offset = offset as usize + 2 + self.key.len();
        let value_size = u16::from_ne_bytes([
            self.block.data[value_offset],
            self.block.data[value_offset + 1],
        ]);
        self.value_range = (value_offset + 2, value_offset + 2 + value_size as usize);
        self.idx = self.idx + 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        let num_entries = self.block.offsets.len();
        if num_entries == 0 {
            return;
        }

        match self.find_key(key) {
            None => {
                self.key = KeyVec::new();
                return;
            }
            Some(idx) => {
                let offset = self.block.offsets[idx];
                self.key =
                    KeyVec::from_vec(Vec::from(self.get_key_at(offset as usize).into_inner()));

                let value_offset = offset as usize + 2 + self.key.len();
                let value_size = u16::from_ne_bytes([
                    self.block.data[value_offset],
                    self.block.data[value_offset + 1],
                ]);
                self.value_range = (value_offset + 2, value_offset + 2 + value_size as usize);
                self.idx = idx;
            }
        }
    }

    fn get_key_at(&self, offset: usize) -> KeySlice {
        let key_size = u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]);
        KeySlice::from_slice(&self.block.data[offset + 2..offset + 2 + key_size as usize])
    }

    fn find_key(&self, target: KeySlice) -> Option<usize> {
        let mut left = 0usize;
        let mut right = self.block.offsets.len() - 1;

        while left + 1 < right {
            let mid = (left + right) / 2;
            let offset = self.block.offsets[mid];
            let cur_key = self.get_key_at(offset as usize);
            if cur_key == target {
                return Some(mid);
            }

            if cur_key < target {
                left = mid
            } else {
                right = mid
            }
        }

        if self.get_key_at(self.block.offsets[left] as usize) >= target {
            return Some(left);
        } else if self.get_key_at(self.block.offsets[right] as usize) >= target {
            return Some(right);
        }

        None
    }
}
