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

use crate::key::{KeySlice, KeyVec};
use nom::ToUsize;

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        let builder = Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
        };
        builder
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let pre_size = self.data.len() as u16;
        let key_size = key.len() as u16;
        let value_size = value.len() as u16;

        if self.size() + (2 + key_size as usize + 2 + value_size as usize + 2) > self.block_size
            && !self.first_key.is_empty()
        {
            return false;
        }

        self.data.extend_from_slice(&key_size.to_ne_bytes());
        self.data.extend_from_slice(key.raw_ref());
        self.data.extend_from_slice(&value_size.to_ne_bytes());
        self.data.extend_from_slice(value);

        self.offsets.append(&mut vec![pre_size]);

        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.first_key.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn size(&self) -> usize {
        // entries + offsets + number of entries
        self.data.len() + self.offsets.len() * 2 + 2
    }
}
