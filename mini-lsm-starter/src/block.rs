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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut block = Vec::new();
        let mut offsets_encoded = Vec::new();
        for offset in &self.offsets {
            offsets_encoded.extend_from_slice(&offset.to_ne_bytes());
        }
        block.extend_from_slice(&self.data[..]);
        block.extend_from_slice(&offsets_encoded[..]);

        let num_entries = self.offsets.len() as u16;
        block.extend_from_slice(&num_entries.to_ne_bytes());

        Bytes::from(block)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        assert!(data.len() >= 2);
        let num_bytes = &data[data.len() - 2..];
        let num = u16::from_ne_bytes([num_bytes[0], num_bytes[1]]);
        let offset_bytes = &data[(data.len() - 2 * (num as usize + 1))..data.len() - 2];
        let mut offsets = Vec::new();

        for i in (0..offset_bytes.len()).step_by(2) {
            offsets.push(u16::from_ne_bytes([offset_bytes[i], offset_bytes[i + 1]]))
        }

        Self {
            offsets,
            data: Vec::from(&data[0..(data.len() - 2 * (num as usize + 1))]),
        }
    }
}
