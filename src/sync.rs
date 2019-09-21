//! This module contains the transfer protocol handler.
//!
//! The general architecture is as follows:
//!
//! ```plain
//!                          +---------+
//!                          |         |  old index
//! +--------+   new index   | handler | <----------+
//! |        | +-----------> |         |
//! | sender |               | (recv)  |
//! |        |               |         |
//! |        | request block |         |
//! |        | <-----------+ |         |
//! |        |               |         |
//! |        |  send block   |         | update files
//! +--------+ +-----------> |         | +---------->
//!                          +---------+
//! ```
//!
//! First the old index is computed and loaded in full.
//!
//! Then, the new index is fed in either all at once or in a streaming fashion.
//!
//! The handler will request blocks that are missing from the destination,
//! which are fed in as they are received.
//!
//! `SyncHandler` is the low-level handler, allowing you to feed file names and
//! blocks that you deserialize yourself, and requesting blocks as hashes.
//!
//! `SyncStream` takes in a combined index/blocks stream and writes a stream of
//! serialized block requests to a buffer. It is used by the SSH mode which
//! uses the remote command's stdin/stdout for communication. It is *not* used
//! by the HTTP download code, which downloads the new index as a SQLite
//! database and requests blocks in separate HTTP requests.

use std::ops::{Deref, DerefMut};
use std::path::Path;

use crate::{Error, HashDigest};
use crate::index::Index;

#[derive(Default)]
pub struct SyncHandler;

impl SyncHandler {
    /// Start on a new file
    pub fn new_file(&mut self, path: &Path) -> Result<(), Error> {
        unimplemented!()
    }

    /// Feed entry from the new index
    pub fn new_block(
        &mut self,
        hash: &HashDigest,
    ) -> Result<(), Error>
    {
        unimplemented!()
    }

    /// Feed a whole new index
    pub fn new_index(&mut self, new_index: &Index) -> Result<(), Error> {
        unimplemented!()
    }

    /// Feed a block that was requested
    pub fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error>
    {
        unimplemented!()
    }

    /// Ask which blocks to get next
    pub fn next_requested_block(
        &mut self,
    ) -> Result<Option<HashDigest>, Error>
    {
        unimplemented!()
    }
}

#[derive(Default)]
pub struct SyncStream {
    handler: SyncHandler,
    buffer: Vec<u8>,
}

impl Deref for SyncStream {
    type Target = SyncHandler;

    fn deref(&self) -> &SyncHandler {
        &self.handler
    }
}

impl DerefMut for SyncStream {
    fn deref_mut(&mut self) -> &mut SyncHandler {
        &mut self.handler
    }
}

impl SyncStream {
    /// Feed bytes from the combined input
    pub fn update(
        &mut self,
        bytes_in: &[u8],
        bytes_out: &mut [u8],
    ) -> Result<usize, Error>
    {
        unimplemented!()
    }
}
