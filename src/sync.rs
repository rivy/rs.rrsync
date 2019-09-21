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

use std::collections::HashMap;
use std::fs::File;
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use crate::{Error, HashDigest};
use crate::index::{Index, IndexTransaction};

pub struct SyncHandler<'a> {
    index: IndexTransaction<'a>,
    current_file: Option<(PathBuf, u32, File)>,
    waiting_blocks: HashMap<HashDigest, (File, usize)>,
}

impl<'a> SyncHandler<'a> {
    /// Create a handler from the local (destination) index
    pub fn new(index: IndexTransaction<'a>) -> SyncHandler<'a> {
        SyncHandler {
            index,
            current_file: None,
            waiting_blocks: HashMap::new(),
        }
    }

    /// Start on a new file
    pub fn new_file(&mut self, path: &Path) -> Result<(), Error> {
        let file_id = unimplemented!(); // TODO: Create temp file in db
        let mut temp_file_name = path.file_name()
            .ok_or_else(|| Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Invalid file name",
            )))?
            .to_os_string();
        temp_file_name.push(".part");
        let file = File::create(path.with_file_name(temp_file_name))?;
        self.current_file = Some((path.to_owned(), file_id, file));
        Ok(())
    }

    /// Feed entry from the new index
    pub fn new_block(
        &mut self,
        hash: &HashDigest,
    ) -> Result<(), Error>
    {
        // TODO: Look it up in index
        // If not in index, add to waiting_blocks and request it
        unimplemented!()
    }

    /// Feed a whole new index
    pub fn new_index(&mut self, new_index: &Index) -> Result<(), Error> {
        // TODO: Go over index and feed it to new_file()/new_block()
        // Maybe can be more efficient? Don't know
        unimplemented!()
    }

    /// Feed a block that was requested
    pub fn feed_block(
        &mut self,
        hash: &HashDigest,
        block: &[u8],
    ) -> Result<(), Error>
    {
        // TODO: Write it to files waiting for it, update temp file in index
        unimplemented!()
    }

    /// Ask which blocks to get next
    pub fn next_requested_block(
        &mut self,
    ) -> Result<Option<HashDigest>, Error>
    {
        // TODO: Read from an additional queue? (VecDeque?)
        unimplemented!()
    }
}

pub struct SyncStream<'a> {
    handler: SyncHandler<'a>,
    buffer: Vec<u8>,
}

impl<'a> Deref for SyncStream<'a> {
    type Target = SyncHandler<'a>;

    fn deref(&self) -> &SyncHandler<'a> {
        &self.handler
    }
}

impl<'a> DerefMut for SyncStream<'a> {
    fn deref_mut(&mut self) -> &mut SyncHandler<'a> {
        &mut self.handler
    }
}

impl<'a> SyncStream<'a> {
    /// Create a handler from the local (destination) index
    pub fn new(index: IndexTransaction<'a>) -> SyncStream<'a> {
        SyncStream {
            handler: SyncHandler::new(index),
            buffer: Vec::new(),
        }
    }

    /// Feed bytes from the combined input
    pub fn update(
        &mut self,
        bytes_in: &[u8],
        bytes_out: &mut [u8],
    ) -> Result<usize, Error>
    {
        // TODO: Deserialize/serialize
        unimplemented!()
    }
}
