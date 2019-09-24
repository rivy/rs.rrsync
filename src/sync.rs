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
//! [`SyncHandler`](struct.SyncHandler.html) is the low-level handler, allowing
//! you to feed file names and blocks that you deserialize yourself, and
//! requesting blocks as hashes.
//!
//! [`SyncStream`](struct.SyncStream.html) takes in a combined index/blocks
//! stream and writes a stream of serialized block requests to a buffer. It is
//! used by the SSH mode which uses the remote command's stdin/stdout for
//! communication. It is *not* used by the HTTP download code, which downloads
//! the new index as a SQLite database and requests blocks in separate HTTP
//! requests.

use cdchunking::{Chunker, ZPAQ};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::collections::hash_map::{Entry, HashMap};
use std::io::{Seek, SeekFrom, Write};
use std::fs::{OpenOptions, File};
use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::rc::Rc;

use crate::{Error, HashDigest};
use crate::index::{MAX_BLOCK_SIZE, ZPAQ_BITS, Index, IndexTransaction};

struct TempFile {
    file: File,
    temp_file_id: u32,
    temp_path: PathBuf,
    destination: PathBuf,
}

impl TempFile {
    fn move_to_destination(self) -> Result<(), std::io::Error> {
        std::fs::rename(self.temp_path, self.destination)
    }
}

fn read_block(path: &Path, offset: usize) -> Result<Vec<u8>, Error> {
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(offset as u64))?;
    let chunker = Chunker::new(
        ZPAQ::new(ZPAQ_BITS)
    ).max_size(MAX_BLOCK_SIZE);
    let block = chunker.whole_chunks(file).next().unwrap()?;
    Ok(block)
}

fn write_block(
    file: &mut File,
    offset: usize,
    block: &[u8],
) -> Result<(), Error>
{
    // FIXME: Can you seek past the end?
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(block)?;
    Ok(())
}

/// Low-level handler, allowing you to feed file names and blocks
///
/// You have to deserialize those yourself, and request blocks through a
/// mechanism of your choice.
pub struct SyncHandler<'a> {
    index: IndexTransaction<'a>,
    current_file: Option<(usize, Rc<RefCell<TempFile>>)>,
    waiting_blocks: HashMap<HashDigest, Vec<(Rc<RefCell<TempFile>>, usize)>>,
    blocks_to_request: VecDeque<HashDigest>,
}

impl<'a> SyncHandler<'a> {
    /// Create a handler from the local (destination) index
    pub fn new(index: IndexTransaction<'a>) -> SyncHandler<'a> {
        SyncHandler {
            index,
            current_file: None,
            waiting_blocks: HashMap::new(),
            blocks_to_request: VecDeque::new(),
        }
    }

    fn finish_file(&mut self, file: TempFile) -> Result<(), Error> {
        info!("File complete: {:?}", file.destination);
        self.index.move_file(file.temp_file_id, &file.destination)?;
        file.move_to_destination()?;
        Ok(())
    }

    /// Start on a new file
    pub fn new_file(
        &mut self,
        path: &Path,
        modified: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), Error>
    {
        if let Some((size, file)) = self.current_file.take() {
            debug!("File {:?} will be {} bytes", file.borrow().destination, size);

            // If we have the last reference to that TempFile, move it
            if let Ok(file) = Rc::try_unwrap(file) {
                self.finish_file(file.into_inner())?;
            }
        }

        // Make temp file path, which will be swapped at the end
        let temp_path = {
            let mut base_name = path.file_name()
                .ok_or(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "Invalid file name",
                )))?
                .to_os_string();
            base_name.push(".part");
            path.with_file_name(base_name)
        };

        // Open it, but check if it existed
        let file_exists = temp_path.is_file();
        let file = OpenOptions::new().read(true).write(true).create(true)
            .open(&temp_path)?;

        // Create temp file entry in the database
        let file_id = self.index.add_file_overwrite(path, modified)?;

        // If file existed, index; it might have content from aborted download
        if file_exists {
            info!("Indexing previous part file {:?}", temp_path);
            self.index.index_file(&temp_path)?;
        }

        let file = TempFile {
            file,
            temp_file_id: file_id,
            temp_path,
            destination: path.to_owned(),
        };
        let file = Rc::new(RefCell::new(file));
        self.current_file = Some((0, file));
        Ok(())
    }

    /// Feed entry from the new index
    pub fn new_block(
        &mut self,
        hash: &HashDigest,
        size: usize,
    ) -> Result<(), Error>
    {
        let &mut (ref mut offset, ref mut file) = &mut self.current_file
            .as_mut()
            .ok_or(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Got a block before any file",
            )))?;

        info!(
            "Next block: {} for {:?} offset={}",
            hash, file.borrow().destination, *offset,
        );

        // We need to write this block to the current file
        match self.index.get_block(hash)? {
            // We know where to get it, copy it from there
            Some((path, read_offset)) => {
                info!("Getting block from {:?} offset={}", path, read_offset);
                let block = read_block(&path, read_offset)?;
                write_block(&mut file.borrow_mut().file, *offset, &block)?;
                self.index.replace_block(
                    &hash,
                    file.borrow().temp_file_id,
                    *offset,
                    block.len(),
                )?;
            }
            // We don't have this block, we'll have to wait for it
            None => {
                // Was it already requested?
                match self.waiting_blocks.entry(hash.clone()) {
                    Entry::Occupied(ref mut destinations) => {
                        // Add this to the list of where to write the block
                        destinations.get_mut().push((file.clone(), *offset));
                        info!("Block has already been requested");
                    }
                    Entry::Vacant(v) => {
                        // Request it
                        v.insert(vec![(file.clone(), *offset)]);
                        self.blocks_to_request.push_back(hash.clone());
                        info!("Requesting block");
                    }
                }
            }
        }
        *offset += size;
        Ok(())
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
        // Write the block to the destinations waiting for it
        if let Some(destinations) = self.waiting_blocks.remove(hash) {
            info!("Got block {}", hash);
            for (file, offset) in destinations.into_iter() {
                info!(
                    "Writing block to {:?} offset={}",
                    file.borrow().destination, offset,
                );
                write_block(&mut file.borrow_mut().file, offset, block)?;
                self.index.replace_block(
                    &hash,
                    file.borrow().temp_file_id,
                    offset,
                    block.len(),
                )?;

                // If we have the last reference to that TempFile, move it
                if let Ok(file) = Rc::try_unwrap(file) {
                    self.finish_file(file.into_inner())?;
                }
            }
        } else {
            warn!("Got block we didn't need: {}", hash);
        }
        Ok(())
    }

    /// Ask which blocks to get next
    pub fn next_requested_block(
        &mut self,
    ) -> Result<Option<HashDigest>, Error>
    {
        Ok(self.blocks_to_request.pop_front())
    }
}

/// Handler for the combined index/blocks stream
///
/// This handles deserializing the index and block requests from a combined
/// stream, and serializes block requests as well.
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
