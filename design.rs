use std::collections::VecDeque;
use std::path::{Path, PathBuf};

fn default<T: Default>() -> T {
    Default::default()
}

type Result<T> = std::result::Result<T, ()>;
type HashDigest = String;
type DateTime = String;

struct RecvToFile {
    blocks: VecDeque<HashDigest>,
}

impl RecvToFile {
    fn new_file(
        &mut self,
        path: &Path,
        _modified: DateTime,
    ) -> Result<()>
    {
        println!("New file: {:?}", path);
        Ok(())
    }

    fn new_block(&mut self, hash: &HashDigest, _size: usize) -> Result<()> {
        println!("New block: {:?}", hash);
        self.blocks.push_back(hash.into());
        Ok(())
    }

    fn feed_block(&mut self, hash: &HashDigest, _block: &[u8]) -> Result<()> {
        println!("Got block data for {:?}", hash);
        Ok(())
    }

    fn next_requested_block(&mut self) -> Result<Option<HashDigest>> {
        Ok(self.blocks.pop_front())
    }

    fn is_missing_blocks(&self) -> Result<bool> {
        Ok(!self.blocks.is_empty())
    }
}

struct SendFromIndex {
    index: VecDeque<IndexEvent>,
    blocks: VecDeque<HashDigest>,
}

enum IndexEvent {
    NewFile(PathBuf),
    NewBlock(HashDigest, usize),
    End,
}

impl SendFromIndex {
    fn next_from_index(&mut self) -> Result<Option<IndexEvent>> {
        Ok(self.index.pop_front())
    }

    fn request_block(&mut self, hash: &HashDigest) -> Result<()> {
        self.blocks.push_back(hash.into());
        Ok(())
    }

    fn get_next_block(&mut self) -> Result<Option<(HashDigest, Vec<u8>)>> {
        Ok(
            self.blocks.pop_front()
                .map(|hash| (hash, vec![1u8, 2, 3, 4, 5]))
        )
    }
}

fn do_stream(recv: &mut RecvToFile, send: &mut SendFromIndex) -> Result<()> {
    let mut instructions = true;
    while instructions || recv.is_missing_blocks()? {
        // Things are done in order so that bandwidth is used in a smart way
        // For example, if you block on sending block data, you will have
        // received more block requests in the next loop, and you'll only
        // transmit (sender side) or process (receiver side) index instructions
        // when there's nothing better to do
        if let Some(hash) = recv.next_requested_block()? {
            // Block requests
            send.request_block(&hash)?; // can block on HTTP receiver side
        } else if let Some((hash, block)) =
            send.get_next_block()? // blocks on receiver side
        {
            // Block data
            recv.feed_block(&hash, &block)?; // blocks on sender side
        } else if let Some(event) = send.next_from_index()? {
            // Index instructions
            match event {
                IndexEvent::NewFile(path) => {
                    recv.new_file(&path, "today".into())?
                }
                IndexEvent::NewBlock(hash, size) => {
                    recv.new_block(&hash, size)?
                }
                IndexEvent::End => {
                    instructions = false;
                }
            }
        }
    }
    Ok(())
}

fn main() {
    let mut recv = RecvToFile { blocks: default() };
    let mut send = SendFromIndex {
        index: {
            use IndexEvent::NewFile as F;
            use IndexEvent::NewBlock as Bl;
            let mut buf = VecDeque::new();
            buf.push_back(F("file1.txt".into()));
            buf.push_back(Bl("abcd".into(), 4));
            buf.push_back(Bl("f1f1".into(), 6));
            buf.push_back(F("other.txt".into()));
            buf.push_back(Bl("4678".into(), 3));
            buf.push_back(Bl("abcd".into(), 4));
            buf.push_back(IndexEvent::End);
            buf
        },
        blocks: default(),
    };
    do_stream(&mut recv, &mut send).unwrap();
}
