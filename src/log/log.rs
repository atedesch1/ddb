use lib::error::{Error, Result};

use std::{
    collections::VecDeque,
    fs::{create_dir_all, File, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

pub struct Log {
    /// A file that stores the on-disk logs
    file: File,
    /// A queue with the uncommitted log entries
    uncommited: VecDeque<Vec<u8>>,
}

impl Log {
    pub fn new(dir: &Path) -> Result<Log> {
        create_dir_all(dir)?;

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(dir.join("log"))
            .unwrap();

        return Ok(Log {
            file,
            uncommited: VecDeque::new(),
        });
    }

    pub fn append(&mut self, entry: Vec<u8>) {
        self.uncommited.push_back(entry);
    }

    pub fn commit(&mut self, num_of_entries: usize) -> Result<()> {
        if self.uncommited.len() < num_of_entries {
            return Err(Error::Internal(format!(
                "Not enough uncommitted entries {}",
                self.uncommited.len()
            )));
        }
        self.file.seek(std::io::SeekFrom::End(0))?;
        for _ in 0..num_of_entries {
            let entry = self
                .uncommited
                .pop_front()
                .ok_or_else(|| Error::Internal("Unexpected end of uncommitted entries".into()))?;
            let len = (entry.len() as u32).to_be_bytes();
            self.file.write_all(&len)?;
            self.file.write_all(&entry)?;
            self.file.flush()?;
        }
        Ok(())
    }

    pub fn read(&mut self) -> Result<Vec<Vec<u8>>> {
        let mut entries: Vec<Vec<u8>> = Vec::new();
        let mut len_bytes = [0u8; 4];
        self.file.seek(std::io::SeekFrom::Start(0))?;
        while let Ok(()) = self.file.read_exact(&mut len_bytes) {
            let len = u32::from_be_bytes(len_bytes);
            let mut entry = vec![0u8; len as usize];
            self.file.read_exact(&mut entry)?;
            entries.push(entry);
        }
        return Ok(entries);
    }
}

#[test]
fn test_commit_read() -> Result<()> {
    let dir = tempdir::TempDir::new("test-log")?;
    let mut log = Log::new(dir.as_ref())?;
    log.append(vec![0x00]);
    log.append(vec![0x01]);
    log.append(vec![0x02]);
    log.append(vec![0x03]);
    log.commit(3)?;

    assert_eq!(log.uncommited.len(), 1);
    assert_eq!(vec![vec![0], vec![1], vec![2]], log.read()?);
    Ok(())
}
