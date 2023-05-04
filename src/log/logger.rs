use crate::error::{Error, Result};

use std::{
    collections::VecDeque,
    fs::{create_dir_all, File, OpenOptions},
    io::{Read, Seek, Write},
    path::Path,
};

#[derive(Debug)]
pub struct IndexEntry {
    position: u64,
    length: u32,
}

#[derive(Debug)]
pub struct Logger {
    /// A file that stores the on-disk logs
    file: File,
    /// A queue with the uncommitted log entries
    uncommited: VecDeque<Vec<u8>>,
    /// Index of entries inside file
    index: Vec<IndexEntry>,
}

impl Logger {
    pub fn new(dir: &Path) -> Result<Self> {
        create_dir_all(dir)?;

        let mut file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(dir.join("log"))
            .unwrap();

        return Ok(Self {
            index: Self::build_index(&mut file)?,
            file,
            uncommited: VecDeque::new(),
        });
    }

    fn build_index(file: &mut File) -> Result<Vec<IndexEntry>> {
        let mut index: Vec<IndexEntry> = Vec::new();
        let mut position: u64 = 4;
        let mut len_bytes = [0u8; 4];
        while let Ok(()) = file.read_exact(&mut len_bytes) {
            let length = u32::from_be_bytes(len_bytes);
            index.push(IndexEntry { position, length });
            position += (length + 4) as u64;
            file.seek(std::io::SeekFrom::Current(length.into()))?;
        }

        return Ok(index);
    }

    pub fn append(&mut self, entry: Vec<u8>) -> () {
        self.uncommited.push_back(entry);
    }

    pub fn commit(&mut self, num_of_entries: usize) -> Result<()> {
        if self.uncommited.len() < num_of_entries {
            return Err(Error::Internal(format!(
                "Not enough uncommitted entries {}",
                self.uncommited.len()
            )));
        }

        let last_index = self.index.last();
        let mut position = match last_index {
            Some(last_index) => last_index.position + (last_index.length + 4) as u64,
            None => 4,
        };
        self.file.seek(std::io::SeekFrom::End(0))?;
        for _ in 0..num_of_entries {
            let entry = self
                .uncommited
                .pop_front()
                .ok_or_else(|| Error::Internal("Unexpected end of uncommitted entries".into()))?;
            let length = entry.len() as u32;

            self.file.write_all(&length.to_be_bytes())?;
            self.file.write_all(&entry)?;
            self.file.flush()?;

            self.index.push(IndexEntry { position, length });
            position += (length + 4) as u64;
        }

        return Ok(());
    }

    pub fn read_exact(&mut self, from_index: usize, num_of_entries: usize) -> Result<Vec<Vec<u8>>> {
        if from_index + num_of_entries > self.index.len() {
            return Err(Error::Internal("Not enough committed entries".into()));
        }

        let mut entries: Vec<Vec<u8>> = Vec::new();
        for i in 0..num_of_entries {
            let index_entry: &IndexEntry = self.index.get(from_index + i).unwrap();
            self.file
                .seek(std::io::SeekFrom::Start(index_entry.position))?;
            let mut entry = vec![0u8; index_entry.length as usize];
            self.file.read_exact(&mut entry)?;
            entries.push(entry);
        }

        return Ok(entries);
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
    let dir = tempdir::TempDir::new("test-commit-read")?;
    let mut l = Logger::new(dir.as_ref())?;
    l.append(vec![0x00]);
    l.append(vec![0x01]);
    l.append(vec![0x02]);
    l.append(vec![0x03]);
    l.commit(3)?;

    assert_eq!(l.uncommited.len(), 1);
    assert_eq!(l.read()?, vec![vec![0], vec![1], vec![2]]);
    Ok(())
}

#[test]
fn test_commit_read_exact() -> Result<()> {
    let dir = tempdir::TempDir::new("test-commit-read-exact")?;
    let mut l = Logger::new(dir.as_ref())?;
    l.append(vec![0x00]);
    l.append(vec![0x01]);
    l.append(vec![0x02]);
    l.append(vec![0x03]);
    l.commit(4)?;

    assert_eq!(l.read_exact(1, 2)?, vec![vec![1], vec![2]]);
    Ok(())
}

#[test]
fn test_index() -> Result<()> {
    let dir = tempdir::TempDir::new("test-index")?;
    {
        let mut l = Logger::new(dir.as_ref())?;
        l.append(vec![0x00]);
        l.append(vec![0x01]);
        l.commit(2)?;

        let entry0 = l.index.get(0).unwrap();
        assert_eq!(entry0.position, 4);
        assert_eq!(entry0.length, 1);
        let entry1 = l.index.get(1).unwrap();
        assert_eq!(entry1.position, (4 + 1) + 4);
        assert_eq!(entry1.length, 1);
    }

    let l = Logger::new(dir.as_ref())?;
    let entry0 = l.index.get(0).unwrap();
    assert_eq!(entry0.position, 4);
    assert_eq!(entry0.length, 1);
    let entry1 = l.index.get(1).unwrap();
    assert_eq!(entry1.position, (4 + 1) + 4);
    assert_eq!(entry1.length, 1);
    Ok(())
}
