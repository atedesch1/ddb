use crate::error::{Error, Result};

use std::{
    collections::VecDeque,
    fs::{create_dir_all, File, OpenOptions},
    io::{BufReader, Read, Seek, Write},
    path::Path,
    sync::{Mutex, RwLock},
};

#[derive(Debug)]
pub struct IndexEntry {
    position: u64,
    length: u32,
}

#[derive(Debug)]
pub struct Logger {
    /// A file that stores the on-disk logs
    file: RwLock<File>,
    /// A queue with the uncommitted log entries
    uncommitted: Mutex<VecDeque<Vec<u8>>>,
    /// Index of entries inside file
    index: RwLock<Vec<IndexEntry>>,
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
            index: RwLock::new(Self::build_index(&mut file)?),
            file: RwLock::new(file),
            uncommitted: Mutex::new(VecDeque::new()),
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

    pub fn append(&self, entry: Vec<u8>) -> Result<()> {
        self.uncommitted.lock()?.push_back(entry);
        return Ok(());
    }

    pub fn committed(&self) -> Result<usize> {
        return Ok(self.index.read()?.len());
    }

    pub fn uncommitted(&self) -> Result<usize> {
        return Ok(self.uncommitted.lock()?.len());
    }

    pub fn commit(&self, num_of_entries: usize) -> Result<()> {
        let mut uncommitted = self.uncommitted.lock()?;
        if uncommitted.len() < num_of_entries {
            return Err(Error::Internal(format!(
                "Not enough uncommitted entries {}",
                uncommitted.len()
            )));
        }

        let mut index = self.index.write()?;

        let last_index = (*index).last();
        let mut position = match last_index {
            Some(last_index) => last_index.position + (last_index.length + 4) as u64,
            None => 4,
        };

        let mut file = self.file.write()?;
        (*file).seek(std::io::SeekFrom::End(0))?;

        for _ in 0..num_of_entries {
            let entry = uncommitted
                .pop_front()
                .ok_or_else(|| Error::Internal("Unexpected end of uncommitted entries".into()))?;
            let length = entry.len() as u32;

            (*file).write_all(&length.to_be_bytes())?;
            (*file).write_all(&entry)?;
            (*file).flush()?;

            (*index).push(IndexEntry { position, length });
            position += (length + 4) as u64;
        }

        return Ok(());
    }

    pub fn get(&self, idx: usize) -> Result<Vec<u8>> {
        let index = self.index.read()?;
        if idx >= (*index).len() {
            return Err(Error::Internal("Index out of bounds".into()));
        }

        let file = self.file.read()?;
        let mut bufreader = BufReader::new(&*file);

        let index_entry: &IndexEntry = (*index).get(idx).unwrap();
        bufreader.seek(std::io::SeekFrom::Start(index_entry.position))?;
        let mut entry = vec![0u8; index_entry.length as usize];
        bufreader.read_exact(&mut entry)?;

        return Ok(entry);
    }

    pub fn read_exact(&self, from_index: usize, num_of_entries: usize) -> Result<Vec<Vec<u8>>> {
        let index = self.index.read()?;
        if from_index + num_of_entries > (*index).len() {
            return Err(Error::Internal("Not enough committed entries".into()));
        }

        let file = self.file.read()?;
        let mut bufreader = BufReader::new(&*file);

        let mut entries: Vec<Vec<u8>> = Vec::new();
        for i in 0..num_of_entries {
            let index_entry: &IndexEntry = (*index).get(from_index + i).unwrap();
            bufreader.seek(std::io::SeekFrom::Start(index_entry.position))?;
            let mut entry = vec![0u8; index_entry.length as usize];
            bufreader.read_exact(&mut entry)?;
            entries.push(entry);
        }

        return Ok(entries);
    }

    pub fn read_all(&self) -> Result<Vec<Vec<u8>>> {
        let mut entries: Vec<Vec<u8>> = Vec::new();
        let mut len_bytes = [0u8; 4];

        let file = self.file.read()?;
        let mut bufreader = BufReader::new(&*file);
        bufreader.seek(std::io::SeekFrom::Start(0))?;

        while let Ok(()) = bufreader.read_exact(&mut len_bytes) {
            let len = u32::from_be_bytes(len_bytes);
            let mut entry = vec![0u8; len as usize];
            bufreader.read_exact(&mut entry)?;
            entries.push(entry);
        }

        return Ok(entries);
    }
}

#[test]
fn test_commit_get() -> Result<()> {
    let dir = tempdir::TempDir::new("test-commit-read")?;
    let l = Logger::new(dir.as_ref())?;
    l.append(vec![0x00])?;
    l.append(vec![0x01])?;
    l.append(vec![0x02])?;
    l.commit(3)?;

    let entry = l.get(1)?;

    assert_eq!(entry, vec![0x01]);
    Ok(())
}

#[test]
fn test_commit_read() -> Result<()> {
    let dir = tempdir::TempDir::new("test-commit-read")?;
    let l = Logger::new(dir.as_ref())?;
    l.append(vec![0x00])?;
    l.append(vec![0x01])?;
    l.append(vec![0x02])?;
    l.append(vec![0x03])?;
    l.commit(3)?;

    assert_eq!(l.committed()?, 3);
    assert_eq!(l.uncommitted.lock()?.len(), 1);
    assert_eq!(l.read_all()?, vec![vec![0], vec![1], vec![2]]);
    Ok(())
}

#[test]
fn test_commit_read_exact() -> Result<()> {
    let dir = tempdir::TempDir::new("test-commit-read-exact")?;
    let l = Logger::new(dir.as_ref())?;
    l.append(vec![0x00])?;
    l.append(vec![0x01])?;
    l.append(vec![0x02])?;
    l.append(vec![0x03])?;
    l.commit(4)?;

    assert_eq!(l.read_exact(1, 2)?, vec![vec![1], vec![2]]);
    Ok(())
}

#[test]
fn test_index() -> Result<()> {
    let dir = tempdir::TempDir::new("test-index")?;
    {
        let l = Logger::new(dir.as_ref())?;
        l.append(vec![0x00])?;
        l.append(vec![0x01])?;
        l.commit(2)?;

        let index = l.index.read()?;

        let entry0 = (*index).get(0).unwrap();
        assert_eq!(entry0.position, 4);
        assert_eq!(entry0.length, 1);
        let entry1 = (*index).get(1).unwrap();
        assert_eq!(entry1.position, (4 + 1) + 4);
        assert_eq!(entry1.length, 1);
    }

    let l = Logger::new(dir.as_ref())?;
    let index = l.index.read()?;
    let entry0 = (*index).get(0).unwrap();
    assert_eq!(entry0.position, 4);
    assert_eq!(entry0.length, 1);
    let entry1 = (*index).get(1).unwrap();
    assert_eq!(entry1.position, (4 + 1) + 4);
    assert_eq!(entry1.length, 1);
    Ok(())
}
