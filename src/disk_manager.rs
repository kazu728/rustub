use crate::{AtomicPageId, PageId, PAGE_SIZE};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub enum DiskManagerError {
    CouldNotRead,
    CouldNotWrite(std::io::Error),
    DiskFull,
    DiskError,
    FileNotOpen(std::io::Error),
    SeekError(std::io::Error),
    Unknown(std::io::Error),
}

#[derive(Debug)]
pub struct DiskManager {
    next_page_id: AtomicPageId,
    db_file: Arc<Mutex<File>>,
    log_file: Arc<Mutex<File>>,
}

impl DiskManager {
    pub fn new(filename: &str) -> Result<Self, DiskManagerError> {
        if let Some(n) = filename.rfind('.') {
            let log_name = filename[..n].to_string() + ".log";

            let db_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(filename)
                .map_err(|err| DiskManagerError::FileNotOpen(err))?;

            let log_file = fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(log_name)
                .map_err(|err| DiskManagerError::FileNotOpen(err))?;

            Ok(Self {
                next_page_id: AtomicPageId::new(0),
                db_file: Arc::new(Mutex::new(db_file)),
                log_file: Arc::new(Mutex::new(log_file)),
            })
        } else {
            Err(DiskManagerError::CouldNotRead)
        }
    }

    pub fn read(&mut self, page_id: PageId, page_data: &mut [u8]) -> Result<(), DiskManagerError> {
        let offset = (page_id * PAGE_SIZE as PageId) as u64;

        let file = &mut self.db_file.lock().unwrap();

        if file.metadata().unwrap().len() < offset {
            return Err(DiskManagerError::CouldNotRead);
        }

        file.seek(SeekFrom::Start(offset))
            .map_err(|err| DiskManagerError::SeekError(err))?;

        let n = file.read(page_data).unwrap();

        if n < PAGE_SIZE {
            println!("Read less than a page, n: {}, page_size: {}", n, PAGE_SIZE);
        }
        Ok(())
    }

    pub fn write(&mut self, page_id: PageId, page_data: &mut [u8]) -> Result<(), DiskManagerError> {
        let offset = (page_id * PAGE_SIZE as PageId) as u64;

        let file = &mut self.db_file.lock().unwrap();

        file.seek(SeekFrom::Start(offset))
            .map_err(|err| DiskManagerError::SeekError(err))?;

        file.write_all(page_data)
            .map_err(|err| DiskManagerError::CouldNotWrite(err))?;
        file.flush().unwrap();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_write() {
        let mut disk_manager = DiskManager::new("test.db").unwrap();

        let text = "Hello, world!";

        let page_data = &mut [0 as u8; PAGE_SIZE];
        page_data[..text.len()].copy_from_slice(text.as_bytes());
        disk_manager.write(0, page_data).unwrap();

        let mut buffer = [0; PAGE_SIZE];
        disk_manager.read(0, &mut buffer).unwrap();

        assert_eq!(buffer, *page_data);
    }

    #[test]
    fn should_not_influence_other_page() {
        let mut disk_manager = DiskManager::new("test.db").unwrap();

        let text = "Hello, world!";

        let page_data = &mut [0 as u8; PAGE_SIZE];
        page_data[..text.len()].copy_from_slice(text.as_bytes());
        disk_manager.write(1, page_data).unwrap();

        let mut buffer = [0; PAGE_SIZE];
        disk_manager.read(2, &mut buffer).unwrap();

        assert_ne!(buffer, *page_data);

        disk_manager.read(1, &mut buffer).unwrap();
        assert_eq!(buffer, *page_data);
    }
}
