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
    PageNotFound,
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
            return Err(DiskManagerError::PageNotFound);
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
    use crate::fixture::{self, tear_down};

    #[test]
    fn read() {
        let file_name = "disk_manager_read.db";
        let mut disk_manager = DiskManager::new(file_name).unwrap();

        let mut buffer = [0; PAGE_SIZE];
        // このテストよりoffsetが大きいページが先にさに書き込まれた場合0埋めされるのでlimitテスト内で最も大きなページIDを指定する
        let result = disk_manager.read(5, &mut buffer);

        assert!(result.is_err());
        assert_eq!(buffer, [0; PAGE_SIZE]);

        tear_down(file_name)
    }

    #[test]
    fn read_write() {
        let file_name = "disk_manager_read_write.db";
        let mut disk_manager = DiskManager::new(file_name).unwrap();

        let page_data = &mut fixture::create_random_binary_page_data();
        disk_manager.write(1, page_data).unwrap();

        let mut buffer = [0; PAGE_SIZE];
        disk_manager.read(1, &mut buffer).unwrap();

        assert_eq!(buffer, *page_data);

        tear_down(file_name);
    }

    #[test]
    fn should_not_influence_other_page() {
        let file_name = "disk_manager_should_not_influence_other_page.db";
        let mut disk_manager = DiskManager::new(file_name).unwrap();

        let page_data = &mut fixture::create_random_binary_page_data();
        disk_manager.write(2, page_data).unwrap();

        let mut buffer = [0; PAGE_SIZE];
        let result = disk_manager.read(3, &mut buffer);

        assert!(result.is_err());
        assert_eq!(buffer, [0; PAGE_SIZE]);

        disk_manager.read(2, &mut buffer).unwrap();
        assert_eq!(buffer, *page_data);

        tear_down(file_name)
    }
}
