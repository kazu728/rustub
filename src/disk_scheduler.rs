use crate::disk_manager::DiskManagerError;
use crate::PAGE_SIZE;
use crate::{disk_manager::DiskManager, PageId};
use std::collections::VecDeque;
use std::io;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Debug)]
pub enum Request {
    Read(PageId, Arc<Mutex<[u8; PAGE_SIZE]>>),
    Write(PageId, Arc<Mutex<[u8; PAGE_SIZE]>>),
    Shutdown,
}

#[derive(Debug)]
pub enum DiskSchedulerError {
    DiskManagerError(DiskManagerError),
}

#[derive(Debug)]
pub struct DiskScheduler {
    pub request_queue: Arc<Mutex<VecDeque<Request>>>,
    pub worker: io::Result<JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(file_name: &str) -> Result<Self, DiskSchedulerError> {
        let request_queue = Arc::new(Mutex::new(VecDeque::new()));
        let disk_manager = Arc::new(Mutex::new(
            DiskManager::new(file_name).map_err(DiskSchedulerError::DiskManagerError)?,
        ));

        let worker = Self::spawn_worker(disk_manager, request_queue.clone());

        Ok(Self {
            request_queue,
            worker,
        })
    }

    pub fn spawn_worker(
        mutex_disk_manager: Arc<Mutex<DiskManager>>,
        request_queue: Arc<Mutex<VecDeque<Request>>>,
    ) -> Result<JoinHandle<()>, io::Error> {
        let handle = std::thread::Builder::new()
            .spawn(move || loop {
                let request = request_queue.lock().unwrap().pop_front();
                let mut disk_manager = mutex_disk_manager.lock().unwrap();

                if let Some(request) = request {
                    match request {
                        Request::Read(page_id, page_data) => {
                            disk_manager
                                .read(page_id, &mut *page_data.lock().unwrap())
                                .unwrap();
                        }

                        Request::Write(page_id, page_data) => disk_manager
                            .write(page_id, &mut *page_data.lock().unwrap())
                            .unwrap(),
                        Request::Shutdown => break,
                    }
                } else {
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            })
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        Ok(handle)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_job() {
        let disk_scheduler = DiskScheduler::new("test.db").unwrap();

        let buf = Arc::new(Mutex::new([0; PAGE_SIZE]));

        disk_scheduler
            .request_queue
            .lock()
            .unwrap()
            .push_back(Request::Read(0, Arc::clone(&buf)));

        disk_scheduler
            .request_queue
            .lock()
            .unwrap()
            .push_back(Request::Shutdown);

        disk_scheduler.worker.unwrap().join().unwrap();

        let buf_locked = buf.lock().unwrap();

        assert_eq!(*buf_locked, [0; PAGE_SIZE]);
    }

    #[test]
    fn write_job() {
        let disk_scheduler = DiskScheduler::new("test.db").unwrap();

        let buf = Arc::new(Mutex::new([1; PAGE_SIZE]));

        disk_scheduler
            .request_queue
            .lock()
            .unwrap()
            .push_back(Request::Write(1, Arc::clone(&buf)));

        disk_scheduler
            .request_queue
            .lock()
            .unwrap()
            .push_back(Request::Shutdown);

        disk_scheduler.worker.unwrap().join().unwrap();

        let buf_locked = buf.lock().unwrap();

        assert_ne!(*buf_locked, [0; PAGE_SIZE]);
    }
}
