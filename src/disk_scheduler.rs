use crate::disk_manager::DiskManagerError;
use crate::{disk_manager::DiskManager, PageId};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Debug)]
pub enum Request {
    Read(PageId, Vec<u8>),
    Write(PageId, Vec<u8>),
}

#[derive(Debug)]
pub enum DiskSchedulerError {
    DiskManagerError(DiskManagerError),
}

#[derive(Debug)]
pub struct DiskScheduler {
    request_queue: Arc<Mutex<VecDeque<Request>>>,
    worker: Option<JoinHandle<()>>,
}

impl DiskScheduler {
    pub fn new(file_name: &str) -> Result<Self, DiskSchedulerError> {
        let request_queue = Arc::new(Mutex::new(VecDeque::new()));

        let disk_manager = Arc::new(Mutex::new(
            DiskManager::new(file_name).map_err(DiskSchedulerError::DiskManagerError)?,
        ));

        let worker = Some(Self::spawn_worker(disk_manager, request_queue.clone()));

        Ok(Self {
            request_queue,
            worker,
        })
    }

    fn spawn_worker(
        disk_manager: Arc<Mutex<DiskManager>>,
        request_queue: Arc<Mutex<VecDeque<Request>>>,
    ) -> JoinHandle<()> {
        // TODO: thread のエラーハンドリング
        std::thread::spawn(move || loop {
            let request = request_queue.lock().unwrap().pop_front();

            if let Some(request) = request {
                match request {
                    Request::Read(page_id, mut page_data) => disk_manager
                        .lock()
                        .unwrap()
                        .read(page_id, &mut page_data)
                        .unwrap(),

                    Request::Write(page_id, mut page_data) => disk_manager
                        .lock()
                        .unwrap()
                        .write(page_id, &mut page_data)
                        .unwrap(),
                }
            } else {
                thread::sleep(std::time::Duration::from_millis(100));
            }
        })
    }
}
