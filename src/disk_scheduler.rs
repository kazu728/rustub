use crate::disk_manager::DiskManagerError;
use crate::PAGE_SIZE;
use crate::{disk_manager::DiskManager, PageId};
use std::collections::VecDeque;
use std::io;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
#[derive(Debug)]
pub enum Request {
    Read(PageId, Arc<Mutex<[u8; PAGE_SIZE]>>, mpsc::Sender<()>),
    Write(PageId, Arc<Mutex<[u8; PAGE_SIZE]>>, mpsc::Sender<()>),
}

#[derive(Debug)]
pub enum DiskSchedulerError {
    DiskManagerError(DiskManagerError),
    SendError,
    RecvError(mpsc::RecvError),
    UnexpectedError,
}

#[derive(Debug)]
pub struct DiskScheduler {
    pub request_queue: Arc<Mutex<VecDeque<Request>>>,
    pub workers: Vec<io::Result<JoinHandle<Result<(), DiskSchedulerError>>>>,
}

impl DiskScheduler {
    pub fn new(file_name: &str) -> Result<DiskScheduler, DiskSchedulerError> {
        let request_queue = Arc::new(Mutex::new(VecDeque::new()));
        let disk_manager = Arc::new(Mutex::new(
            DiskManager::new(file_name).map_err(DiskSchedulerError::DiskManagerError)?,
        ));

        let mut workers = Vec::with_capacity(4);

        for _ in 0..4 {
            let worker =
                DiskScheduler::spawn_worker(Arc::clone(&disk_manager), Arc::clone(&request_queue));
            workers.push(worker);
        }

        Ok(Self {
            request_queue,
            workers,
        })
    }

    // TODO: ロックの範囲を狭める
    // ディスクの書き込みが遅いのである程度バッファに溜めてから書き込むようにする
    fn spawn_worker(
        mutex_disk_manager: Arc<Mutex<DiskManager>>,
        request_queue: Arc<Mutex<VecDeque<Request>>>,
    ) -> Result<JoinHandle<Result<(), DiskSchedulerError>>, io::Error> {
        let handle = std::thread::Builder::new()
            .spawn(move || loop {
                let request = request_queue.lock().unwrap().pop_front();
                let mut disk_manager = mutex_disk_manager.lock().unwrap();

                if let Some(request) = request {
                    match request {
                        Request::Read(page_id, page_data, sender) => {
                            disk_manager
                                .read(page_id, &mut *page_data.lock().unwrap())
                                .map_err(|err| DiskSchedulerError::DiskManagerError(err))
                                .and_then(|_| {
                                    sender.send(()).map_err(|_| DiskSchedulerError::SendError)
                                })?;
                        }

                        Request::Write(page_id, page_data, sender) => {
                            disk_manager
                                .write(page_id, &mut *page_data.lock().unwrap())
                                .map_err(|err| DiskSchedulerError::DiskManagerError(err))
                                .and_then(|_| {
                                    sender.send(()).map_err(|_| DiskSchedulerError::SendError)
                                })?;
                        }
                    }
                } else {
                    thread::sleep(std::time::Duration::from_millis(100));
                }
            })
            .map_err(|err| io::Error::new(io::ErrorKind::Other, err.to_string()))?;
        Ok(handle)
    }

    pub fn read(
        &self,
        page_id: PageId,
        page_data: Arc<Mutex<[u8; PAGE_SIZE]>>,
    ) -> Result<(), DiskSchedulerError> {
        let (sender, receiver) = mpsc::channel();

        self.request_queue.lock().unwrap().push_back(Request::Read(
            page_id,
            Arc::clone(&page_data),
            sender,
        ));

        receiver
            .recv()
            .map_err(|err| DiskSchedulerError::RecvError(err))?;

        Ok(())
    }

    pub fn write(
        &self,
        page_id: PageId,
        page_data: Arc<Mutex<[u8; PAGE_SIZE]>>,
    ) -> Result<(), DiskSchedulerError> {
        let (sender, receiver) = mpsc::channel();

        self.request_queue
            .lock()
            .unwrap()
            .push_back(Request::Write(page_id, page_data, sender));

        receiver
            .recv()
            .map_err(|err| DiskSchedulerError::RecvError(err))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixture::{self, tear_down};

    #[test]
    fn read_job() {
        let file_name = "disk_scheduler_read_job.db";
        let disk_scheduler = DiskScheduler::new(file_name).unwrap();

        let buffer = Arc::new(Mutex::new(fixture::create_random_binary_page_data()));

        let result = disk_scheduler.read(0, Arc::clone(&buffer));

        assert!(result.is_err());

        tear_down(file_name)
    }

    #[test]
    fn write_job() {
        let file_name = "disk_scheduler_write_job.db";
        let disk_scheduler = DiskScheduler::new(&file_name).unwrap();

        let page_data = fixture::create_random_binary_page_data();
        let buf = Arc::new(Mutex::new(page_data));

        disk_scheduler.write(1, Arc::clone(&buf)).unwrap();

        assert_ne!(*buf.lock().unwrap(), [0; PAGE_SIZE]);

        let buffer = Arc::new(Mutex::new([0; PAGE_SIZE]));

        disk_scheduler.read(1, Arc::clone(&buffer)).unwrap();
        assert_eq!(*buffer.lock().unwrap(), page_data);

        tear_down(file_name)
    }
}
