use crate::disk_scheduler::{DiskScheduler, Request};
use crate::replacer::{LRUReplacer, Replacer};
use crate::{FrameId, Page, PageId, PAGE_SIZE};
use std::collections::{HashMap, LinkedList};
use std::sync::{mpsc, Arc, Mutex};

pub struct BufferPoolManager {
    pages: HashMap<FrameId, Page>,
    page_table: HashMap<PageId, FrameId>,
    free_list: LinkedList<FrameId>,
    disk_scheduler: DiskScheduler,
    replacer: LRUReplacer,
}

#[derive(Debug)]
enum BufferPoolManagerError {
    PageNotFound,
    InConsistentState(String),
}

enum Replacement {
    Free(FrameId),
    Victim(FrameId),
    Full,
}

impl BufferPoolManager {
    fn new(pool_size: usize, file_name: &str) -> Self {
        let mut free_list = LinkedList::new();

        for i in 1..=pool_size {
            free_list.push_back(i);
        }
        BufferPoolManager {
            pages: HashMap::new(),
            page_table: HashMap::new(),
            free_list,
            disk_scheduler: DiskScheduler::new(file_name).unwrap(),
            replacer: LRUReplacer::new(),
        }
    }

    fn fetch_page(&mut self, page_id: PageId) -> Result<Option<&mut Page>, BufferPoolManagerError> {
        if let Some(frame_id) = self.page_table.get(&page_id) {
            let frame = self
                .pages
                .get_mut(frame_id)
                .expect("frame_id exists in page_table but not in pages");

            frame.pin();
            //  TODO: [perf] replacerでvictim を選択する際に全件走査になっている。
            //  パフォーマンス上の懸念があるためpinする際にreplacer側に stack で pin, unpin候補を積む。一旦動く状態にしたいので全件走査で実装
            return Ok(self.pages.get_mut(&frame_id));
        }

        match self.find_replacement() {
            Replacement::Free(frame_id) => {
                let (sender, receiver) = mpsc::channel();

                let write_buf = [0; PAGE_SIZE];
                let buf = Arc::new(Mutex::new([0 as u8; PAGE_SIZE]));

                self.disk_scheduler
                    .request_queue
                    .lock()
                    .unwrap()
                    .push_back(Request::Read(page_id, buf, sender));

                receiver.recv().expect("Sender dropped");

                self.pages.insert(frame_id, Page::new(page_id, write_buf));
                self.page_table.insert(page_id, frame_id);

                return Ok(self.pages.get_mut(&frame_id));
            }
            Replacement::Victim(frame_id) => {
                let (sender, receiver) = mpsc::channel();
                let page = self.pages.get_mut(&frame_id).unwrap();

                if page.is_dirty {
                    self.disk_scheduler
                        .request_queue
                        .lock()
                        .unwrap()
                        .push_back(Request::Write(
                            page.id,
                            Arc::new(Mutex::new(page.data)),
                            sender.clone(),
                        ));
                }

                let write_buf = [0; PAGE_SIZE];
                let buf = Arc::new(Mutex::new([0 as u8; PAGE_SIZE]));

                self.disk_scheduler
                    .request_queue
                    .lock()
                    .unwrap()
                    .push_back(Request::Read(page_id, buf, sender));

                receiver.recv().expect("Sender dropped");

                self.page_table.remove(&frame_id);
                self.pages.remove(&frame_id);

                self.pages.insert(page_id, Page::new(page_id, write_buf));
                self.page_table.insert(page_id, frame_id);

                return Ok(self.pages.get_mut(&page_id));
            }
            Replacement::Full => {
                return Err(BufferPoolManagerError::InConsistentState(
                    "BufferPoolManager is full".to_string(),
                ));
            }
        }
    }

    fn flush_page(&self, page_id: PageId, buf: Arc<Mutex<[u8; PAGE_SIZE]>>) {
        let (sender, receiver) = mpsc::channel();
        self.disk_scheduler
            .request_queue
            .lock()
            .unwrap()
            .push_back(Request::Write(page_id, buf, sender));

        receiver.recv().expect("Sender dropped");
    }

    fn find_replacement(&mut self) -> Replacement {
        if let Some(frame_id) = self.free_list.pop_front() {
            return Replacement::Free(frame_id);
        } else if let Some(frame_id) = self.replacer.victim(&mut self.pages) {
            return Replacement::Victim(frame_id);
        }
        Replacement::Full
    }

    // TODO: pin, unpin
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::fixture::{self, tear_down};

    const BUFFER_POOL_SIZE: usize = 5;
    #[test]
    fn test_buffer_pool_manager() {
        let file_name = "buffer_pool_manager_test.db";
        let mut buffer_pool_manager = BufferPoolManager::new(BUFFER_POOL_SIZE, file_name);

        // Buffer をfullにする
        for i in 1..=BUFFER_POOL_SIZE {
            let buf = Arc::new(Mutex::new(fixture::create_random_binary_page_data()));

            let page_id = i;

            buffer_pool_manager.flush_page(page_id, buf);

            let page = buffer_pool_manager.fetch_page(page_id).unwrap();

            assert!(page.is_some());
            assert_eq!(buffer_pool_manager.pages.len(), i);
            assert_eq!(buffer_pool_manager.free_list.len(), BUFFER_POOL_SIZE - i);
        }

        // Buffer pool がfullの時に新しいページを取得しようとすると置き換えられるか
        let buf = Arc::new(Mutex::new(fixture::create_random_binary_page_data()));

        let page_id = 100;

        buffer_pool_manager.flush_page(page_id, buf);

        let page = buffer_pool_manager.fetch_page(page_id).unwrap();

        assert!(page.is_some());
        assert_eq!(buffer_pool_manager.pages.len(), BUFFER_POOL_SIZE);
        assert_eq!(buffer_pool_manager.free_list.len(), 0);
        assert!(buffer_pool_manager.pages.get(&1).is_none());
        assert!(buffer_pool_manager.pages.contains_key(&100));

        tear_down(file_name)
    }
}
