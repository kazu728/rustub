use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

pub mod disk_manager;
pub mod disk_scheduler;
pub mod replacer;

type PageId = usize;
type FrameId = usize;
type AtomicPageId = AtomicUsize;

#[derive(Debug, PartialEq, Clone, Copy)]

pub struct Page {
    id: PageId,
    data: [u8; PAGE_SIZE],
    pin_count: u32, // ページが現在使用中であるかどうかを示す。
    is_dirty: bool,
    last_used_at: i64,
}

impl Page {
    pub fn new(id: PageId, data: [u8; PAGE_SIZE]) -> Self {
        Self {
            id,
            data,
            pin_count: 0,
            is_dirty: false,
            last_used_at: 0,
        }
    }

    pub fn get_id(&self) -> PageId {
        self.id
    }
}
type Frames = HashMap<FrameId, Page>;

const PAGE_SIZE: usize = 4096;
