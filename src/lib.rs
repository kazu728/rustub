use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;

use chrono::Utc;

pub mod buffer_pool_manager;
pub mod disk_manager;
pub mod disk_scheduler;
pub mod fixture;
pub mod replacer;

type PageId = usize;
type FrameId = usize;
type AtomicPageId = AtomicUsize;

pub const PAGE_SIZE: usize = 4096;

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
            last_used_at: Utc::now().timestamp_millis(),
        }
    }

    pub fn is_unused(&self) -> bool {
        self.pin_count == 0
    }

    pub fn get_id(&self) -> PageId {
        self.id
    }

    pub fn pin(&mut self) {
        self.pin_count += 1;
        // TODO: 最終更新日時はReplacerの関心なのでReplacerに移した方がいいかもしれない(replace戦略に依存する)
        self.last_used_at = Utc::now().timestamp_millis();
    }

    pub fn unpin(&mut self) {
        self.pin_count -= 1;
    }
}
type Frames = HashMap<FrameId, Page>;
