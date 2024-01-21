use std::collections::HashMap;

pub mod replacer;

type PageId = usize;
type FrameId = usize;

#[derive(Debug, PartialEq, Clone, Copy)]
struct Page {
    id: PageId,
    data: [u8; PAGE_SIZE],
    pin_count: u32, // ページが現在使用中であるかどうかを示す。
    is_dirty: bool,
    last_used_at: i64,
}

type Frames = HashMap<FrameId, Page>;

const PAGE_SIZE: usize = 4096;
