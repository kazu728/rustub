use crate::{FrameId, Frames};

pub trait Replacer {
    fn new() -> Self;
    fn victim(&self, frames: &mut Frames) -> Option<FrameId>;
}

pub struct LRUReplacer;
impl Replacer for LRUReplacer {
    fn new() -> Self {
        LRUReplacer
    }

    fn victim(&self, frames: &mut Frames) -> Option<FrameId> {
        frames
            .iter()
            .filter(|(_, frame)| frame.is_unused())
            .min_by_key(|(_, frame)| frame.last_used_at)
            .map(|(key, _)| key)
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Page, PageId};
    use chrono::Utc;

    fn create_page(id: PageId, pin_count: u32, last_used_at: i64) -> Page {
        Page {
            id,
            data: [0; crate::PAGE_SIZE],
            pin_count,
            is_dirty: false,
            last_used_at,
        }
    }

    #[test]
    fn replace_oldest_page() {
        let mut frames = Frames::new();
        frames.insert(1, create_page(1, 0, Utc::now().timestamp_millis() - 1000));
        frames.insert(2, create_page(2, 0, Utc::now().timestamp_millis() - 2000));
        frames.insert(3, create_page(3, 0, Utc::now().timestamp_millis() - 3000));

        let frame_id = LRUReplacer::new().victim(&mut frames).unwrap();

        assert_eq!(frame_id, 3);
    }

    #[test]
    fn replace_unpinned_oldest_page() {
        let mut frames = Frames::new();
        frames.insert(1, create_page(1, 0, Utc::now().timestamp_millis() - 1000));
        frames.insert(2, create_page(2, 1, Utc::now().timestamp_millis() - 2000));
        frames.insert(3, create_page(3, 1, Utc::now().timestamp_millis() - 3000));

        let frame_id = LRUReplacer::new().victim(&mut frames).unwrap();

        assert_eq!(frame_id, 1);
    }

    #[test]
    fn should_fail_on_empty_frames() {
        let mut frames = Frames::new();

        let maybe_frames_id = LRUReplacer::new().victim(&mut frames);

        assert_eq!(maybe_frames_id, None);
    }
}
