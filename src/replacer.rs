use crate::{Frames, Page};

#[derive(Debug, PartialEq, Eq)]
enum ReplacerError {
    FrameEmpty,
}

trait Replacer {
    fn replace(frames: &mut Frames, new_page: Page) -> Result<&Frames, ReplacerError>;
}

struct LRUReplacer;
impl Replacer for LRUReplacer {
    fn replace(frames: &mut Frames, new_page: Page) -> Result<&Frames, ReplacerError> {
        let oldest_frame_key = frames
            .iter()
            .filter(|(_, frame)| frame.pin_count == 0)
            .min_by_key(|(_, frame)| frame.last_used_at)
            .map(|(key, _)| key)
            .ok_or(ReplacerError::FrameEmpty)?;

        frames.insert(*oldest_frame_key, new_page);
        Ok(frames)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PageId;
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

        let new_page = create_page(4, 0, Utc::now().timestamp_millis());

        let frames = LRUReplacer::replace(&mut frames, new_page).unwrap();

        assert_eq!(frames.get(&3).unwrap(), &new_page);
    }

    #[test]
    fn replace_unpinned_oldest_page() {
        let mut frames = Frames::new();
        frames.insert(1, create_page(1, 0, Utc::now().timestamp_millis() - 1000));
        frames.insert(2, create_page(2, 1, Utc::now().timestamp_millis() - 2000));
        frames.insert(3, create_page(3, 1, Utc::now().timestamp_millis() - 3000));

        let new_page = create_page(4, 0, Utc::now().timestamp_millis());

        let frames = LRUReplacer::replace(&mut frames, new_page).unwrap();

        assert_eq!(frames.get(&1).unwrap(), &new_page);
    }

    #[test]
    fn should_fail_on_empty_frames() {
        let mut frames = Frames::new();
        let new_page = create_page(4, 0, Utc::now().timestamp_millis());

        let frames = LRUReplacer::replace(&mut frames, new_page);

        assert_eq!(frames, Err(ReplacerError::FrameEmpty));
    }
}
