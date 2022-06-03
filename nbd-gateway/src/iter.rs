use crate::BLOCK_SIZE;

/// Iterates on block-aligned parts.
pub fn list_blocks(start: usize, size: usize) -> ListBlocks {
    ListBlocks {
        buf_pos: 0,
        device_pos: start,
        remaining_size: size,
    }
}

pub struct ListBlocks {
    buf_pos: usize,
    device_pos: usize,
    remaining_size: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ListBlockItem {
    buf_start: usize,
    device_start: usize,
    size: usize,
}

impl ListBlockItem {
    pub fn buf_start(&self) -> usize {
        self.buf_start
    }

    pub fn buf_end(&self) -> usize {
        self.buf_start + self.size
    }

    pub fn device_start(&self) -> usize {
        self.device_start
    }

    pub fn block_num(&self) -> usize {
        self.device_start / BLOCK_SIZE
    }

    pub fn block_offset(&self) -> usize {
        self.device_start % BLOCK_SIZE
    }

    pub fn size(&self) -> usize {
        self.size
    }
}

impl Iterator for ListBlocks {
    type Item = ListBlockItem;

    fn next(&mut self) -> Option<ListBlockItem> {
        if self.remaining_size > 0 {
            let block = self.device_pos / BLOCK_SIZE;
            let end_block = (block + 1) * BLOCK_SIZE;
            let size = self.remaining_size.min(end_block - self.device_pos);
            let item = ListBlockItem {
                buf_start: self.buf_pos,
                device_start: self.device_pos,
                size,
            };
            self.buf_pos += size;
            self.device_pos += size;
            self.remaining_size -= size;
            Some(item)
        } else {
            None
        }
    }
}

#[test]
fn test_iter() {
    assert_eq!(
        list_blocks(512, 1024).collect::<Vec<_>>(),
        vec![
            ListBlockItem {
                buf_start: 0,
                device_start: 512,
                size: 512,
            },
            ListBlockItem {
                buf_start: 512,
                device_start: 1024,
                size: 512,
            },
        ],
    );

    assert_eq!(
        list_blocks(536, 200).collect::<Vec<_>>(),
        vec![
            ListBlockItem {
                buf_start: 0,
                device_start: 536,
                size: 200,
            },
        ],
    );

    assert_eq!(
        list_blocks(536, 700).collect::<Vec<_>>(),
        vec![
            ListBlockItem {
                buf_start: 0,
                device_start: 536,
                size: 488,
            },
            ListBlockItem {
                buf_start: 488,
                device_start: 1024,
                size: 212,
            },
        ],
    );
}
