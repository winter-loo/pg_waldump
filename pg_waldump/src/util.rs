use crate::{pgtypes::XLogRecPtr, constant::XLOG_BLCKSZ};

const MAXIMUM_ALIGNOF: u32 = 8;

pub fn max_align(len: u32) -> u32 {
    ((len) + (MAXIMUM_ALIGNOF - 1)) & !(MAXIMUM_ALIGNOF - 1)
}

pub fn page_offset(rec_ptr: XLogRecPtr) -> u32 {
    rec_ptr as u32 & (XLOG_BLCKSZ - 1)
}

pub fn page_addr(rec_ptr: XLogRecPtr) -> u64 {
    rec_ptr & !(XLOG_BLCKSZ as u64 - 1)
}