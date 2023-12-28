use crate::pgtypes::*;
use crate::constant::*;

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

fn xlog_byte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: u32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64 as XLogSegNo
}

fn xlog_segment_offset(xlogptr: XLogRecPtr, wal_segsz_bytes: u32) -> u32 {
    (xlogptr & (wal_segsz_bytes - 1) as u64) as u32
}


// Is an XLogRecPtr within a particular XLOG segment?
//
// For XLByteInSeg, do the computation at face value.  For XLByteInPrevSeg,
// a boundary byte is taken to be in the previous segment.
fn byte_in_seg(xlrp: XLogRecPtr, log_seg_no: XLogSegNo, wal_segsz_bytes: u32) -> bool {
    (xlrp / wal_segsz_bytes as u64) as u64 == log_seg_no
}

// Compute a segment number from an XLogRecPtr.
//
// For XLByteToSeg, do the computation at face value.  For XLByteToPrevSeg,
// a boundary byte is taken to be in the previous segment.  This is suitable
// for deciding which segment to write given a pointer to a record end,
// for example.
fn byte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: u32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

// Error information from wal_read that both backend and frontend caller can
// process.  Currently only errors from pg_pread can be reported.
#[derive(Debug)]
struct WALReadError {
    errno: i32,          // errno set by the last pg_pread()
    off: u32,            // Offset we tried to read from.
    req: u32,            // Bytes requested to be read.
    read: u32,           // Bytes read by the last read().
    seg: WALOpenSegment, // Segment we tried to read from.
}

impl std::fmt::Display for WALReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "WALReadError: errno: {}, offset: {}, requested: {}, read: {}, segment: {}",
            self.errno, self.off, self.req, self.read, self.seg
        )
    }
}


impl std::error::Error for WALReadError {}