use crate::constant::*;
use crate::pgtypes::*;
use std::path::PathBuf;

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

pub fn xlog_byte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: u32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64 as XLogSegNo
}

pub fn xlog_segment_offset(xlogptr: XLogRecPtr, wal_segsz_bytes: u32) -> u32 {
    (xlogptr & (wal_segsz_bytes - 1) as u64) as u32
}

// Is an XLogRecPtr within a particular XLOG segment?
//
// For XLByteInSeg, do the computation at face value.  For XLByteInPrevSeg,
// a boundary byte is taken to be in the previous segment.
pub fn byte_in_seg(xlrp: XLogRecPtr, log_seg_no: XLogSegNo, wal_segsz_bytes: u32) -> bool {
    (xlrp / wal_segsz_bytes as u64) as u64 == log_seg_no
}

// Compute a segment number from an XLogRecPtr.
//
// For XLByteToSeg, do the computation at face value.  For XLByteToPrevSeg,
// a boundary byte is taken to be in the previous segment.  This is suitable
// for deciding which segment to write given a pointer to a record end,
// for example.
pub fn byte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: u32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64
}

// Error information from wal_read that both backend and frontend caller can
// process.  Currently only errors from pg_pread can be reported.
#[derive(Debug)]
pub struct WALReadError {
    pub errno: i32,          // errno set by the last pg_pread()
    pub off: u32,            // Offset we tried to read from.
    pub req: u32,            // Bytes requested to be read.
    pub read: u32,           // Bytes read by the last read().
    pub seg: WALOpenSegment, // Segment we tried to read from.
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

#[inline]
pub fn prefix_length(s: &str, set: &str) -> usize {
    s.chars().take_while(|&c| set.contains(c)).count()
}

#[inline]
pub fn is_xlog_filename(fname: &std::path::PathBuf) -> bool {
    let fname = fname.file_name().unwrap();
    fname.len() == XLOG_FNAME_LEN
        && prefix_length(fname.to_str().unwrap(), "0123456789ABCDEF") == XLOG_FNAME_LEN
}

#[inline]
pub fn xlog_segments_per_xlog_id(wal_seg_sz: u32) -> u32 {
    (0x100000000u64 / wal_seg_sz as u64) as u32
}

#[inline]
pub fn xlog_filename(tli: TimeLineID, log_seg_no: XLogSegNo, wal_seg_sz: u32) -> PathBuf {
    let n = xlog_segments_per_xlog_id(wal_seg_sz) as u64;
    let s = format!(
        "{:08X}{:08X}{:08X}",
        tli,
        (log_seg_no / n) as u32,
        (log_seg_no % n) as u32
    );
    PathBuf::from(s)
}

#[inline]
pub fn xlog_from_file_name(
    fname: &PathBuf,
    timeline: &mut TimeLineID,
    segno: &mut XLogSegNo,
    wal_seg_sz: u32,
) {
    let fname = fname.to_str().unwrap();
    *timeline = fname[0..8].parse::<u32>().unwrap();
    let log = fname[8..16].parse::<u64>().unwrap();
    let seg = fname[16..24].parse::<u64>().unwrap();
    *segno = log * (0x10000_0000u64 / wal_seg_sz as u64) + seg;
}
