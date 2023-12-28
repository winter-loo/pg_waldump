use std::collections::LinkedList;

use crate::pgtypes::*;

#[derive(Default)]
pub(crate) struct XLogReaderState {
    pub errmsg: String,
    pub(crate) private_data: XLogDumpPrivate,
    // Start and end point of last record read.  EndRecPtr is also used as the
    // position to read next.  Calling XLogBeginRead() sets EndRecPtr to the
    // starting position and ReadRecPtr to invalid.
    //
    // Start and end point of last record returned by XLogReadRecord().  These
    // are also available as record->lsn and record->next_lsn.
    pub(crate) read_recptr: XLogRecPtr, // start of last record read
    pub(crate) end_recptr: XLogRecPtr,  // end+1 of last record read

    pub(crate) segoff: u32,

    // Buffer for currently read page (XLOG_BLCKSZ bytes, valid up to at least
    // read_len bytes)
    pub(crate) read_buf: Vec<u8>,
    pub(crate) read_len: u32,
    pub(crate) page_hdr_size: u32,
    pub(crate) cross_page_record_buf: Vec<u8>,

    // ----------------------------------------
    // Decoded representation of current record
    //
    // Use XLogRecGet* functions to investigate the record; these fields
    // should not be accessed directly.
    // ----------------------------------------
    // Start and end point of the last record read and decoded by
    // XLogReadRecordInternal().  NextRecPtr is also used as the position to
    // decode next.  Calling XLogBeginRead() sets NextRecPtr and EndRecPtr to
    // the requested starting position.
    pub(crate) decode_recptr: XLogRecPtr, // start of last record decoded
    pub(crate) next_recptr: XLogRecPtr,   // end+1 of last record decoded
    pub(crate) prev_recptr: XLogRecPtr,   // start of previous record decoded

    // Last record returned by XLogReadRecord().
    pub(crate) record: Option<DecodedXLogRecord>,

    pub(crate) decode_queue: LinkedList<DecodedXLogRecord>,

    // last read XLOG position for data currently in readBuf
    pub(crate) segcxt: WALSegmentContext,
    pub(crate) seg: WALOpenSegment,

    // beginning of the WAL record being read.
    pub(crate) curr_recptr: XLogRecPtr,
    // timeline to read it from, 0 if a lookup is required
    pub(crate) curr_tli: TimeLineID,

    // beginning of prior page read, and its TLI.  Doesn't necessarily
    // correspond to what's in readBuf; used for timeline sanity checks.
    pub(crate) latest_page_ptr: XLogRecPtr,
    pub(crate) latest_page_tli: TimeLineID,
}
