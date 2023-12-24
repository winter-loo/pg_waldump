#![allow(unused)]
mod cli;
mod constant;
mod pgtypes;
mod state;
mod util;
mod waldec;
mod rmgr;

use clap::Parser;
use constant::*;
use pgtypes::*;
use state::*;
use std::error::Error;
use std::io::Read;
use std::mem::size_of;
use std::path::PathBuf;
use util::*;
use rmgr::*;

use crate::waldec::{lsn_out, XLogPageHeaderData};

fn search_directory(waldir: &std::path::PathBuf, fname: &std::path::PathBuf) -> bool {
    let mut srched = std::path::PathBuf::new();
    if fname.as_os_str().is_empty() {
        for de in std::fs::read_dir(waldir).unwrap() {
            let de = de.unwrap();
            let path = de.path();
            if path.is_file() && is_xlog_filename(&path) {
                srched = path;
                break;
            }
        }
    } else {
        srched = fname.clone();
    }
    if srched.as_os_str().is_empty() {
        panic!("no valid wal segment file found");
    }

    let mut fpath = waldir.clone();
    fpath.push(srched);
    let mut file = match std::fs::File::open(fpath) {
        Err(_) => return false,
        Ok(file) => file,
    };

    waldec::check_first_page_header(&mut file, &fname)
}

fn identify_target_directory(waldir: PathBuf, fname: &PathBuf) -> PathBuf {
    if !waldir.as_os_str().is_empty() {
        if search_directory(&waldir, &fname) {
            return waldir;
        }

        let mut waldir = waldir.clone();
        waldir.push(XLOGDIR);
        if search_directory(&waldir, &fname) {
            return waldir;
        }
    } else {
        let dir = std::path::PathBuf::from(".");
        if search_directory(&dir, &fname) {
            return dir;
        }

        let dir = std::path::PathBuf::from(XLOGDIR);
        if search_directory(&dir, &fname) {
            return dir;
        }

        let datadir = std::env::var("PGDATA").unwrap();
        if !datadir.is_empty() {
            let mut dir = std::path::PathBuf::from(datadir);
            dir.push(XLOGDIR);
            if search_directory(&dir, &fname) {
                return dir;
            }
        }
    }

    if !fname.as_os_str().is_empty() {
        panic!("could not locate WAL file {}", fname.display());
    } else {
        panic!("could not find any WAL file");
    }
    // not reached
    std::path::PathBuf::new()
}

#[inline]
fn prefix_length(s: &str, set: &str) -> usize {
    s.chars().take_while(|&c| set.contains(c)).count()
}

#[inline]
fn is_xlog_filename(fname: &std::path::PathBuf) -> bool {
    fname.as_os_str().len() == XLOG_FNAME_LEN
        && prefix_length(fname.to_str().unwrap(), "0123456789ABCDEF") == XLOG_FNAME_LEN
}

#[inline]
fn xlog_segments_per_xlog_id(wal_seg_sz: u32) -> u32 {
    (0x100000000u64 / wal_seg_sz as u64) as u32
}

#[inline]
fn xlog_filename(tli: TimeLineID, log_seg_no: XLogSegNo, wal_seg_sz: u32) -> PathBuf {
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
fn xlog_from_file_name(
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

fn xlog_show_record(state: &XLogReaderState) {
    let record = state.decode_queue.front().unwrap();
    record.header.xl_rmid;
}

fn reset_decoder(state: &mut XLogReaderState) {}

fn xlog_begin_read(state: &mut XLogReaderState, rec_ptr: XLogRecPtr) {
    assert!(!xlog_recptr_is_invalid(rec_ptr));

    reset_decoder(state);

    // Begin at the passed-in record pointer.
    state.end_recptr = rec_ptr;
    state.next_recptr = rec_ptr;
    state.read_recptr = INVALID_XLOG_RECPTR;
    state.decode_recptr = INVALID_XLOG_RECPTR;
}

// Try to decode the next available record, and return it.  The record will
// also be returned to XLogNextRecord(), which must be called to 'consume'
// each record.
//
// If nonblocking is true, may return NULL due to lack of data or WAL decoding
// space.

fn xlog_read_ahead(state: &mut XLogReaderState) -> bool {
    waldec::xlog_decode_next_record(state)
}

fn xlog_next_record(state: &mut XLogReaderState) {
    let record = state.decode_queue.front().unwrap();
    state.read_recptr = record.lsn;
    state.end_recptr = record.next_lsn;
}

// Attempt to read an XLOG record.
//
// XLogBeginRead() or XLogFindNextRecord() must be called before the first call
// to XLogReadRecord().
//
// If the page_read callback fails to read the requested data, NULL is
// returned.  The callback is expected to have reported the error; errormsg
// is set to NULL.
//
// If the reading fails for some other reason, NULL is also returned, and
// *errormsg is set to a string with details of the failure.
//
// The returned pointer (or *errormsg) points to an internal buffer that's
// valid until the next call to XLogReadRecord.

fn xlog_read_record(state: &mut XLogReaderState) -> bool {
    if !xlog_read_ahead(state) {
        return false;
    }

    // Consume the head record or error.
    xlog_next_record(state);
    return true;
}

fn xlog_byte_to_seg(xlrp: XLogRecPtr, wal_segsz_bytes: u32) -> XLogSegNo {
    xlrp / wal_segsz_bytes as u64 as XLogSegNo
}

fn xlog_segment_offset(xlogptr: XLogRecPtr, wal_segsz_bytes: u32) -> u32 {
    (xlogptr & (wal_segsz_bytes - 1) as u64) as u32
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

fn open_segment(state: &mut XLogReaderState, next_seg_no: XLogSegNo, tli: TimeLineID) {
    let fname = xlog_filename(tli, next_seg_no, state.segcxt.ws_segsize);
    let path = &mut state.segcxt.ws_dir.clone();
    path.push(fname);
    state.seg.file = Some(std::fs::File::open(path).unwrap());
}

fn close_segment(state: &mut XLogReaderState) {
    state.seg.file = None;
}

fn wal_read(
    state: &mut XLogReaderState,
    startptr: XLogRecPtr,
    count: usize,
    tli: TimeLineID,
) -> Result<bool, WALReadError> {
    let mut recptr = startptr;
    let mut nbytes = count;

    while nbytes > 0 {
        let startoff = xlog_segment_offset(recptr, state.segcxt.ws_segsize);

        // If the data we want is not in a segment we have open, close what we
        // have (if anything) and open the next one, using the caller's
        // provided segment_open callback.
        if state.seg.file.is_none()
            || !byte_in_seg(recptr, state.seg.segno, state.segcxt.ws_segsize)
            || tli != state.seg.tli
        {
            if state.seg.file.is_some() {
                close_segment(state);
            }

            let next_seg_no = byte_to_seg(recptr, state.segcxt.ws_segsize);
            open_segment(state, next_seg_no, tli);

            // This shouldn't happen -- indicates a bug in segment_open
            assert!(state.seg.file.is_some());

            /* Update the current segment info. */
            state.seg.tli = tli;
            state.seg.segno = next_seg_no;
        }

        let mut segbytes = 0;
        // How many bytes are within this segment?
        if nbytes > (state.segcxt.ws_segsize - startoff) as usize {
            segbytes = state.segcxt.ws_segsize - startoff;
        } else {
            segbytes = nbytes as u32;
        }

        // Reset errno first; eases reporting non-errno-affecting errors
        // errno = 0;
        // readbytes = pg_pread(state.seg.ws_file, p, segbytes, (off_t) startoff);
        let buf = state.read_buf[0..segbytes as usize].as_mut();
        match state.seg.file.as_mut().unwrap().read_exact(buf) {
            Err(_) => {
                return Err(WALReadError {
                    errno: 0,
                    off: startoff,
                    req: segbytes,
                    read: segbytes,
                    seg: state.seg.clone(),
                })
            }
            Ok(_) => (),
        }

        // Update state for read
        recptr += segbytes as u64;
        nbytes -= segbytes as usize;
    }

    return Ok(true);
}

fn wal_dump_read_page(
    state: &mut XLogReaderState,
    target_page_ptr: XLogRecPtr,
    req_len: u32,
    target_ptr: XLogRecPtr,
) -> i32 {
    let private = &mut state.private_data;
    let mut count = XLOG_BLCKSZ;

    if private.endptr != INVALID_XLOG_RECPTR {
        if target_page_ptr + XLOG_BLCKSZ as u64 <= private.endptr {
            count = XLOG_BLCKSZ;
        } else if target_page_ptr + req_len as u64 <= private.endptr {
            count = (private.endptr - target_page_ptr) as u32;
        } else {
            private.endptr_reached = true;
            return -1;
        }
    }

    let private = &state.private_data;

    match wal_read(state, target_page_ptr, count as usize, private.timeline) {
        Err(errinfo) => {
            let seg = &errinfo.seg;
            let fname = xlog_filename(seg.tli, seg.segno, state.segcxt.ws_segsize);

            if errinfo.errno != 0 {
                panic!(
                    "could not read from file \"{}\", offset {}",
                    fname.display(),
                    errinfo.off
                );
            } else {
                panic!(
                    "could not read from file \"{}\", {}",
                    fname.display(),
                    errinfo
                );
            }
        }
        Ok(_) => (),
    }

    count as i32
}

// Read a single xlog page including at least [pageptr, reqLen] of valid data
// via the page_read() callback.
//
// We fetch the page from a reader-local cache if we know we have the required
// data and if there hasn't been any error since caching the data.
fn read_page(state: &mut XLogReaderState, pageptr: XLogRecPtr, req_len: u32) -> u32 {
    assert_eq!((pageptr % XLOG_BLCKSZ as u64), 0);

    let target_seg_no = xlog_byte_to_seg(pageptr, state.segcxt.ws_segsize);
    let target_page_off = xlog_segment_offset(pageptr, state.segcxt.ws_segsize);

    /* check whether we have all the requested data already */
    if target_seg_no == state.seg.segno
        && target_page_off == state.segoff
        && req_len <= state.read_len
    {
        return state.read_len;
    }

    /*
     * Invalidate contents of internal buffer before read attempt.  Just set
     * the length to 0, rather than a full XLogReaderInvalReadState(), so we
     * don't forget the segment we last successfully read.
     */
    state.read_len = 0;

    let mut read_len = 0;
    // Data is not in our buffer.
    //
    // Every time we actually read the segment, even if we looked at parts of
    // it before, we need to do verification as the page_read callback might
    // now be rereading data from a different source.
    //
    // Whenever switching to a new WAL segment, we read the first page of the
    // file and validate its header, even if that's not where the target
    // record is.  This is so that we can check the additional identification
    // info that is present in the first page's "long" header.
    if target_seg_no != state.seg.segno && target_page_off != 0 {
        let target_segment_ptr = pageptr - target_page_off as u64;

        read_len = wal_dump_read_page(state, target_segment_ptr, XLOG_BLCKSZ, state.curr_recptr);
        // if (read_len == XLREAD_WOULDBLOCK)
        // 	return XLREAD_WOULDBLOCK;
        // else if (readLen < 0)
        // 	goto err;
        if read_len < 0 {
            xlog_reader_inval_read_state(state);
            panic!("could not read a page");
        }

        /* we can be sure to have enough WAL available, we scrolled back */
        assert_eq!(read_len as u32, XLOG_BLCKSZ);

        if !waldec::xlog_reader_validate_page_header(state, target_segment_ptr) {
            panic!("unexpected page magic number");
        }
    }

    // First, read the requested data length, but at least a short page header
    // so that we can validate it.
    let n = std::cmp::max(req_len, std::mem::size_of::<XLogPageHeaderData>() as u32);
    let read_len = wal_dump_read_page(state, pageptr, n, state.curr_recptr);
    if read_len < 0 {
        return 0;
    }

    // Now that we know we have the full header, validate it.
    if !waldec::xlog_reader_validate_page_header(state, pageptr) {
        panic!("unexpected page magic number");
    }

    // update read state information
    state.seg.segno = target_seg_no;
    state.segoff = target_page_off;
    state.read_len = read_len as u32;

    return read_len as u32;

    // xlog_reader_inval_read_state(state);
}

fn xlog_find_next_record(state: &mut XLogReaderState) -> XLogRecPtr {
    let rec_ptr = state.private_data.startptr;
    assert!(!xlog_recptr_is_invalid(rec_ptr));

    // skip over potential continuation data, keeping in mind that it may span
    // multiple pages
    let mut tmp_rec_ptr = rec_ptr;
    loop {
        let target_rec_off = page_offset(tmp_rec_ptr);
        let target_page_ptr = page_addr(tmp_rec_ptr);

        /* Read the page containing the record */
        let read_len = read_page(state, target_page_ptr, target_rec_off);

        let (_, header) = waldec::page_header(&state.read_buf).unwrap();

        let page_header_size = waldec::xlog_page_header_size(&header);

        // make sure we have enough data for the page header
        // readLen = ReadPageInternal(state, targetPagePtr, pageHeaderSize);
        // if (readLen < 0)
        // 	goto err;

        /* skip over potential continuation data */
        if header.xlp_info & XLP_FIRST_IS_CONTRECORD == 1 {
            // If the length of the remaining continuation data is more than
            // what can fit in this page, the continuation record crosses over
            // this page. Read the next page and try again. xlp_rem_len in the
            // next page header will contain the remaining length of the
            // continuation data
            //
            // Note that record headers are MAXALIGN'ed
            if max_align(header.xlp_rem_len) >= (XLOG_BLCKSZ - page_header_size as u32) {
                tmp_rec_ptr = target_page_ptr.saturating_add(XLOG_BLCKSZ as u64);
            } else {
                // The previous continuation record ends in this page. Set
                // tmpRecPtr to point to the first valid record
                tmp_rec_ptr = target_page_ptr
                    + page_header_size as u64
                    + max_align(header.xlp_rem_len) as u64;
                break;
            }
        } else {
            tmp_rec_ptr = target_page_ptr + page_header_size as u64;
            break;
        }
    }

    // we know now that tmpRecPtr is an address pointing to a valid XLogRecord
    // because either we're at the first record after the beginning of a page
    // or we just jumped over the remaining data of a continuation.
    xlog_begin_read(state, tmp_rec_ptr);
    while xlog_read_record(state) {
        // past the record we've found, break out
        if rec_ptr <= state.read_recptr {
            // Rewind the reader to the beginning of the last record.
            let found = state.read_recptr;
            xlog_begin_read(state, found);
            return found;
        }
    }

    xlog_reader_inval_read_state(state);

    return INVALID_XLOG_RECPTR;
}

// Invalidate the xlogreader's read state to force a re-read.
fn xlog_reader_inval_read_state(state: &mut XLogReaderState) {
    state.seg.segno = 0;
    state.segoff = 0;
    state.read_len = 0;
}

fn xlog_reader_state_init<'a>(
    wal_seg_sz: u32,
    waldir: PathBuf,
    private_data: XLogDumpPrivate,
) -> XLogReaderState {
    let mut state = XLogReaderState::default();
    state.private_data = private_data;
    state.segcxt.ws_dir = waldir;
    state.segcxt.ws_segsize = wal_seg_sz;

    state.read_buf = vec![0; XLOG_BLCKSZ as usize];
    state
}

fn main() {
    let rmgr = &RMGR_DESC_TABLE[RmgrIds::MAX as usize - 1];
    println!("{:?}", rmgr);

    let args = cli::Cli::parse();
    let mut private = XLogDumpPrivate::default();
    private.timeline = args.timeline.unwrap();

    let mut waldir = std::path::PathBuf::new();
    if let Some(path) = args.path {
        waldir = path;
    }

    if let Some(startseg) = args.startseg {
        let mut segno: XLogSegNo = 0;

        let fname = startseg.file_name().unwrap().into();
        if let Some(dir) = startseg.parent() {
            if waldir.as_os_str().is_empty() {
                waldir = dir.to_path_buf();
            }
        }
        waldir = identify_target_directory(waldir, &fname);
        println!("Bytes per WAL segment: {}", waldec::get_wal_seg_sz());

        // parse position from file
        xlog_from_file_name(
            &fname,
            &mut private.timeline,
            &mut segno,
            waldec::get_wal_seg_sz(),
        );

        match args.start {
            Some(start) => {
                if start / waldec::get_wal_seg_sz() as u64 != segno {
                    panic!("start WAL location {} is not in file {}", start, segno);
                }
                private.startptr = start;
            }
            None => {
                private.startptr = segno * waldec::get_wal_seg_sz() as u64;
            }
        }

        if let Some(endseg) = args.endseg {
            let fname: PathBuf = endseg.file_name().unwrap().into();
            let fpath = PathBuf::from(&waldir).join(&fname);
            if std::fs::File::open(fpath).is_err() {
                panic!("could not open file {}", endseg.display());
            }

            let mut endsegno: XLogSegNo = 0;
            xlog_from_file_name(
                &fname,
                &mut private.timeline,
                &mut endsegno,
                waldec::get_wal_seg_sz(),
            );
            if endsegno < segno {
                panic!("ENDSEG {} is before STARTSEG {}", endsegno, segno);
            }
            match args.end {
                Some(end) => {
                    if end / waldec::get_wal_seg_sz() as u64 != endsegno {
                        panic!(
                            "end WAL location {} is not in file {}",
                            end,
                            fname.display()
                        );
                    }
                    private.endptr = end;
                }
                None => {
                    private.endptr = (endsegno + 1) * waldec::get_wal_seg_sz() as u64;
                }
            }
        }
    } else {
        waldir = identify_target_directory(waldir, &PathBuf::new());
    }

    if private.startptr == XLOG_INVALID_RECPTR {
        panic!("no start WAL location given");
    }

    let mut xlogreader_state =
        xlog_reader_state_init(waldec::get_wal_seg_sz(), waldir, private.clone());
    let first_record = xlog_find_next_record(&mut xlogreader_state);

    if first_record == INVALID_XLOG_RECPTR {
        panic!(
            "could not find a valid record after {}",
            waldec::lsn_out(private.startptr.clone())
        );
    }

    // Display a message that we're skipping data if `from` wasn't a pointer
    // to the start of a record and also wasn't a pointer to the beginning of
    // a segment (e.g. we were used in file mode).
    if first_record != private.startptr
        && xlog_segment_offset(private.startptr, waldec::get_wal_seg_sz()) != 0
    {
        println!(
            "first record is after {}, at {}, skipping over {} byte(s)",
            waldec::lsn_out(private.startptr),
            waldec::lsn_out(first_record),
            (first_record - private.startptr) as u32
        );
    }

    loop {
        if !xlog_read_record(&mut xlogreader_state) {
            break;
        }
        if let Some(quiet) = args.quiet {
            // nothing to do
        } else {
            xlog_show_record(&xlogreader_state);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xlog_from_file_name() {
        let mut tli: TimeLineID = 0;
        let mut segno: u64 = 0;
        xlog_from_file_name(
            &PathBuf::from("000000010000000000000001"),
            &mut tli,
            &mut segno,
            16 * 1024 * 1024,
        );
        assert_eq!(tli, 1);
        assert_eq!(segno, 1);

        xlog_from_file_name(
            &PathBuf::from("000000020000000100000001"),
            &mut tli,
            &mut segno,
            16 * 1024 * 1024,
        );
        assert_eq!(tli, 1);
        assert_eq!(segno, 0x40001);
    }
}
