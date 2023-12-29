#![allow(unused)]
mod cli;
mod constant;
mod pgtypes;
mod rmgr;
mod state;
mod util;
mod waldec;

use clap::Parser;
use constant::*;
use pgtypes::*;
use rmgr::*;
use state::*;
use std::io::Read;
use std::mem::size_of;
use std::path::PathBuf;
use util::*;

use crate::waldec::{bkpimage_compressed, lsn_out, BkpImageCompressMethod, XLogPageHeaderData};

fn search_directory(waldir: &std::path::PathBuf, fname: &std::path::PathBuf) -> bool {
    let mut srched = std::path::PathBuf::new();
    if fname.as_os_str().is_empty() {
        match std::fs::read_dir(waldir) {
            Err(_) => return false,
            Ok(dentries) => {
                for de in dentries {
                    let de = de.unwrap();
                    let path = de.path();
                    // println!("path: {}", path.display());
                    if path.is_file() && is_xlog_filename(&path) {
                        srched = path;
                        break;
                    }
                }
            }
        }
    } else {
        srched = fname.clone();
    }
    if srched.as_os_str().is_empty() {
        return false;
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

fn xlog_rec_has_block_image(record: &DecodedXLogRecord, blk_id: i8) -> bool {
    record.blocks[blk_id as usize].has_image
}

fn xlog_rec_has_block_ref(record: &DecodedXLogRecord, blk_id: i8) -> bool {
    record.max_block_id >= blk_id && record.blocks[blk_id as usize].in_use
}

fn xlog_rec_get_len(record: &DecodedXLogRecord) -> (u32, u32) {
    let mut fpi_len: u32 = 0;

    for blk_id in 0..=record.max_block_id {
        if !xlog_rec_has_block_ref(record, blk_id) {
            continue;
        }

        if xlog_rec_has_block_image(record, blk_id) {
            fpi_len += record.blocks[blk_id as usize].bimg_len as u32;
        }
    }

    (record.header.xl_tot_len - fpi_len as u32, fpi_len)
}

fn xlog_rec_get_block_tag_extended(
    record: &DecodedXLogRecord,
    bid: i8,
) -> Option<(&RelFileLocator, &ForkNumber, &BlockNumber)> {
    if !xlog_rec_has_block_ref(record, bid) {
        return None;
    }

    let bkpb = &record.blocks[bid as usize];
    Some((&bkpb.rlocator, &bkpb.forknum, &bkpb.blkno))
}

// Returns a string giving information about all the blocks in an
// XLogRecord.
fn xlog_rec_get_block_ref_info(state: &XLogReaderState) -> String {
    let mut retval = String::new();
    retval.push('\n');

    let record = state.record.as_ref().unwrap();

    for bid in 0..=record.max_block_id {
        if let Some((rlocator, forknum, blk)) = xlog_rec_get_block_tag_extended(record, bid) {
            retval.push('\t');
            let forknum: i8 = *forknum as i8;
            assert!(forknum >= 0);

            let s = format!(
                "blkref #{}: rel {}/{}/{} fork {} blk {}",
                bid,
                rlocator.spc_oid,
                rlocator.db_oid,
                rlocator.rel_oid,
                FORK_NAMES[forknum as usize],
                blk
            );
            retval.push_str(&s);

            if xlog_rec_has_block_image(record, bid) {
                let bimg_info = record.blocks[bid as usize].bimg_info;
                // if fpi_len {
                //     fpi_len += record.blocks[bid as usize].bimg_len;
                // }
                let target = if record.blocks[bid as usize].apply_image {
                    ""
                } else {
                    " for WAL verification"
                };
                let blk = &record.blocks[bid as usize];

                let s = if bkpimage_compressed(bimg_info) {
                    format!(
                        " (FPW{}); hole: offset: {}, length: {}, compression saved: {}, method: {}",
                        target,
                        blk.hole_offset,
                        blk.hole_length,
                        (XLOG_BLCKSZ - blk.hole_length as u32 - blk.bimg_len as u32),
                        BkpImageCompressMethod::from(bimg_info)
                    )
                } else {
                    format!(" (FPW{}); hole: offset: {},length: {}", target, blk.hole_offset, blk.hole_length)
                };
                retval.push_str(&s);
            }
            retval.push('\n');
        }
    }
    retval
}

fn xlog_show_record(state: &XLogReaderState) {
    let record = state.record.as_ref().unwrap();
    let desc = get_rmgr_desc(record.header.xl_rmid);
    let info = record.header.xl_info;
    let xl_prev = record.header.xl_prev;

    let (rec_len, fpi_len) = xlog_rec_get_len(record);

    print!(
        "rmgr {} len (rec/tot) {}/{}, tx {}, lsn {}, prev {}, ",
        desc.rm_name,
        rec_len,
        record.header.xl_tot_len,
        record.header.xl_xid,
        lsn_out(state.read_recptr),
        lsn_out(xl_prev)
    );

    let id = (desc.rm_identify)(info);
    if id.len() == 0 {
        print!("desc UNKNOWN ({:X}) ", info & !XLR_INFO_MASK);
    } else {
        print!("desc {} ", id);
    }

    let s = (desc.rm_desc)(state);
    print!("{}", s);

    let s = xlog_rec_get_block_ref_info(&state);
    println!("{}", s);
}

fn reset_decoder(state: &mut XLogReaderState) {
    state.decode_queue.clear();
}

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
    state.record = state.decode_queue.pop_front();
    if let Some(record) = &state.record {
        state.read_recptr = record.lsn;
        state.end_recptr = record.next_lsn;
    }
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
        let read_len = waldec::read_page(state, target_page_ptr, target_rec_off);

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

    state.invalidate();

    return INVALID_XLOG_RECPTR;
}

fn main() {
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
        // println!("Bytes per WAL segment: {}", waldec::get_wal_seg_sz());

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
        XLogReaderState::new(waldec::get_wal_seg_sz(), waldir, private.clone());
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

    let mut records_displayed: u32 = 0;
    loop {
        if !xlog_read_record(&mut xlogreader_state) {
            break;
        }
        if let Some(quiet) = args.quiet {
            if !quiet {
                xlog_show_record(&xlogreader_state);
            }
        }
        records_displayed += 1;
        if records_displayed >= args.limit.unwrap_or(u32::MAX) {
            break;
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
