#![allow(unused)]
use crate::constant::*;
use crate::pgtypes::*;
use crate::rmgr::*;
use crate::state::*;
use crate::util::*;
use crate::xlog_reader_inval_read_state;
use nom::bytes::streaming::take as bytes_take;
use nom::combinator::map;
use nom::number::streaming::{le_u16, le_u32, le_u64, le_u8};
use nom::sequence;
use nom::IResult;
use std::fs::File;
use std::io::BufRead;
use std::io::Read;
use std::path::PathBuf;

fn is_valid_xlog_record_header(
    state: &mut XLogReaderState,
    rec_ptr: XLogRecPtr,
    prev_recptr: XLogRecPtr,
    record: &XLogRecord,
) -> bool {
    if record.xl_tot_len < std::mem::size_of::<XLogRecord>() as u32 {
        state.errmsg = format!(
            "invalid record length at {}: expected at least {}, got {}",
            lsn_out(rec_ptr),
            record.xl_tot_len,
            std::mem::size_of::<XLogRecord>() as u32
        );
        return false;
    }

    if !rmgr_id_is_valid(record.xl_rmid) {
        state.errmsg = format!(
            "invalid resource manager ID {} at {}",
            record.xl_rmid,
            lsn_out(rec_ptr)
        );
        return false;
    }

    if record.xl_prev != prev_recptr {
        state.errmsg = format!(
            "record with incorrect prev-link {} at {}",
            lsn_out(prev_recptr),
            lsn_out(rec_ptr)
        );
        return false;
    }
    return true;
}

pub fn lsn_out(rec_ptr: XLogRecPtr) -> String {
    format!("{:X}/{:X}", rec_ptr >> 32, rec_ptr as u32)
}

pub enum BkpImageCompressMethod {
    PGLZ = 0x04,
    LZ4 = 0x08,
    ZSTD = 0x10,
}

impl From<u8> for BkpImageCompressMethod {
    fn from(value: u8) -> Self {
        if value == Self::PGLZ as u8 {
            Self::PGLZ
        } else if value == Self::LZ4 as u8 {
            Self::LZ4
        } else if value == Self::ZSTD as u8 {
            Self::ZSTD
        } else {
            panic!("invalid value for enum BkpImageCompressMethod");
        }
    }
}

impl std::fmt::Display for BkpImageCompressMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::PGLZ => "pglz",
            Self::LZ4 => "lz4",
            Self::ZSTD => "zstd",
        };
        write!(f, "{}", s)
    }
}

pub fn bkpimage_compressed(info: u8) -> bool {
    (info
        & (BkpImageCompressMethod::PGLZ as u8
            | BkpImageCompressMethod::LZ4 as u8
            | BkpImageCompressMethod::ZSTD as u8))
        != 0
}

pub fn decode_xlog_record_payload(
    state: &mut XLogReaderState,
    record: &XLogRecord,
    lsn: XLogRecPtr,
) -> Option<DecodedXLogRecord> {
    let mut decoded = DecodedXLogRecord::default();
    decoded.max_block_id = -1;
    decoded.header = record.clone();
    decoded.lsn = lsn;
    let mut buf = state.read_buf.as_slice();
    let hdrsz = std::mem::size_of::<XLogRecord>() as u32;
    let rec_payload_off = page_offset(lsn) + hdrsz;
    buf = &buf[rec_payload_off as usize..];

    let mut remaining = record.xl_tot_len - hdrsz;

    let mut datatotal = 0;
    let mut blk_id = 0;
    while remaining > datatotal {
        (buf, blk_id) = byte_to_u8(buf).unwrap();
        remaining -= 1;

        if blk_id == XLR_BLOCK_ID_DATA_SHORT {
            let mut main_data_len = 0;
            (buf, main_data_len) = byte_to_u8(buf).unwrap();
            remaining -= 1;

            decoded.main_data_len = main_data_len as u32;
            datatotal += main_data_len as u32;
            break;
        } else if blk_id == XLR_BLOCK_ID_DATA_LONG {
            let main_data_len: u32;
            (buf, main_data_len) = byte_to_u32(buf).unwrap();
            remaining -= 4;

            decoded.main_data_len = main_data_len;
            datatotal += main_data_len;
            break;
        } else if blk_id == XLR_BLOCK_ID_ORIGIN {
            let record_origin: u16;
            (buf, record_origin) = byte_to_u16(buf).unwrap();
            remaining -= 2;

            decoded.record_origin = record_origin;
        } else if blk_id == XLR_BLOCK_ID_TOPLEVEL_XID {
            let top_level_xid: TransactionId;
            (buf, top_level_xid) = byte_to_u32(buf).unwrap();
            remaining -= 4;

            decoded.toplevel_xid = top_level_xid;
        } else if blk_id <= XLR_MAX_BLOCK_ID {
            decoded.blocks = vec![DecodedBkpBlock::default(); (blk_id as i8 - decoded.max_block_id) as usize];
            for i in decoded.max_block_id + 1..blk_id as i8 {
                let blocks = decoded.blocks.as_mut_slice();
                blocks[i as usize].in_use = false;
            }
            if blk_id as i8 <= decoded.max_block_id {
                state.errmsg = format!(
                    "out-of-order block_id {} at {}",
                    blk_id,
                    lsn_out(state.read_recptr)
                );
                return None;
            }
            decoded.max_block_id = blk_id as i8;
            let blk = &mut decoded.blocks[blk_id as usize];
            blk.in_use = true;
            blk.apply_image = false;
            let mut fork_flags = 0;
            (buf, fork_flags) = byte_to_u8(buf).unwrap();
            remaining -= 1;

            blk.forknum = ForkNumber::from((fork_flags & BKPBLOCK_FORK_MASK) as i8);
            blk.flags = fork_flags;
            blk.has_image = (fork_flags & BKPBLOCK_HAS_IMAGE) != 0;
            blk.has_data = (fork_flags & BKPBLOCK_HAS_DATA) != 0;
            // blk.prefetch_buffer = InvalidBuffer;
            (buf, blk.data_len) = byte_to_u16(buf).unwrap();
            remaining -= 2;

            if blk.has_data && blk.data_len == 0 {
                state.errmsg = format!(
                    "BKPBLOCK_HAS_DATA set, but no data included at {}",
                    lsn_out(state.read_recptr)
                );
                return None;
            }

            if !blk.has_data && blk.data_len != 0 {
                state.errmsg = format!(
                    "BKPBLOCK_HAS_DATA not set, but data length is {} at {}",
                    blk.data_len,
                    lsn_out(state.read_recptr)
                );
                return None;
            }
            datatotal += blk.data_len as u32;

            if blk.has_image {
                (buf, blk.bimg_len) = byte_to_u16(buf).unwrap();
                remaining -= 2;
                (buf, blk.hole_offset) = byte_to_u16(buf).unwrap();
                remaining -= 2;
                (buf, blk.bimg_info) = byte_to_u8(buf).unwrap();
                remaining -= 1;

                blk.apply_image = (blk.bimg_info & BKPIMAGE_APPLY) != 0;

                if bkpimage_compressed(blk.bimg_info) {
                    if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0 {
                        (buf, blk.hole_length) = byte_to_u16(buf).unwrap();
                        remaining -= 2;
                    } else {
                        blk.hole_length = 0;
                    }
                } else {
                    blk.hole_length = XLOG_BLCKSZ as u16 - blk.bimg_len;
                }
                datatotal += blk.bimg_len as u32;

                // cross-check that hole_offset > 0, hole_length > 0 and
                // bimg_len < BLCKSZ if the HAS_HOLE flag is set.
                if blk.bimg_info & BKPIMAGE_HAS_HOLE != 0
                    && (blk.hole_offset == 0
                        || blk.hole_length == 0
                        || blk.bimg_len == XLOG_BLCKSZ as u16)
                {
                    state.errmsg = format!("BKPIMAGE_HAS_HOLE set, but hole offset {} length {} block image length {} at {}",
										   blk.hole_offset,
										   blk.hole_length,
										   blk.bimg_len,
										  lsn_out(state.read_recptr));
                    return None;
                }

                // cross-check that hole_offset == 0 and hole_length == 0 if
                // the HAS_HOLE flag is not set.
                if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                    && (blk.hole_offset != 0 || blk.hole_length != 0)
                {
                    state.errmsg = format!(
                        "BKPIMAGE_HAS_HOLE not set, but hole offset {} length {} at {}",
                        blk.hole_offset,
                        blk.hole_length,
                        lsn_out(state.read_recptr)
                    );
                    return None;
                }

                // Cross-check that bimg_len < BLCKSZ if it is compressed.
                if bkpimage_compressed(blk.bimg_info) && blk.bimg_len == XLOG_BLCKSZ as u16 {
                    state.errmsg = format!(
                        "BKPIMAGE_COMPRESSED set, but block image length {} at {}",
                        blk.bimg_len,
                        lsn_out(state.read_recptr)
                    );
                    return None;
                }

                // cross-check that bimg_len = BLCKSZ if neither HAS_HOLE is
                // set nor COMPRESSED().
                if blk.bimg_info & BKPIMAGE_HAS_HOLE == 0
                    && !bkpimage_compressed(blk.bimg_info)
                    && blk.bimg_len != XLOG_BLCKSZ as u16
                {
                    state.errmsg = format!("neither BKPIMAGE_HAS_HOLE nor BKPIMAGE_COMPRESSED set, but block image length is {} at {}",
										   blk.data_len,
										  lsn_out(state.read_recptr));
                    return None;
                }
            }

            if fork_flags & BKPBLOCK_SAME_REL == 0 {
                (buf, blk.rlocator) = parse_rel_file_locator(buf).unwrap();
                remaining -= std::mem::size_of::<RelFileLocator>() as u32;
            // rlocator = &blk.rlocator;
            } else {
                // if (rlocator == NULL)
                // {
                // 	report_invalid_record(state,
                // 						  "BKPBLOCK_SAME_REL set but no previous rel at %X/%X",
                // 						  LSN_FORMAT_ARGS(state->ReadRecPtr));
                // 	goto err;
                // }
            }
            (buf, blk.blkno) = byte_to_u32(buf).unwrap();
            remaining -= 4;
        } else {
            panic!("invalid block id: {}", blk_id);
        }
    }

    if remaining != datatotal {
        state.errmsg = format!(
            "record with invalid length at {}",
            lsn_out(state.read_recptr)
        );
        return None;
    }

    let mut decoded_size: u32 = 0;
    for block_id in 0..=decoded.max_block_id {
        let blk = &mut decoded.blocks[block_id as usize];
        if !blk.in_use {
            continue;
        }
        assert!(blk.has_image || !blk.apply_image);

        if blk.has_image {
            blk.bkp_image = buf[0..blk.bimg_len as usize].to_vec();
            decoded_size += blk.bimg_len as u32;
        }
        if blk.has_data {
            blk.data = buf[0..blk.data_len as usize].to_vec();
            decoded_size += blk.data_len as u32;
        }
    }

    // and finally, the main data
    if decoded.main_data_len > 0 {
        decoded.main_data = buf[0..decoded.main_data_len as usize].to_vec();
        decoded_size += decoded.main_data_len;
    }

    // report the actual size we used
    decoded.size = max_align(decoded_size) as usize;
    assert!(decode_xlog_record_required_space(record.xl_tot_len as usize) >= decoded.size);

    Some(decoded)
}

// Compute the maximum possible amount of padding that could be required to
// decode a record, given xl_tot_len from the record's header.  This is the
// amount of output buffer space that we need to decode a record, though we
// might not finish up using it all.
//
// This computation is pessimistic and assumes the maximum possible number of
// blocks, due to lack of better information.
fn decode_xlog_record_required_space(xl_tot_len: usize) -> usize {
    let mut size: usize = 0;

    /* Account for the fixed size part of the decoded record struct. */
    size += std::mem::size_of::<DecodedXLogRecord>();
    /* Account for the flexible blocks array of maximum possible size. */
    size += std::mem::size_of::<DecodedBkpBlock>() * (XLR_MAX_BLOCK_ID + 1) as usize;
    /* Account for all the raw main and block data. */
    size += xl_tot_len;
    /* We might insert padding before main_data. */
    size += 7;
    /* We might insert padding before each block's data. */
    size += 7 * (XLR_MAX_BLOCK_ID + 1) as usize;
    // We might insert padding at the end.
    size += 7;

    size
}

fn parse_rel_file_locator(input: &[u8]) -> IResult<&[u8], RelFileLocator> {
    map(
        sequence::tuple((le_u32, le_u32, le_u32)),
        |(spc_oid, db_oid, rel_oid)| RelFileLocator {
            spc_oid,
            db_oid,
            rel_oid,
        },
    )(input)
}

pub(crate) fn xlog_decode_next_record(state: &mut XLogReaderState) -> bool {
    let mut rec_ptr = state.next_recptr;
    println!("rec_ptr: {:X}", rec_ptr);

    // state.curr_recptr = rec_ptr;
    let target_page_ptr = page_addr(rec_ptr);
    let mut target_rec_off = page_offset(rec_ptr);

    if target_rec_off == 0 {
        rec_ptr += state.page_hdr_size as u64;
        target_rec_off = state.page_hdr_size;
    } else if target_rec_off < state.page_hdr_size {
        state.errmsg = format!(
            "invalid record offset at {}: expected at least {}, got {}",
            lsn_out(rec_ptr),
            state.page_hdr_size,
            target_rec_off
        );
        return false;
    }

    let (buf, hdr) = page_header(&state.read_buf).unwrap();

    if (hdr.xlp_info & XLP_FIRST_IS_CONTRECORD) == XLP_FIRST_IS_CONTRECORD
        && target_rec_off == state.page_hdr_size
    {
        state.errmsg = format!("contrecord is requested by {}", lsn_out(rec_ptr));
        return false;
    }

    let buf = &state.read_buf[target_rec_off as usize..];
    let (buf, record) = xlog_record(buf).unwrap();
    let total_len = record.xl_tot_len;
    let mut gotheader = false;
    if target_rec_off <= XLOG_BLCKSZ - std::mem::size_of::<XLogRecord>() as u32 {
        if !is_valid_xlog_record_header(state, rec_ptr, state.decode_recptr, &record) {
            return false;
        }
        gotheader = true;
    } else {
        let hdr_sz = std::mem::size_of::<XLogRecord>() as u32;
        if total_len < hdr_sz {
            state.errmsg = format!(
                "invalid record length at {}: expected at least {}, got {}",
                lsn_out(rec_ptr),
                hdr_sz,
                total_len
            );
            return false;
        }
        gotheader = false;
    }

    let len = XLOG_BLCKSZ - ((rec_ptr as u32) & (XLOG_BLCKSZ - 1));
    if total_len > len {
        panic!("record length too big: {}", total_len);
    } else {
        // TODO: crc check xlog record
        // if !ValidXLogRecord(state, record, RecPtr)

        state.next_recptr = rec_ptr + max_align(total_len) as u64;
        state.decode_recptr = rec_ptr;
    }

    // Special processing if it's an XLOG SWITCH record
    if record.xl_rmid == RmgrIds::XLOG as u8
        && (record.xl_info & !XLR_INFO_MASK) == XLogInfo::Switch as u8
    {
        // Pretend it extends to end of segment
        state
            .next_recptr
            .saturating_add((state.segcxt.ws_segsize - 1) as u64);
        state.next_recptr -= xlog_segment_offset(state.next_recptr, state.segcxt.ws_segsize) as u64;
    }

    if let Some(mut decoded) = decode_xlog_record_payload(state, &record, rec_ptr) {
        decoded.next_lsn = state.next_recptr;
        if !decoded.oversized {
            assert_eq!(decoded.size, max_align(decoded.size as u32) as usize);
            state.decode_queue.push_back(decoded);
            return true;
        }
    } else {
        xlog_reader_inval_read_state(state);
    }
    return false;
}

#[repr(align(8))]
pub(crate) struct XLogPageHeaderData {
    pub xlp_magic: u16,
    pub xlp_info: u16,
    pub xlp_tli: TimeLineID,
    pub xlp_pageaddr: XLogRecPtr,
    // When there is not enough space on current page for whole record, we
    // continue on the next page.  xlp_rem_len is the number of bytes
    // remaining from a previous page.
    //
    // Note that xlp_rem_len includes backup-block data; that is, it tracks
    // xl_tot_len not xl_len in the initial header.  Also note that the
    // continuation data isn't necessarily aligned.
    pub xlp_rem_len: u32,
}

impl XLogPageHeaderData {
    fn new() -> Self {
        XLogPageHeaderData {
            xlp_magic: XLOG_PAGE_MAGIC,
            xlp_info: 0,
            xlp_tli: 0,
            xlp_pageaddr: 0,
            xlp_rem_len: 0,
        }
    }
}

pub(crate) type XLogPageHeader = XLogPageHeaderData;

pub fn page_header(input: &[u8]) -> IResult<&[u8], XLogPageHeaderData> {
    map(
        sequence::tuple((le_u16, le_u16, le_u32, le_u64, le_u32, bytes_take(4usize))),
        |(magic, info, tli, paguaddr, rem_len, _)| {
            if magic != XLOG_PAGE_MAGIC {
                panic!("invalid magic: {:#06X}", magic);
            }
            XLogPageHeaderData {
                xlp_magic: XLOG_PAGE_MAGIC,
                xlp_info: info,
                xlp_tli: tli,
                xlp_pageaddr: paguaddr,
                xlp_rem_len: rem_len,
            }
        },
    )(input)
}

#[repr(align(8))]
pub(crate) struct XLogLongPageHeaderData {
    std: XLogPageHeaderData,
    xlp_sysid: u64,
    xlp_seg_size: u32,
    xlp_xlog_blcksz: u32,
}

pub(crate) fn first_page_header(input: &[u8]) -> IResult<&[u8], XLogLongPageHeaderData> {
    map(
        sequence::tuple((page_header, le_u64, le_u32, le_u32)),
        |(hdr, sysid, seg_size, blcksz)| XLogLongPageHeaderData {
            std: hdr,
            xlp_sysid: sysid,
            xlp_seg_size: seg_size,
            xlp_xlog_blcksz: blcksz,
        },
    )(input)
}

pub(crate) fn xlog_record(input: &[u8]) -> IResult<&[u8], XLogRecord> {
    map(
        sequence::tuple((
            le_u32,
            le_u32,
            le_u64,
            le_u8,
            le_u8,
            bytes_take(2usize),
            le_u32,
        )),
        |(tot_len, xid, prev, info, rmid, _, crc)| XLogRecord {
            xl_tot_len: tot_len,
            xl_xid: xid,
            xl_prev: prev,
            xl_info: info,
            xl_rmid: rmid,
            xl_crc: crc,
        },
    )(input)
}

#[derive(PartialEq, Debug)]
pub(crate) enum XLogRecordDataHeader {
    Short(XLogRecordDataHeaderShort),
    Long(XLogRecordDataHeaderLong),
    Origin(XLogRecordDataHeaderOrigin),
    TopLevelXid(XLogRecordDataHeaderTopLevelXid),
    Block(XLogRecordBlockHeader),
}

pub(crate) fn xlog_record_data_header(input: &[u8]) -> IResult<&[u8], XLogRecordDataHeader> {
    let (input, id) = le_u8(input)?;
    match id {
        XLR_BLOCK_ID_DATA_SHORT => map(xlog_record_data_header_short(id), |hdr| {
            XLogRecordDataHeader::Short(hdr)
        })(input),
        XLR_BLOCK_ID_DATA_LONG => map(xlog_record_data_header_long(id), |hdr| {
            XLogRecordDataHeader::Long(hdr)
        })(input),
        XLR_BLOCK_ID_ORIGIN => map(xlog_record_data_header_origin(id), |hdr| {
            XLogRecordDataHeader::Origin(hdr)
        })(input),
        XLR_BLOCK_ID_TOPLEVEL_XID => map(xlog_record_data_header_top_level_xid(id), |hdr| {
            XLogRecordDataHeader::TopLevelXid(hdr)
        })(input),
        val if val <= XLR_MAX_BLOCK_ID => map(xlog_record_block_header(id), |hdr| {
            XLogRecordDataHeader::Block(hdr)
        })(input),
        _ => panic!("invalid XLogRecordDataHeader id: {}", id),
    }
}

// XLogRecordDataHeaderShort/Long are used for the "main data" portion of
// the record. If the length of the data is less than 256 bytes, the short
// form is used, with a single byte to hold the length. Otherwise the long
// form is used.
#[repr(packed)]
#[derive(PartialEq, Debug)]
pub(crate) struct XLogRecordDataHeaderShort {
    // XLR_BLOCK_ID_DATA_SHORT
    id: u8,
    // number of payload bytes
    data_length: u8,
}

pub(crate) fn xlog_record_data_header_short(
    id: u8,
) -> impl FnMut(&[u8]) -> IResult<&[u8], XLogRecordDataHeaderShort> {
    move |input: &[u8]| {
        let (input, data_length) = le_u8(input)?;
        Ok((input, XLogRecordDataHeaderShort { id, data_length }))
    }
}

#[repr(packed)]
#[derive(PartialEq, Debug)]
pub(crate) struct XLogRecordDataHeaderLong {
    // XLR_BLOCK_ID_DATA_LONG
    id: u8,
    data_length: u32,
}

pub(crate) fn xlog_record_data_header_long(
    id: u8,
) -> impl FnMut(&[u8]) -> IResult<&[u8], XLogRecordDataHeaderLong> {
    move |input: &[u8]| {
        let (input, data_length) = le_u32(input)?;
        Ok((input, XLogRecordDataHeaderLong { id, data_length }))
    }
}

#[repr(packed)]
#[derive(PartialEq, Debug)]
pub(crate) struct XLogRecordDataHeaderOrigin {
    id: u8,
    record_origin: u16,
}

pub(crate) fn xlog_record_data_header_origin(
    id: u8,
) -> impl FnMut(&[u8]) -> IResult<&[u8], XLogRecordDataHeaderOrigin> {
    move |input: &[u8]| {
        let (input, record_origin) = le_u16(input)?;
        Ok((input, XLogRecordDataHeaderOrigin { id, record_origin }))
    }
}

#[repr(packed)]
#[derive(PartialEq, Debug)]
pub(crate) struct XLogRecordDataHeaderTopLevelXid {
    id: u8,
    top_level_xid: TransactionId,
}

pub(crate) fn xlog_record_data_header_top_level_xid(
    id: u8,
) -> impl FnMut(&[u8]) -> IResult<&[u8], XLogRecordDataHeaderTopLevelXid> {
    move |input: &[u8]| {
        let (input, top_level_xid) = le_u32(input)?;
        Ok((input, XLogRecordDataHeaderTopLevelXid { id, top_level_xid }))
    }
}

// Header info for block data appended to an XLOG record.
//
// 'data_length' is the length of the rmgr-specific payload data associated
// with this block. It does not include the possible full page image, nor
// XLogRecordBlockHeader struct itself.
//
// Note that we don't attempt to align the XLogRecordBlockHeader struct!
// So, the struct must be copied to aligned local storage before use.
///
#[repr(packed)]
#[derive(PartialEq, Debug)]
pub(crate) struct XLogRecordBlockHeader {
    // block reference ID
    id: u8,
    // fork within the relation, and flags
    fork_flags: u8,
    // number of payload bytes (not including page image)
    data_length: u16,
    // If BKPBLOCK_HAS_IMAGE, an XLogRecordBlockImageHeader struct follows
    // If BKPBLOCK_SAME_REL is not set, a RelFileNode follows
    // BlockNumber follows
}

pub(crate) fn xlog_record_block_header(
    id: u8,
) -> impl FnMut(&[u8]) -> IResult<&[u8], XLogRecordBlockHeader> {
    move |input: &[u8]| {
        map(
            sequence::tuple((le_u8, le_u16)),
            |(fork_flags, data_length)| XLogRecordBlockHeader {
                id,
                fork_flags,
                data_length,
            },
        )(input)
    }
}

#[repr(packed)]
pub(crate) struct XLogRecordBlockImageHeader {
    // number of page image bytes
    length: u16,
    // number of bytes before "hole"
    hole_offset: u16,
    // flag bits, see below
    bimg_info: u8,
    // If BKPIMAGE_HAS_HOLE and BKPIMAGE_IS_COMPRESSED, an
    // XLogRecordBlockCompressHeader struct follows.
}

pub(crate) fn xlog_record_block_image_header(
    input: &[u8],
) -> IResult<&[u8], XLogRecordBlockImageHeader> {
    map(
        sequence::tuple((le_u16, le_u16, le_u8)),
        |(length, hole_offset, bimg_info)| XLogRecordBlockImageHeader {
            length,
            hole_offset,
            bimg_info,
        },
    )(input)
}

pub fn byte_to_u8(input: &[u8]) -> IResult<&[u8], u8> {
    le_u8(input)
}

pub fn byte_to_u16(input: &[u8]) -> IResult<&[u8], u16> {
    le_u16(input)
}

pub fn byte_to_u32(input: &[u8]) -> IResult<&[u8], u32> {
    le_u32(input)
}

pub(crate) struct XLogRecordBlockCompressHeader {
    // number of bytes in "hole"
    hole_length: u16,
}

pub(crate) fn xlog_page_header_size(hdr: &XLogPageHeaderData) -> u32 {
    if hdr.xlp_info & XLP_LONG_HEADER == XLP_LONG_HEADER {
        std::mem::size_of::<XLogLongPageHeaderData>() as u32
    } else {
        std::mem::size_of::<XLogPageHeaderData>() as u32
    }
}

static mut WAL_SEG_SZ: u32 = 16 * 1024 * 1024;

pub(crate) fn get_wal_seg_sz() -> u32 {
    unsafe { WAL_SEG_SZ }
}

pub(crate) fn set_wal_seg_sz(sz: u32) {
    unsafe {
        WAL_SEG_SZ = sz;
    }
}

fn is_valid_wal_segment_size(sz: u32) -> bool {
    (sz > 0 && (sz & (sz - 1) == 0)) && sz >= WAL_SEG_MIN_SIZE && sz <= WAL_SEG_MAX_SIZE
}

pub(crate) fn check_first_page_header(file: &mut File, fname: &PathBuf) -> bool {
    let mut buf = [0u8; XLOG_BLCKSZ as usize];
    match file.read(&mut buf) {
        Ok(n) => {
            if n == XLOG_BLCKSZ as usize {
                let (_, hdr) = first_page_header(&buf).unwrap();
                if !is_valid_wal_segment_size(hdr.xlp_seg_size) {
                    panic!("invalid wal segment size");
                }
                set_wal_seg_sz(hdr.xlp_seg_size);
                return true;
            } else {
                panic!(
                    "could not read file {}: read {} of {}",
                    fname.display(),
                    n,
                    XLOG_BLCKSZ
                );
            }
        }
        Err(e) => {
            panic!("could not read file {}: {}", fname.display(), e);
        }
    }
    return false;
}

fn xlog_segment_offset(xlogptr: XLogRecPtr, wal_segsz_bytes: u32) -> u32 {
    (xlogptr & (wal_segsz_bytes - 1) as u64) as u32
}

// Validate a page header.
//
// Check if 'phdr' is valid as the header of the XLog page at position
// 'recptr'.
pub(crate) fn xlog_reader_validate_page_header(
    state: &mut XLogReaderState,
    recptr: XLogRecPtr,
) -> bool {
    let (buf, hdr) = page_header(&state.read_buf).unwrap();

    assert_eq!((recptr % XLOG_BLCKSZ as u64), 0);

    // XLByteToSeg(recptr, segno, state.segcxt.ws_segsize);
    let offset = xlog_segment_offset(recptr, state.segcxt.ws_segsize);

    if hdr.xlp_magic != XLOG_PAGE_MAGIC {
        // char		fname[MAXFNAMELEN];

        // XLogFileName(fname, state.seg.ws_tli, segno, state.segcxt.ws_segsize);

        // report_invalid_record(state,
        // 					  "invalid magic number %04X in WAL segment %s, LSN %X/%X, offset %u",
        // 					  hdr.xlp_magic,
        // 					  fname,
        // 					  LSN_FORMAT_ARGS(recptr),
        // 					  offset);
        return false;
    }

    if (hdr.xlp_info & !XLP_ALL_FLAGS) != 0 {
        // char		fname[MAXFNAMELEN];

        // XLogFileName(fname, state.seg.ws_tli, segno, state.segcxt.ws_segsize);

        // report_invalid_record(state,
        // 					  "invalid info bits %04X in WAL segment %s, LSN %X/%X, offset %u",
        // 					  hdr.xlp_info,
        // 					  fname,
        // 					  LSN_FORMAT_ARGS(recptr),
        // 					  offset);
        return false;
    }

    if hdr.xlp_info & XLP_LONG_HEADER == XLP_LONG_HEADER {
        let (buf, longhdr) = first_page_header(&state.read_buf).unwrap();

        // if longhdr.xlp_sysid != state.system_identifier {
        //     // report_invalid_record(state,
        //     // 					  "WAL file is from different database system: WAL file database system identifier is %llu, pg_control database system identifier is %llu",
        //     // 					  (unsigned long long) longhdr.xlp_sysid,
        //     // 					  (unsigned long long) state.system_identifier);
        //     return false;
        // }
        if longhdr.xlp_seg_size != state.segcxt.ws_segsize {
            // report_invalid_record(state,
            // 					  "WAL file is from different database system: incorrect segment size in page header");
            return false;
        } else if longhdr.xlp_xlog_blcksz != XLOG_BLCKSZ {
            // report_invalid_record(state,
            // 					  "WAL file is from different database system: incorrect XLOG_BLCKSZ in page header");
            return false;
        }
    } else if offset == 0 {
        // char		fname[MAXFNAMELEN];

        // XLogFileName(fname, state.seg.ws_tli, segno, state.segcxt.ws_segsize);

        // /* hmm, first page of file doesn't have a long header? */
        // report_invalid_record(state,
        // 					  "invalid info bits %04X in WAL segment %s, LSN %X/%X, offset %u",
        // 					  hdr.xlp_info,
        // 					  fname,
        // 					  LSN_FORMAT_ARGS(recptr),
        // 					  offset);
        return false;
    }

    /*
     * Check that the address on the page agrees with what we expected. This
     * check typically fails when an old WAL segment is recycled, and hasn't
     * yet been overwritten with new data yet.
     */
    if hdr.xlp_pageaddr != recptr {
        // char		fname[MAXFNAMELEN];

        // XLogFileName(fname, state.seg.ws_tli, segno, state.segcxt.ws_segsize);

        // report_invalid_record(state,
        // 					  "unexpected pageaddr %X/%X in WAL segment %s, LSN %X/%X, offset %u",
        // 					  LSN_FORMAT_ARGS(hdr.xlp_pageaddr),
        // 					  fname,
        // 					  LSN_FORMAT_ARGS(recptr),
        // 					  offset);
        return false;
    }

    /*
     * Since child timelines are always assigned a TLI greater than their
     * immediate parent's TLI, we should never see TLI go backwards across
     * successive pages of a consistent WAL sequence.
     *
     * Sometimes we re-read a segment that's already been (partially) read. So
     * we only verify TLIs for pages that are later than the last remembered
     * LSN.
     */
    if recptr > state.latest_page_ptr {
        if hdr.xlp_tli < state.latest_page_tli {
            // char		fname[MAXFNAMELEN];

            // XLogFileName(fname, state.seg.ws_tli, segno, state.segcxt.ws_segsize);

            // report_invalid_record(state,
            // 					  "out-of-sequence timeline ID %u (after %u) in WAL segment %s, LSN %X/%X, offset %u",
            // 					  hdr.xlp_tli,
            // 					  state.latestPageTLI,
            // 					  fname,
            // 					  LSN_FORMAT_ARGS(recptr),
            // 					  offset);
            return false;
        }
    }
    state.latest_page_ptr = recptr;
    state.latest_page_tli = hdr.xlp_tli;
    state.page_hdr_size = xlog_page_header_size(&hdr);

    return true;
}

#[cfg(test)]
mod tests {
    use super::*;

    const WAL_FILE: &[u8] = include_bytes!("../test/000000010000000000000001");

    #[test]
    fn test_page_header() {
        #[rustfmt::skip]
        let input = [
            0x10, 0xD1, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        let (_, hdr) = page_header(&input).unwrap();
        assert_eq!(hdr.xlp_magic, XLOG_PAGE_MAGIC);
        assert_eq!(hdr.xlp_info, XLP_LONG_HEADER);
        assert_eq!(hdr.xlp_tli, 1);
        assert_eq!(hdr.xlp_pageaddr, 0x01000000);
        assert_eq!(hdr.xlp_rem_len, 0);
    }

    #[test]
    fn wal_file_first_page_header() {
        let (_, hdr) = first_page_header(WAL_FILE).unwrap();
        assert_eq!(hdr.std.xlp_magic, XLOG_PAGE_MAGIC);
        assert_eq!(hdr.std.xlp_info, XLP_LONG_HEADER);
        assert_eq!(hdr.std.xlp_tli, 1);
        assert_eq!(hdr.std.xlp_pageaddr, 0x01000000);
        assert_eq!(hdr.std.xlp_rem_len, 0);
        assert_eq!(hdr.xlp_sysid, 0x657d48e8d9177b4b);
        assert_eq!(hdr.xlp_seg_size, 16 * 1024 * 1024);
        assert_eq!(hdr.xlp_xlog_blcksz, 8 * 1024);
    }

    #[test]
    fn test_xlog_record() {
        #[rustfmt::skip]
        let input = [
            0x72, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x8a, 0xff, 0xa2, 0x51,
        ];
        let (_, xlog) = xlog_record(&input).unwrap();
        assert_eq!(xlog.xl_tot_len, 114);
        assert_eq!(xlog.xl_xid, 0);
        assert_eq!(xlog.xl_prev, 0);
        assert_eq!(xlog.xl_info, 0);
        assert_eq!(xlog.xl_rmid, 0);
        assert_eq!(xlog.xl_crc, 0x51a2ff8a);
    }

    #[test]
    fn test_xlog_record_data_header_short() {
        #[rustfmt::skip]
        let input = [
            0xff, 0x00,
        ];
        let (_, hdr) = xlog_record_data_header(&input).unwrap();
        assert_eq!(
            hdr,
            XLogRecordDataHeader::Short(XLogRecordDataHeaderShort {
                id: 0xff,
                data_length: 0,
            })
        );
    }

    #[test]
    fn test_xlog_record_data_header_long() {
        #[rustfmt::skip]
        let input = [
            0xfe, 0x01, 0x02, 0x03, 0x04,
        ];
        let (_, hdr) = xlog_record_data_header(&input).unwrap();
        assert_eq!(
            hdr,
            XLogRecordDataHeader::Long(XLogRecordDataHeaderLong {
                id: 0xfe,
                data_length: 0x04030201,
            })
        );
    }

    #[test]
    fn test_xlog_record_data_header_origin() {
        #[rustfmt::skip]
        let input = [
            0xfd, 0x01, 0x02,
        ];
        let (_, hdr) = xlog_record_data_header(&input).unwrap();
        assert_eq!(
            hdr,
            XLogRecordDataHeader::Origin(XLogRecordDataHeaderOrigin {
                id: 0xfd,
                record_origin: 0x0201,
            })
        );
    }

    #[test]
    fn test_xlog_record_data_header_top_level_xid() {
        #[rustfmt::skip]
        let input = [
            0xfc, 0x01, 0x02, 0x03, 0x04,
        ];
        let (_, hdr) = xlog_record_data_header(&input).unwrap();
        assert_eq!(
            hdr,
            XLogRecordDataHeader::TopLevelXid(XLogRecordDataHeaderTopLevelXid {
                id: 0xfc,
                top_level_xid: 0x04030201,
            })
        );
    }

    #[test]
    fn test_xlog_record_block_header() {
        #[rustfmt::skip]
        let input = [
            0x01, 0x02, 0x03, 0x04,
        ];
        let (_, hdr) = xlog_record_data_header(&input).unwrap();
        assert_eq!(
            hdr,
            XLogRecordDataHeader::Block(XLogRecordBlockHeader {
                id: 0x01,
                fork_flags: 0x02,
                data_length: 0x0403,
            })
        );
    }

    #[test]
    fn wal_file_parse() {
        let (remaining, long_hdr) = first_page_header(WAL_FILE).unwrap();
        let (remaining, xlog_rrd) = xlog_record(remaining).unwrap();
        let (remaining, data_hdr) = xlog_record_data_header(remaining).unwrap();
        assert_eq!(
            data_hdr,
            XLogRecordDataHeader::Short(XLogRecordDataHeaderShort {
                id: 0xff,
                data_length: 0x58,
            })
        );
        let data_hdr = match data_hdr {
            XLogRecordDataHeader::Short(hdr) => hdr,
            _ => panic!("unexpected data header"),
        };
        assert_eq!(
            xlog_rrd.xl_tot_len as usize,
            data_hdr.data_length as usize
                + std::mem::size_of::<XLogRecord>()
                + std::mem::size_of::<XLogRecordDataHeaderShort>()
        );
        let info = xlog_rrd.xl_info & XLR_RMGR_INFO_MASK;
        assert_eq!(info, XLogInfo::CheckpointShutdown as u8);
        assert_eq!(
            data_hdr.data_length as usize,
            std::mem::size_of::<CheckPoint>()
        );

        let ckp: CheckPoint = unsafe { std::ptr::read(remaining.as_ptr() as *const CheckPoint) };
        println!("{}", ckp);
    }
}
