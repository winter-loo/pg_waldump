use super::utils::*;
use crate::constant::*;
use crate::pgtypes::*;
use crate::state::*;
use enumname_derive::EnumName;

// WAL record definitions for heapam.c's WAL operations
//
// XLOG allows to store some information in high 4 bits of log
// record xl_info field.  We use 3 for opcode and one for init bit.
const XLOG_HEAP_INSERT: u8 = 0x00;
const XLOG_HEAP_DELETE: u8 = 0x10;
const XLOG_HEAP_UPDATE: u8 = 0x20;
const XLOG_HEAP_TRUNCATE: u8 = 0x30;
const XLOG_HEAP_HOT_UPDATE: u8 = 0x40;
const XLOG_HEAP_CONFIRM: u8 = 0x50;
const XLOG_HEAP_LOCK: u8 = 0x60;
const XLOG_HEAP_INPLACE: u8 = 0x70;

const XLOG_HEAP_OPMASK: u8 = 0x70;

// When we insert 1st item on new page in INSERT, UPDATE, HOT_UPDATE,
// or MULTI_INSERT, we can (and we do) restore entire page in redo
const XLOG_HEAP_INIT_PAGE: u8 = 0x80;

#[repr(C)]
struct XLogHeapInsert {
    //  inserted tuple's offset
    offnum: OffsetNumber,
    flags: u8,
}

#[repr(C)]
struct XLogHeapDelete {
    //  xmax of the deleted tuple
    xmax: TransactionId,
    //  deleted tuple's offset
    offnum: OffsetNumber,
    //  infomask bits
    infobits_set: u8,
    flags: u8,
}

#[repr(C)]
struct XLogHeapUpdate {
    //  xmax of the old tuple
    old_xmax: TransactionId,
    //  old tuple's offset
    old_offnum: OffsetNumber,
    //  infomask bits to set on old tuple
    old_infobits_set: u8,
    flags: u8,
    //  xmax of the new tuple
    new_xmax: TransactionId,
    //  new tuple's offset
    new_offnum: OffsetNumber,
    // If XLH_UPDATE_CONTAINS_OLD_TUPLE or XLH_UPDATE_CONTAINS_OLD_KEY flags
    // are set, xl_heap_header and tuple data for the old tuple follow.
}

#[repr(C)]
struct XLogHeapTruncate {
    db_id: Oid,
    nrelids: u32,
    flags: u8,
    // relids: [Oid; FLEXIBLE_ARRAY_MEMBER];
}

impl XLogHeapTruncate {
    fn relids(&self) -> Vec<Oid> {
        vec![0; self.nrelids as usize]
    }
}

#[repr(C)]
struct XLogHeapConfirm {
    //  confirmed tuple's offset on page
    offnum: OffsetNumber,
}

#[repr(C)]
struct XLogHeapLock {
    //  might be a MultiXactId
    xmax: TransactionId,
    //  locked tuple's offset on page
    offnum: OffsetNumber,
    //  infomask and infomask2 bits to set
    infobits_set: u8,
    //  XLH_LOCK_* flag bits
    flags: u8,
}

#[repr(C)]
struct XLogHeapInplace {
    //  updated tuple's offset on page
    offnum: OffsetNumber,
}

// flags for infobits_set
const XLHL_XMAX_IS_MULTI: u8 = 0x01;
const XLHL_XMAX_LOCK_ONLY: u8 = 0x02;
const XLHL_XMAX_EXCL_LOCK: u8 = 0x04;
const XLHL_XMAX_KEYSHR_LOCK: u8 = 0x08;
const XLHL_KEYS_UPDATED: u8 = 0x10;

// flag bits for xl_heap_lock / xl_heap_lock_updated's flag field
const XLH_LOCK_ALL_FROZEN_CLEARED: u8 = 0x01;

// xl_heap_truncate flag values, 8 bits are available.
const XLH_TRUNCATE_CASCADE: u8 = (1 << 0);
const XLH_TRUNCATE_RESTART_SEQS: u8 = (1 << 1);

fn infobits_desc(infobits: u8, keyname: &str) -> String {
    let mut buf = String::new();
    buf += &format!("{}: [", keyname);

    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        buf.push_str("IS_MULTI, ");
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        buf.push_str("LOCK_ONLY, ");
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        buf.push_str("EXCL_LOCK, ");
    }
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        buf.push_str("KEYSHR_LOCK, ");
    }
    if infobits & XLHL_KEYS_UPDATED != 0 {
        buf.push_str("KEYS_UPDATED, ");
    }

    if buf.as_bytes()[(buf.len() - 1) as usize] == b' ' {
        // Truncate-away final unneeded ", "
        assert!(buf.as_bytes()[(buf.len() - 2) as usize] == b',');
        buf.pop();
        buf.pop();
    }

    buf.push(']');
    buf
}

fn truncate_flags_desc(flags: u8) -> String {
    let mut buf = String::new();
    buf.push_str("flags: [");

    if flags & XLH_TRUNCATE_CASCADE != 0 {
        buf.push_str("CASCADE, ");
    }
    if flags & XLH_TRUNCATE_RESTART_SEQS != 0 {
        buf.push_str("RESTART_SEQS, ");
    }

    if (buf.as_bytes()[buf.len() - 1] == b' ') {
        // Truncate-away final unneeded ", "
        assert!(buf.as_bytes()[buf.len() - 2] == b',');
        buf.pop();
        buf.pop();
    }

    buf.push(']');
    buf
}

pub fn heap_desc(state: &XLogReaderState) -> String {
    let record = state.record.as_ref().unwrap();
    if record.main_data.is_none() {
        return String::new();
    }
    let main_data = record.main_data.as_ref().unwrap();
    let rec = main_data.as_slice();
    let info = record.header.xl_info & !XLR_INFO_MASK;
    let info = info & XLOG_HEAP_OPMASK;
    // println!("\n---- main data: {:02X?}", rec);

    let mut rst = String::new();

    match info {
        XLOG_HEAP_INSERT => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapInsert) };

            rst += &format!("off: {}, flags: 0x{:02X}", xlrec.offnum, xlrec.flags);
        }

        XLOG_HEAP_DELETE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapDelete) };

            rst += &format!("xmax: {}, off: {}, ", xlrec.xmax, xlrec.offnum);
            rst += &infobits_desc(xlrec.infobits_set, "infobits");
            rst += &format!(", flags: 0x{:02X}", xlrec.flags);
        }

        XLOG_HEAP_UPDATE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapUpdate) };

            rst += &format!(
                "old_xmax: {}, old_off: {}, ",
                xlrec.old_xmax, xlrec.old_offnum,
            );
            rst += &infobits_desc(xlrec.old_infobits_set, "old_infobits");
            rst += &format!(
                ", flags: 0x{:02X}, new_xmax: {}, new_off: {}",
                xlrec.flags, xlrec.new_xmax, xlrec.new_offnum,
            );
        }

        XLOG_HEAP_HOT_UPDATE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapUpdate) };

            rst += &format!(
                "old_xmax: {}, old_off: {}, ",
                xlrec.old_xmax, xlrec.old_offnum,
            );
            rst += &infobits_desc(xlrec.old_infobits_set, "old_infobits");
            rst += &format!(
                ", flags: 0x{:02X}, new_xmax: {}, new_off: {}",
                xlrec.flags, xlrec.new_xmax, xlrec.new_offnum,
            );
        }

        XLOG_HEAP_TRUNCATE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapTruncate) };

            rst += &truncate_flags_desc(xlrec.flags);
            rst += &format!(", nrelids: {}", xlrec.nrelids);
            rst += ", relids:";
            let mut relids = xlrec.relids();
            unsafe {
                rec.as_ptr()
                    .add(std::mem::size_of::<XLogHeapTruncate>())
                    .copy_to_nonoverlapping(
                        relids.as_mut_ptr() as *mut u8,
                        std::mem::size_of::<Oid>() * relids.len(),
                    );
            }
            rst += &array_desc(relids.as_slice(), oid_elem_desc);
        }

        XLOG_HEAP_CONFIRM => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapConfirm) };

            rst += &format!("off: {}", xlrec.offnum);
        }

        XLOG_HEAP_LOCK => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapLock) };

            rst += &format!("xmax: {}, off: {}, ", xlrec.xmax, xlrec.offnum);
            rst += &infobits_desc(xlrec.infobits_set, "infobits");
            rst += &format!(", flags: 0x{:02X}", xlrec.flags);
        }

        XLOG_HEAP_INPLACE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogHeapInplace) };

            rst += &format!("off: {}", xlrec.offnum);
        }

        _ => panic!("unknown heap info {}", info),
    }

    rst
}

pub fn heap_identify(info: u8) -> String {
    let info2 = info & !XLR_INFO_MASK;

    let infostr = match info2 {
        XLOG_HEAP_INSERT => "INSERT",
        n if n == (XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE) => "INSERT+INIT",
        XLOG_HEAP_DELETE => "DELETE",
        XLOG_HEAP_UPDATE => "UPDATE",
        n if n == (XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE) => "UPDATE+INIT",
        XLOG_HEAP_HOT_UPDATE => "HOT_UPDATE",
        n if n == (XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE) => "HOT_UPDATE+INIT",
        XLOG_HEAP_TRUNCATE => "TRUNCATE",
        XLOG_HEAP_CONFIRM => "HEAP_CONFIRM",
        XLOG_HEAP_LOCK => "LOCK",
        XLOG_HEAP_INPLACE => "INPLACE",
        _ => panic!("unknown heap info: {}", info),
    };
    String::from(infostr)
}
