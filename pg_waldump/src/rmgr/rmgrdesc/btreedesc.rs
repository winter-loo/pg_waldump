use super::utils::*;
use crate::constant::*;
use crate::pgtypes::*;
use crate::state::*;
use crate::util;
use std::mem;
use std::cell::RefCell;

#[repr(C)]
struct XLogBtreeInsert {
    offnum: OffsetNumber,
    //  POSTING SPLIT OFFSET FOLLOWS (INSERT_POST case)
    //  NEW TUPLE ALWAYS FOLLOWS AT THE END
}

#[inline]
fn size_of_btree_insert() -> usize {
    // (mem::offset_of!(XLogBtreeInsert, offnum) + mem::size_of::<OffsetNumber>())
    mem::size_of::<XLogBtreeInsert>()
}

#[repr(C)]
struct XLogBtreeSplit {
    level: u32,                  //  tree level of page being split
    firstrightoff: OffsetNumber, //  first origpage item on rightpage
    newitemoff: OffsetNumber,    //  new item's offset
    postingoff: u16,             //  offset inside orig posting tuple
}

#[inline]
fn size_of_btree_split() -> usize {
    // (mem::offset_of!(XLogBtreeSplit, postingoff) + mem::size_of::<u16>())
    mem::size_of::<XLogBtreeSplit>()
}

#[repr(C)]
struct XLogBtreeDedup {
    nintervals: u16,
    //  DEDUPLICATION INTERVALS FOLLOW
}

#[inline]
fn size_of_btree_dedup() -> usize {
    // (mem::offset_of!(XLogBtreeDedup, nintervals) + mem::size_of::<u16>())
    mem::size_of::<XLogBtreeDedup>()
}

#[repr(C)]
struct XLogBtreeReusePage {
    locator: RelFileLocator,
    block: BlockNumber,
    snapshot_conflict_horizon: FullTransactionId,
    // to handle recovery conflict during logical
    // decoding on standby
    is_catalog_rel: bool,
}

#[inline]
fn size_of_btree_reuse_page() -> usize {
    // (mem::offset_of!(XLogBtreeReusePage, is_catalog_rel) + mem::size_of::<bool>())
    mem::size_of::<XLogBtreeReusePage>()
}

#[repr(C)]
struct XLogBtreeVacuum {
    ndeleted: u16,
    nupdated: u16,
    // In payload of blk 0 :
    // - DELETED TARGET OFFSET NUMBERS
    // - UPDATED TARGET OFFSET NUMBERS
    // - UPDATED TUPLES METADATA (xl_btree_update) ITEMS
}

#[inline]
fn size_of_btree_vacuum() -> usize {
    // (mem::offset_of!(XLogBtreeVacuum, nupdated) + mem::size_of::<u16>())
    mem::size_of::<XLogBtreeVacuum>()
}

#[repr(C)]
struct XLogBtreeDelete {
    snapshot_conflict_horizon: TransactionId,
    ndeleted: u16,
    nupdated: u16,
    // to handle recovery conflict during logical
    // decoding on standby
    is_catalog_rel: bool,
    // In payload of blk 0 :
    // - DELETED TARGET OFFSET NUMBERS
    // - UPDATED TARGET OFFSET NUMBERS
    // - UPDATED TUPLES METADATA (xl_btree_update) ITEMS
}

#[inline]
fn size_of_btree_delete() -> usize {
    // (mem::offset_of!(XLogBtreeDelete, isCatalogRel) + mem::size_of::<bool>())
    mem::size_of::<XLogBtreeDelete>()
}

struct XLogBtreeUpdate {
    ndeletedtids: u16,
    // POSTING LIST uint16 OFFSETS TO A DELETED TID FOLLOW
}

const SIZE_OF_BTREE_UPDATE: usize = 2;

#[repr(C)]
struct XLogBtreeMarkPageHalfDead {
    poffset: OffsetNumber, //  deleted tuple id in parent page

    //  information needed to recreate the leaf page:
    leafblk: BlockNumber,   //  leaf block ultimately being deleted
    leftblk: BlockNumber,   //  leaf block's left sibling, if any
    rightblk: BlockNumber,  //  leaf block's right sibling
    topparent: BlockNumber, //  topmost internal page in the subtree
}

#[inline]
fn size_of_btree_mark_page_half_dead() -> usize {
    // (mem::offset_of!(XLogBtreeMarkPageHalfDead, topparent) + mem::size_of::<BlockNumber>())
    mem::size_of::<XLogBtreeMarkPageHalfDead>()
}

#[repr(C)]
struct XLogBtreeUnlinkPage {
    leftsib: BlockNumber,       //  target block's left sibling, if any
    rightsib: BlockNumber,      //  target block's right sibling
    level: u32,                 //  target block's level
    safexid: FullTransactionId, //  target block's BTPageSetDeleted() XID

    // Information needed to recreate a half-dead leaf page with correct
    // topparent link.  The fields are only used when deletion operation's
    // target page is an internal page.  REDO routine creates half-dead page
    // from scratch to keep things simple (this is the same convenient
    // approach used for the target page itself).
    leafleftsib: BlockNumber,
    leafrightsib: BlockNumber,
    leaftopparent: BlockNumber, //  next child down in the subtree

                                //  xl_btree_metadata FOLLOWS IF XLOG_BTREE_UNLINK_PAGE_META
}

#[inline]
fn size_of_btree_unlink_page() -> usize {
    // (mem::offset_of!(XLogBtreeUnlinkPage, leaftopparent) + mem::size_of::<BlockNumber>())
    mem::size_of::<XLogBtreeUnlinkPage>()
}

#[repr(C)]
struct XLogBtreeNewroot {
    rootblk: BlockNumber, //  location of new root (redundant with blk 0)
    level: u32,           //  its tree level
}

#[inline]
fn size_of_btree_newroot() -> usize {
    // (mem::offset_of!(XLogBtreeNewroot, level) + mem::size_of::<u32>())
    mem::size_of::<XLogBtreeNewroot>()
}

#[repr(C)]
struct XLogBtreeMetadata {
    version: u32,
    root: BlockNumber,
    level: u32,
    fastroot: BlockNumber,
    fastlevel: u32,
    last_cleanup_num_delpages: u32,
    allequalimage: bool,
}

// XLOG records for btree operations
//
// XLOG allows to store some information in high 4 bits of log
// record xl_info field
//
//  add index tuple without split
const XLOG_BTREE_INSERT_LEAF: u8 = 0x00;
//  same, on a non-leaf page
const XLOG_BTREE_INSERT_UPPER: u8 = 0x10;
//  same, plus update metapage
const XLOG_BTREE_INSERT_META: u8 = 0x20;
//  add index tuple with split
const XLOG_BTREE_SPLIT_L: u8 = 0x30;
//  as above, new item on right
const XLOG_BTREE_SPLIT_R: u8 = 0x40;
//  add index tuple with posting split
const XLOG_BTREE_INSERT_POST: u8 = 0x50;
//  deduplicate tuples for a page
const XLOG_BTREE_DEDUP: u8 = 0x60;
//  delete leaf index tuples for a page
const XLOG_BTREE_DELETE: u8 = 0x70;
//  delete a half-dead page
const XLOG_BTREE_UNLINK_PAGE: u8 = 0x80;
//  same, and update metapage
const XLOG_BTREE_UNLINK_PAGE_META: u8 = 0x90;
//  new root page
const XLOG_BTREE_NEWROOT: u8 = 0xA0;
//  mark a leaf as half-dead
const XLOG_BTREE_MARK_PAGE_HALFDEAD: u8 = 0xB0;
// delete entries on a page during vacuum
const XLOG_BTREE_VACUUM: u8 = 0xC0;
// old page is about to be reused from FSM
const XLOG_BTREE_REUSE_PAGE: u8 = 0xD0;
// update cleanup-related data in the metapage
const XLOG_BTREE_META_CLEANUP: u8 = 0xE0;

#[inline]
fn xlog_rec_has_block_data(decoder: &XLogReaderState, block_id: u8) -> bool {
    let record = decoder.record.as_ref().unwrap();
    let blocks = record.blocks.as_ref().unwrap();
    blocks[block_id as usize].borrow().has_data
}

fn delvacuum_desc(block_data: &[u8], ndeleted: u16, nupdated: u16) -> String {
    let mut rst = String::new();
    // Output deleted page offset number array
    rst.push_str(", deleted:");
    let deletedoffsets = vec![OffsetNumber::default(); ndeleted as usize];
    unsafe {
        std::ptr::copy_nonoverlapping(
            block_data.as_ptr(),
            deletedoffsets.as_ptr() as *mut u8,
            ndeleted as usize * mem::size_of::<OffsetNumber>(),
        );
    }
    rst += &array_desc(deletedoffsets.as_slice(), offset_elem_desc);

    // Output updates as an array of "update objects", where each element
    // contains a page offset number from updated array.  (This is not the
    // most literal representation of the underlying physical data structure
    // that we could use.  Readability seems more important here.)
    rst.push_str(", updated: [");
    let updatedoffsets = unsafe {
        std::slice::from_raw_parts(
            block_data
                .as_ptr()
                .add(ndeleted as usize * mem::size_of::<OffsetNumber>())
                as *const OffsetNumber,
            nupdated as usize,
        )
    };
    let mut updates = unsafe {
        &*((updatedoffsets.as_ptr() as *const u8)
            .add((nupdated as usize) * mem::size_of::<OffsetNumber>())
            as *mut XLogBtreeUpdate)
    };
    for i in 0..nupdated as usize {
        let off = updatedoffsets[i];

        assert!(offset_number_is_valid(off));
        assert!(updates.ndeletedtids > 0);

        // "ptid" is the symbol name used when building each xl_btree_update's
        // array of offsets into a posting list tuple's ItemPointerData array.
        // xl_btree_update describes a subset of the existing TIDs to delete.
        rst += &format!(
            "{{ off: {}, nptids: {}, ptids: [",
            off, updates.ndeletedtids
        );
        for p in 0..updates.ndeletedtids {
            let ptid = unsafe {
                *((updates as *const XLogBtreeUpdate as *const u8)
                    .add(SIZE_OF_BTREE_UPDATE) as *const u16).add(p as usize)
            };
            rst += &format!("{}", ptid);

            if (p < updates.ndeletedtids - 1) {
                rst.push_str(", ");
            }
        }
        rst.push_str("] }");
        if i < nupdated as usize - 1 {
            rst.push_str(", ");
        }

        updates = unsafe {
            &*((updates as *const XLogBtreeUpdate as *const u8)
                .add(SIZE_OF_BTREE_UPDATE + updates.ndeletedtids as usize * mem::size_of::<u16>())
                as *mut XLogBtreeUpdate)
        };
    }
    rst.push(']');
    rst
}

// Returns the data associated with a block reference, or NULL if there is
// no data (e.g. because a full-page image was taken instead). The returned
// pointer points to a MAXALIGNed buffer.
fn xlog_rec_get_block_data(state: &XLogReaderState, block_id: u8) -> Option<&RefCell<DecodedBkpBlock>> {
    let record = state.record.as_ref().unwrap();
    let blocks = record.blocks.as_ref().unwrap();
    let bkpb = blocks[block_id as usize].borrow();

    if block_id > record.max_block_id as u8 || !bkpb.in_use || !bkpb.has_data {
        None
    } else {
        // Some(bkpb.data.as_slice())
        Some(&blocks[block_id as usize])
    }
}

pub fn btree_desc(state: &XLogReaderState) -> String {
    let record = state.record.as_ref().unwrap();
    if record.main_data.is_none() {
        return String::new();
    }
    let main_data = record.main_data.as_ref().unwrap();
    let rec = main_data.as_slice();
    let info = record.header.xl_info & !XLR_INFO_MASK;
    let mut rst = String::new();

    match info {
        XLOG_BTREE_INSERT_LEAF
        | XLOG_BTREE_INSERT_UPPER
        | XLOG_BTREE_INSERT_META
        | XLOG_BTREE_INSERT_POST => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeInsert) };

            rst += &format!("off: {}", xlrec.offnum);
        }
        XLOG_BTREE_SPLIT_L | XLOG_BTREE_SPLIT_R => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeSplit) };

            rst += &format!(
                "level: {}, firstrightoff: {}, newitemoff: {}, postingoff: {}",
                xlrec.level, xlrec.firstrightoff, xlrec.newitemoff, xlrec.postingoff
            );
        }
        XLOG_BTREE_DEDUP => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeDedup) };

            rst += &format!("nintervals: {}", xlrec.nintervals);
        }
        XLOG_BTREE_VACUUM => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeVacuum) };

            rst += &format!("ndeleted: {}, nupdated: {}", xlrec.ndeleted, xlrec.nupdated);

            if xlog_rec_has_block_data(state, 0) {
                if let Some(data) = xlog_rec_get_block_data(state, 0) {
                    let data = data.borrow();
                    let data = data.data.as_slice();
                    rst += &delvacuum_desc(data, xlrec.ndeleted, xlrec.nupdated);
                }
            }
        }
        XLOG_BTREE_DELETE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeDelete) };

            rst += &format!(
                "snapshotConflictHorizon: {}, ndeleted: {}, nupdated: {}, isCatalogRel: {}",
                xlrec.snapshot_conflict_horizon,
                xlrec.ndeleted,
                xlrec.nupdated,
                if xlrec.is_catalog_rel { 'T' } else { 'F' }
            );

            if xlog_rec_has_block_data(state, 0) {
                if let Some(data) = xlog_rec_get_block_data(state, 0) {
                    let data = data.borrow();
                    let data = data.data.as_slice();
                    rst += &delvacuum_desc(data, xlrec.ndeleted, xlrec.nupdated);
                }
            }
        }
        XLOG_BTREE_MARK_PAGE_HALFDEAD => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeMarkPageHalfDead) };

            rst += &format!(
                "topparent: {}, leaf: {}, left: {}, right: {}",
                xlrec.topparent, xlrec.leafblk, xlrec.leftblk, xlrec.rightblk
            );
        }
        XLOG_BTREE_UNLINK_PAGE_META | XLOG_BTREE_UNLINK_PAGE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeUnlinkPage) };

            rst += &format!(
                "left: {}, right: {}, level: {}, safexid: {}:{}, ",
                xlrec.leftsib,
                xlrec.rightsib,
                xlrec.level,
                util::epoch_from_full_transaction_id(xlrec.safexid),
                util::xid_from_full_transaction_id(xlrec.safexid)
            );
            rst += &format!(
                "leafleft: {}, leafright: {}, leaftopparent: {}",
                xlrec.leafleftsib, xlrec.leafrightsib, xlrec.leaftopparent
            );
        }
        XLOG_BTREE_NEWROOT => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeNewroot) };

            rst += &format!("level: {}", xlrec.level);
        }
        XLOG_BTREE_REUSE_PAGE => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XLogBtreeReusePage) };

            rst += &format!(
                "rel: {}/{}/{}, snapshotConflictHorizon: {}:{}, isCatalogRel: {}",
                xlrec.locator.spc_oid,
                xlrec.locator.db_oid,
                xlrec.locator.rel_oid,
                util::epoch_from_full_transaction_id(xlrec.snapshot_conflict_horizon),
                util::xid_from_full_transaction_id(xlrec.snapshot_conflict_horizon),
                if xlrec.is_catalog_rel { 'T' } else { 'F' }
            );
        }
        XLOG_BTREE_META_CLEANUP => {
            if let Some(data) = xlog_rec_get_block_data(state, 0) {
                let xlrec = unsafe { &*(data.as_ptr() as *const XLogBtreeMetadata) };
                rst += &format!(
                    "last_cleanup_num_delpages: {}",
                    xlrec.last_cleanup_num_delpages
                );
            }
        }
        _ => panic!("invalid info for btree: {}", info),
    }
    rst
}

pub fn btree_identify(info: u8) -> String {
    let info2 = info & !XLR_INFO_MASK;

    match info2 {
        XLOG_BTREE_INSERT_LEAF => "INSERT_LEAF",
        XLOG_BTREE_INSERT_UPPER => "INSERT_UPPER",
        XLOG_BTREE_INSERT_META => "INSERT_META",
        XLOG_BTREE_SPLIT_L => "SPLIT_L",
        XLOG_BTREE_SPLIT_R => "SPLIT_R",
        XLOG_BTREE_INSERT_POST => "INSERT_POST",
        XLOG_BTREE_DEDUP => "DEDUP",
        XLOG_BTREE_VACUUM => "VACUUM",
        XLOG_BTREE_DELETE => "DELETE",
        XLOG_BTREE_MARK_PAGE_HALFDEAD => "MARK_PAGE_HALFDEAD",
        XLOG_BTREE_UNLINK_PAGE => "UNLINK_PAGE",
        XLOG_BTREE_UNLINK_PAGE_META => "UNLINK_PAGE_META",
        XLOG_BTREE_NEWROOT => "NEWROOT",
        XLOG_BTREE_REUSE_PAGE => "REUSE_PAGE",
        XLOG_BTREE_META_CLEANUP => "META_CLEANUP",
        _ => panic!("Unknown btree info {}", info),
    }
    .to_string()
}
