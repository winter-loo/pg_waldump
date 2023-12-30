#![allow(unused)]
use std::path::PathBuf;

pub(crate) type Oid = u32;
pub(crate) type TransactionId = u32;
pub(crate) type RmgrId = u8;
pub(crate) type PgCrc32c = u32;
pub(crate) type TimeLineID = u32;
pub(crate) type XLogRecPtr = u64;
pub(crate) type BlockNumber = u32;
// A 64 bit value that contains an epoch and a TransactionId
pub(crate) type FullTransactionId = u64;
pub(crate) type MultiXactId = TransactionId;
pub(crate) type MultiXactOffset = u32;
pub(crate) type PgTime = i64;
// Replication origin id - this is located in this file to avoid having to
// include origin.h in a bunch of xlog related places.
pub(crate) type RepOriginId = u16;
// physical log file sequence number.
pub(crate) type XLogSegNo = u64;

// Stuff for fork names.
//
// The physical storage of a relation consists of one or more forks.
// The main fork is always created, but in addition to that there can be
// additional forks for storing various metadata. ForkNumber is used when
// we need to refer to a specific fork in a relation.
#[derive(Clone, Copy)]
pub(crate) enum ForkNumber {
    Invalid = -1,
    Main = 0,
    Fsm,
    VisibilityMap,
    Init,
}

impl Default for ForkNumber {
    fn default() -> Self {
        ForkNumber::Invalid
    }
}

impl From<i8> for ForkNumber {
    fn from(f: i8) -> Self {
        match f {
            0 => ForkNumber::Main,
            1 => ForkNumber::Fsm,
            2 => ForkNumber::VisibilityMap,
            3 => ForkNumber::Init,
            _ => ForkNumber::Invalid,
        }
    }
}

impl From<ForkNumber> for i8 {
    fn from(f: ForkNumber) -> i8 {
        match f {
            ForkNumber::Main => 0,
            ForkNumber::Fsm => 1,
            ForkNumber::VisibilityMap => 2,
            ForkNumber::Init => 3,
            ForkNumber::Invalid => -1,
        }
    }
}

pub type RelFileNumber = Oid;

// RelFileLocator must provide all that we need to know to physically access
// a relation, with the exception of the backend ID, which can be provided
// separately. Note, however, that a "physical" relation is comprised of
// multiple files on the filesystem, as each fork is stored as a separate
// file, and each fork can be divided into multiple segments. See md.c.
//
// spcOid identifies the tablespace of the relation.  It corresponds to
// pg_tablespace.oid.
//
// dbOid identifies the database of the relation.  It is zero for
// "shared" relations (those common to all databases of a cluster).
// Nonzero dbOid values correspond to pg_database.oid.
//
// relNumber identifies the specific relation.  relNumber corresponds to
// pg_class.relfilenode (NOT pg_class.oid, because we need to be able
// to assign new physical files to relations in some situations).
// Notice that relNumber is only unique within a database in a particular
// tablespace.
//
// Note: spcOid must be GLOBALTABLESPACE_OID if and only if dbOid is
// zero.  We support shared relations only in the "global" tablespace.
//
// Note: in pg_class we allow reltablespace == 0 to denote that the
// relation is stored in its database's "default" tablespace (as
// identified by pg_database.dattablespace).  However this shorthand
// is NOT allowed in RelFileLocator structs --- the real tablespace ID
// must be supplied when setting spcOid.
//
// Note: in pg_class, relfilenode can be zero to denote that the relation
// is a "mapped" relation, whose current true filenode number is available
// from relmapper.c.  Again, this case is NOT allowed in RelFileLocators.
//
// Note: various places use RelFileLocator in hashtable keys.  Therefore,
// there *must not* be any unused padding bytes in this struct.  That
// should be safe as long as all the fields are of type Oid.
#[repr(align(1))]
#[derive(Default, Clone)]
pub struct RelFileLocator {
    pub spc_oid: Oid, // tablespace
    pub db_oid: Oid,  // database
    pub rel_oid: Oid, // relation
}

#[derive(Default, Clone)]
pub(crate) struct DecodedBkpBlock {
    // Is this block ref in use?
    pub in_use: bool,

    // Identify the block this refers to
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub blkno: BlockNumber,

    // Prefetching workspace.
    // prefetch_buffer: Buffer,

    // copy of the fork_flags field from the XLogRecordBlockHeader
    pub flags: u8,

    // Information on full-page image, if any
    pub has_image: bool,   // has image, even for consistency checking
    pub apply_image: bool, // has image that should be restored
    pub bkp_image: Vec<u8>,
    pub hole_offset: u16,
    pub hole_length: u16,
    pub bimg_len: u16,
    pub bimg_info: u8,

    // Buffer holding the rmgr-specific data associated with this block
    pub has_data: bool,
    pub data: Vec<u8>,
    pub data_len: u16,
    pub data_bufsz: u16,
}

// WALOpenSegment represents a WAL segment being read.
#[derive(Default, Debug)]
pub(crate) struct WALOpenSegment {
    pub file: Option<std::fs::File>, // segment file descriptor
    pub segno: XLogSegNo,            // segment number
    pub tli: TimeLineID,             // timeline ID of the currently open file
}

impl Clone for WALOpenSegment {
    fn clone(&self) -> Self {
        WALOpenSegment {
            file: None,
            segno: self.segno,
            tli: self.tli,
        }
    }
}

impl std::fmt::Display for WALOpenSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "segno: {}, tli: {}", self.segno, self.tli)
    }
}

// WALSegmentContext carries context information about WAL segments to read
#[derive(Default)]
pub(crate) struct WALSegmentContext {
    pub ws_dir: PathBuf,
    pub ws_segsize: u32,
}

#[derive(Debug, Default, Clone)]
#[repr(align(8))]
pub(crate) struct XLogRecord {
    // total len of entire record
    pub xl_tot_len: u32,
    // xact id
    pub xl_xid: TransactionId,
    // ptr to previous record in log
    pub xl_prev: XLogRecPtr,
    // flag bits, see below
    pub xl_info: u8,
    // resource manager for this record
    pub xl_rmid: RmgrId,
    // CRC for this record
    pub xl_crc: PgCrc32c,
    // XLogRecordBlockHeaders and XLogRecordDataHeader follow, no padding
}

#[derive(Default, Clone)]
pub(crate) struct XLogDumpPrivate {
    pub timeline: TimeLineID,
    pub startptr: XLogRecPtr,
    pub endptr: XLogRecPtr,
    pub endptr_reached: bool,
}

#[derive(Default)]
pub(crate) struct DecodedXLogRecord {
    // Private member used for resource management.
    pub size: usize,     // total size of decoded record
    pub oversized: bool, // outside the regular decode buffer?

    // Public members.
    pub lsn: XLogRecPtr,      // location
    pub next_lsn: XLogRecPtr, // location of next record
    pub header: XLogRecord,   // header
    pub record_origin: RepOriginId,
    pub toplevel_xid: TransactionId, // XID of top-level transaction
    pub main_data: Vec<u8>,          // record's main data portion
    pub main_data_len: u32,
    pub max_block_id: i8, // highest block_id in use (-1 if none)
    pub blocks: Vec<DecodedBkpBlock>,
}

pub(crate) const INVALID_XLOG_RECPTR: XLogRecPtr = 0;

#[inline]
pub(crate) fn xlog_recptr_is_invalid(r: XLogRecPtr) -> bool {
    r == INVALID_XLOG_RECPTR
}

#[repr(C)]
#[derive(Default)]
pub struct CheckPoint {
    // next RecPtr available when we began to
    // create CheckPoint (i.e. REDO start point)
    pub redo: XLogRecPtr,
    // current TLI
    pub time_line_id: TimeLineID,
    // previous TLI, if this record begins a new
    // timeline (equals ThisTimeLineID otherwise)
    pub prev_time_line_id: TimeLineID,
    // current full_page_writes
    pub full_page_writes: bool,
    // next free transaction ID
    pub next_xid: FullTransactionId,
    // next free OID
    pub next_oid: Oid,
    // next free MultiXactId
    pub next_multi: MultiXactId,
    // next free MultiXact offset
    pub next_multi_offset: MultiXactOffset,
    // cluster-wide minimum datfrozenxid
    pub oldest_xid: TransactionId,
    // database with minimum datfrozenxid
    pub oldest_xid_db: Oid,
    // cluster-wide minimum datminmxid
    pub oldest_multi: MultiXactId,
    // database with minimum datminmxid
    // time stamp of checkpoint
    pub oldest_multi_db: Oid,
    pub time: PgTime,
    // oldest Xid with valid commit timestamp
    pub oldest_commit_ts_xid: TransactionId,
    // newest Xid with valid commit timestamp
    pub newest_commit_ts_xid: TransactionId,

    // Oldest XID still running. This is only needed to initialize hot standby
    // mode from an online checkpoint, so we only bother calculating this for
    // online checkpoints and only when wal_level is replica. Otherwise it's
    // set to InvalidTransactionId.
    pub oldest_active_xid: TransactionId,
}

impl std::fmt::Display for CheckPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::new();

        s.push_str(&format!(
            "redo {:X}/{:X}; ",
            (self.redo >> 32) as u32,
            self.redo as u32
        ));
        s.push_str(&format!("tli {}; ", self.time_line_id));
        s.push_str(&format!("prev tli {}; ", self.prev_time_line_id));
        s.push_str(&format!("fpw {}; ", self.full_page_writes));
        s.push_str(&format!(
            "xid {}:{}; ",
            (self.next_xid >> 32) as u32,
            self.next_xid as u32
        ));
        s.push_str(&format!("oid {}; ", self.next_oid));
        s.push_str(&format!("multi {}; ", self.next_multi));
        s.push_str(&format!("offset {}; ", self.next_multi_offset));
        s.push_str(&format!(
            "oldest xid {} in DB {}; ",
            self.oldest_xid, self.oldest_xid_db
        ));
        s.push_str(&format!(
            "oldest multi {} in DB {}; ",
            self.oldest_multi, self.oldest_multi_db
        ));
        // s.push_str(&format!("time: {};", self.time));
        s.push_str(&format!(
            "oldest/newest commit timestamp xid {}/{}; ",
            self.oldest_commit_ts_xid, self.newest_commit_ts_xid
        ));
        s.push_str(&format!("oldest running xid {}; ", self.oldest_active_xid));

        write!(f, "{}", s)
    }
}
