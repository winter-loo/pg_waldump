#![allow(unused)]

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
