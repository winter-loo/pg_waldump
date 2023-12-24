use crate::pgtypes::*;
use crate::state::*;

#[repr(u8)]
pub(crate) enum RmgrIds {
    XLOG,
    XACT,
    SMGR,
    CLOG,
    DBASE,
    TBLSPC,
    MULTIXACT,
    RELMAP,
    STANDBY,
    HEAP2,
    HEAP,
    BTREE,
    HASH,
    GIN,
    GIST,
    SEQ,
    SPGIST,
    BRIN,
    COMMITTS,
    REPLORIGIN,
    GENERIC,
    LOGICALMSG,
    MAX,
}

const RM_MIN_CUSTOM_ID: u8 = 128;
const RM_MAX_CUSTOM_ID: u8 = u8::MAX;

pub fn rmgr_id_is_builtin(rmid: u8) -> bool {
    rmid <= RmgrIds::MAX as u8
}

pub fn rmgr_id_is_custom(rmid: u8) -> bool {
    rmid >= RM_MIN_CUSTOM_ID && rmid <= RM_MAX_CUSTOM_ID
}

pub fn rmgr_id_is_valid(rmid: u8) -> bool {
    rmgr_id_is_builtin(rmid) || rmgr_id_is_custom(rmid)
}

type RmDesc = fn(buf: String, state: &XLogReaderState);
type RmIdentify = fn(info: u8) -> String;

#[derive(Debug)]
pub struct RmgrDescData {
    pub rm_name: &'static str,
    pub rm_desc: RmDesc,
    pub rm_identify: RmIdentify,
}

macro_rules! pg_rmgr {
    ($symname:ident, $name:literal, $redo:ident, $desc:ident, $identify:ident, $startup:ident, $cleanup:ident, $mask:ident, $decode:ident) => {
        RmgrDescData {
            rm_name: $name,
            rm_desc: $desc,
            rm_identify: $identify,
        }
    };
}

fn xlog_desc(buf: String, state: &XLogReaderState) {}

fn xlog_identify(info: u8) -> String {
    String::from("xlog")
}

fn xact_desc(buf: String, state: &XLogReaderState) {}

fn xact_identify(info: u8) -> String {
    String::from("")
}

fn smgr_desc(buf: String, state: &XLogReaderState) {}

fn smgr_identify(info: u8) -> String {
    String::from("")
}

fn clog_desc(buf: String, state: &XLogReaderState) {}

fn clog_identify(info: u8) -> String {
    String::from("")
}

fn dbase_desc(buf: String, state: &XLogReaderState) {}

fn dbase_identify(info: u8) -> String {
    String::from("")
}

fn tblspc_desc(buf: String, state: &XLogReaderState) {}

fn tblspc_identify(info: u8) -> String {
    String::from("")
}

fn multixact_desc(buf: String, state: &XLogReaderState) {}

fn multixact_identify(info: u8) -> String {
    String::from("")
}

fn relmap_desc(buf: String, state: &XLogReaderState) {}

fn relmap_identify(info: u8) -> String {
    String::from("")
}

fn standby_desc(buf: String, state: &XLogReaderState) {}

fn standby_identify(info: u8) -> String {
    String::from("")
}

fn heap2_desc(buf: String, state: &XLogReaderState) {}

fn heap2_identify(info: u8) -> String {
    String::from("")
}

fn heap_desc(buf: String, state: &XLogReaderState) {}

fn heap_identify(info: u8) -> String {
    String::from("")
}

fn btree_desc(buf: String, state: &XLogReaderState) {}

fn btree_identify(info: u8) -> String {
    String::from("")
}

fn hash_desc(buf: String, state: &XLogReaderState) {}

fn hash_identify(info: u8) -> String {
    String::from("")
}

fn gin_desc(buf: String, state: &XLogReaderState) {}

fn gin_identify(info: u8) -> String {
    String::from("")
}

fn gist_desc(buf: String, state: &XLogReaderState) {}

fn gist_identify(info: u8) -> String {
    String::from("")
}

fn seq_desc(buf: String, state: &XLogReaderState) {}

fn seq_identify(info: u8) -> String {
    String::from("")
}

fn spg_desc(buf: String, state: &XLogReaderState) {}

fn spg_identify(info: u8) -> String {
    String::from("")
}

fn brin_desc(buf: String, state: &XLogReaderState) {}

fn brin_identify(info: u8) -> String {
    String::from("")
}

fn commit_ts_desc(buf: String, state: &XLogReaderState) {}

fn commit_ts_identify(info: u8) -> String {
    String::from("")
}

fn replorigin_desc(buf: String, state: &XLogReaderState) {}

fn replorigin_identify(info: u8) -> String {
    String::from("")
}

fn generic_desc(buf: String, state: &XLogReaderState) {}

fn generic_identify(info: u8) -> String {
    String::from("")
}

fn logicalmsg_desc(buf: String, state: &XLogReaderState) {}

fn logicalmsg_identify(info: u8) -> String {
    String::from("")
}

pub const RMGR_DESC_TABLE: [RmgrDescData; RmgrIds::MAX as usize] = include!("./rmgrlist.h");

fn get_rmgr_desc(rmid: RmgrId) -> &'static RmgrDescData {
    assert!(rmgr_id_is_valid(rmid));

    if rmgr_id_is_builtin(rmid) {
        return &RMGR_DESC_TABLE[rmid as usize];
    } else {
        todo!("cutom rmgr decoding is not supported yet");
        // if (!CustomRmgrDescInitialized)
        // 	initialize_custom_rmgrs();
        // return &CustomRmgrDesc[rmid - RM_MIN_CUSTOM_ID];
    }
}
