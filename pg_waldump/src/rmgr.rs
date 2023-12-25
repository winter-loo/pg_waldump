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

type RmDesc = fn(state: &XLogReaderState) -> String;
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

fn xlog_desc(state: &XLogReaderState) -> String {
    String::from("xlog_desc")
}

fn xlog_identify(info: u8) -> String {
    String::from("xlog_identify")
}

fn xact_desc(state: &XLogReaderState) -> String {
    String::from("xact_desc")
}

fn xact_identify(info: u8) -> String {
    String::from("xact_identify")
}

fn smgr_desc(state: &XLogReaderState) -> String {
    String::from("smgr_desc")
}

fn smgr_identify(info: u8) -> String {
    String::from("smgr_identify")
}

fn clog_desc(state: &XLogReaderState) -> String {
    String::from("clog_desc")
}

fn clog_identify(info: u8) -> String {
    String::from("clog_identify")
}

fn dbase_desc(state: &XLogReaderState) -> String {
    String::from("dbase_desc")
}

fn dbase_identify(info: u8) -> String {
    String::from("dbase_identify")
}

fn tblspc_desc(state: &XLogReaderState) -> String {
    String::from("tblspc_desc")
}

fn tblspc_identify(info: u8) -> String {
    String::from("tblspc_identify")
}

fn multixact_desc(state: &XLogReaderState) -> String {
    String::from("multixact_desc")
}

fn multixact_identify(info: u8) -> String {
    String::from("multixact_identify")
}

fn relmap_desc(state: &XLogReaderState) -> String {
    String::from("relmap_desc")
}

fn relmap_identify(info: u8) -> String {
    String::from("relmap_identify")
}

fn standby_desc(state: &XLogReaderState) -> String {
    String::from("standby_desc")
}

fn standby_identify(info: u8) -> String {
    String::from("standby_identify")
}

fn heap2_desc(state: &XLogReaderState) -> String {
    String::from("heap2_desc")
}

fn heap2_identify(info: u8) -> String {
    String::from("heap2_identify")
}

fn heap_desc(state: &XLogReaderState) -> String {
    String::from("heap_desc")
}

fn heap_identify(info: u8) -> String {
    String::from("heap_identify")
}

fn btree_desc(state: &XLogReaderState) -> String {
    String::from("btree_desc")
}

fn btree_identify(info: u8) -> String {
    String::from("btree_identify")
}

fn hash_desc(state: &XLogReaderState) -> String {
    String::from("hash_desc")
}

fn hash_identify(info: u8) -> String {
    String::from("hash_identify")
}

fn gin_desc(state: &XLogReaderState) -> String {
    String::from("gin_desc")
}

fn gin_identify(info: u8) -> String {
    String::from("gin_identify")
}

fn gist_desc(state: &XLogReaderState) -> String {
    String::from("gist_desc")
}

fn gist_identify(info: u8) -> String {
    String::from("gist_identify")
}

fn seq_desc(state: &XLogReaderState) -> String {
    String::from("seq_desc")
}

fn seq_identify(info: u8) -> String {
    String::from("seq_identify")
}

fn spg_desc(state: &XLogReaderState) -> String {
    String::from("spg_desc")
}

fn spg_identify(info: u8) -> String {
    String::from("spg_identify")
}

fn brin_desc(state: &XLogReaderState) -> String {
    String::from("brin_desc")
}

fn brin_identify(info: u8) -> String {
    String::from("brin_identify")
}

fn commit_ts_desc(state: &XLogReaderState) -> String {
    String::from("commit_ts_desc")
}

fn commit_ts_identify(info: u8) -> String {
    String::from("commit_ts_identify")
}

fn replorigin_desc(state: &XLogReaderState) -> String {
    String::from("replorigin_desc")
}

fn replorigin_identify(info: u8) -> String {
    String::from("replorigin_identify")
}

fn generic_desc(state: &XLogReaderState) -> String {
    String::from("generic_desc")
}

fn generic_identify(info: u8) -> String {
    String::from("generic_identify")
}

fn logicalmsg_desc(state: &XLogReaderState) -> String {
    String::from("logicalmsg_desc")
}

fn logicalmsg_identify(info: u8) -> String {
    String::from("logicalmsg_identify")
}

pub const RMGR_DESC_TABLE: [RmgrDescData; RmgrIds::MAX as usize] = include!("./rmgrlist.h");

pub fn get_rmgr_desc(rmid: RmgrId) -> &'static RmgrDescData {
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
