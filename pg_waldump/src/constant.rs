#![allow(unused)]
use enumname_derive::EnumName;

pub(crate) const XLOG_BLCKSZ: u32 = 8192;
pub(crate) const XLOGDIR: &str = "pg_wal";
pub(crate) const XLOG_INVALID_RECPTR: u64 = 0;
// These macros encapsulate knowledge about the exact layout of XLog file
// names, timeline history file names, and archive-status file names.
pub(crate) const MAXFNAMELEN: usize = 64;
pub(crate) const XLOG_FNAME_LEN: usize = 24;

pub(crate) const XLOG_PAGE_MAGIC: u16 = 0xD114;
pub(crate) const XLR_INFO_MASK: u8 = 0x0F;
pub(crate) const XLR_RMGR_INFO_MASK: u8 = 0xF0;

pub(crate) const XLR_MAX_BLOCK_ID: u8 = 32;
pub(crate) const XLR_BLOCK_ID_DATA_SHORT: u8 = 255;
pub(crate) const XLR_BLOCK_ID_DATA_LONG: u8 = 254;
pub(crate) const XLR_BLOCK_ID_ORIGIN: u8 = 253;
pub(crate) const XLR_BLOCK_ID_TOPLEVEL_XID: u8 = 252;

pub(crate) const WAL_SEG_MIN_SIZE: u32 = 1024 * 1024;
pub(crate) const WAL_SEG_MAX_SIZE: u32 = 1024 * 1024 * 1024;
pub(crate) const DEFAULT_MIN_WAL_SEGS: u32 = 64;
pub(crate) const DEFAULT_MAX_WAL_SEGS: u32 = 1024;

// for xlp_info
pub(crate) const XLP_FIRST_IS_CONTRECORD: u16 = 0x0001;
pub(crate) const XLP_LONG_HEADER: u16 = 0x0002;
pub(crate) const XLP_BKP_REMOVABLE: u16 = 0x0004;
pub(crate) const XLP_FIRST_IS_OVERWRITE_CONTRECORD: u16 = 0x0008;
pub(crate) const XLP_ALL_FLAGS: u16 = 0x000F;

// page image has "hole"
pub(crate) const BKPIMAGE_HAS_HOLE: u8 = 0x01;
// page image should be restored during replay
pub(crate) const BKPIMAGE_APPLY: u8 = 0x02;

pub(crate) const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub(crate) const BKPBLOCK_FLAG_MASK: u8 = 0xF0;
// block data is an XLogRecordBlockImage
pub(crate) const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
pub(crate) const BKPBLOCK_HAS_DATA: u8 = 0x20;
// redo will re-init the page
pub(crate) const BKPBLOCK_WILL_INIT: u8 = 0x40;
// RelFileNode omitted,
pub(crate) const BKPBLOCK_SAME_REL: u8 = 0x80;

#[repr(u8)]
#[derive(EnumName, PartialEq)]
pub(crate) enum XLogInfo {
    CheckpointShutdown = 0x00,
    CheckpointOnline = 0x10,
    NoOp = 0x20,
    NextOid = 0x30,
    Switch = 0x40,
    BackupEnd = 0x50,
    ParameterChange = 0x60,
    RestorePoint = 0x70,
    FpwChange = 0x80,
    EndOfRecovery = 0x90,
    FPIForHint = 0xA0,
    FPI = 0xB0,
    // 0xC0 is used in Postgres 9.5-11
    OverwriteContrecord = 0xD0,
    CheckpointRedo = 0xE0,
}

impl From<u8> for XLogInfo {
    fn from(info: u8) -> Self {
        match info {
            0x00 => XLogInfo::CheckpointShutdown,
            0x10 => XLogInfo::CheckpointOnline,
            0x20 => XLogInfo::NoOp,
            0x30 => XLogInfo::NextOid,
            0x40 => XLogInfo::Switch,
            0x50 => XLogInfo::BackupEnd,
            0x60 => XLogInfo::ParameterChange,
            0x70 => XLogInfo::RestorePoint,
            0x80 => XLogInfo::FpwChange,
            0x90 => XLogInfo::EndOfRecovery,
            0xA0 => XLogInfo::FPIForHint,
            0xB0 => XLogInfo::FPI,
            0xD0 => XLogInfo::OverwriteContrecord,
            0xE0 => XLogInfo::CheckpointRedo,
            _ => panic!("unknown xlog info: {}", info),
        }
    }
}

pub const FORK_NAMES: [&'static str; 4] = ["main", "fsm", "vm", "init"];
