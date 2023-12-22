#![allow(unused)]
use enumname_derive::EnumName;


pub(crate) const XLOG_BLCKSZ: u32 = 8192;
pub(crate) const XLOGDIR: &str = "pg_wal";
pub(crate) const XLOG_INVALID_RECPTR: u64 = 0;

// physical log file sequence number.
pub(crate) type XLogSegNo = u64;
pub(crate) const XLOG_FNAME_LEN: usize = 24;

pub(crate) const XLOG_PAGE_MAGIC: u16 = 0xD110;
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
// page image is compressed
pub(crate) const BKPIMAGE_IS_COMPRESSED: u8 = 0x02;
// page image should be restored during replay
pub(crate) const BKPIMAGE_APPLY: u8 = 0x04;

pub(crate) const BKPBLOCK_FORK_MASK: u8 = 0x0F;
pub(crate) const BKPBLOCK_FLAG_MASK: u8 = 0xF0;
// block data is an XLogRecordBlockImage
pub(crate) const BKPBLOCK_HAS_IMAGE: u8 = 0x10;
pub(crate) const BKPBLOCK_HAS_DATA: u8 = 0x20;
// redo will re-init the page
pub(crate) const BKPBLOCK_WILL_INIT: u8 = 0x40;
// RelFileNode omitted, same as previous
pub(crate) const BKPBLOCK_SAME_REL: u8 = 0x80;

#[repr(u8)]
#[derive(EnumName)]
pub(crate) enum XLogRmgrId {
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
}
