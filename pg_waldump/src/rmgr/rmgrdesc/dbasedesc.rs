use crate::constant::*;
use crate::pgtypes::*;
use crate::state::*;
use enumname_derive::EnumName;

#[derive(EnumName)]
enum XLogInfoDbase {
    CreateFileCopy = 0x00,
    CreateWalLog = 0x10,
    Drop = 0x20,
}

impl From<u8> for XLogInfoDbase {
    fn from(info: u8) -> Self {
        match info {
            0x00 => XLogInfoDbase::CreateFileCopy,
            0x10 => XLogInfoDbase::CreateWalLog,
            0x20 => XLogInfoDbase::Drop,
            _ => panic!("Unknown XLogInfo: {}", info),
        }
    }
}

// Single WAL record for an entire CREATE DATABASE operation. This is used
// by the FILE_COPY strategy.
#[repr(C)]
struct XlDbaseCreateFileCopyRec {
    db_id: Oid,
    tablespace_id: Oid,
    src_db_id: Oid,
    src_tablespace_id: Oid,
}

// WAL record for the beginning of a CREATE DATABASE operation, when the
// WAL_LOG strategy is used. Each individual block will be logged separately
// afterward.
#[repr(C)]
struct XlDbaseCreateWalLogRec {
    db_id: Oid,
    tablespace_id: Oid,
}

#[repr(C)]
struct XlDbaseDropRec {
    db_id: Oid,
    // number of tablespace IDs
    ntablespaces: i32,
    // tablespace_ids: [Oid; 0],
}

pub fn dbase_desc(state: &XLogReaderState) -> String {
    let record = state.record.as_ref().unwrap();
    if record.main_data.is_none() {
        return String::new();
    }
    let main_data = record.main_data.as_ref().unwrap();
    let rec = main_data.as_slice();
    let info = record.header.xl_info & !XLR_INFO_MASK;
    let info = XLogInfoDbase::from(info);
    let mut rst = String::new();

    match info {
        XLogInfoDbase::CreateFileCopy => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XlDbaseCreateFileCopyRec) };

            rst += &format!(
                "copy dir {}/{} to {}/{}",
                xlrec.src_tablespace_id, xlrec.src_db_id, xlrec.tablespace_id, xlrec.db_id
            );
        }
        XLogInfoDbase::CreateWalLog => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XlDbaseCreateWalLogRec) };

            rst += &format!("create dir {}/{}", xlrec.tablespace_id, xlrec.db_id);
        }
        XLogInfoDbase::Drop => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XlDbaseDropRec) };

            rst.push_str("dir");

            let mut tblsps: Vec<Oid> = vec![0; xlrec.ntablespaces as usize];
            unsafe {
                rec.as_ptr()
                    .add(std::mem::size_of::<XlDbaseDropRec>())
                    .copy_to_nonoverlapping(
                        tblsps.as_mut_ptr() as *mut u8,
                        std::mem::size_of::<Oid>() * tblsps.len(),
                    );
            }
            for i in 0..xlrec.ntablespaces as usize {
                rst += &format!(" {}/{}", tblsps[i], xlrec.db_id);
            }
        }
    }

    rst
}

pub fn dbase_identify(info: u8) -> String {
    let info = XLogInfoDbase::from(info & !XLR_INFO_MASK);
    format!("{}", info)
}
