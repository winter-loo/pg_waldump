use crate::constant::*;
use crate::guc::*;
use crate::pg_control::*;
use crate::pgtypes::*;
use crate::state::XLogReaderState;
use crate::util::*;
use crate::waldec;
use crate::xlog::*;

pub fn xlog_desc(state: &XLogReaderState) -> String {
    let record = state.record.as_ref().unwrap();
    if record.main_data.is_none() {
        return String::new();
    }
    let rec = record.main_data.as_ref().unwrap().as_slice();
    let info = record.header.xl_info & !XLR_INFO_MASK;
    let info = XLogInfo::from(info);
    let mut rst = String::new();

    match info {
        XLogInfo::CheckpointShutdown | XLogInfo::CheckpointOnline => {
            let checkpoint = unsafe { &*(rec.as_ptr() as *const CheckPoint) };
            rst += &format!(
                "{} {}",
                checkpoint,
                if info == XLogInfo::CheckpointShutdown {
                    "shutdown"
                } else {
                    "online"
                }
            );
        }
        XLogInfo::NextOid => {
            let next_oid = unsafe { *(rec.as_ptr() as *const Oid) };

            rst += &format!("{}", next_oid);
        }
        XLogInfo::RestorePoint => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XlRestorePoint) };

            rst += &format!("{}", to_string(&xlrec.rp_name));
        }
        XLogInfo::FPI | XLogInfo::FPIForHint => {
            // no further information to print
        }
        XLogInfo::BackupEnd => {
            let startpoint = unsafe { *(rec.as_ptr() as *const XLogRecPtr) };

            rst += &format!("{}", waldec::lsn_out(startpoint));
        }
        XLogInfo::ParameterChange => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XlParameterChange) };

            // Find a string representation for wal_level
            let mut wal_level_str = "";
            for entry in WAL_LEVEL_OPTIONS.iter() {
                if entry.val == xlrec.wal_level {
                    wal_level_str = entry.name;
                    break;
                }
            }

            let s = format!(
                "max_connections={} max_worker_processes={} \
                            max_wal_senders={} max_prepared_xacts={} \
                            max_locks_per_xact={} wal_level={} \
                            wal_log_hints={} track_commit_timestamp={}",
                xlrec.max_connections,
                xlrec.max_worker_processes,
                xlrec.max_wal_senders,
                xlrec.max_prepared_xacts,
                xlrec.max_locks_per_xact,
                wal_level_str,
                if xlrec.wal_log_hints { "on" } else { "off" },
                if xlrec.track_commit_timestamp {
                    "on"
                } else {
                    "off"
                }
            );
            rst += &s;
        }
        XLogInfo::FpwChange => {
            let fpw = unsafe { *(rec.as_ptr() as *const bool) };
            rst += &format!("{}", if fpw { "true" } else { "false" });
        }
        XLogInfo::EndOfRecovery => {
            let xlrec = unsafe { &*(rec.as_ptr() as *const XlEndOfRecovery) };
            let s = format!(
                "tli {}; prev tli {}; time {}",
                xlrec.this_timeline_id,
                xlrec.prev_timeline_id,
                timestamptz_to_str(xlrec.end_time)
            );
            rst += &s;
        }
        XLogInfo::OverwriteContrecord => {
            // TODO: implement
        }
        XLogInfo::CheckpointRedo => {
            // No details to write out
        }
        XLogInfo::NoOp => {
            // No details to write out
        }
        XLogInfo::Switch => {
            // No details to write out
        }
    }

    rst
}

pub fn xlog_identify(info: u8) -> String {
    let info = XLogInfo::from(info);
    format!("{}", info)
}

// GUC support
const WAL_LEVEL_OPTIONS: [ConfigEnumEntry; 5] = [
    ConfigEnumEntry {
        name: "minimal",
        val: WalLevel::Minimal as i32,
        hidden: false,
    },
    ConfigEnumEntry {
        name: "replica",
        val: WalLevel::Replica as i32,
        hidden: false,
    },
    ConfigEnumEntry {
        name: "archive",
        val: WalLevel::Replica as i32,
        hidden: true,
    }, /* deprecated */
    ConfigEnumEntry {
        name: "hot_standby",
        val: WalLevel::Replica as i32,
        hidden: true,
    }, /* deprecated */
    ConfigEnumEntry {
        name: "logical",
        val: WalLevel::Logical as i32,
        hidden: false,
    },
];
