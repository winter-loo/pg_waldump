use crate::pgtypes::*;

pub enum WalLevel {
    Minimal = 0,
    Replica,
    Logical,
}

impl From<WalLevel> for i32 {
    fn from(wal_level: WalLevel) -> Self {
        wal_level as i32
    }
}

// End of recovery mark, when we don't do an END_OF_RECOVERY checkpoint
pub struct XlEndOfRecovery {
    pub end_time: TimestampTz,
    // new TLI
    pub this_timeline_id: TimeLineID,
    // previous TLI we forked off from
    pub prev_timeline_id: TimeLineID,
}
