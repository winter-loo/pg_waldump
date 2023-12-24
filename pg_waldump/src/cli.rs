use crate::pgtypes::*;
pub use clap::Parser;
use std::path::PathBuf;

fn verify_directory(p: &str) -> Result<std::path::PathBuf, String> {
    let path = std::path::PathBuf::from(p);
    if path.is_dir() {
        Ok(path)
    } else {
        Err(format!("no such directory: {}", path.display()))
    }
}

fn parse_lsn(s: &str) -> Result<XLogRecPtr, String> {
    let parts: Vec<&str> = s.split("/").collect();
    if parts.len() != 2 {
        return Err(format!("invalid WAL location: {}", s));
    }
    let xlogid = match u64::from_str_radix(parts[0], 16) {
        Ok(t) => t,
        Err(_) => {
            return Err(format!("invalid WAL location: {}", s));
        }
    };
    let xrecoff = match u64::from_str_radix(parts[1], 16) {
        Ok(t) => t,
        Err(_) => {
            return Err(format!("invalid WAL location: {}", s));
        }
    };
    Ok(xlogid << 32 | xrecoff)
}

/// pg_waldump decodes and displays PostgreSQL write-ahead logs for debugging.
#[derive(Parser)]
#[command(author, version, about, long_about = None, disable_colored_help=true,
help_template="\
{about-with-newline}
{usage-heading}
  {usage}

Options:
{options}
")]
pub(crate) struct Cli {
    pub startseg: Option<PathBuf>,
    pub endseg: Option<PathBuf>,

    /// output detailed information about backup blocks
    #[arg(short, long, action=clap::ArgAction::SetTrue)]
    pub bkp_details: Option<bool>,

    /// with --relation, only show records that modify block N
    #[arg(short = 'B', long, value_name = "N")]
    pub block: Option<u32>,

    /// start reading at WAL location RECPTR
    #[arg(short, long, value_name = "RECPTR", value_parser=parse_lsn)]
    pub start: Option<XLogRecPtr>,

    /// stop reading at WAL location RECPTR
    #[arg(short, long, value_name = "RECPTR", value_parser=parse_lsn)]
    pub end: Option<XLogRecPtr>,

    /// keep retrying after reaching end of WAL
    #[arg(short, long, action=clap::ArgAction::SetTrue)]
    pub follow: Option<bool>,

    #[arg(
        short = 'F',
        long,
        value_name = "FORK",
        value_parser=[
            "main",
            "fsm",
            "vm",
            "init",
        ],
        hide_possible_values=true,
        help = "\
only show records that modify blocks in fork FORK;
valid names are main, fsm, vm, init"
    )]
    pub fork: Option<String>,

    /// number of records to display
    #[arg(short = 'n', long, value_name = "N")]
    pub limit: Option<u32>,

    #[arg(
        short,
        long,
        value_parser=verify_directory,
        hide_default_value=true,
        help = "\
directory in which to find log segment files or a
directory with a ./pg_wal that contains such files
(default: current directory, ./pg_wal, $PGDATA/pg_wal)"
    )]
    pub path: Option<PathBuf>,

    /// do not print any output, except for errors
    #[arg(short, long, action=clap::ArgAction::SetTrue)]
    pub quiet: Option<bool>,

    #[arg(
        short,
        long,
        help = "\
only show records generated by resource manager RMGR;
use --rmgr=list to list valid resource manager names"
    )]
    pub rmgr: Option<String>,

    /// only show records that modify blocks in relation T/D/R
    #[arg(short = 'R', long, value_name = "T/D/R")]
    pub relation: Option<String>,

    #[arg(
        short,
        long,
        value_name = "TLI",
        default_value = "1",
        hide_default_value = true,
        help = "\
timeline from which to read log records
(default: 1 or the value used in STARTSEG)"
    )]
    pub timeline: Option<TimeLineID>,

    /// only show records with a full page write
    #[arg(short='w', long, action=clap::ArgAction::SetTrue)]
    pub fullpage: Option<bool>,

    /// only show records with transaction ID XID
    #[arg(short, long)]
    pub xid: Option<String>,

    #[arg(
        short = 'z',
        long,
        value_name = "record",
        help = "\
show statistics instead of records
(optionally, show per-record statistics)"
    )]
    // this is an optional argument and the argument value is also optional
    pub stats: Option<Option<String>>,
}
