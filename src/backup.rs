//! `chattrs backup`: an online copy of the broker database (OPS-1).
//! The source opens read-only and is never modified. The copy goes to a
//! temp file next to the destination, then renames into place.

use std::path::{Path, PathBuf};

use rusqlite::backup::{Backup, StepResult};
use rusqlite::{Connection, OpenFlags};

use crate::cli::BackupArgs;
use crate::store::Store;
use crate::RunError;

const PAGES_PER_STEP: i32 = 256;

pub fn run(args: BackupArgs) -> Result<(), RunError> {
    let source = args.db.unwrap_or_else(Store::default_path);
    let messages = backup(&source, &args.file, args.force).map_err(RunError::Message)?;
    eprintln!("chattrs: backed up {messages} messages to {}", args.file.display());
    Ok(())
}

/// Copy `source` to `dest` and return the message count of the copy.
fn backup(source: &Path, dest: &Path, force: bool) -> Result<i64, String> {
    if !source.is_file() {
        return Err(format!("no database at {}", source.display()));
    }
    if dest.exists() && !force {
        return Err(format!("{} exists; pass --force to replace it", dest.display()));
    }
    let temp = temp_path(dest)?;
    let result = copy_to(source, &temp).and_then(|count| {
        std::fs::rename(&temp, dest)
            .map_err(|e| format!("cannot move the copy to {}: {e}", dest.display()))?;
        Ok(count)
    });
    if result.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    result
}

fn temp_path(dest: &Path) -> Result<PathBuf, String> {
    let name = dest.file_name().ok_or_else(|| format!("{} is not a file path", dest.display()))?;
    let mut temp = name.to_os_string();
    temp.push(format!(".tmp-{}", std::process::id()));
    Ok(dest.with_file_name(temp))
}

fn copy_to(source: &Path, temp: &Path) -> Result<i64, String> {
    create_private(temp)?;
    let from = Connection::open_with_flags(source, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|e| format!("cannot open {}: {e}", source.display()))?;
    from.busy_timeout(std::time::Duration::from_secs(5)).map_err(|e| e.to_string())?;
    let mut to = Connection::open(temp).map_err(|e| format!("cannot open the copy: {e}"))?;
    {
        let backup = Backup::new(&from, &mut to).map_err(|e| format!("backup failed: {e}"))?;
        loop {
            match backup.step(PAGES_PER_STEP).map_err(|e| format!("backup failed: {e}"))? {
                StepResult::Done => break,
                StepResult::More => {}
                // Busy or Locked: the broker holds a lock. Retry after a pause.
                _ => std::thread::sleep(std::time::Duration::from_millis(50)),
            }
        }
    }
    let count = to
        .query_row("SELECT COUNT(*) FROM messages", [], |row| row.get(0))
        .map_err(|e| format!("the copy is not a chattrs database: {e}"))?;
    to.close().map_err(|(_, e)| format!("cannot close the copy: {e}"))?;
    Ok(count)
}

/// Create the temp file with mode 0600 (messages are private).
fn create_private(path: &Path) -> Result<(), String> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map(drop).map_err(|e| format!("cannot create {}: {e}", path.display()))
}
