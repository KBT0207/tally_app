"""
logging_config.py
==================
Configures the root logger for TallySyncManager.

Phase 3 fixes:
  - Replaced bare FileHandler with a custom _DailyDateFileHandler that rolls
    over at midnight to a new date-stamped file (keeps our DD-Mon-YYYY naming
    convention that logs_page relies on, but properly closes old handles).
  - Added _purge_old_logs() called at import time to delete files beyond the
    retention window — cleans up accumulation from multi-day gaps.
  - Retention window read from tally_config.ini [tally] log_retention_days
    (default 30 days); 0 = keep forever.

Phase 4 fixes:
  - LOG_DIR now reads log_dir from tally_config.ini [tally] section.
    This allows the installer to let the user choose any drive/folder
    for log storage (e.g. D:\logs\TallySyncManager).
  - Falls back to <exe_dir>\logs if log_dir is missing or empty in the ini.
"""

import logging
import logging.handlers
import os
import sys
import glob
import configparser
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────────────────
#  EXE-safe path resolution
#
#  When frozen by PyInstaller:
#    sys.frozen = True  and  sys._MEIPASS = temp extraction folder
#    sys.executable     = path to the actual .exe file
#
#  LOG_DIR must be writable → resolve relative to the .exe folder (or project
#  root in dev), NOT _MEIPASS which is deleted on exit.
#
#  tally_config.ini is a read-only bundled resource → resolve from _MEIPASS
#  (or project root in dev).
# ─────────────────────────────────────────────────────────────────────────────
def _get_exe_dir() -> str:
    """Folder containing the .exe (frozen) or main.py (dev)."""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))

def _get_bundle_dir() -> str:
    """Extraction folder in frozen mode (_MEIPASS), else project root."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return sys._MEIPASS  # type: ignore[attr-defined]
    return os.path.dirname(os.path.abspath(__file__))


# ─────────────────────────────────────────────────────────────────────────────
#  Read log_dir and retention setting from tally_config.ini
# ─────────────────────────────────────────────────────────────────────────────
_ADVANCED_CFG           = os.path.join(_get_bundle_dir(), "tally_config.ini")
_DEFAULT_RETENTION_DAYS = 30


def _read_ini() -> configparser.ConfigParser:
    """Read tally_config.ini and return the parser. Safe to call at import time."""
    cfg = configparser.ConfigParser()
    if os.path.exists(_ADVANCED_CFG):
        cfg.read(_ADVANCED_CFG, encoding="utf-8")
    return cfg


def _resolve_log_dir() -> str:
    """
    Return the log directory to use.

    Priority:
      1. [tally] log_dir in tally_config.ini  (set by installer, any drive)
      2. <exe_dir>\\logs                        (safe fallback for dev / fresh install)
    """
    try:
        cfg     = _read_ini()
        log_dir = cfg.get("tally", "log_dir", fallback="").strip()
        if log_dir:
            return log_dir
    except Exception:
        pass
    # Fallback: logs folder next to the .exe (or main.py in dev)
    return os.path.join(_get_exe_dir(), "logs")


def _read_retention_days() -> int:
    try:
        cfg = _read_ini()
        val = cfg.get("tally", "log_retention_days", fallback=str(_DEFAULT_RETENTION_DAYS))
        days = int(val)
        return days if days >= 0 else _DEFAULT_RETENTION_DAYS
    except Exception:
        return _DEFAULT_RETENTION_DAYS


# ─────────────────────────────────────────────────────────────────────────────
#  Resolve and create the log directory
# ─────────────────────────────────────────────────────────────────────────────
LOG_DIR = _resolve_log_dir()
os.makedirs(LOG_DIR, exist_ok=True)

today_date     = datetime.now().strftime("%d-%b-%Y")
main_log_file  = os.path.join(LOG_DIR, f"main_{today_date}.log")
error_log_file = os.path.join(LOG_DIR, f"error_{today_date}.log")


# ─────────────────────────────────────────────────────────────────────────────
#  Startup log purge
# ─────────────────────────────────────────────────────────────────────────────
def _purge_old_logs(retention_days: int) -> int:
    """Delete .log files in LOG_DIR older than retention_days. 0 = keep forever."""
    if retention_days == 0:
        return 0
    cutoff  = datetime.now() - timedelta(days=retention_days)
    deleted = 0
    for fpath in glob.glob(os.path.join(LOG_DIR, "*.log")):
        try:
            if datetime.fromtimestamp(os.path.getmtime(fpath)) < cutoff:
                os.remove(fpath)
                deleted += 1
        except Exception:
            pass
    return deleted


# ─────────────────────────────────────────────────────────────────────────────
#  Custom daily-rotating handler that keeps date-stamped filenames
#
#  Python's built-in TimedRotatingFileHandler renames the current file on
#  rollover (appending a date suffix) which breaks logs_page's file-discovery
#  logic. Instead we simply open a new date-stamped file at midnight.
# ─────────────────────────────────────────────────────────────────────────────
class _DailyDateFileHandler(logging.handlers.BaseRotatingHandler):
    """
    Writes to logs/<prefix>_DD-Mon-YYYY.log.
    Rolls at midnight to a fresh date-stamped file.
    backup_count: max number of <prefix>_*.log files to keep (0 = unlimited).
    """

    def __init__(self, log_dir: str, prefix: str,
                 level: int = logging.DEBUG,
                 backup_count: int = 30,
                 encoding: str = "utf-8"):
        self._log_dir      = log_dir
        self._prefix       = prefix
        self._backup_count = backup_count
        filename = self._current_filename()
        super().__init__(filename, mode="a", encoding=encoding, delay=False)
        self.setLevel(level)
        self._set_next_rollover()

    def _current_filename(self) -> str:
        return os.path.join(
            self._log_dir,
            f"{self._prefix}_{datetime.now().strftime('%d-%b-%Y')}.log"
        )

    def _set_next_rollover(self):
        now      = datetime.now()
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        self._next_rollover = (midnight + timedelta(days=1)).timestamp()

    def shouldRollover(self, record) -> bool:  # noqa: N802
        return datetime.now().timestamp() >= self._next_rollover

    def doRollover(self):  # noqa: N802
        if self.stream:
            self.stream.flush()
            self.stream.close()
            self.stream = None

        self.baseFilename = self._current_filename()
        self.stream       = self._open()
        self._set_next_rollover()

        # Trim oldest files if backup_count is set
        if self._backup_count > 0:
            pattern = os.path.join(self._log_dir, f"{self._prefix}_*.log")
            files   = sorted(glob.glob(pattern), key=os.path.getmtime)
            for old in files[:max(0, len(files) - self._backup_count)]:
                try:
                    os.remove(old)
                except Exception:
                    pass


# ─────────────────────────────────────────────────────────────────────────────
#  Wire everything up
# ─────────────────────────────────────────────────────────────────────────────
_retention = _read_retention_days()
_purged    = _purge_old_logs(_retention)

_fmt = logging.Formatter(
    "%(asctime)s | %(levelname)-8s | %(filename)s:%(lineno)d | %(message)s"
)

_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.DEBUG)
_console_handler.setFormatter(_fmt)

_main_handler = _DailyDateFileHandler(
    log_dir      = LOG_DIR,
    prefix       = "main",
    level        = logging.DEBUG,
    backup_count = _retention if _retention > 0 else 0,
)
_main_handler.setFormatter(_fmt)

_error_handler = _DailyDateFileHandler(
    log_dir      = LOG_DIR,
    prefix       = "error",
    level        = logging.ERROR,
    backup_count = _retention if _retention > 0 else 0,
)
_error_handler.setFormatter(_fmt)

root = logging.getLogger()
root.setLevel(logging.DEBUG)
root.addHandler(_console_handler)
root.addHandler(_main_handler)
root.addHandler(_error_handler)

logger = logging.getLogger(__name__)

if _purged:
    logger.info(
        f"[LogConfig] Startup purge: removed {_purged} log file(s) "
        f"older than {_retention} days"
    )

logger.info(f"[LogConfig] Log directory: {LOG_DIR}")
