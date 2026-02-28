"""
gui/config_manager.py
======================
Central config manager for Tally Sync Manager.

Saves and loads all app configuration to/from:
  Windows : C:\\Users\\<user>\\AppData\\Roaming\\TallySyncManager\\config.json
  Linux   : ~/.TallySyncManager/config.json   (fallback for dev)
  Mac     : ~/.TallySyncManager/config.json   (fallback)

Why AppData?
  - Survives app updates and reinstalls
  - Works correctly when app is packaged as .exe
  - Per-user config — each Windows user has their own
  - User never accidentally deletes it
  - Standard location for all desktop apps on Windows

Usage:
    from gui.config_manager import ConfigManager

    cfg = ConfigManager()

    # Read
    db  = cfg.get_db_config()     # dict
    tal = cfg.get_tally_config()  # dict
    ok  = cfg.is_setup_complete() # bool

    # Write
    cfg.save_db_config({"host": "localhost", "port": 3306, ...})
    cfg.save_tally_config({"host": "localhost", "port": 9000})
    cfg.mark_setup_complete()
    cfg.mark_setup_incomplete()   # force re-setup on next launch
"""

import os
import json
import shutil
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Constants
# ─────────────────────────────────────────────────────────────────────────────
APP_NAME    = "TallySyncManager"
CONFIG_FILE = "config.json"
BACKUP_FILE = "config.backup.json"


# ─────────────────────────────────────────────────────────────────────────────
#  Default values
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_CONFIG = {
    "db": {
        "host":     "localhost",
        "port":     3306,
        "username": "root",
        "password": "",
        "database": "tally_db",
    },
    "tally": {
        "host": "localhost",
        "port": 9000,
    },
    "app": {
        "setup_complete": False,
        "first_run":      True,
    },
}


# ─────────────────────────────────────────────────────────────────────────────
#  ConfigManager
# ─────────────────────────────────────────────────────────────────────────────
class ConfigManager:
    """
    Reads and writes config.json from the OS-appropriate user data folder.
    All methods are safe to call even if the file does not exist yet.
    """

    def __init__(self):
        self._folder = self._resolve_folder()
        self._path   = os.path.join(self._folder, CONFIG_FILE)
        self._backup = os.path.join(self._folder, BACKUP_FILE)
        self._data   = self._load()

    # ─────────────────────────────────────────────────────────────────────────
    #  Path resolution
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _resolve_folder() -> str:
        """
        Returns the correct config folder path for the current OS.

        Windows : %APPDATA%\\TallySyncManager
        Others  : ~/.TallySyncManager
        """
        appdata = os.environ.get("APPDATA")   # set on Windows only
        if appdata:
            folder = os.path.join(appdata, APP_NAME)
        else:
            folder = os.path.join(os.path.expanduser("~"), f".{APP_NAME}")

        os.makedirs(folder, exist_ok=True)
        return folder

    @property
    def config_path(self) -> str:
        """Full path to config.json — useful for showing user in Settings page."""
        return self._path

    @property
    def config_folder(self) -> str:
        """Full path to config folder."""
        return self._folder

    # ─────────────────────────────────────────────────────────────────────────
    #  Load
    # ─────────────────────────────────────────────────────────────────────────
    def _load(self) -> dict:
        """
        Load config from disk.
        If file missing or corrupt → return defaults.
        If file corrupt → save a backup of the bad file, then return defaults.
        """
        if not os.path.exists(self._path):
            logger.info(f"[ConfigManager] No config file found at {self._path} — using defaults")
            return self._deep_copy(DEFAULT_CONFIG)

        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Merge with defaults so any new keys added in future versions
            # are automatically present even in old config files
            merged = self._deep_copy(DEFAULT_CONFIG)
            self._deep_merge(merged, data)
            logger.info(f"[ConfigManager] Config loaded from {self._path}")
            return merged

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"[ConfigManager] Config file corrupt: {e} — backing up and using defaults")
            try:
                shutil.copy2(self._path, self._backup)
                logger.warning(f"[ConfigManager] Bad config backed up to {self._backup}")
            except Exception:
                pass
            return self._deep_copy(DEFAULT_CONFIG)

    # ─────────────────────────────────────────────────────────────────────────
    #  Save
    # ─────────────────────────────────────────────────────────────────────────
    def _save(self):
        """Write current _data to disk as pretty-printed JSON."""
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            logger.debug(f"[ConfigManager] Config saved to {self._path}")
        except IOError as e:
            logger.error(f"[ConfigManager] Failed to save config: {e}")
            raise

    # ─────────────────────────────────────────────────────────────────────────
    #  DB config
    # ─────────────────────────────────────────────────────────────────────────
    def get_db_config(self) -> dict:
        """
        Returns DB config dict with keys:
          host, port (int), username, password, database
        Always returns a copy — modifying the return value won't affect stored config.
        """
        return self._deep_copy(self._data.get("db", DEFAULT_CONFIG["db"]))

    def save_db_config(self, db: dict):
        """
        Save DB config.
        Expected keys: host, port, username, password, database
        port is coerced to int automatically.
        """
        self._data["db"] = {
            "host":     str(db.get("host",     "localhost")),
            "port":     int(db.get("port",     3306)),
            "username": str(db.get("username", "root")),
            "password": str(db.get("password", "")),
            "database": str(db.get("database", "tally_db")),
        }
        self._save()
        logger.info(f"[ConfigManager] DB config saved: {db.get('host')}:{db.get('port')}/{db.get('database')}")

    # ─────────────────────────────────────────────────────────────────────────
    #  Tally config
    # ─────────────────────────────────────────────────────────────────────────
    def get_tally_config(self) -> dict:
        """
        Returns Tally config dict with keys:
          host, port (int)
        """
        return self._deep_copy(self._data.get("tally", DEFAULT_CONFIG["tally"]))

    def save_tally_config(self, tally: dict):
        """
        Save Tally connection defaults.
        Expected keys: host, port
        """
        self._data["tally"] = {
            "host": str(tally.get("host", "localhost")),
            "port": int(tally.get("port", 9000)),
        }
        self._save()
        logger.info(f"[ConfigManager] Tally config saved: {tally.get('host')}:{tally.get('port')}")

    # ─────────────────────────────────────────────────────────────────────────
    #  App / setup flags
    # ─────────────────────────────────────────────────────────────────────────
    def is_setup_complete(self) -> bool:
        """
        Returns True only if the user has completed the first-run setup wizard.
        If False → show setup wizard on startup.
        """
        return bool(self._data.get("app", {}).get("setup_complete", False))

    def is_first_run(self) -> bool:
        """Returns True if this is the very first launch (config file was just created)."""
        return bool(self._data.get("app", {}).get("first_run", True))

    def mark_setup_complete(self):
        """Call this after the setup wizard finishes successfully."""
        self._data.setdefault("app", {})
        self._data["app"]["setup_complete"] = True
        self._data["app"]["first_run"]      = False
        self._save()
        logger.info("[ConfigManager] Setup marked as complete")

    def mark_setup_incomplete(self):
        """
        Call this to force the setup wizard to show again on next launch.
        Useful when DB connection fails on startup.
        """
        self._data.setdefault("app", {})
        self._data["app"]["setup_complete"] = False
        self._save()
        logger.info("[ConfigManager] Setup marked as incomplete — wizard will show on next launch")

    # ─────────────────────────────────────────────────────────────────────────
    #  Convenience: reload from disk
    # ─────────────────────────────────────────────────────────────────────────
    def reload(self):
        """Re-read config from disk. Call after external changes."""
        self._data = self._load()

    # ─────────────────────────────────────────────────────────────────────────
    #  Reset
    # ─────────────────────────────────────────────────────────────────────────
    def reset_to_defaults(self):
        """
        Wipe config and reset to defaults.
        This will cause the setup wizard to show on next launch.
        Use in Settings page as a 'Reset App' option.
        """
        self._data = self._deep_copy(DEFAULT_CONFIG)
        self._save()
        logger.warning("[ConfigManager] Config reset to defaults")

    # ─────────────────────────────────────────────────────────────────────────
    #  Helpers
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _deep_copy(data: dict) -> dict:
        """Simple deep copy via JSON round-trip (avoids importing copy module)."""
        return json.loads(json.dumps(data))

    @staticmethod
    def _deep_merge(base: dict, override: dict):
        """
        Recursively merge override into base IN PLACE.
        Keys in override replace keys in base.
        Nested dicts are merged, not replaced.
        """
        for key, value in override.items():
            if (
                key in base
                and isinstance(base[key], dict)
                and isinstance(value, dict)
            ):
                ConfigManager._deep_merge(base[key], value)
            else:
                base[key] = value

    # ─────────────────────────────────────────────────────────────────────────
    #  Debug
    # ─────────────────────────────────────────────────────────────────────────
    def __repr__(self) -> str:
        db  = self._data.get("db",  {})
        tal = self._data.get("tally", {})
        return (
            f"<ConfigManager "
            f"db={db.get('host')}:{db.get('port')}/{db.get('database')} "
            f"tally={tal.get('host')}:{tal.get('port')} "
            f"setup={self.is_setup_complete()}>"
        )