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
import copy
import shutil
import logging
import base64

from typing import Optional

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
#  Secure password storage
#  Priority: Windows Credential Store (keyring) → base64 obfuscation fallback
#  The raw password is NEVER written to config.json.
# ─────────────────────────────────────────────────────────────────────────────
try:
    import keyring
    _HAS_KEYRING = True
except ImportError:
    _HAS_KEYRING = False

_KEYRING_SERVICE = "TallySyncManager"
_KEYRING_USERNAME = "db_password"
_PASSWORD_PLACEHOLDER = "__keyring__"   # sentinel stored in config.json
_B64_PREFIX = "b64:"                    # prefix for obfuscated fallback


def _store_password(password: str) -> str:
    """
    Store password securely. Returns a token to put in config.json.
    - keyring available  → stores in OS credential store, returns '__keyring__'
    - keyring missing    → stores base64-obfuscated value inline, returns 'b64:<data>'
    """
    if _HAS_KEYRING:
        try:
            keyring.set_password(_KEYRING_SERVICE, _KEYRING_USERNAME, password)
            return _PASSWORD_PLACEHOLDER
        except Exception as e:
            logger.warning(f"[ConfigManager] keyring unavailable ({e}) — falling back to b64")
    # Fallback: base64 obfuscation (not encryption, but not plain text)
    return _B64_PREFIX + base64.b64encode(password.encode()).decode()


def _load_password(token: str) -> str:
    """Reverse of _store_password. Returns the plain password."""
    if not token:
        return ""
    if token == _PASSWORD_PLACEHOLDER:
        if _HAS_KEYRING:
            try:
                pwd = keyring.get_password(_KEYRING_SERVICE, _KEYRING_USERNAME)
                return pwd or ""
            except Exception as e:
                logger.error(f"[ConfigManager] Could not read from keyring: {e}")
                return ""
        return ""
    if token.startswith(_B64_PREFIX):
        try:
            return base64.b64decode(token[len(_B64_PREFIX):]).decode()
        except Exception:
            return ""
    # Legacy: plain text password from old config — migrate it on next save
    return token

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
    # ── Admin protection — guards DB settings behind password + email OTP ────
    "admin": {
        "password_hash": "",        # SHA-256 hash — never plain text
        "email":         "",        # OTP is sent to this address
        "smtp_host":     "",        # e.g. smtp.gmail.com
        "smtp_port":     587,
        "smtp_user":     "",
        "smtp_password": "",        # stored via keyring when available
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
        Password is decoded from secure storage before being returned.
        Always returns a copy — modifying the return value won't affect stored config.
        """
        raw = self._deep_copy(self._data.get("db", DEFAULT_CONFIG["db"]))
        # Decode the password token → plain password for use by callers
        raw["password"] = _load_password(raw.get("password", ""))
        return raw

    def save_db_config(self, db: dict):
        """
        Save DB config. Password is stored securely (keyring / b64),
        never as plain text in config.json.
        Expected keys: host, port, username, password, database
        """
        password = str(db.get("password", ""))
        token    = _store_password(password)

        self._data["db"] = {
            "host":          str(db.get("host",     "localhost")),
            "port":          int(db.get("port",     3306)),
            "username":      str(db.get("username", "root")),
            "password":      token,   # ← token, NOT plain text
            "database":      str(db.get("database", "tally_db")),
        }
        self._save()
        logger.info(
            f"[ConfigManager] DB config saved: "
            f"{db.get('host')}:{db.get('port')}/{db.get('database')} "
            f"(password stored via {'keyring' if _HAS_KEYRING else 'b64'})"
        )

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
    #  Update password  (read from .env — never stored in config.json)
    # ─────────────────────────────────────────────────────────────────────────
    @staticmethod
    def _read_env_file() -> dict:
        """
        Manually parse .env without the dotenv library (excluded from PyInstaller).
        Looks for .env next to the EXE (frozen) or at the project root (dev).
        """
        import sys
        if getattr(sys, 'frozen', False):
            base = os.path.dirname(sys.executable)
        else:
            # config_manager.py lives in gui/ — project root is one level up
            base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        env_path = os.path.join(base, '.env')
        result = {}
        if not os.path.exists(env_path):
            logger.warning(f"[ConfigManager] .env not found at {env_path}")
            return result

        with open(env_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, _, value = line.partition('=')
                    result[key.strip()] = value.strip().strip("'\"")
        return result

    def get_update_password(self) -> str:
        """Returns the update password from .env (key: update_pass), or '' if not set."""
        return self._read_env_file().get('update_pass', '')

    def verify_update_password(self, password: str) -> bool:
        """Returns True if the given password matches the update_pass in .env."""
        stored = self.get_update_password()
        if not stored:
            return False
        return password == stored


    # ─────────────────────────────────────────────────────────────────────────
    #  Admin protection config  (password hash + email + SMTP)
    # ─────────────────────────────────────────────────────────────────────────
    def get_admin_config(self) -> dict:
        """
        Returns admin protection config with keys:
          password_hash, email, smtp_host, smtp_port, smtp_user, smtp_password
        smtp_password is decoded from keyring / b64 before returning.
        """
        import copy
        raw = copy.deepcopy(self._data.get("admin", DEFAULT_CONFIG["admin"]))
        # Reuse the same secure loader used for the DB password
        raw["smtp_password"] = _load_password(raw.get("smtp_password", ""))
        return raw

    def save_admin_config(self, admin: dict) -> None:
        """
        Save admin protection settings.
        Expected keys: password_hash, email, smtp_host, smtp_port,
                       smtp_user, smtp_password
        smtp_password is stored via keyring / b64 — never plain text.
        """
        smtp_pass = str(admin.get("smtp_password", ""))
        # Use a dedicated keyring key so it doesn't collide with the DB password
        if _HAS_KEYRING:
            try:
                import keyring as _kr
                _kr.set_password("TallySyncManager", "smtp_password", smtp_pass)
                token = "__keyring_smtp__"
            except Exception:
                token = "b64:" + __import__("base64").b64encode(smtp_pass.encode()).decode()
        else:
            token = "b64:" + __import__("base64").b64encode(smtp_pass.encode()).decode()

        self._data["admin"] = {
            "password_hash": str(admin.get("password_hash", "")),
            "email":         str(admin.get("email", "")),
            "smtp_host":     str(admin.get("smtp_host", "")),
            "smtp_port":     int(admin.get("smtp_port", 587)),
            "smtp_user":     str(admin.get("smtp_user", "")),
            "smtp_password": token,
        }
        self._save()
        logger.info("[ConfigManager] Admin protection config saved")

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
        """Deep copy using copy.deepcopy — safe for all value types."""
        return copy.deepcopy(data)

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