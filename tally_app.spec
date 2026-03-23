# -*- mode: python ; coding: utf-8 -*-
#
# tally_app.spec
# ==============
# PyInstaller build spec for TallySyncManager — Windows EXE
#
# HOW TO BUILD:
#   1. Install PyInstaller:   uv add pyinstaller --dev
#   2. Run from project root: pyinstaller tally_app.spec
#   3. Output EXE is at:      dist\TallySyncManager\TallySyncManager.exe
#
# IMPORTANT: Run this from the project root directory (where main.py lives).
#
# CHANGELOG:
#   - global_config_dialog now supports tally_host / tally_port bulk-apply
#   - app.py save_company_to_db() now accepts tally_host + tally_port params
#   - hooks/hook-cryptography.py overrides the broken _pyinstaller_hooks_contrib
#     hook that crashes on machines with AppLocker/WDAC Application Control
#     policies (DLL load blocked in isolated subprocess).
#     The local hook skips OpenSSL detection entirely; binaries are collected
#     binaries are collected by hooks/hook-cryptography.py instead.
#   - Removed xlwings from excludes (it is a real dependency in pyproject.toml)
#     and added it to hiddenimports so it is bundled correctly.
#   - Fixed python-dotenv exclude entry (module name is 'dotenv', not 'python-dotenv').
#   - XML data files use globs (utils/*.xml, utils/cdc/*.xml, utils/guid/*.xml,
#     utils/reports/*.xml) so added/deleted XMLs are handled automatically at
#     build time — no spec changes needed when XMLs change.
#
# FOLDER LAYOUT REQUIRED:
#   tally_app/
#   ├── main.py
#   ├── tally_app.spec
#   └── hooks/
#       └── hook-cryptography.py   ← must exist (create alongside this spec)

import os
from PyInstaller.utils.hooks import collect_all, collect_submodules, collect_data_files

# ─────────────────────────────────────────────────────────────────────────────
#  Collect packages that need ALL their data files bundled
# ─────────────────────────────────────────────────────────────────────────────

# SQLAlchemy needs its dialect files (mysql, etc.)
sqlalchemy_datas, sqlalchemy_binaries, sqlalchemy_hiddenimports = collect_all('sqlalchemy')

# APScheduler needs its executors, jobstores, triggers
apscheduler_datas, apscheduler_binaries, apscheduler_hiddenimports = collect_all('apscheduler')

# Pillow needs its image format plugins (used by pystray icon generation)
pillow_datas, pillow_binaries, pillow_hiddenimports = collect_all('PIL')

# keyring needs its backend plugins (Windows Credential Store)
keyring_datas, keyring_binaries, keyring_hiddenimports = collect_all('keyring')

# ─────────────────────────────────────────────────────────────────────────────
#  Data files — all non-Python files that must be bundled into the EXE
#  Format: (source_path, dest_folder_inside_bundle)
# ─────────────────────────────────────────────────────────────────────────────
added_datas = [
    # ── XML request templates ────────────────────────────────────────────────
    ('utils/*.xml',         'utils'),
    ('utils/cdc/*.xml',     'utils/cdc'),
    ('utils/guid/*.xml',    'utils/guid'),
    ('utils/reports/*.xml', 'utils/reports'),

    # ── PyAutoGUI screen-detection PNG images ────────────────────────────────
    ('assets/*.png',        'assets'),

    # ── Tally config (log retention, etc.) ───────────────────────────────────
    ('tally_config.ini',    '.'),
]

# Merge in collected package datas
added_datas += sqlalchemy_datas
added_datas += apscheduler_datas
added_datas += pillow_datas
added_datas += keyring_datas

# ─────────────────────────────────────────────────────────────────────────────
#  Hidden imports — modules PyInstaller cannot detect automatically
#  (dynamic imports, plugin systems, lazy imports inside functions)
# ─────────────────────────────────────────────────────────────────────────────
hidden_imports = [

    # ── Database models (all lazy-imported via _get_model() functions) ────────
    'database.models.company',
    'database.models.sync_state',
    'database.models.scheduler_config',
    'database.models.tally_settings',
    'database.models.automation_settings',
    'database.models.ledger',
    'database.models.ledger_voucher',
    'database.models.inventory_voucher',
    'database.models.item',
    'database.models.trial_balance',
    'database.models.base',
    'database.models.outstanding_models',

    # ── SQLAlchemy MySQL dialect — critical for DB connection ─────────────────
    'sqlalchemy.dialects.mysql',
    'sqlalchemy.dialects.mysql.pymysql',
    'pymysql',
    'pymysql.cursors',
    'pymysql.connections',

    # ── APScheduler components ────────────────────────────────────────────────
    'apscheduler.schedulers.background',
    'apscheduler.jobstores.sqlalchemy',
    'apscheduler.jobstores.memory',
    'apscheduler.triggers.interval',
    'apscheduler.triggers.cron',
    'apscheduler.triggers.date',
    'apscheduler.executors.pool',
    'apscheduler.events',

    # ── GUI controllers (lazy-imported in background threads) ─────────────────
    'gui.controllers.company_controller',
    'gui.controllers.sync_controller',
    'gui.controllers.sync_queue_controller',
    'gui.controllers.scheduler_controller',
    'gui.controllers.missed_sync_checker',

    # ── GUI pages (loaded lazily in _load_pages) ──────────────────────────────
    'gui.pages.home_page',
    'gui.pages.sync_page',
    'gui.pages.scheduler_page',
    'gui.pages.logs_page',
    'gui.pages.settings_page',

    # ── GUI components ────────────────────────────────────────────────────────
    'gui.components.company_card',
    'gui.components.configure_company_dialog',
    'gui.components.global_config_dialog',       # ← bulk host/port/creds/type dialog
    'gui.components.setup_wizard',
    'gui.components.initial_snapshot_dialog',
    'gui.components.status_badge',
    'gui.components.sync_progress_panel',
    'gui.components.date_range_picker',
    'gui.components.image_test_overlay',
    'gui.components.voucher_selector',

    # ── GUI core modules ──────────────────────────────────────────────────────
    'gui.app',
    'gui.config_manager',
    'gui.state',
    'gui.styles',
    'gui.tray_manager',
    'gui.scale',

    # ── Services ──────────────────────────────────────────────────────────────
    'services.tally_connector',
    'services.tally_launcher',
    'services.sync_service',
    'services.data_processor',
    'services.currency_extractor',

    # ── Database core ─────────────────────────────────────────────────────────
    'database.db_connector',
    'database.database_processor',

    # ── System tray (pystray) ─────────────────────────────────────────────────
    'pystray',
    'pystray._win32',

    # ── Pillow image plugins needed by pystray icon ───────────────────────────
    'PIL.Image',
    'PIL.ImageDraw',
    'PIL._imaging',

    # ── PyAutoGUI + screen automation ─────────────────────────────────────────
    'pyautogui',
    'cv2',
    'psutil',
    'pygetwindow',

    # ── keyring Windows backend ───────────────────────────────────────────────
    'keyring.backends.Windows',
    'keyring.backends.fail',

    # ── cryptography (used by keyring) ──────────────────────────────────────
    # _rust.pyd is collected as a binary above; these cover the pure-Python
    # wrapper layer that imports it at runtime.
    'cryptography',
    'cryptography.fernet',
    'cryptography.hazmat',
    'cryptography.hazmat.primitives',
    'cryptography.hazmat.primitives.ciphers',
    'cryptography.hazmat.primitives.ciphers.algorithms',
    'cryptography.hazmat.primitives.ciphers.modes',
    'cryptography.hazmat.primitives.hashes',
    'cryptography.hazmat.primitives.hmac',
    'cryptography.hazmat.primitives.padding',
    'cryptography.hazmat.primitives.serialization',
    'cryptography.hazmat.primitives.asymmetric',
    'cryptography.hazmat.primitives.asymmetric.rsa',
    'cryptography.hazmat.backends',
    'cryptography.hazmat.backends.openssl',
    'cryptography.hazmat.bindings._rust',          # the Rust extension itself
    'cryptography.hazmat.bindings._rust.openssl',  # loaded lazily at runtime
    'cryptography.x509',

    # ── tkinter (must be explicitly included on some Python builds) ───────────
    'tkinter',
    'tkinter.ttk',
    'tkinter.messagebox',
    'tkinter.filedialog',

    # ── tkcalendar (optional date picker) ────────────────────────────────────
    'tkcalendar',

    # ── Standard lib modules sometimes missed ────────────────────────────────
    'queue',
    'threading',
    'configparser',
    'logging.handlers',
    'xml.etree.ElementTree',
    'urllib.parse',

    # ── Data packages ─────────────────────────────────────────────────────────
    'pandas',
    'openpyxl',
    'lxml',
    'lxml.etree',
    'xmltodict',

]

# Merge collected hidden imports
hidden_imports += sqlalchemy_hiddenimports
hidden_imports += apscheduler_hiddenimports
hidden_imports += pillow_hiddenimports
hidden_imports += keyring_hiddenimports

# ─────────────────────────────────────────────────────────────────────────────
#  Analysis
# ─────────────────────────────────────────────────────────────────────────────
a = Analysis(
    ['main.py'],
    pathex=['.'],
    binaries=sqlalchemy_binaries + apscheduler_binaries + pillow_binaries + keyring_binaries,
    datas=added_datas,
    hiddenimports=hidden_imports,
    hookspath=['hooks'],   # local hooks/ overrides broken _pyinstaller_hooks_contrib hooks
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # These are not used and bloat the EXE significantly
        'matplotlib',
        'scipy',
        'notebook',
        'IPython',
        'pytest',
        'rq',
        'customtkinter',
        'ttkbootstrap',
        'dotenv',
    ],
    noarchive=False,
    optimize=0,
)

# ─────────────────────────────────────────────────────────────────────────────
#  PYZ archive
# ─────────────────────────────────────────────────────────────────────────────
pyz = PYZ(a.pure)

# ─────────────────────────────────────────────────────────────────────────────
#  EXE
# ─────────────────────────────────────────────────────────────────────────────
exe = EXE(
    pyz,
    a.scripts,
    [],
    exclude_binaries=True,
    name='TallySyncManager',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,            # compress binaries (requires UPX installed — optional)
    console=False,       # NO black terminal window behind the GUI
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    # icon='assets/tally_icon.ico',  # ← uncomment and add a .ico file to enable
)

# ─────────────────────────────────────────────────────────────────────────────
#  COLLECT — gathers EXE + all DLLs + data files into dist\TallySyncManager\
# ─────────────────────────────────────────────────────────────────────────────
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='TallySyncManager',
)