"""
gui/pages/settings_page.py
============================
Settings page — configuration for Tally, DB, Sync, and App.

Phase 3 fixes:
  - Removed all .env file references — now reads/writes via ConfigManager
    (AppData/TallySyncManager/config.json)
  - DB "Edit Credentials" now saves to ConfigManager and reconnects engine,
    not to .env
  - Config file path in version info now shows actual AppData path
  - bind_all("<MouseWheel>") replaced with Enter/Leave scoped binding
  - Tally settings saved to ConfigManager tally section (not tally_config.ini)
  - tally_config.ini still used for advanced settings (timeouts, retries, sync
    defaults, log settings) since these don't belong in the core config

Sections:
  1 — Tally Connection   (host, port, timeout, retries, test)
  2 — Database           (show current, edit + reconnect via ConfigManager)
  3 — Sync Defaults      (mode, chunk, workers)
  4 — Application        (log level, retention, open logs, version info)
"""

import os
import shutil
import configparser
import threading
import tkinter as tk
from tkinter import messagebox, filedialog
from datetime import datetime

from gui.state          import AppState
from gui.styles         import Color, Font, Spacing
from gui.config_manager import ConfigManager

# Advanced settings that don't belong in config.json
# (timeouts, chunk sizes etc. — these stay in tally_config.ini beside the exe)
ADVANCED_CONFIG_FILE = "tally_config.ini"


# ─────────────────────────────────────────────────────────────────────────────
#  Advanced config I/O (tally_config.ini — NOT config.json)
# ─────────────────────────────────────────────────────────────────────────────
def _load_advanced_config() -> dict:
    defaults = {
        "tally_timeout_connect": "60",
        "tally_timeout_read":    "1800",
        "tally_max_retries":     "3",
        "sync_default_mode":     "incremental",
        "sync_chunk_months":     "3",
        "sync_parallel_workers": "2",
        "log_level":             "INFO",
        "log_retention_days":    "30",
        "db_pool_size":          "10",
        "db_pool_recycle":       "3600",
    }
    cfg = configparser.ConfigParser()
    if os.path.exists(ADVANCED_CONFIG_FILE):
        cfg.read(ADVANCED_CONFIG_FILE)
        if "tally" in cfg:
            defaults.update(cfg["tally"])
    return defaults


def _save_advanced_config(data: dict):
    cfg = configparser.ConfigParser()
    if os.path.exists(ADVANCED_CONFIG_FILE):
        cfg.read(ADVANCED_CONFIG_FILE)
    cfg["tally"] = data
    with open(ADVANCED_CONFIG_FILE, "w", encoding="utf-8") as f:
        cfg.write(f)


# ─────────────────────────────────────────────────────────────────────────────
#  Small helper: labeled input row
# ─────────────────────────────────────────────────────────────────────────────
def _field_row(parent, row, label, var, hint="", width=24, secret=False, readonly=False):
    tk.Label(
        parent, text=label,
        font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
        anchor="w", width=22,
    ).grid(row=row, column=0, sticky="w", pady=4)

    entry = tk.Entry(
        parent, textvariable=var,
        font=Font.BODY, width=width,
        bg=Color.BG_INPUT if not readonly else Color.BG_TABLE_HEADER,
        fg=Color.TEXT_PRIMARY,
        relief="solid", bd=1,
        show="●" if secret else "",
        state="normal" if not readonly else "readonly",
    )
    entry.grid(row=row, column=1, sticky="w", pady=4, padx=(Spacing.SM, 0))

    if hint:
        tk.Label(
            parent, text=hint,
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w",
        ).grid(row=row, column=2, sticky="w", padx=(Spacing.SM, 0))

    return entry


# ─────────────────────────────────────────────────────────────────────────────
#  Settings Page
# ─────────────────────────────────────────────────────────────────────────────
class SettingsPage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        # Phase 3: ConfigManager for DB + Tally core config
        self._cfg_manager: ConfigManager = getattr(app, '_config', ConfigManager())

        # Advanced settings (timeouts etc.) from tally_config.ini
        self._adv_cfg = _load_advanced_config()

        self._vars: dict[str, tk.Variable] = {}
        self._db_editing = False
        self._db_entries: dict[str, tk.Entry] = {}

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._build()

    # ─────────────────────────────────────────────────────────────────────────
    #  Master layout
    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        self.rowconfigure(0, weight=1)
        self.rowconfigure(1, weight=0)

        # Scrollable canvas
        self._canvas = tk.Canvas(self, bg=Color.BG_ROOT, highlightthickness=0, bd=0)
        self._canvas.grid(row=0, column=0, sticky="nsew")

        vsb = tk.Scrollbar(self, orient="vertical", command=self._canvas.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self._canvas.configure(yscrollcommand=vsb.set)

        inner = tk.Frame(self._canvas, bg=Color.BG_ROOT)
        inner.columnconfigure(0, weight=1)
        cw = self._canvas.create_window((0, 0), window=inner, anchor="nw")

        inner.bind("<Configure>",
                   lambda e: self._canvas.configure(scrollregion=self._canvas.bbox("all")))
        self._canvas.bind("<Configure>",
                          lambda e: self._canvas.itemconfig(cw, width=e.width))

        # Phase 3: scope mousewheel to canvas only (same fix as home_page)
        self._canvas.bind("<Enter>", self._on_canvas_enter)
        self._canvas.bind("<Leave>", self._on_canvas_leave)

        # Build all sections
        self._build_tally_section(inner, row=0)
        self._build_db_section(inner,    row=1)
        self._build_sync_section(inner,  row=2)
        self._build_app_section(inner,   row=3)
        self._build_automation_section(inner, row=4)   # Phase 3

        # Sticky save bar
        self._build_save_bar()

    def _on_canvas_enter(self, e):
        self._canvas.bind_all("<MouseWheel>",
                               lambda ev: self._canvas.yview_scroll(int(-1 * (ev.delta / 120)), "units"))
        self._canvas.bind_all("<Button-4>",
                               lambda ev: self._canvas.yview_scroll(-1, "units"))
        self._canvas.bind_all("<Button-5>",
                               lambda ev: self._canvas.yview_scroll(1, "units"))

    def _on_canvas_leave(self, e):
        self._canvas.unbind_all("<MouseWheel>")
        self._canvas.unbind_all("<Button-4>")
        self._canvas.unbind_all("<Button-5>")

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 1 — Tally Connection
    # ─────────────────────────────────────────────────────────────────────────
    def _build_tally_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="🔌  Tally Connection")

        # Host + Port from ConfigManager (core config)
        tally_cfg = self._cfg_manager.get_tally_config()
        self._var("tally_host", tally_cfg.get("host", "localhost"))
        self._var("tally_port", str(tally_cfg.get("port", 9000)))

        _field_row(card, 1, "Host", self._vars["tally_host"],
                   hint="IP address or hostname where Tally is running")
        _field_row(card, 2, "Port", self._vars["tally_port"],
                   hint="Default Tally port is 9000", width=8)

        # Advanced: Timeout + Retries (from tally_config.ini)
        self._var("tally_timeout_connect", self._adv_cfg.get("tally_timeout_connect", "60"))
        self._var("tally_timeout_read",    self._adv_cfg.get("tally_timeout_read",    "1800"))

        timeout_row = tk.Frame(card, bg=Color.BG_CARD)
        timeout_row.grid(row=3, column=0, columnspan=3, sticky="w", pady=4)

        tk.Label(timeout_row, text="Timeout  (seconds)",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 anchor="w", width=22).pack(side="left")
        tk.Label(timeout_row, text="Connect:",
                 font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED).pack(side="left", padx=(Spacing.SM, 4))
        tk.Entry(timeout_row, textvariable=self._vars["tally_timeout_connect"],
                 font=Font.BODY, width=6,
                 bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY, relief="solid", bd=1).pack(side="left")
        tk.Label(timeout_row, text="  Read:",
                 font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED).pack(side="left", padx=(Spacing.SM, 4))
        tk.Entry(timeout_row, textvariable=self._vars["tally_timeout_read"],
                 font=Font.BODY, width=7,
                 bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY, relief="solid", bd=1).pack(side="left")
        tk.Label(timeout_row, text="  (Read timeout should be large for big datasets)",
                 font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED).pack(side="left", padx=(Spacing.SM, 0))

        self._var("tally_max_retries", self._adv_cfg.get("tally_max_retries", "3"))
        _field_row(card, 4, "Max Retries", self._vars["tally_max_retries"],
                   hint="Retry failed Tally requests N times before giving up", width=5)

        # Test connection
        test_row = tk.Frame(card, bg=Color.BG_CARD)
        test_row.grid(row=5, column=0, columnspan=3, sticky="w", pady=(Spacing.MD, 0))

        self._tally_test_btn = tk.Button(
            test_row, text="⚡  Test Tally Connection",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._test_tally,
        )
        self._tally_test_btn.pack(side="left")

        self._tally_status_lbl = tk.Label(
            test_row, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._tally_status_lbl.pack(side="left", padx=(Spacing.LG, 0))

        self._tally_info_lbl = tk.Label(
            card, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY, anchor="w",
        )
        self._tally_info_lbl.grid(row=6, column=0, columnspan=3, sticky="w", pady=(4, 0))

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 2 — Database Connection
    # ─────────────────────────────────────────────────────────────────────────
    def _build_db_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="🗄️  Database Connection  (MySQL / MariaDB)")

        # Phase 3: read from ConfigManager, not .env
        db_cfg = self._cfg_manager.get_db_config()

        self._var("db_host",     db_cfg.get("host",     "localhost"))
        self._var("db_port",     str(db_cfg.get("port", 3306)))
        self._var("db_username", db_cfg.get("username", "root"))
        self._var("db_password", db_cfg.get("password", ""))
        self._var("db_database", db_cfg.get("database", ""))

        # Pool settings from advanced config
        self._var("db_pool_size",    self._adv_cfg.get("db_pool_size",    "10"))
        self._var("db_pool_recycle", self._adv_cfg.get("db_pool_recycle", "3600"))

        field_defs = [
            ("Host",     "db_host",     False, "hostname or IP"),
            ("Port",     "db_port",     False, "default 3306"),
            ("Username", "db_username", False, ""),
            ("Password", "db_password", True,  ""),
            ("Database", "db_database", False, ""),
        ]

        for i, (lbl, key, is_secret, hint) in enumerate(field_defs, start=1):
            tk.Label(
                card, text=lbl,
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                anchor="w", width=22,
            ).grid(row=i, column=0, sticky="w", pady=4)

            e = tk.Entry(
                card, textvariable=self._vars[key],
                font=Font.BODY, width=26,
                bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
                show="●" if is_secret else "",
                state="readonly",
            )
            e.grid(row=i, column=1, sticky="w", pady=4, padx=(Spacing.SM, 0))
            self._db_entries[key] = e

            if hint:
                tk.Label(card, text=hint, font=Font.BODY_SM,
                         bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                         ).grid(row=i, column=2, sticky="w", padx=(Spacing.SM, 0))

        # Pool settings
        sep = tk.Frame(card, bg=Color.BORDER, height=1)
        sep.grid(row=len(field_defs)+1, column=0, columnspan=3, sticky="ew",
                 pady=(Spacing.MD, Spacing.SM))

        pool_row = tk.Frame(card, bg=Color.BG_CARD)
        pool_row.grid(row=len(field_defs)+2, column=0, columnspan=3, sticky="w")

        tk.Label(pool_row, text="Connection Pool Size:",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=22, anchor="w").pack(side="left")
        tk.Entry(pool_row, textvariable=self._vars["db_pool_size"],
                 font=Font.BODY, width=6,
                 bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                 relief="solid", bd=1).pack(side="left", padx=(Spacing.SM, Spacing.LG))
        tk.Label(pool_row, text="Recycle (seconds):",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY).pack(side="left")
        tk.Entry(pool_row, textvariable=self._vars["db_pool_recycle"],
                 font=Font.BODY, width=8,
                 bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                 relief="solid", bd=1).pack(side="left", padx=(Spacing.SM, 0))

        # Action buttons
        btn_row = tk.Frame(card, bg=Color.BG_CARD)
        btn_row.grid(row=len(field_defs)+3, column=0, columnspan=3, sticky="w",
                     pady=(Spacing.MD, 0))

        self._db_edit_btn = tk.Button(
            btn_row, text="✎  Edit Credentials",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._toggle_db_edit,
        )
        self._db_edit_btn.pack(side="left", padx=(0, Spacing.SM))

        self._db_test_btn = tk.Button(
            btn_row, text="⚡  Test DB Connection",
            font=Font.BUTTON_SM, bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._test_db,
        )
        self._db_test_btn.pack(side="left", padx=(0, Spacing.SM))

        self._db_apply_btn = tk.Button(
            btn_row, text="✔  Apply & Reconnect",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._apply_db_changes,
        )
        # Hidden until edit mode
        self._db_apply_btn.pack_forget()

        self._db_status_lbl = tk.Label(
            btn_row, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._db_status_lbl.pack(side="left", padx=(Spacing.SM, 0))

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 3 — Sync Defaults
    # ─────────────────────────────────────────────────────────────────────────
    def _build_sync_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="🔄  Sync Defaults")

        self._var("sync_default_mode",     self._adv_cfg.get("sync_default_mode",     "incremental"))
        self._var("sync_chunk_months",     self._adv_cfg.get("sync_chunk_months",     "3"))
        self._var("sync_parallel_workers", self._adv_cfg.get("sync_parallel_workers", "2"))

        tk.Label(card, text="Default Sync Mode",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 anchor="w", width=22).grid(row=1, column=0, sticky="w", pady=4)

        mode_frame = tk.Frame(card, bg=Color.BG_CARD)
        mode_frame.grid(row=1, column=1, columnspan=2, sticky="w", padx=(Spacing.SM, 0))

        for val, lbl in [
            ("incremental", "Incremental  (CDC — recommended)"),
            ("snapshot",    "Initial Snapshot  (full pull)"),
        ]:
            tk.Radiobutton(
                mode_frame, text=lbl, value=val,
                variable=self._vars["sync_default_mode"],
                font=Font.BODY, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
                fg=Color.TEXT_PRIMARY,
            ).pack(side="left", padx=(0, Spacing.LG))

        _field_row(card, 2, "Snapshot Chunk Size  (months)",
                   self._vars["sync_chunk_months"],
                   hint="Months of data fetched per API call. Lower = safer, higher = faster.",
                   width=5)

        _field_row(card, 3, "Parallel Voucher Workers",
                   self._vars["sync_parallel_workers"],
                   hint="Concurrent threads per company for voucher types. (2 recommended)",
                   width=5)

        tk.Label(
            card,
            text="ℹ  These are defaults. You can override them per sync run on the Sync page.",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w",
        ).grid(row=4, column=0, columnspan=3, sticky="w", pady=(Spacing.SM, 0))

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 4 — Application
    # ─────────────────────────────────────────────────────────────────────────
    def _build_app_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="⚙️  Application")

        self._var("log_level",          self._adv_cfg.get("log_level",          "INFO"))
        self._var("log_retention_days", self._adv_cfg.get("log_retention_days", "30"))

        tk.Label(card, text="Log Level",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 anchor="w", width=22).grid(row=1, column=0, sticky="w", pady=4)

        level_frame = tk.Frame(card, bg=Color.BG_CARD)
        level_frame.grid(row=1, column=1, columnspan=2, sticky="w", padx=(Spacing.SM, 0))

        for lvl in ["DEBUG", "INFO", "WARNING", "ERROR"]:
            tk.Radiobutton(
                level_frame, text=lvl, value=lvl,
                variable=self._vars["log_level"],
                font=Font.BODY, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
                fg=Color.TEXT_PRIMARY,
            ).pack(side="left", padx=(0, Spacing.MD))

        _field_row(card, 2, "Log Retention  (days)",
                   self._vars["log_retention_days"],
                   hint="Auto-delete log files older than N days  (0 = keep forever)",
                   width=6)

        sep = tk.Frame(card, bg=Color.BORDER, height=1)
        sep.grid(row=3, column=0, columnspan=3, sticky="ew", pady=(Spacing.MD, Spacing.SM))

        btn_row = tk.Frame(card, bg=Color.BG_CARD)
        btn_row.grid(row=4, column=0, columnspan=3, sticky="w")

        tk.Button(
            btn_row, text="📁  Open Logs Folder",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._open_logs_folder,
        ).pack(side="left", padx=(0, Spacing.SM))

        tk.Button(
            btn_row, text="🗑  Clean Old Logs",
            font=Font.BUTTON_SM, bg=Color.DANGER_BG, fg=Color.DANGER_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._clean_old_logs,
        ).pack(side="left")

        sep2 = tk.Frame(card, bg=Color.BORDER, height=1)
        sep2.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(Spacing.MD, Spacing.SM))

        # Version info — Phase 3: show real AppData config path
        info_frame = tk.Frame(card, bg=Color.BG_CARD)
        info_frame.grid(row=6, column=0, columnspan=3, sticky="w")

        from gui.styles import APP_VERSION
        for label, value in [
            ("App Version",    APP_VERSION),
            ("Config File",    self._cfg_manager.config_path),  # ← real AppData path
            ("Config Folder",  self._cfg_manager.config_folder),
            ("Advanced Config", os.path.abspath(ADVANCED_CONFIG_FILE)),
        ]:
            row_f = tk.Frame(info_frame, bg=Color.BG_CARD)
            row_f.pack(fill="x", pady=1)
            tk.Label(row_f, text=f"{label}:",
                     font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                     anchor="w", width=20).pack(side="left")
            lbl = tk.Label(row_f, text=value,
                           font=Font.MONO_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                           anchor="w", cursor="hand2")
            lbl.pack(side="left")
            # Click to copy path
            lbl.bind("<Button-1>", lambda e, v=value: self._copy_to_clipboard(v))

        tk.Label(
            info_frame,
            text="💡 Click any path to copy it to clipboard",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).pack(anchor="w", pady=(4, 0))

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 5 — Automation  (Phase 3)
    # ─────────────────────────────────────────────────────────────────────────
    def _build_automation_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="🤖  Tally Automation  (PyAutoGUI)")

        # ── Sub-section A: Tally exe path ────────────────────────────────────
        tk.Label(
            card, text="Tally Executable",
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            anchor="w",
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 6))

        self._var("tally_exe_path", getattr(self.state, 'tally_exe_path', '') or '')

        tk.Label(card, text="Tally.exe Path:",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 anchor="w", width=20).grid(row=1, column=0, sticky="w", pady=4)

        exe_row = tk.Frame(card, bg=Color.BG_CARD)
        exe_row.grid(row=1, column=1, columnspan=2, sticky="w", pady=4)

        tk.Entry(
            exe_row, textvariable=self._vars["tally_exe_path"],
            font=Font.BODY, width=38,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        ).pack(side="left", padx=(0, 6))

        tk.Button(
            exe_row, text="Browse…",
            font=Font.BUTTON_SM,
            bg=Color.PRIMARY_LIGHT, fg=Color.PRIMARY,
            relief="solid", bd=1, padx=8, pady=3, cursor="hand2",
            command=self._browse_tally_exe,
        ).pack(side="left")

        tk.Label(
            card, text="e.g.  C:\\Program Files\\TallyPrime\\tally.exe",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w",
        ).grid(row=2, column=1, columnspan=2, sticky="w", pady=(0, 8))

        # ── Separator ────────────────────────────────────────────────────────
        tk.Frame(card, bg=Color.BORDER, height=1).grid(
            row=3, column=0, columnspan=3, sticky="ew", pady=(4, 12))

        # ── Sub-section B: PyAutoGUI Controls ────────────────────────────────
        tk.Label(
            card, text="PyAutoGUI Controls",
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            anchor="w",
        ).grid(row=4, column=0, columnspan=3, sticky="w", pady=(0, 6))

        aut = getattr(self.state, 'automation', None)
        self._var("automation_confidence",   str(getattr(aut, 'confidence',       0.80)))
        self._var("automation_click_delay",  str(getattr(aut, 'click_delay_ms',   500)))
        self._var("automation_timeout",      str(getattr(aut, 'wait_timeout_sec', 30)))
        self._var("automation_retries",      str(getattr(aut, 'retry_attempts',   3)))

        ctrl_rows = [
            ("Confidence Threshold", "automation_confidence",
             "0.50–1.00 — how closely screenshots must match. Start at 0.80."),
            ("Click Delay  (ms)",    "automation_click_delay",
             "Milliseconds between PyAutoGUI actions. Increase on slow PCs."),
            ("Wait Timeout  (sec)",  "automation_timeout",
             "Seconds to wait for a screen image before giving up."),
            ("Retry Attempts",       "automation_retries",
             "Times to retry a failed image search before marking error."),
        ]

        for i, (lbl, key, hint) in enumerate(ctrl_rows, start=5):
            tk.Label(card, text=lbl,
                     font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                     anchor="w", width=24,
                     ).grid(row=i, column=0, sticky="w", pady=4)
            tk.Entry(card, textvariable=self._vars[key],
                     font=Font.BODY, width=8,
                     bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                     relief="solid", bd=1,
                     ).grid(row=i, column=1, sticky="w", pady=4, padx=(Spacing.SM, 0))
            tk.Label(card, text=hint,
                     font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w",
                     ).grid(row=i, column=2, sticky="w", padx=(Spacing.SM, 0))

        # ── Separator ────────────────────────────────────────────────────────
        tk.Frame(card, bg=Color.BORDER, height=1).grid(
            row=9, column=0, columnspan=3, sticky="ew", pady=(8, 12))

        # ── Sub-section C: Screen Images ─────────────────────────────────────
        tk.Label(
            card, text="Screen Images  (screenshots used for image recognition)",
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            anchor="w",
        ).grid(row=10, column=0, columnspan=3, sticky="w", pady=(0, 4))

        tk.Label(
            card,
            text=(
                "If images don't match on this PC, use Browse to replace them with "
                "fresh screenshots.\n"
                "Use Test to verify each image is found on screen before running automation."
            ),
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            anchor="w", justify="left",
        ).grid(row=11, column=0, columnspan=3, sticky="w", pady=(0, 8))

        # Column headers
        hdr = tk.Frame(card, bg=Color.BG_TABLE_HEADER)
        hdr.grid(row=12, column=0, columnspan=3, sticky="ew", pady=(0, 2))
        for col_txt, col_w in [("Image Name", 24), ("File", 28), ("Actions", 20)]:
            tk.Label(hdr, text=col_txt,
                     font=Font.BODY_SM, bg=Color.BG_TABLE_HEADER,
                     fg=Color.TEXT_SECONDARY, anchor="w",
                     width=col_w).pack(side="left", padx=(8, 0), pady=4)

        # Image rows — key matches state.tally_images dict
        self._image_rows = {}   # key → {result_lbl, filename_var}
        image_defs = [
            ("gateway",      "Gateway Screen",          "Tally gateway / company list screen"),
            ("search_box",   "Company Search Box",       "Search field in the company list"),
            ("username",     "Username Field",           "Login prompt username input"),
            ("password",     "Password Field",           "Login prompt password input"),
            ("select_title", "Select Company Title",     "Header text of select company screen"),
            ("change_path",  "Change Path Button",       "Button to change data directory"),
            ("remote_tab",   "Remote Tab",               "Remote/TDS tab in Gateway"),
            ("tds_field",    "TDS Server Field",         "TDS server IP input field"),
        ]

        tally_images = getattr(self.state, 'tally_images', {})

        for r_idx, (key, label, tooltip) in enumerate(image_defs):
            row_num = 13 + r_idx
            bg = Color.BG_CARD if r_idx % 2 == 0 else Color.BG_TABLE_HEADER

            row_f = tk.Frame(card, bg=bg)
            row_f.grid(row=row_num, column=0, columnspan=3, sticky="ew", pady=1)
            row_f.columnconfigure(1, weight=1)

            # Label
            tk.Label(row_f, text=label,
                     font=Font.BODY, bg=bg, fg=Color.TEXT_PRIMARY,
                     anchor="w", width=24,
                     ).grid(row=0, column=0, sticky="w", padx=(8, 0), pady=6)

            # Filename display
            current_filename = tally_images.get(key, f"tally_{key}.png")
            fname_var = tk.StringVar(value=current_filename)

            tk.Label(row_f, textvariable=fname_var,
                     font=Font.MONO_SM, bg=bg, fg=Color.TEXT_SECONDARY,
                     anchor="w", width=30,
                     ).grid(row=0, column=1, sticky="w", padx=(8, 0))

            # Buttons
            btn_f = tk.Frame(row_f, bg=bg)
            btn_f.grid(row=0, column=2, sticky="e", padx=(0, 8))

            tk.Button(
                btn_f, text="Browse",
                font=Font.BUTTON_SM,
                bg=Color.PRIMARY_LIGHT, fg=Color.PRIMARY,
                relief="solid", bd=1, padx=8, pady=2, cursor="hand2",
                command=lambda k=key, fv=fname_var: self._browse_image(k, fv),
            ).pack(side="left", padx=(0, 4))

            result_lbl = tk.Label(
                btn_f, text="",
                font=Font.BODY_SM, bg=bg, fg=Color.TEXT_MUTED, width=22, anchor="w",
            )
            result_lbl.pack(side="left")

            tk.Button(
                btn_f, text="Test",
                font=Font.BUTTON_SM,
                bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1, padx=8, pady=2, cursor="hand2",
                command=lambda k=key, rl=result_lbl: self._test_image(k, rl),
            ).pack(side="left", padx=(4, 0))

            self._image_rows[key] = {"result_lbl": result_lbl, "filename_var": fname_var}

        # ── Test All + Save buttons ───────────────────────────────────────────
        end_row = 13 + len(image_defs)

        action_row = tk.Frame(card, bg=Color.BG_CARD)
        action_row.grid(row=end_row, column=0, columnspan=3, sticky="w", pady=(12, 0))

        tk.Button(
            action_row, text="⚡  Test All Images",
            font=Font.BUTTON_SM,
            bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2",
            command=self._test_all_images,
        ).pack(side="left", padx=(0, Spacing.SM))

        self._test_all_lbl = tk.Label(
            action_row, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._test_all_lbl.pack(side="left", padx=(Spacing.SM, 0))

        tk.Button(
            action_row, text="✓  Save Automation Settings",
            font=Font.BUTTON_SM,
            bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2",
            command=self._save_automation,
        ).pack(side="right")

        self._automation_save_lbl = tk.Label(
            card, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._automation_save_lbl.grid(
            row=end_row + 1, column=0, columnspan=3, sticky="w", pady=(4, 0))

    # ─────────────────────────────────────────────────────────────────────────
    #  Automation helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _browse_tally_exe(self):
        path = filedialog.askopenfilename(
            title="Select Tally.exe",
            filetypes=[("Executable", "*.exe"), ("All files", "*.*")],
            parent=self,
        )
        if path:
            self._vars["tally_exe_path"].set(os.path.normpath(path))

    def _browse_image(self, key: str, fname_var: tk.StringVar):
        """Let user pick a PNG → copy to assets/ → update filename in state + DB."""
        path = filedialog.askopenfilename(
            title=f"Select screenshot for: {key}",
            filetypes=[("PNG Images", "*.png"), ("All images", "*.png *.jpg *.bmp")],
            parent=self,
        )
        if not path:
            return

        # Copy to assets folder
        assets_dir = os.path.join(os.path.dirname(os.path.dirname(
            os.path.dirname(os.path.abspath(__file__)))), "assets")
        os.makedirs(assets_dir, exist_ok=True)

        # Use a consistent naming: tally_<key>.png
        dest_name = f"tally_{key}.png"
        dest_path = os.path.join(assets_dir, dest_name)

        try:
            shutil.copy2(path, dest_path)
        except Exception as e:
            messagebox.showerror("Copy Failed", f"Could not copy image:\n{e}")
            return

        # Update UI
        fname_var.set(dest_name)

        # Update state
        if not hasattr(self.state, 'tally_images') or self.state.tally_images is None:
            self.state.tally_images = {}
        self.state.tally_images[key] = dest_name

        # Persist to DB
        self._save_image_to_db(key, dest_name)

        row_data = self._image_rows.get(key)
        if row_data:
            row_data["result_lbl"].configure(
                text="✓ Replaced", fg=Color.SUCCESS)
            self.after(3000, lambda: row_data["result_lbl"].configure(text=""))

    def _save_image_to_db(self, key: str, filename: str):
        """Persist a single image filename change to tally_settings table."""
        engine = self.state.db_engine
        if not engine:
            return
        col_map = {
            "gateway":      "image_gateway",
            "search_box":   "image_search_box",
            "username":     "image_username",
            "password":     "image_password",
            "select_title": "image_select_title",
            "change_path":  "image_change_path",
            "remote_tab":   "image_remote_tab",
            "tds_field":    "image_tds_field",
        }
        col = col_map.get(key)
        if not col:
            return
        try:
            from sqlalchemy.orm import sessionmaker
            from database.models.tally_settings import TallySettings
            Session = sessionmaker(bind=engine)
            db = Session()
            try:
                ts = db.query(TallySettings).filter_by(id=1).first()
                if not ts:
                    ts = TallySettings(id=1)
                    db.add(ts)
                setattr(ts, col, filename)
                db.commit()
            finally:
                db.close()
        except Exception as e:
            from logging_config import logger
            logger.error(f"[Settings] Failed to save image to DB: {e}")

    def _test_image(self, key: str, result_lbl: tk.Label):
        """
        Test image matching with a 20-second wait loop.

        Flow:
          1. Show countdown in result label (so user can switch to Tally)
          2. Minimize app window after 3s countdown
          3. Try locateOnScreen every second for up to 20 seconds
          4. If found → show overlay + restore app
          5. If not found in 20s → show error with confidence hint
        """
        try:
            import pyautogui
        except ImportError:
            result_lbl.configure(text="✗ pyautogui not installed", fg=Color.DANGER)
            return

        images     = getattr(self.state, 'tally_images', {})
        filename   = images.get(key, f"tally_{key}.png")
        assets_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "assets",
        )
        img_path = os.path.join(assets_dir, filename)

        if not os.path.exists(img_path):
            result_lbl.configure(text="✗ File missing — use Browse to add it", fg=Color.DANGER)
            return

        confidence = float(
            self._vars.get("automation_confidence", tk.StringVar(value="0.80")).get() or 0.80
        )
        root = self.winfo_toplevel()

        # ── Phase 1: 3s countdown so user can navigate to Tally ─────────────
        COUNTDOWN = 3
        WAIT_SECS = 20

        def _tick(remaining):
            """Countdown on main thread before minimizing."""
            if remaining > 0:
                result_lbl.configure(
                    text=f"⏳ Switch to Tally… {remaining}s",
                    fg=Color.WARNING_FG if hasattr(Color, "WARNING_FG") else Color.TEXT_MUTED,
                )
                self.after(1000, lambda: _tick(remaining - 1))
            else:
                result_lbl.configure(text="🔍 Searching (20s)…", fg=Color.TEXT_MUTED)
                # Minimize after countdown finishes
                try:
                    root.iconify()
                except Exception:
                    pass
                threading.Thread(target=_search_worker, daemon=True).start()

        def _search_worker():
            """Background: try locateOnScreen every second for WAIT_SECS."""
            import time
            time.sleep(0.4)   # let minimize animation complete

            loc      = None
            deadline = time.time() + WAIT_SECS

            while time.time() < deadline:
                try:
                    loc = pyautogui.locateOnScreen(
                        img_path, confidence=confidence, grayscale=True
                    )
                    if loc:
                        break
                except Exception:
                    pass
                # Update countdown label every second
                remaining = max(0, int(deadline - time.time()))
                root.after(0, lambda r=remaining: result_lbl.configure(
                    text=f"🔍 Searching… {r}s left", fg=Color.TEXT_MUTED
                ))
                time.sleep(1)

            # Restore app
            root.after(0, root.deiconify)

            if loc:
                x = int(loc.left)
                y = int(loc.top)
                w = int(loc.width)
                h = int(loc.height)

                def _show_found():
                    result_lbl.configure(text=f"✓ Found at ({x}, {y})", fg=Color.SUCCESS)
                    try:
                        from gui.components.image_test_overlay import ImageTestOverlay
                        ImageTestOverlay(root, x, y, w, h, duration_ms=3000)
                    except Exception:
                        pass
                    self.after(5000, lambda: result_lbl.configure(text=""))

                root.after(400, _show_found)

            else:
                lower_conf = max(0.50, round(confidence - 0.10, 2))

                def _show_not_found():
                    result_lbl.configure(
                        text=f"✗ Not found — try confidence {lower_conf:.2f}",
                        fg=Color.DANGER,
                    )
                    self.after(8000, lambda: result_lbl.configure(text=""))

                root.after(400, _show_not_found)

        # Start the countdown on the main thread
        _tick(COUNTDOWN)


    def _test_all_images(self):
        """Test all 8 images sequentially, show summary when done."""
        try:
            import pyautogui
        except ImportError:
            self._test_all_lbl.configure(
                text="✗ pyautogui not installed — pip install pyautogui",
                fg=Color.DANGER)
            return

        keys = list(self._image_rows.keys())
        total = len(keys)

        self._test_all_lbl.configure(
            text=f"Testing 1/{total}…", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        results = {}   # key → bool

        def run_all():
            import time
            root = self.winfo_toplevel()

            try:
                root.iconify()
            except Exception:
                pass
            time.sleep(0.6)

            confidence = float(self._vars.get(
                "automation_confidence", tk.StringVar(value="0.80")).get() or 0.80)

            assets_dir = os.path.join(os.path.dirname(os.path.dirname(
                os.path.dirname(os.path.abspath(__file__)))), "assets")

            images = getattr(self.state, 'tally_images', {})

            for idx, key in enumerate(keys):
                root.after(0, lambda i=idx: self._test_all_lbl.configure(
                    text=f"Testing {i+1}/{total}…", fg=Color.TEXT_MUTED))

                filename = images.get(key, f"tally_{key}.png")
                img_path = os.path.join(assets_dir, filename)

                found = False
                if os.path.exists(img_path):
                    try:
                        loc = pyautogui.locateOnScreen(
                            img_path, confidence=confidence, grayscale=True
                        )
                        found = loc is not None
                    except Exception:
                        found = False

                results[key] = found

                # Update individual row result label
                row_data = self._image_rows.get(key)
                if row_data:
                    rl = row_data["result_lbl"]
                    if found:
                        root.after(0, lambda l=rl: l.configure(
                            text="✓ Found", fg=Color.SUCCESS))
                    else:
                        root.after(0, lambda l=rl: l.configure(
                            text="✗ Not found", fg=Color.DANGER))

                time.sleep(0.3)

            try:
                root.after(0, root.deiconify)
            except Exception:
                pass

            found_count = sum(1 for v in results.values() if v)
            miss_count  = total - found_count

            def show_summary():
                if miss_count == 0:
                    self._test_all_lbl.configure(
                        text=f"✓ All {total} images found on screen",
                        fg=Color.SUCCESS)
                else:
                    self._test_all_lbl.configure(
                        text=f"{found_count}/{total} found — {miss_count} need attention",
                        fg=Color.WARNING_FG if hasattr(Color, "WARNING_FG") else Color.DANGER)

            root.after(700, show_summary)

        threading.Thread(target=run_all, daemon=True).start()

    def _save_automation(self):
        """Validate and persist automation settings (controls + exe path + images) to DB."""
        errors = []

        conf_str = self._vars.get("automation_confidence", tk.StringVar(value="0.80")).get().strip()
        try:
            conf = float(conf_str)
            if not (0.5 <= conf <= 1.0):
                errors.append("Confidence must be between 0.50 and 1.00")
        except ValueError:
            errors.append("Confidence must be a decimal number (e.g. 0.80)")

        for key, label in [
            ("automation_click_delay", "Click Delay"),
            ("automation_timeout",     "Wait Timeout"),
            ("automation_retries",     "Retry Attempts"),
        ]:
            val = self._vars.get(key, tk.StringVar()).get().strip()
            if val and not val.isdigit():
                errors.append(f"{label} must be a whole number")

        if errors:
            messagebox.showerror("Validation Error", "\n".join(errors))
            return

        engine = self.state.db_engine
        if not engine:
            self._automation_save_lbl.configure(
                text="✗ No DB connection — cannot save", fg=Color.DANGER)
            return

        try:
            from sqlalchemy.orm import sessionmaker
            from database.models.tally_settings      import TallySettings
            from database.models.automation_settings import AutomationSettings
            from gui.state import AutomationConfig

            Session = sessionmaker(bind=engine)
            db = Session()
            try:
                # ── Save TallySettings ────────────────────────────────────────
                ts = db.query(TallySettings).filter_by(id=1).first()
                if not ts:
                    ts = TallySettings(id=1)
                    db.add(ts)

                ts.exe_path = self._vars["tally_exe_path"].get().strip() or None

                # Save all image filenames
                images = getattr(self.state, 'tally_images', {})
                col_map = {
                    "gateway":      "image_gateway",
                    "search_box":   "image_search_box",
                    "username":     "image_username",
                    "password":     "image_password",
                    "select_title": "image_select_title",
                    "change_path":  "image_change_path",
                    "remote_tab":   "image_remote_tab",
                    "tds_field":    "image_tds_field",
                }
                for k, col in col_map.items():
                    row_data = self._image_rows.get(k)
                    if row_data:
                        fname = row_data["filename_var"].get().strip()
                        if fname:
                            setattr(ts, col, fname)
                            images[k] = fname

                # ── Save AutomationSettings ───────────────────────────────────
                aut = db.query(AutomationSettings).filter_by(id=1).first()
                if not aut:
                    aut = AutomationSettings(id=1)
                    db.add(aut)

                aut.confidence       = float(self._vars["automation_confidence"].get() or 0.80)
                aut.click_delay_ms   = int(self._vars["automation_click_delay"].get()  or 500)
                aut.wait_timeout_sec = int(self._vars["automation_timeout"].get()       or 30)
                aut.retry_attempts   = int(self._vars["automation_retries"].get()       or 3)

                db.commit()

                # ── Update live AppState ──────────────────────────────────────
                self.state.tally_exe_path = ts.exe_path or ""
                self.state.tally_images   = images
                self.state.automation     = AutomationConfig(
                    confidence       = aut.confidence,
                    click_delay_ms   = aut.click_delay_ms,
                    wait_timeout_sec = aut.wait_timeout_sec,
                    retry_attempts   = aut.retry_attempts,
                )

                self._automation_save_lbl.configure(
                    text=f"✓ Saved at {datetime.now().strftime('%H:%M:%S')}",
                    fg=Color.SUCCESS)
                self.after(4000, lambda: self._automation_save_lbl.configure(text=""))

            finally:
                db.close()

        except Exception as e:
            from logging_config import logger
            logger.error(f"[Settings] Failed to save automation settings: {e}")
            self._automation_save_lbl.configure(
                text=f"✗ Save failed: {str(e)[:60]}", fg=Color.DANGER)

    # ─────────────────────────────────────────────────────────────────────────
    #  Save bar
    # ─────────────────────────────────────────────────────────────────────────
    def _build_save_bar(self):
        bar = tk.Frame(
            self, bg=Color.BG_HEADER,
            highlightthickness=1, highlightbackground=Color.BORDER,
            pady=Spacing.MD, padx=Spacing.XL,
        )
        bar.grid(row=1, column=0, columnspan=2, sticky="ew")
        bar.columnconfigure(0, weight=1)

        self._save_status_lbl = tk.Label(
            bar, text="",
            font=Font.BODY_SM, bg=Color.BG_HEADER, fg=Color.TEXT_MUTED,
        )
        self._save_status_lbl.grid(row=0, column=0, sticky="w")

        btns = tk.Frame(bar, bg=Color.BG_HEADER)
        btns.grid(row=0, column=1)

        tk.Button(
            btns, text="↺  Reset to Defaults",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=Spacing.MD, pady=5,
            cursor="hand2", command=self._reset_defaults,
        ).pack(side="left", padx=(0, Spacing.SM))

        tk.Button(
            btns, text="✓  Save Settings",
            font=Font.BUTTON, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XL, pady=5,
            cursor="hand2", command=self._save,
        ).pack(side="left")

    # ─────────────────────────────────────────────────────────────────────────
    #  UI helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _make_card(self, parent, row: int, title: str) -> tk.Frame:
        outer = tk.Frame(
            parent, bg=Color.BG_CARD,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        outer.grid(row=row, column=0, sticky="ew", padx=Spacing.XL, pady=(0, Spacing.MD))
        outer.columnconfigure(0, weight=1)

        hdr = tk.Frame(outer, bg=Color.PRIMARY, pady=Spacing.SM)
        hdr.grid(row=0, column=0, sticky="ew")
        tk.Label(
            hdr, text=title,
            font=Font.LABEL_BOLD, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            anchor="w", padx=Spacing.LG,
        ).pack(fill="x")

        content = tk.Frame(outer, bg=Color.BG_CARD, padx=Spacing.XL, pady=Spacing.LG)
        content.grid(row=1, column=0, sticky="ew")
        content.columnconfigure(1, weight=1)
        return content

    def _var(self, key: str, default: str = "") -> tk.StringVar:
        if key not in self._vars:
            self._vars[key] = tk.StringVar(value=str(default))
        return self._vars[key]

    def _copy_to_clipboard(self, text: str):
        self.clipboard_clear()
        self.clipboard_append(text)
        self._save_status_lbl.configure(text=f"Copied: {text[:60]}", fg=Color.TEXT_MUTED)
        self.after(3000, lambda: self._save_status_lbl.configure(text=""))

    # ─────────────────────────────────────────────────────────────────────────
    #  Test Tally
    # ─────────────────────────────────────────────────────────────────────────
    def _test_tally(self):
        self._tally_test_btn.configure(state="disabled", text="Testing...")
        self._tally_status_lbl.configure(text="Connecting...", fg=Color.TEXT_MUTED)
        self._tally_info_lbl.configure(text="")
        self.update_idletasks()

        host    = self._vars["tally_host"].get().strip()
        port    = self._vars["tally_port"].get().strip()
        timeout = (
            int(self._vars["tally_timeout_connect"].get() or 60),
            int(self._vars["tally_timeout_read"].get()    or 1800),
        )
        retries = int(self._vars["tally_max_retries"].get() or 3)

        def worker():
            try:
                from services.tally_connector import TallyConnector
                tc = TallyConnector(
                    host=host, port=int(port),
                    timeout=timeout, max_retries=retries,
                )
                connected = (tc.status == "Connected")
                self.after(0, lambda: self._on_tally_test_result(connected, host, port))
            except Exception as e:
                self.after(0, lambda err=e: self._on_tally_test_result(False, host, port, str(err)))

        threading.Thread(target=worker, daemon=True).start()

    def _on_tally_test_result(self, ok: bool, host: str, port: str, err: str = ""):
        self._tally_test_btn.configure(state="normal", text="⚡  Test Tally Connection")
        if ok:
            self._tally_status_lbl.configure(text="✓  Connected successfully", fg=Color.SUCCESS)
            self._tally_info_lbl.configure(
                text=f"Tally is reachable at  {host}:{port}", fg=Color.TEXT_SECONDARY,
            )
            self.state.tally.host       = host
            self.state.tally.port       = int(port)
            self.state.tally.connected  = True
            self.state.tally.last_check = datetime.now()
        else:
            self._tally_status_lbl.configure(
                text=f"✗  Failed — {err or 'Connection refused'}", fg=Color.DANGER,
            )
            self.state.tally.connected = False

    # ─────────────────────────────────────────────────────────────────────────
    #  DB edit / apply — Phase 3: saves via ConfigManager, not .env
    # ─────────────────────────────────────────────────────────────────────────
    def _toggle_db_edit(self):
        self._db_editing = not self._db_editing
        new_state = "normal" if self._db_editing else "readonly"
        active_bg = Color.BG_INPUT if self._db_editing else Color.BG_TABLE_HEADER

        for key, entry in self._db_entries.items():
            entry.configure(state=new_state, bg=active_bg)

        if self._db_editing:
            self._db_edit_btn.configure(
                text="✖  Cancel Edit",
                bg=Color.DANGER_BG if hasattr(Color, "DANGER_BG") else "#fde8e8",
                fg=Color.DANGER, relief="flat",
            )
            self._db_apply_btn.pack(side="left", padx=(0, Spacing.SM))
            self._db_status_lbl.configure(
                text="Edit credentials, then Test or Apply & Reconnect.",
                fg=Color.TEXT_MUTED,
            )
            self._db_entries["db_host"].focus_set()
        else:
            # Revert to saved values from ConfigManager
            db_cfg = self._cfg_manager.get_db_config()
            self._vars["db_host"].set(db_cfg.get("host",     "localhost"))
            self._vars["db_port"].set(str(db_cfg.get("port", 3306)))
            self._vars["db_username"].set(db_cfg.get("username", "root"))
            self._vars["db_password"].set(db_cfg.get("password", ""))
            self._vars["db_database"].set(db_cfg.get("database", ""))
            self._db_edit_btn.configure(
                text="✎  Edit Credentials",
                bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY, relief="solid",
            )
            self._db_apply_btn.pack_forget()
            self._db_status_lbl.configure(text="", fg=Color.TEXT_MUTED)

    def _get_live_db_cfg(self) -> dict:
        return {
            "host":     self._vars["db_host"].get().strip(),
            "port":     self._vars["db_port"].get().strip(),
            "username": self._vars["db_username"].get().strip(),
            "password": self._vars["db_password"].get(),
            "database": self._vars["db_database"].get().strip(),
        }

    def _test_db(self):
        self._db_test_btn.configure(state="disabled", text="Testing...")
        self._db_status_lbl.configure(text="Connecting...", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        cfg = self._get_live_db_cfg()

        def worker():
            try:
                from database.db_connector import DatabaseConnector
                conn = DatabaseConnector(
                    username=cfg.get("username", "root"),
                    password=cfg.get("password", ""),
                    host=cfg.get("host", "localhost"),
                    port=int(cfg.get("port", 3306)),
                    database=cfg.get("database", ""),
                )
                ok = conn.test_connection()
                self.after(0, lambda: self._on_db_test_result(ok))
            except Exception as e:
                self.after(0, lambda err=e: self._on_db_test_result(False, str(err)))

        threading.Thread(target=worker, daemon=True).start()

    def _on_db_test_result(self, ok: bool, err: str = ""):
        self._db_test_btn.configure(state="normal", text="⚡  Test DB Connection")
        if ok:
            self._db_status_lbl.configure(text="✓  Connected", fg=Color.SUCCESS)
        else:
            self._db_status_lbl.configure(
                text=f"✗  {err or 'Connection failed'}", fg=Color.DANGER,
            )

    def _apply_db_changes(self):
        """
        Test new credentials, save via ConfigManager, reconnect engine.
        Phase 3: writes to config.json in AppData, not .env
        """
        cfg = self._get_live_db_cfg()

        if not cfg["database"]:
            messagebox.showerror("Validation Error", "Database name cannot be empty.")
            return
        if not str(cfg["port"]).isdigit():
            messagebox.showerror("Validation Error", "Port must be a number.")
            return

        self._db_apply_btn.configure(state="disabled", text="Applying...")
        self._db_status_lbl.configure(text="Testing connection...", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        def worker():
            try:
                from database.db_connector import DatabaseConnector

                conn = DatabaseConnector(
                    username=cfg["username"],
                    password=cfg["password"],
                    host=cfg["host"],
                    port=int(cfg["port"]),
                    database=cfg["database"],
                )
                ok = conn.test_connection()
                if not ok:
                    raise RuntimeError("Connection test returned False")

                # Save to ConfigManager (AppData/config.json)
                self._cfg_manager.save_db_config(cfg)

                # Update live state
                self.state.db_config = self._cfg_manager.get_db_config()

                # Rebuild engine
                conn2 = DatabaseConnector(
                    username=cfg["username"],
                    password=cfg["password"],
                    host=cfg["host"],
                    port=int(cfg["port"]),
                    database=cfg["database"],
                )
                conn2.create_database_if_not_exists()
                conn2.create_tables()
                new_engine = conn2.get_engine()

                if self.state.db_engine:
                    try:
                        self.state.db_engine.dispose()
                    except Exception:
                        pass
                self.state.db_engine = new_engine

                self.after(0, self._on_apply_success)

            except Exception as e:
                self.after(0, lambda err=str(e): self._on_apply_failure(err))

        threading.Thread(target=worker, daemon=True).start()

    def _on_apply_success(self):
        self._db_apply_btn.configure(state="normal", text="✔  Apply & Reconnect")
        self._db_status_lbl.configure(text="✓  Reconnected successfully", fg=Color.SUCCESS)
        self._db_editing = False
        for entry in self._db_entries.values():
            entry.configure(state="readonly", bg=Color.BG_TABLE_HEADER)
        self._db_edit_btn.configure(
            text="✎  Edit Credentials",
            bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY, relief="solid",
        )
        self._db_apply_btn.pack_forget()
        # Update header DB indicator
        try:
            self.app._db_status_lbl.configure(text="● DB: Connected", fg=Color.SUCCESS)
        except Exception:
            pass

    def _on_apply_failure(self, err: str):
        self._db_apply_btn.configure(state="normal", text="✔  Apply & Reconnect")
        self._db_status_lbl.configure(text=f"✗  {err[:80]}", fg=Color.DANGER)

    # ─────────────────────────────────────────────────────────────────────────
    #  Save all settings
    # ─────────────────────────────────────────────────────────────────────────
    def _save(self):
        errs = self._validate()
        if errs:
            messagebox.showerror("Validation Error", "\n".join(errs))
            return

        # 1. Save Tally host/port to ConfigManager (core config)
        self._cfg_manager.save_tally_config({
            "host": self._vars["tally_host"].get().strip() or "localhost",
            "port": int(self._vars["tally_port"].get().strip() or 9000),
        })

        # 2. Save advanced settings to tally_config.ini
        adv_keys = [
            "tally_timeout_connect", "tally_timeout_read", "tally_max_retries",
            "sync_default_mode", "sync_chunk_months", "sync_parallel_workers",
            "log_level", "log_retention_days", "db_pool_size", "db_pool_recycle",
        ]
        adv_data = {k: self._vars[k].get().strip() for k in adv_keys if k in self._vars}
        _save_advanced_config(adv_data)

        # 3. Apply to live state
        self._apply_to_state()

        self._save_status_lbl.configure(
            text=f"✓  Saved at {datetime.now().strftime('%H:%M:%S')}",
            fg=Color.SUCCESS,
        )
        self.after(4000, lambda: self._save_status_lbl.configure(text=""))

    def _validate(self) -> list[str]:
        errors = []
        port = self._vars.get("tally_port", tk.StringVar()).get().strip()
        if not port.isdigit() or not (1 <= int(port) <= 65535):
            errors.append("Tally Port must be a number between 1 and 65535.")

        for key, label in [
            ("tally_timeout_connect", "Connect Timeout"),
            ("tally_timeout_read",    "Read Timeout"),
            ("tally_max_retries",     "Max Retries"),
            ("sync_chunk_months",     "Snapshot Chunk Size"),
            ("sync_parallel_workers", "Parallel Workers"),
            ("log_retention_days",    "Log Retention"),
        ]:
            val = self._vars.get(key, tk.StringVar()).get().strip()
            if val and not val.isdigit():
                errors.append(f"{label} must be a whole number.")
        return errors

    def _apply_to_state(self):
        host = self._vars["tally_host"].get().strip() or "localhost"
        port = int(self._vars["tally_port"].get().strip() or 9000)
        self.state.tally.host = host
        self.state.tally.port = port

        # Update tally_config on AppState so other components pick it up
        if self.state.tally_config:
            self.state.tally_config["host"] = host
            self.state.tally_config["port"] = port

        # Update per-company defaults only if they haven't been individually set
        for co in self.state.companies.values():
            if co.tally_host == "localhost":
                co.tally_host = host
            if co.tally_port == 9000:
                co.tally_port = port

        # Patch sync_service chunk/workers at runtime
        try:
            import services.sync_service as ss
            chunk   = int(self._vars.get("sync_chunk_months",     tk.StringVar(value="3")).get() or 3)
            workers = int(self._vars.get("sync_parallel_workers", tk.StringVar(value="2")).get() or 2)
            if chunk   > 0: ss.SNAPSHOT_CHUNK_MONTHS = chunk
            if workers > 0: ss.VOUCHER_WORKERS        = workers
        except Exception:
            pass

    def _reset_defaults(self):
        if not messagebox.askyesno(
            "Reset Defaults",
            "Reset all settings to their default values?\n\nThis will not affect your DB credentials.",
        ):
            return
        defaults = {
            "tally_host":            "localhost",
            "tally_port":            "9000",
            "tally_timeout_connect": "60",
            "tally_timeout_read":    "1800",
            "tally_max_retries":     "3",
            "sync_default_mode":     "incremental",
            "sync_chunk_months":     "3",
            "sync_parallel_workers": "2",
            "log_level":             "INFO",
            "log_retention_days":    "30",
            "db_pool_size":          "10",
            "db_pool_recycle":       "3600",
        }
        for k, v in defaults.items():
            if k in self._vars:
                self._vars[k].set(v)

        self._save_status_lbl.configure(
            text="Defaults restored — click Save to apply.", fg=Color.WARNING_FG,
        )

    # ─────────────────────────────────────────────────────────────────────────
    #  App utilities
    # ─────────────────────────────────────────────────────────────────────────
    def _open_logs_folder(self):
        path = os.path.abspath("logs")
        if not os.path.exists(path):
            messagebox.showinfo("Logs Folder", f"Logs folder not found:\n{path}")
            return
        try:
            import subprocess, sys
            if sys.platform == "win32":
                os.startfile(path)
            elif sys.platform == "darwin":
                subprocess.run(["open", path])
            else:
                subprocess.run(["xdg-open", path])
        except Exception:
            messagebox.showinfo("Logs Folder", f"Open manually:\n{path}")

    def _clean_old_logs(self):
        days_str = self._vars.get("log_retention_days", tk.StringVar(value="30")).get().strip()
        try:
            days = int(days_str)
        except ValueError:
            days = 30

        if days == 0:
            messagebox.showinfo("Clean Logs", "Retention is set to 0 (keep forever). No files deleted.")
            return

        if not messagebox.askyesno(
            "Clean Old Logs",
            f"Delete log files older than {days} days from the logs/ folder?\n\nThis cannot be undone.",
        ):
            return

        from datetime import timedelta
        cutoff  = datetime.now() - timedelta(days=days)
        deleted = 0
        errors  = 0

        logs_path = os.path.abspath("logs")
        if not os.path.exists(logs_path):
            messagebox.showinfo("Clean Logs", "No logs folder found.")
            return

        for fname in os.listdir(logs_path):
            fpath = os.path.join(logs_path, fname)
            if not fname.endswith(".log"):
                continue
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                if mtime < cutoff:
                    os.remove(fpath)
                    deleted += 1
            except Exception:
                errors += 1

        msg = f"Deleted {deleted} log file(s)."
        if errors:
            msg += f"\n{errors} file(s) could not be deleted."
        messagebox.showinfo("Clean Logs", msg)

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time page is navigated to — reload fresh values."""
        # Reload advanced config from file
        self._adv_cfg = _load_advanced_config()
        for k, var in self._vars.items():
            if k in self._adv_cfg:
                var.set(self._adv_cfg[k])

        # Sync Tally host/port from ConfigManager (authoritative) + live state
        tally_cfg = self._cfg_manager.get_tally_config()
        self._vars["tally_host"].set(
            self.state.tally.host or tally_cfg.get("host", "localhost")
        )
        self._vars["tally_port"].set(
            str(self.state.tally.port or tally_cfg.get("port", 9000))
        )

        # Reload DB fields from ConfigManager
        db_cfg = self._cfg_manager.get_db_config()
        self._vars["db_host"].set(db_cfg.get("host",     "localhost"))
        self._vars["db_port"].set(str(db_cfg.get("port", 3306)))
        self._vars["db_username"].set(db_cfg.get("username", "root"))
        self._vars["db_password"].set(db_cfg.get("password", ""))
        self._vars["db_database"].set(db_cfg.get("database", ""))

        # Ensure DB fields are readonly if not in edit mode
        if not self._db_editing:
            for entry in self._db_entries.values():
                entry.configure(state="readonly", bg=Color.BG_TABLE_HEADER)
        self._db_status_lbl.configure(text="")

        # ── Phase 3: Reload automation settings ──────────────────────────────
        self._vars["tally_exe_path"].set(
            getattr(self.state, 'tally_exe_path', '') or '')

        aut = getattr(self.state, 'automation', None)
        if aut:
            self._vars["automation_confidence"].set(str(getattr(aut, 'confidence',       0.80)))
            self._vars["automation_click_delay"].set(str(getattr(aut, 'click_delay_ms',  500)))
            self._vars["automation_timeout"].set(str(getattr(aut, 'wait_timeout_sec',    30)))
            self._vars["automation_retries"].set(str(getattr(aut, 'retry_attempts',      3)))

        # Reload image filenames from live state
        images = getattr(self.state, 'tally_images', {})
        for key, row_data in self._image_rows.items():
            current = images.get(key, f"tally_{key}.png")
            row_data["filename_var"].set(current)
            row_data["result_lbl"].configure(text="")