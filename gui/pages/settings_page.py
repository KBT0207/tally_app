"""
gui/pages/settings_page.py
============================
Settings page — industry-level configuration for:

  Section 1 — Tally Connection
    • Host, Port
    • Timeout (connect / read)
    • Max retries
    • Test Connection button with live status
    • Per-company override toggle

  Section 2 — Database Connection
    • Host, Port, Username, Password, Database
    • Pool size, Pool recycle
    • Test Connection
    • Re-open DB config (calls app.open_db_settings)

  Section 3 — Sync Defaults
    • Default sync mode (Incremental / Snapshot)
    • Chunk size (months per API call)
    • Parallel workers
    • Default to_date (today / custom)

  Section 4 — Application
    • App version info
    • Log level (INFO / DEBUG / WARNING)
    • Log retention (days)
    • Open logs folder button

All settings saved to  tally_config.ini  next to run_gui.py.
Loaded on app start and injected into state.tally.*
"""

import os
import json
import configparser
import threading
import tkinter as tk
from tkinter import messagebox, filedialog
from datetime import datetime

from gui.state  import AppState
from gui.styles import Color, Font, Spacing

CONFIG_FILE = "tally_config.ini"


# ─────────────────────────────────────────────────────────────────────────────
#  Config I/O helpers
# ─────────────────────────────────────────────────────────────────────────────
def load_tally_config() -> dict:
    """Read tally_config.ini → dict. Returns defaults if file absent."""
    defaults = {
        # Tally
        "tally_host":       "localhost",
        "tally_port":       "9000",
        "tally_timeout_connect": "60",
        "tally_timeout_read":    "1800",
        "tally_max_retries":     "3",
        # Sync defaults
        "sync_default_mode":     "incremental",
        "sync_chunk_months":     "3",
        "sync_parallel_workers": "2",
        # App
        "log_level":        "INFO",
        "log_retention_days": "30",
    }
    cfg = configparser.ConfigParser()
    if os.path.exists(CONFIG_FILE):
        cfg.read(CONFIG_FILE)
        if "tally" in cfg:
            defaults.update(cfg["tally"])
    return defaults


def save_tally_config(data: dict):
    """Write settings dict → tally_config.ini."""
    cfg = configparser.ConfigParser()
    # Read existing to preserve other sections (e.g. [database])
    if os.path.exists(CONFIG_FILE):
        cfg.read(CONFIG_FILE)
    cfg["tally"] = data
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        cfg.write(f)


# ─────────────────────────────────────────────────────────────────────────────
#  Small helper: labeled input row
# ─────────────────────────────────────────────────────────────────────────────
def _field_row(
    parent,
    row:      int,
    label:    str,
    var:      tk.Variable,
    hint:     str  = "",
    width:    int  = 24,
    secret:   bool = False,
    readonly: bool = False,
) -> tk.Entry:
    """Render label + entry on a grid row. Returns the Entry widget."""
    tk.Label(
        parent, text=label,
        font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
        anchor="w", width=22,
    ).grid(row=row, column=0, sticky="w", pady=4)

    entry = tk.Entry(
        parent,
        textvariable=var,
        font=Font.BODY,
        width=width,
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
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            anchor="w",
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

        self._cfg     = load_tally_config()
        self._vars: dict[str, tk.Variable] = {}
        self._unsaved = False

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._build()

    # ─────────────────────────────────────────────────────────────────────────
    #  Master layout: scrollable left content + sticky save bar
    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        self.rowconfigure(0, weight=1)
        self.rowconfigure(1, weight=0)

        # ── Scrollable content ────────────────────────────
        canvas = tk.Canvas(self, bg=Color.BG_ROOT, highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")

        vsb = tk.Scrollbar(self, orient="vertical", command=canvas.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        inner = tk.Frame(canvas, bg=Color.BG_ROOT)
        inner.columnconfigure(0, weight=1)
        cw = canvas.create_window((0, 0), window=inner, anchor="nw")

        inner.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.bind("<Configure>", lambda e: canvas.itemconfig(cw, width=e.width))
        canvas.bind_all(
            "<MouseWheel>",
            lambda e: canvas.yview_scroll(int(-1 * (e.delta / 120)), "units")
        )

        # Build all sections
        self._build_tally_section(inner,   row=0)
        self._build_db_section(inner,      row=1)
        self._build_sync_section(inner,    row=2)
        self._build_app_section(inner,     row=3)

        # ── Save bar (always visible at bottom) ───────────
        self._build_save_bar()

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 1 — Tally Connection
    # ─────────────────────────────────────────────────────────────────────────
    def _build_tally_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="🔌  Tally Connection")

        # Host
        self._var("tally_host", "localhost")
        _field_row(card, 1, "Host",
                   self._vars["tally_host"],
                   hint="IP address or hostname where Tally is running")

        # Port
        self._var("tally_port", "9000")
        _field_row(card, 2, "Port",
                   self._vars["tally_port"],
                   hint="Default Tally port is 9000", width=8)

        # Timeout
        self._var("tally_timeout_connect", "60")
        self._var("tally_timeout_read",    "1800")

        timeout_row = tk.Frame(card, bg=Color.BG_CARD)
        timeout_row.grid(row=3, column=0, columnspan=3, sticky="w", pady=4)

        tk.Label(timeout_row, text="Timeout  (seconds)",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 anchor="w", width=22).pack(side="left")

        tk.Label(timeout_row, text="Connect:",
                 font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                 ).pack(side="left", padx=(Spacing.SM, 4))

        tk.Entry(
            timeout_row, textvariable=self._vars["tally_timeout_connect"],
            font=Font.BODY, width=6,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY, relief="solid", bd=1,
        ).pack(side="left")

        tk.Label(timeout_row, text="  Read:",
                 font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                 ).pack(side="left", padx=(Spacing.SM, 4))

        tk.Entry(
            timeout_row, textvariable=self._vars["tally_timeout_read"],
            font=Font.BODY, width=7,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY, relief="solid", bd=1,
        ).pack(side="left")

        tk.Label(timeout_row,
                 text="  (Read timeout should be large for big datasets)",
                 font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                 ).pack(side="left", padx=(Spacing.SM, 0))

        # Max retries
        self._var("tally_max_retries", "3")
        _field_row(card, 4, "Max Retries",
                   self._vars["tally_max_retries"],
                   hint="Retry failed Tally requests N times before giving up",
                   width=5)

        # Test connection button + status
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

        # Tally version info (populated after test)
        self._tally_info_lbl = tk.Label(
            card, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            anchor="w",
        )
        self._tally_info_lbl.grid(row=6, column=0, columnspan=3, sticky="w", pady=(4, 0))

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 2 — Database Connection
    # ─────────────────────────────────────────────────────────────────────────
    def _build_db_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="🗄️  Database Connection  (MySQL / MariaDB)")

        # ── Load current values from .env / state ─────────
        db_cfg = self._read_db_config()

        # StringVars for all DB fields — editable when unlocked
        self._var("db_host",     db_cfg.get("host",     "localhost"))
        self._var("db_port",     db_cfg.get("port",     "3306"))
        self._var("db_username", db_cfg.get("username", "root"))
        self._var("db_password", db_cfg.get("password", ""))
        self._var("db_database", db_cfg.get("database", ""))

        # Set initial values from config
        self._vars["db_host"].set(db_cfg.get("host",     "localhost"))
        self._vars["db_port"].set(db_cfg.get("port",     "3306"))
        self._vars["db_username"].set(db_cfg.get("username", "root"))
        self._vars["db_password"].set(db_cfg.get("password", ""))
        self._vars["db_database"].set(db_cfg.get("database", ""))

        # ── Field rows ────────────────────────────────────
        self._db_entries = {}  # key → Entry widget

        field_defs = [
            ("Host",     "db_host",     False, "hostname or IP"),
            ("Port",     "db_port",     False, "default 3306"),
            ("Username", "db_username", False, ""),
            ("Password", "db_password", True,  ""),
            ("Database", "db_database", False, "DB_NAME from .env"),
        ]

        for i, (lbl, key, is_secret, hint) in enumerate(field_defs, start=1):
            tk.Label(
                card, text=lbl,
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                anchor="w", width=22,
            ).grid(row=i, column=0, sticky="w", pady=4)

            e = tk.Entry(
                card,
                textvariable=self._vars[key],
                font=Font.BODY, width=26,
                bg=Color.BG_TABLE_HEADER,
                fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
                show="●" if is_secret else "",
                state="readonly",
            )
            e.grid(row=i, column=1, sticky="w", pady=4, padx=(Spacing.SM, 0))
            self._db_entries[key] = e

            if hint:
                tk.Label(
                    card, text=hint,
                    font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                ).grid(row=i, column=2, sticky="w", padx=(Spacing.SM, 0))

        self._db_editing = False  # track edit mode

        # ── Pool settings ─────────────────────────────────
        sep = tk.Frame(card, bg=Color.BORDER, height=1)
        sep.grid(row=len(field_defs)+1, column=0, columnspan=3, sticky="ew",
                 pady=(Spacing.MD, Spacing.SM))

        pool_row = tk.Frame(card, bg=Color.BG_CARD)
        pool_row.grid(row=len(field_defs)+2, column=0, columnspan=3, sticky="w")

        self._var("db_pool_size",    "10")
        self._var("db_pool_recycle", "3600")

        tk.Label(pool_row, text="Connection Pool Size:",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=22, anchor="w").pack(side="left")
        tk.Entry(pool_row, textvariable=self._vars["db_pool_size"],
                 font=Font.BODY, width=6,
                 bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                 relief="solid", bd=1).pack(side="left", padx=(Spacing.SM, Spacing.LG))

        tk.Label(pool_row, text="Recycle (seconds):",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 ).pack(side="left")
        tk.Entry(pool_row, textvariable=self._vars["db_pool_recycle"],
                 font=Font.BODY, width=8,
                 bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                 relief="solid", bd=1).pack(side="left", padx=(Spacing.SM, 0))

        # ── Action buttons ────────────────────────────────
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
        # Hidden until in edit mode
        self._db_apply_btn.pack(side="left", padx=(0, Spacing.SM))
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

        # Default sync mode
        self._var("sync_default_mode", "incremental")
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

        # Chunk size
        self._var("sync_chunk_months", "3")
        _field_row(card, 2, "Snapshot Chunk Size  (months)",
                   self._vars["sync_chunk_months"],
                   hint="Months of data fetched per API call. Lower = safer, higher = faster.",
                   width=5)

        # Parallel workers
        self._var("sync_parallel_workers", "2")
        _field_row(card, 3, "Parallel Voucher Workers",
                   self._vars["sync_parallel_workers"],
                   hint="Concurrent threads per company for voucher types. (2 recommended)",
                   width=5)

        # Info note
        tk.Label(
            card,
            text="ℹ  These are defaults. You can override them per sync run on the Sync page.",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            anchor="w",
        ).grid(row=4, column=0, columnspan=3, sticky="w", pady=(Spacing.SM, 0))

    # ─────────────────────────────────────────────────────────────────────────
    #  Section 4 — Application
    # ─────────────────────────────────────────────────────────────────────────
    def _build_app_section(self, parent, row: int):
        card = self._make_card(parent, row=row, title="⚙️  Application")

        # Log level
        self._var("log_level", "INFO")
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

        # Log retention
        self._var("log_retention_days", "30")
        _field_row(card, 2, "Log Retention  (days)",
                   self._vars["log_retention_days"],
                   hint="Auto-delete log files older than N days  (0 = keep forever)",
                   width=6)

        # Open logs folder
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

        # Version info
        sep2 = tk.Frame(card, bg=Color.BORDER, height=1)
        sep2.grid(row=5, column=0, columnspan=3, sticky="ew", pady=(Spacing.MD, Spacing.SM))

        info_frame = tk.Frame(card, bg=Color.BG_CARD)
        info_frame.grid(row=6, column=0, columnspan=3, sticky="w")

        from gui.styles import APP_VERSION
        for label, value in [
            ("App Version",     APP_VERSION),
            ("Config File",     os.path.abspath(CONFIG_FILE)),
            ("Scheduler Config",os.path.abspath("scheduler_config.json")),
            ("DB Config",       os.path.abspath("db_config.ini")),
        ]:
            row_f = tk.Frame(info_frame, bg=Color.BG_CARD)
            row_f.pack(fill="x", pady=1)
            tk.Label(row_f, text=f"{label}:",
                     font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                     anchor="w", width=20).pack(side="left")
            tk.Label(row_f, text=value,
                     font=Font.MONO_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                     anchor="w").pack(side="left")

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
        """Create a section card with a header label. Returns the inner grid frame."""
        outer = tk.Frame(
            parent, bg=Color.BG_CARD,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        outer.grid(row=row, column=0, sticky="ew",
                   padx=Spacing.XL, pady=(0, Spacing.MD))
        outer.columnconfigure(0, weight=1)

        # Section header strip
        hdr = tk.Frame(outer, bg=Color.PRIMARY, pady=Spacing.SM)
        hdr.grid(row=0, column=0, sticky="ew")
        tk.Label(
            hdr, text=title,
            font=Font.LABEL_BOLD, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            anchor="w", padx=Spacing.LG,
        ).pack(fill="x")

        # Content grid
        content = tk.Frame(outer, bg=Color.BG_CARD,
                           padx=Spacing.XL, pady=Spacing.LG)
        content.grid(row=1, column=0, sticky="ew")
        content.columnconfigure(1, weight=1)

        return content

    def _var(self, key: str, default: str = ""):
        """Get or create a StringVar, seeded from loaded config."""
        if key not in self._vars:
            val = self._cfg.get(key, default)
            self._vars[key] = tk.StringVar(value=val)
        return self._vars[key]

    # ─────────────────────────────────────────────────────────────────────────
    #  Test Tally Connection
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
                    host        = host,
                    port        = int(port),
                    timeout     = timeout,
                    max_retries = retries,
                )
                connected = (tc.status == "Connected")
                self.after(0, lambda: self._on_tally_test_result(connected, host, port))
            except Exception as e:
                self.after(0, lambda err=e: self._on_tally_test_result(False, host, port, str(err)))

        threading.Thread(target=worker, daemon=True).start()

    def _on_tally_test_result(self, ok: bool, host: str, port: str, err: str = ""):
        self._tally_test_btn.configure(state="normal", text="⚡  Test Tally Connection")
        if ok:
            self._tally_status_lbl.configure(
                text="✓  Connected successfully", fg=Color.SUCCESS,
            )
            self._tally_info_lbl.configure(
                text=f"Tally is reachable at  {host}:{port}",
                fg=Color.TEXT_SECONDARY,
            )
            # Update app state
            self.state.tally.host      = host
            self.state.tally.port      = int(port)
            self.state.tally.connected = True
            self.state.tally.last_check = datetime.now()
        else:
            msg = err or "Connection refused"
            self._tally_status_lbl.configure(
                text=f"✗  Failed — {msg}", fg=Color.DANGER,
            )
            self.state.tally.connected = False

    # ─────────────────────────────────────────────────────────────────────────
    #  Test DB Connection
    # ─────────────────────────────────────────────────────────────────────────
    def _get_live_db_cfg(self) -> dict:
        """Return DB config from the currently displayed (possibly edited) fields."""
        return {
            "host":     self._vars["db_host"].get().strip(),
            "port":     self._vars["db_port"].get().strip(),
            "username": self._vars["db_username"].get().strip(),
            "password": self._vars["db_password"].get(),
            "database": self._vars["db_database"].get().strip(),
        }

    def _toggle_db_edit(self):
        """Toggle between view-only and edit mode for DB credential fields."""
        self._db_editing = not self._db_editing

        new_state = "normal" if self._db_editing else "readonly"
        active_bg = Color.BG_INPUT if self._db_editing else Color.BG_TABLE_HEADER

        for key, entry in self._db_entries.items():
            entry.configure(state=new_state, bg=active_bg)

        if self._db_editing:
            self._db_edit_btn.configure(
                text="✖  Cancel Edit",
                bg=Color.DANGER_BG if hasattr(Color, "DANGER_BG") else "#fde8e8",
                fg=Color.DANGER if hasattr(Color, "DANGER") else "#c0392b",
                relief="flat",
            )
            self._db_apply_btn.pack(side="left", padx=(0, Spacing.SM))
            self._db_status_lbl.configure(
                text="Edit credentials, then Test or Apply & Reconnect.",
                fg=Color.TEXT_MUTED,
            )
            # Focus host field
            self._db_entries["db_host"].focus_set()
        else:
            # Revert fields to last saved state
            db_cfg = self._read_db_config()
            self._vars["db_host"].set(db_cfg.get("host",     "localhost"))
            self._vars["db_port"].set(db_cfg.get("port",     "3306"))
            self._vars["db_username"].set(db_cfg.get("username", "root"))
            self._vars["db_password"].set(db_cfg.get("password", ""))
            self._vars["db_database"].set(db_cfg.get("database", ""))
            self._db_edit_btn.configure(
                text="✎  Edit Credentials",
                bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY, relief="solid",
            )
            self._db_apply_btn.pack_forget()
            self._db_status_lbl.configure(text="", fg=Color.TEXT_MUTED)

    def _apply_db_changes(self):
        """
        Test the new credentials, write to .env, reconnect the live engine,
        then exit edit mode.
        """
        cfg = self._get_live_db_cfg()

        # Basic validation
        if not cfg["database"]:
            messagebox.showerror("Validation Error", "Database name (DB_NAME) cannot be empty.")
            return
        if not cfg["port"].isdigit():
            messagebox.showerror("Validation Error", "Port must be a number.")
            return

        self._db_apply_btn.configure(state="disabled", text="Applying...")
        self._db_status_lbl.configure(text="Testing connection...", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        def worker():
            try:
                from database.db_connector import DatabaseConnector
                conn = DatabaseConnector(
                    username = cfg["username"],
                    password = cfg["password"],
                    host     = cfg["host"],
                    port     = int(cfg["port"]),
                    database = cfg["database"],
                )
                ok = conn.test_connection()
                if not ok:
                    raise RuntimeError("Connection test returned False")

                # Write back to .env
                self._write_env(cfg)

                # Update live state so rest of app uses new credentials
                self.state.db_config = cfg

                # Rebuild engine
                from database.db_connector import DatabaseConnector
                from database.models.scheduler_config import Base as SchedBase
                connector = DatabaseConnector(
                    username = cfg["username"],
                    password = cfg["password"],
                    host     = cfg["host"],
                    port     = int(cfg["port"]),
                    database = cfg["database"],
                )
                connector.create_database_if_not_exists()
                connector.create_tables()
                new_engine = connector.get_engine()
                SchedBase.metadata.create_all(new_engine, checkfirst=True)
                self.state.db_engine = new_engine

                self.after(0, lambda: self._on_apply_success())
            except Exception as e:
                self.after(0, lambda err=str(e): self._on_apply_failure(err))

        threading.Thread(target=worker, daemon=True).start()

    def _on_apply_success(self):
        self._db_apply_btn.configure(state="normal", text="✔  Apply & Reconnect")
        self._db_status_lbl.configure(text="✓  Reconnected successfully", fg=Color.SUCCESS)
        # Exit edit mode cleanly (don't revert fields — keep new values)
        self._db_editing = False
        for entry in self._db_entries.values():
            entry.configure(state="readonly", bg=Color.BG_TABLE_HEADER)
        self._db_edit_btn.configure(
            text="✎  Edit Credentials",
            bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY, relief="solid",
        )
        self._db_apply_btn.pack_forget()
        # Refresh header DB status label
        self.app._db_status_lbl.configure(text="● DB: Connected", fg=Color.SUCCESS)

    def _on_apply_failure(self, err: str):
        self._db_apply_btn.configure(state="normal", text="✔  Apply & Reconnect")
        self._db_status_lbl.configure(
            text=f"✗  {err[:80]}", fg=Color.DANGER,
        )

    @staticmethod
    def _write_env(cfg: dict):
        """Overwrite .env with new DB credentials. Preserves non-DB lines."""
        env_path = ".env"
        import re
        db_keys = {"DB_HOST", "DB_PORT", "DB_USERNAME", "DB_PASSWORD", "DB_NAME"}
        existing_lines = []
        if os.path.exists(env_path):
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    key = line.split("=")[0].strip().upper()
                    if key not in db_keys:
                        existing_lines.append(line.rstrip())

        db_lines = [
            f"DB_HOST={cfg['host']}",
            f"DB_PORT={cfg['port']}",
            f"DB_USERNAME={cfg['username']}",
            f"DB_PASSWORD={cfg['password']}",
            f"DB_NAME={cfg['database']}",
        ]
        all_lines = existing_lines + db_lines
        with open(env_path, "w", encoding="utf-8") as f:
            f.write("\n".join(all_lines) + "\n")

    def _test_db(self):
        """Test using whatever is currently in the fields (live or edited)."""
        self._db_test_btn.configure(state="disabled", text="Testing...")
        self._db_status_lbl.configure(text="Connecting...", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        cfg = self._get_live_db_cfg()

        def worker():
            try:
                from database.db_connector import DatabaseConnector
                conn = DatabaseConnector(
                    username = cfg.get("username", "root"),
                    password = cfg.get("password", ""),
                    host     = cfg.get("host",     "localhost"),
                    port     = int(cfg.get("port", 3306)),
                    database = cfg.get("database", ""),
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

    # ─────────────────────────────────────────────────────────────────────────
    #  Save
    # ─────────────────────────────────────────────────────────────────────────
    def _save(self):
        # Validate
        errs = self._validate()
        if errs:
            messagebox.showerror("Validation Error", "\n".join(errs))
            return

        data = {k: v.get().strip() for k, v in self._vars.items()}
        save_tally_config(data)

        # Apply to live state
        self._apply_to_state(data)

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

    def _apply_to_state(self, data: dict):
        """Push saved settings into live AppState and sync_service constants."""
        host = data.get("tally_host", "localhost").strip()
        port = int(data.get("tally_port", "9000"))
        self.state.tally.host = host
        self.state.tally.port = port

        # Also update all per-company overrides if not set individually
        for co in self.state.companies.values():
            if co.tally_host == "localhost":
                co.tally_host = host
            if co.tally_port == 9000:
                co.tally_port = port

        # Patch sync_service chunk/workers at runtime
        try:
            import services.sync_service as ss
            chunk = int(data.get("sync_chunk_months", "3"))
            workers = int(data.get("sync_parallel_workers", "2"))
            if chunk > 0:
                ss.SNAPSHOT_CHUNK_MONTHS = chunk
            if workers > 0:
                ss.VOUCHER_WORKERS = workers
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

        self._save_status_lbl.configure(text="Defaults restored — click Save to apply.", fg=Color.WARNING_FG)

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
        except Exception as e:
            messagebox.showinfo("Logs Folder", f"Open manually:\n{path}")

    def _clean_old_logs(self):
        days = self._vars.get("log_retention_days", tk.StringVar(value="30")).get().strip()
        try:
            days = int(days)
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

        for fname in os.listdir("logs"):
            fpath = os.path.join("logs", fname)
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
    #  Helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _read_db_config(self) -> dict:
        """
        Read DB config from .env file (same logic as app._load_db_config).
        Falls back to state.db_config (already loaded at startup) if .env missing.
        Always returns the most up-to-date values including any live edits.
        """
        # Prefer live state.db_config (loaded at startup, always authoritative)
        live = getattr(self.state, 'db_config', None)
        if live:
            return dict(live)

        # Fallback: parse .env directly
        env: dict[str, str] = {}
        for path in (".env", os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")):
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        key, _, val = line.partition("=")
                        env[key.strip().upper()] = val.strip().strip("'\"")
                break

        return {
            "host":     env.get("DB_HOST",     "localhost"),
            "port":     env.get("DB_PORT",     "3306"),
            "username": env.get("DB_USERNAME", "root"),
            "password": env.get("DB_PASSWORD", ""),
            "database": env.get("DB_NAME",     ""),
        }

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time page is navigated to — reload config from file/state."""
        self._cfg = load_tally_config()
        for k, var in self._vars.items():
            if k in self._cfg:
                var.set(self._cfg[k])

        # Sync tally host/port from live state
        self._vars["tally_host"].set(self.state.tally.host)
        self._vars["tally_port"].set(str(self.state.tally.port))

        # Reload DB fields from .env / live state (not db_config.ini)
        db_cfg = self._read_db_config()
        self._vars["db_host"].set(db_cfg.get("host",     "localhost"))
        self._vars["db_port"].set(db_cfg.get("port",     "3306"))
        self._vars["db_username"].set(db_cfg.get("username", "root"))
        self._vars["db_password"].set(db_cfg.get("password", ""))
        self._vars["db_database"].set(db_cfg.get("database", ""))

        # Ensure fields are readonly (in case we navigated away mid-edit)
        if not self._db_editing:
            for entry in self._db_entries.values():
                entry.configure(state="readonly", bg=Color.BG_TABLE_HEADER)
        self._db_status_lbl.configure(text="")