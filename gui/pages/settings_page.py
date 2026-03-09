"""
gui/pages/settings_page.py
============================
Settings page — 4 sections:

  1. Tally Connection   — host, port, test button
  2. Database           — show credentials, edit + reconnect
  3. Tally Automation   — exe path, PyAutoGUI controls (confidence / delay / timeout)
  4. Screen Images      — 5 images used by tally_launcher.py (browse + test each)

All saves go through ConfigManager (config.json) or DB as appropriate.
State is updated live so changes take effect immediately without restart.
"""

import os
import shutil
import threading
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime

from gui.state          import AppState, AutomationConfig
from gui.styles         import Color, Font, Spacing
from gui.config_manager import ConfigManager

# PIL for image thumbnails in settings — optional, gracefully skipped if not installed
try:
    from PIL import Image, ImageTk
    HAS_PIL = True
except ImportError:
    HAS_PIL = False


# ── The 5 images tally_launcher.py actually uses ──────────────────────────────
IMAGE_DEFS = [
    # (key,           label,                   description)
    ("gateway",      "Gateway Screen",         "Confirms company is fully open in Tally"),
    ("search_box",   "Company Search Box",     "Yellow search box on the Select Company screen"),
    ("username",     "Username Field",         "Username label on the company login dialog"),
    ("data_server",  "Data Server Button",     "Data Server button — TDS companies only"),
    ("local_path",   "Local Path Screen",      "Path selection screen after clicking Data Server — TDS only"),
]

DEFAULT_FILENAMES = {
    "gateway":     "tally_gateway_screen.png",
    "search_box":  "tally_company_search_box.png",
    "username":    "tally_username_field.png",
    "data_server": "tally_dataserver_image.png",
    "local_path":  "tally_local_path_image.png",
}


# ── Small helper: label + entry row ───────────────────────────────────────────
def _row(parent, grid_row, label, var, width=24, secret=False, readonly=False, hint=""):
    tk.Label(
        parent, text=label,
        font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
        anchor="w", width=22,
    ).grid(row=grid_row, column=0, sticky="w", pady=4)

    entry = tk.Entry(
        parent, textvariable=var,
        font=Font.BODY, width=width,
        bg=Color.BG_INPUT if not readonly else Color.BG_TABLE_HEADER,
        fg=Color.TEXT_PRIMARY,
        relief="solid", bd=1,
        show="●" if secret else "",
        state="normal" if not readonly else "readonly",
    )
    entry.grid(row=grid_row, column=1, sticky="w", pady=4, padx=(Spacing.SM, 0))

    if hint:
        tk.Label(
            parent, text=hint,
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w",
        ).grid(row=grid_row, column=2, sticky="w", padx=(Spacing.SM, 0))

    return entry


# ─────────────────────────────────────────────────────────────────────────────
#  SettingsPage
# ─────────────────────────────────────────────────────────────────────────────
class SettingsPage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._cfg = getattr(app, '_config', ConfigManager())

        # tk.StringVars for all fields
        self._v = {}

        # DB section state
        self._db_editing = False
        self._db_entries = {}   # key → tk.Entry (for toggling readonly)

        # Image rows
        self._image_rows = {}   # key → {"filename_var": StringVar, "result_lbl": Label}

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._build()

    # ──────────────────────────────────────────────
    #  MASTER LAYOUT
    # ──────────────────────────────────────────────

    def _build(self):
        self.rowconfigure(0, weight=1)
        self.rowconfigure(1, weight=0)

        # Scrollable canvas
        self._canvas = tk.Canvas(self, bg=Color.BG_ROOT, highlightthickness=0)
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

        # Scoped mousewheel
        self._canvas.bind("<Enter>", lambda e: self._canvas.bind_all(
            "<MouseWheel>",
            lambda ev: self._canvas.yview_scroll(int(-1 * ev.delta / 120), "units")))
        self._canvas.bind("<Leave>", lambda e: self._canvas.unbind_all("<MouseWheel>"))

        # Build all 4 sections
        self._build_tally_section(inner,      row=0)
        self._build_db_section(inner,         row=1)
        self._build_automation_section(inner, row=2)
        self._build_images_section(inner,     row=3)

        # Sticky save bar at bottom
        self._build_save_bar()

    # ──────────────────────────────────────────────
    #  SECTION 1 — Tally Connection
    # ──────────────────────────────────────────────

    def _build_tally_section(self, parent, row):
        card = self._card(parent, row, "🔌  Tally Connection")

        tally_cfg = self._cfg.get_tally_config()
        self._v["tally_host"] = tk.StringVar(value=tally_cfg.get("host", "localhost"))
        self._v["tally_port"] = tk.StringVar(value=str(tally_cfg.get("port", 9000)))

        _row(card, 0, "Host", self._v["tally_host"],
             hint="IP address or hostname where Tally is running")
        _row(card, 1, "Port", self._v["tally_port"],
             width=8, hint="Default Tally port is 9000")

        # Test button row
        btn_row = tk.Frame(card, bg=Color.BG_CARD)
        btn_row.grid(row=2, column=0, columnspan=3, sticky="w", pady=(Spacing.MD, 0))

        self._tally_test_btn = tk.Button(
            btn_row, text="⚡  Test Connection",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._test_tally,
        )
        self._tally_test_btn.pack(side="left")

        self._tally_status_lbl = tk.Label(
            btn_row, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._tally_status_lbl.pack(side="left", padx=(Spacing.LG, 0))

    def _test_tally(self):
        self._tally_test_btn.configure(state="disabled", text="Testing…")
        self._tally_status_lbl.configure(text="Connecting…", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        host = self._v["tally_host"].get().strip()
        port = self._v["tally_port"].get().strip()

        def worker():
            try:
                from services.tally_connector import TallyConnector
                tc = TallyConnector(host=host, port=int(port))
                ok = tc.status == "Connected"
                self.after(0, lambda: self._on_tally_result(ok, host, port))
            except Exception as e:
                self.after(0, lambda err=str(e): self._on_tally_result(False, host, port, err))

        threading.Thread(target=worker, daemon=True).start()

    def _on_tally_result(self, ok, host, port, err=""):
        self._tally_test_btn.configure(state="normal", text="⚡  Test Connection")
        if ok:
            self._tally_status_lbl.configure(
                text=f"✓ Connected to {host}:{port}", fg=Color.SUCCESS)
            self.state.tally.host      = host
            self.state.tally.port      = int(port)
            self.state.tally.connected = True
        else:
            self._tally_status_lbl.configure(
                text=f"✗ Failed — {err or 'Connection refused'}", fg=Color.DANGER)
            self.state.tally.connected = False

    # ──────────────────────────────────────────────
    #  SECTION 2 — Database
    # ──────────────────────────────────────────────

    def _build_db_section(self, parent, row):
        card = self._card(parent, row, "🗄️  Database Connection  (MySQL / MariaDB)")

        db_cfg = self._cfg.get_db_config()
        fields = [
            ("db_host",     "Host",     False, "hostname or IP"),
            ("db_port",     "Port",     False, "default 3306"),
            ("db_username", "Username", False, ""),
            ("db_password", "Password", True,  ""),
            ("db_database", "Database", False, ""),
        ]
        defaults = {
            "db_host":     db_cfg.get("host",     "localhost"),
            "db_port":     str(db_cfg.get("port", 3306)),
            "db_username": db_cfg.get("username", "root"),
            "db_password": db_cfg.get("password", ""),
            "db_database": db_cfg.get("database", ""),
        }

        for i, (key, label, secret, hint) in enumerate(fields):
            self._v[key] = tk.StringVar(value=defaults[key])

            tk.Label(
                card, text=label,
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                anchor="w", width=22,
            ).grid(row=i, column=0, sticky="w", pady=4)

            entry = tk.Entry(
                card, textvariable=self._v[key],
                font=Font.BODY, width=26,
                bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
                show="●" if secret else "",
                state="readonly",
            )
            entry.grid(row=i, column=1, sticky="w", pady=4, padx=(Spacing.SM, 0))
            self._db_entries[key] = entry

            if hint:
                tk.Label(card, text=hint,
                         font=Font.BODY_SM, bg=Color.BG_CARD,
                         fg=Color.TEXT_MUTED).grid(
                    row=i, column=2, sticky="w", padx=(Spacing.SM, 0))

        # Buttons
        btn_row = tk.Frame(card, bg=Color.BG_CARD)
        btn_row.grid(row=len(fields), column=0, columnspan=3,
                     sticky="w", pady=(Spacing.MD, 0))

        self._db_edit_btn = tk.Button(
            btn_row, text="✎  Edit Credentials",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._toggle_db_edit,
        )
        self._db_edit_btn.pack(side="left", padx=(0, Spacing.SM))

        self._db_test_btn = tk.Button(
            btn_row, text="⚡  Test DB",
            font=Font.BUTTON_SM, bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._test_db,
        )
        self._db_test_btn.pack(side="left", padx=(0, Spacing.SM))

        self._db_apply_btn = tk.Button(
            btn_row, text="✔  Apply & Reconnect",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._apply_db,
        )
        # Hidden until edit mode is active
        self._db_apply_btn.pack_forget()

        self._db_status_lbl = tk.Label(
            btn_row, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._db_status_lbl.pack(side="left", padx=(Spacing.SM, 0))

    def _toggle_db_edit(self):
        self._db_editing = not self._db_editing

        if self._db_editing:
            # Unlock all fields
            for entry in self._db_entries.values():
                entry.configure(state="normal", bg=Color.BG_INPUT)
            self._db_edit_btn.configure(text="✖  Cancel", bg="#fde8e8",
                                        fg=Color.DANGER, relief="flat")
            self._db_apply_btn.pack(side="left", padx=(0, Spacing.SM))
            self._db_status_lbl.configure(
                text="Edit credentials then click Apply & Reconnect",
                fg=Color.TEXT_MUTED)
            self._db_entries["db_host"].focus_set()
        else:
            # Cancel — reload from ConfigManager
            db_cfg = self._cfg.get_db_config()
            self._v["db_host"].set(db_cfg.get("host",     "localhost"))
            self._v["db_port"].set(str(db_cfg.get("port", 3306)))
            self._v["db_username"].set(db_cfg.get("username", "root"))
            self._v["db_password"].set(db_cfg.get("password", ""))
            self._v["db_database"].set(db_cfg.get("database", ""))

            for entry in self._db_entries.values():
                entry.configure(state="readonly", bg=Color.BG_TABLE_HEADER)
            self._db_edit_btn.configure(text="✎  Edit Credentials",
                                        bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
                                        relief="solid")
            self._db_apply_btn.pack_forget()
            self._db_status_lbl.configure(text="")

    def _get_db_cfg_from_ui(self):
        return {
            "host":     self._v["db_host"].get().strip(),
            "port":     self._v["db_port"].get().strip(),
            "username": self._v["db_username"].get().strip(),
            "password": self._v["db_password"].get(),
            "database": self._v["db_database"].get().strip(),
        }

    def _test_db(self):
        self._db_test_btn.configure(state="disabled", text="Testing…")
        self._db_status_lbl.configure(text="Connecting…", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        cfg = self._get_db_cfg_from_ui()

        def worker():
            try:
                from database.db_connector import DatabaseConnector
                conn = DatabaseConnector(
                    username=cfg["username"], password=cfg["password"],
                    host=cfg["host"], port=int(cfg["port"]),
                    database=cfg["database"],
                )
                ok = conn.test_connection()
                self.after(0, lambda: self._on_db_test(ok))
            except Exception as e:
                self.after(0, lambda err=str(e): self._on_db_test(False, err))

        threading.Thread(target=worker, daemon=True).start()

    def _on_db_test(self, ok, err=""):
        self._db_test_btn.configure(state="normal", text="⚡  Test DB")
        if ok:
            self._db_status_lbl.configure(text="✓ Connected", fg=Color.SUCCESS)
        else:
            self._db_status_lbl.configure(
                text=f"✗ {err or 'Connection failed'}", fg=Color.DANGER)

    def _apply_db(self):
        cfg = self._get_db_cfg_from_ui()

        if not cfg["database"]:
            messagebox.showerror("Error", "Database name cannot be empty.")
            return
        if not str(cfg["port"]).isdigit():
            messagebox.showerror("Error", "Port must be a number.")
            return

        self._db_apply_btn.configure(state="disabled", text="Applying…")
        self._db_status_lbl.configure(text="Testing then saving…", fg=Color.TEXT_MUTED)
        self.update_idletasks()

        def worker():
            try:
                from database.db_connector import DatabaseConnector

                # Test first
                conn = DatabaseConnector(
                    username=cfg["username"], password=cfg["password"],
                    host=cfg["host"], port=int(cfg["port"]),
                    database=cfg["database"],
                )
                if not conn.test_connection():
                    raise RuntimeError("Connection test failed")

                # Save to config.json via ConfigManager
                self._cfg.save_db_config(cfg)
                self.state.db_config = self._cfg.get_db_config()

                # Rebuild engine
                conn2 = DatabaseConnector(
                    username=cfg["username"], password=cfg["password"],
                    host=cfg["host"], port=int(cfg["port"]),
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

                self.after(0, self._on_db_apply_ok)

            except Exception as e:
                self.after(0, lambda err=str(e): self._on_db_apply_fail(err))

        threading.Thread(target=worker, daemon=True).start()

    def _on_db_apply_ok(self):
        self._db_apply_btn.configure(state="normal", text="✔  Apply & Reconnect")
        self._db_status_lbl.configure(text="✓ Reconnected successfully", fg=Color.SUCCESS)
        self._db_editing = False

        for entry in self._db_entries.values():
            entry.configure(state="readonly", bg=Color.BG_TABLE_HEADER)
        self._db_edit_btn.configure(text="✎  Edit Credentials",
                                    bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
                                    relief="solid")
        self._db_apply_btn.pack_forget()

        # Update header DB indicator if it exists
        try:
            self.app._db_status_lbl.configure(text="● DB: Connected", fg=Color.SUCCESS)
        except Exception:
            pass

    def _on_db_apply_fail(self, err):
        self._db_apply_btn.configure(state="normal", text="✔  Apply & Reconnect")
        self._db_status_lbl.configure(text=f"✗ {err[:80]}", fg=Color.DANGER)

    # ──────────────────────────────────────────────
    #  SECTION 3 — Tally Automation (exe + controls)
    # ──────────────────────────────────────────────

    def _build_automation_section(self, parent, row):
        card = self._card(parent, row, "🤖  Tally Automation  (PyAutoGUI)")

        # ── Exe path ──────────────────────────────────────────────────────────
        tk.Label(
            card, text="Tally.exe Path",
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            anchor="w",
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))

        self._v["tally_exe"] = tk.StringVar(
            value=getattr(self.state, 'tally_exe_path', '') or '')

        tk.Label(card, text="Tally.exe Path:",
                 font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 anchor="w", width=22).grid(row=1, column=0, sticky="w", pady=4)

        exe_row = tk.Frame(card, bg=Color.BG_CARD)
        exe_row.grid(row=1, column=1, columnspan=2, sticky="w", pady=4)

        tk.Entry(
            exe_row, textvariable=self._v["tally_exe"],
            font=Font.BODY, width=40,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        ).pack(side="left", padx=(0, 8))

        tk.Button(
            exe_row, text="Browse…",
            font=Font.BUTTON_SM, bg=Color.PRIMARY_LIGHT, fg=Color.PRIMARY,
            relief="solid", bd=1, padx=8, pady=3, cursor="hand2",
            command=self._browse_exe,
        ).pack(side="left")

        tk.Label(
            card, text="e.g.  C:\\Program Files\\TallyPrime\\tally.exe",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w",
        ).grid(row=2, column=1, columnspan=2, sticky="w", pady=(0, 10))

        # ── Separator ─────────────────────────────────────────────────────────
        tk.Frame(card, bg=Color.BORDER, height=1).grid(
            row=3, column=0, columnspan=3, sticky="ew", pady=(4, 12))

        # ── PyAutoGUI controls ────────────────────────────────────────────────
        tk.Label(
            card, text="PyAutoGUI Controls",
            font=Font.LABEL_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            anchor="w",
        ).grid(row=4, column=0, columnspan=3, sticky="w", pady=(0, 4))

        aut = getattr(self.state, 'automation', None)
        self._v["confidence"] = tk.StringVar(
            value=str(getattr(aut, 'confidence', 0.80)))
        self._v["click_delay"] = tk.StringVar(
            value=str(getattr(aut, 'click_delay_ms', 500)))
        self._v["timeout"] = tk.StringVar(
            value=str(getattr(aut, 'wait_timeout_sec', 30)))

        ctrl_rows = [
            ("Confidence",        "confidence",   "0.50–1.00  How closely screenshots must match. 0.80 is a good start."),
            ("Click Delay  (ms)", "click_delay",  "Milliseconds between PyAutoGUI actions. Increase on slow PCs."),
            ("Wait Timeout  (s)", "timeout",      "Seconds to wait for each screen image before giving up."),
        ]

        for i, (label, key, hint) in enumerate(ctrl_rows, start=5):
            tk.Label(card, text=label,
                     font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                     anchor="w", width=22,
                     ).grid(row=i, column=0, sticky="w", pady=5)
            tk.Entry(card, textvariable=self._v[key],
                     font=Font.BODY, width=8,
                     bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                     relief="solid", bd=1,
                     ).grid(row=i, column=1, sticky="w", pady=5, padx=(Spacing.SM, 0))
            tk.Label(card, text=hint,
                     font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
                     anchor="w",
                     ).grid(row=i, column=2, sticky="w", padx=(Spacing.SM, 0))

        # ── Save automation button ────────────────────────────────────────────
        save_row = tk.Frame(card, bg=Color.BG_CARD)
        save_row.grid(row=8, column=0, columnspan=3, sticky="w", pady=(Spacing.MD, 0))

        tk.Button(
            save_row, text="✓  Save Automation Settings",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.LG, pady=5,
            cursor="hand2", command=self._save_automation,
        ).pack(side="left")

        self._automation_status_lbl = tk.Label(
            save_row, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._automation_status_lbl.pack(side="left", padx=(Spacing.LG, 0))

    def _browse_exe(self):
        path = filedialog.askopenfilename(
            title="Select Tally.exe",
            filetypes=[("Executable", "*.exe"), ("All files", "*.*")],
            parent=self,
        )
        if path:
            self._v["tally_exe"].set(os.path.normpath(path))

    def _save_automation(self):
        # Validate
        try:
            conf = float(self._v["confidence"].get())
            if not (0.5 <= conf <= 1.0):
                raise ValueError()
        except ValueError:
            messagebox.showerror("Error", "Confidence must be between 0.50 and 1.00")
            return

        for label, key in [("Click Delay", "click_delay"), ("Wait Timeout", "timeout")]:
            if not self._v[key].get().strip().isdigit():
                messagebox.showerror("Error", f"{label} must be a whole number")
                return

        engine = self.state.db_engine
        if not engine:
            self._automation_status_lbl.configure(
                text="✗ No DB connection", fg=Color.DANGER)
            return

        try:
            from sqlalchemy.orm import sessionmaker
            from database.models.tally_settings      import TallySettings
            from database.models.automation_settings import AutomationSettings

            Session = sessionmaker(bind=engine)
            db      = Session()

            try:
                # Save exe path
                ts = db.query(TallySettings).filter_by(id=1).first()
                if not ts:
                    ts = TallySettings(id=1)
                    db.add(ts)
                ts.exe_path = self._v["tally_exe"].get().strip() or None

                # Save automation controls
                aut = db.query(AutomationSettings).filter_by(id=1).first()
                if not aut:
                    aut = AutomationSettings(id=1)
                    db.add(aut)

                aut.confidence       = float(self._v["confidence"].get())
                aut.click_delay_ms   = int(self._v["click_delay"].get())
                aut.wait_timeout_sec = int(self._v["timeout"].get())

                db.commit()

                # Update live state — tally_launcher picks this up immediately
                self.state.tally_exe_path = ts.exe_path or ""
                self.state.automation = AutomationConfig(
                    confidence       = aut.confidence,
                    click_delay_ms   = aut.click_delay_ms,
                    wait_timeout_sec = aut.wait_timeout_sec,
                    retry_attempts   = getattr(aut, 'retry_attempts', 3),
                )

                self._automation_status_lbl.configure(
                    text=f"✓ Saved at {datetime.now().strftime('%H:%M:%S')}",
                    fg=Color.SUCCESS)
                self.after(4000, lambda: self._automation_status_lbl.configure(text=""))

            finally:
                db.close()

        except Exception as e:
            from logging_config import logger
            logger.error(f"[Settings] Automation save failed: {e}")
            self._automation_status_lbl.configure(
                text=f"✗ Save failed: {str(e)[:60]}", fg=Color.DANGER)

    # ──────────────────────────────────────────────
    #  SECTION 4 — Screen Images
    # ──────────────────────────────────────────────

    def _build_images_section(self, parent, row):
        card = self._card(parent, row, "🖼️  Screen Images  (PyAutoGUI image recognition)")

        # Description
        tk.Label(
            card,
            text=(
                "These PNG screenshots tell PyAutoGUI what to look for on screen during automation.\n"
                "If automation fails on this PC, take a fresh screenshot of each element and use Browse.\n"
                "Use Test to verify the image is visible on screen right now."
            ),
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            anchor="w", justify="left",
        ).grid(row=0, column=0, columnspan=4, sticky="w", pady=(0, 10))

        # Column headers
        hdr = tk.Frame(card, bg=Color.BG_TABLE_HEADER)
        hdr.grid(row=1, column=0, columnspan=4, sticky="ew", pady=(0, 2))
        for text, width in [("Image", 28), ("File", 32), ("", 24)]:
            tk.Label(hdr, text=text,
                     font=Font.BODY_SM, bg=Color.BG_TABLE_HEADER,
                     fg=Color.TEXT_SECONDARY, anchor="w", width=width,
                     ).pack(side="left", padx=(8, 0), pady=5)

        # One row per image
        tally_images = getattr(self.state, 'tally_images', {}) or {}
        # Keep references to PhotoImage objects so they aren't garbage collected
        self._thumbnails = {}

        for idx, (key, label, desc) in enumerate(IMAGE_DEFS):
            bg = Color.BG_CARD if idx % 2 == 0 else Color.BG_TABLE_HEADER

            row_f = tk.Frame(card, bg=bg)
            row_f.grid(row=2 + idx, column=0, columnspan=5, sticky="ew", pady=1)
            row_f.columnconfigure(1, minsize=200)
            row_f.columnconfigure(2, minsize=220)
            row_f.columnconfigure(3, weight=1)

            # Column 0 — thumbnail preview (60x40)
            current = tally_images.get(key, DEFAULT_FILENAMES.get(key, f"tally_{key}.png"))
            img_path_now = os.path.join(self._assets_dir(), current)
            thumb = self._load_thumbnail(img_path_now)
            self._thumbnails[key] = thumb   # keep reference

            thumb_lbl = tk.Label(
                row_f,
                image=thumb if thumb else None,
                text="" if thumb else "No\npreview",
                font=Font.BODY_SM, bg=bg, fg=Color.TEXT_MUTED,
                width=62, height=42,
                relief="solid", bd=1,
            )
            thumb_lbl.grid(row=0, column=0, padx=(8, 6), pady=6, sticky="w")

            # Column 1 — Label + description
            left = tk.Frame(row_f, bg=bg)
            left.grid(row=0, column=1, sticky="w", padx=(0, 4), pady=6)

            tk.Label(left, text=label,
                     font=Font.BODY, bg=bg, fg=Color.TEXT_PRIMARY,
                     anchor="w").pack(anchor="w")
            tk.Label(left, text=desc,
                     font=Font.BODY_SM, bg=bg, fg=Color.TEXT_MUTED,
                     anchor="w", wraplength=195, justify="left").pack(anchor="w")

            # Column 2 — Filename
            fname_var = tk.StringVar(value=current)

            tk.Label(row_f, textvariable=fname_var,
                     font=Font.MONO_SM, bg=bg, fg=Color.TEXT_SECONDARY,
                     anchor="w",
                     ).grid(row=0, column=2, sticky="w", padx=(8, 0))

            # Column 3 — Browse + Test + result
            btn_f = tk.Frame(row_f, bg=bg)
            btn_f.grid(row=0, column=3, sticky="w", padx=(8, 8))

            tk.Button(
                btn_f, text="Browse",
                font=Font.BUTTON_SM, bg=Color.PRIMARY_LIGHT, fg=Color.PRIMARY,
                relief="solid", bd=1, padx=10, pady=3, cursor="hand2",
                command=lambda k=key, fv=fname_var, tl=thumb_lbl: self._browse_image(k, fv, tl),
            ).pack(side="left", padx=(0, 6))

            tk.Button(
                btn_f, text="Test",
                font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1, padx=10, pady=3, cursor="hand2",
                command=lambda k=key: self._test_image(k),
            ).pack(side="left", padx=(0, 10))

            result_lbl = tk.Label(
                btn_f, text="",
                font=Font.BODY_SM, bg=bg, fg=Color.TEXT_MUTED,
                anchor="w",
            )
            result_lbl.pack(side="left")

            self._image_rows[key] = {
                "filename_var": fname_var,
                "result_lbl":   result_lbl,
                "thumb_lbl":    thumb_lbl,
            }

        # Test All + status
        end = 2 + len(IMAGE_DEFS)
        tk.Frame(card, bg=Color.BORDER, height=1).grid(
            row=end, column=0, columnspan=4, sticky="ew", pady=(10, 8))

        bottom = tk.Frame(card, bg=Color.BG_CARD)
        bottom.grid(row=end + 1, column=0, columnspan=4, sticky="w")

        tk.Button(
            bottom, text="⚡  Test All Images",
            font=Font.BUTTON_SM, bg=Color.SUCCESS_BG, fg=Color.SUCCESS_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=6,
            cursor="hand2", command=self._test_all_images,
        ).pack(side="left", padx=(0, Spacing.MD))

        self._test_all_lbl = tk.Label(
            bottom, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._test_all_lbl.pack(side="left")

    def _browse_image(self, key, fname_var, thumb_lbl=None):
        """Pick a PNG, copy to assets/, save to DB, update state and refresh thumbnail."""
        path = filedialog.askopenfilename(
            title=f"Select image for: {key}",
            filetypes=[("PNG Images", "*.png"), ("All images", "*.png *.jpg *.bmp")],
            parent=self,
        )
        if not path:
            return

        assets_dir = self._assets_dir()
        os.makedirs(assets_dir, exist_ok=True)

        dest_name = DEFAULT_FILENAMES.get(key, f"tally_{key}.png")
        dest_path = os.path.join(assets_dir, dest_name)

        try:
            shutil.copy2(path, dest_path)
        except Exception as e:
            messagebox.showerror("Copy Failed", f"Could not copy image:\n{e}")
            return

        fname_var.set(dest_name)

        if not hasattr(self.state, 'tally_images') or self.state.tally_images is None:
            self.state.tally_images = {}
        self.state.tally_images[key] = dest_name

        self._save_image_to_db(key, dest_name)

        # Refresh thumbnail preview
        if thumb_lbl:
            new_thumb = self._load_thumbnail(dest_path)
            if new_thumb:
                self._thumbnails[key] = new_thumb
                thumb_lbl.configure(image=new_thumb, text="")
            else:
                thumb_lbl.configure(image="", text="No\npreview")

        row = self._image_rows.get(key)
        if row:
            row["result_lbl"].configure(text="Replaced", fg=Color.SUCCESS)
            self.after(3000, lambda: row["result_lbl"].configure(text=""))

    def _save_image_to_db(self, key, filename):
        """Save a single image filename to tally_settings table in DB."""
        engine = self.state.db_engine
        if not engine:
            return
        col_map = {
            "gateway":     "image_gateway",
            "search_box":  "image_search_box",
            "username":    "image_username",
            "data_server": "image_data_server",
            "local_path":  "image_local_path",
        }
        col = col_map.get(key)
        if not col:
            return
        try:
            from sqlalchemy.orm import sessionmaker
            from database.models.tally_settings import TallySettings
            Session = sessionmaker(bind=engine)
            db      = Session()
            try:
                ts = db.query(TallySettings).filter_by(id=1).first()
                if not ts:
                    ts = TallySettings(id=1)
                    db.add(ts)
                if hasattr(ts, col):
                    setattr(ts, col, filename)
                db.commit()
            finally:
                db.close()
        except Exception as e:
            from logging_config import logger
            logger.error(f"[Settings] Failed to save image '{key}' to DB: {e}")

    def _test_image(self, key):
        """
        Click Test -> sleep 5s (user switches to Tally) -> search screen
        at confidence 0.90 / 0.80 / 0.70 -> show result with red box overlay.
        No minimize, no countdown -- simple and reliable.
        """
        try:
            import pyautogui
        except ImportError:
            self._set_result(key, "x pyautogui not installed", Color.DANGER)
            return

        row = self._image_rows.get(key)
        if not row:
            return

        filename = row["filename_var"].get()
        img_path = os.path.join(self._assets_dir(), filename)

        if not os.path.exists(img_path):
            self._set_result(key, "x File missing -- use Browse", Color.DANGER)
            return

        root = self.winfo_toplevel()
        self._set_result(key, "Searching in 5s -- open Tally now...", Color.TEXT_MUTED)

        def search():
            import time
            time.sleep(5)   # user has 5 seconds to switch to Tally

            loc       = None
            used_conf = 0.90

            for conf in [0.90, 0.80, 0.70]:
                try:
                    loc = pyautogui.locateOnScreen(img_path, confidence=conf, grayscale=True)
                    if loc:
                        used_conf = conf
                        break
                except Exception:
                    pass

            if loc:
                x, y, w, h = int(loc.left), int(loc.top), int(loc.width), int(loc.height)

                def found():
                    conf_note = f" (conf {used_conf})" if used_conf < 0.90 else ""
                    self._set_result(key, f"Found at ({x}, {y}){conf_note}", Color.SUCCESS)
                    try:
                        from gui.components.image_test_overlay import ImageTestOverlay
                        ImageTestOverlay(root, x, y, w, h, duration_ms=3000)
                    except Exception:
                        pass
                    self.after(5000, lambda: self._set_result(key, "", Color.TEXT_MUTED))

                root.after(0, found)

            else:
                def not_found():
                    self._set_result(
                        key, "Not found -- retake screenshot with Browse", Color.DANGER)
                    self.after(8000, lambda: self._set_result(key, "", Color.TEXT_MUTED))

                root.after(0, not_found)

        threading.Thread(target=search, daemon=True).start()

    def _test_all_images(self):
        """Test all 5 images — minimize app, test each, show summary."""
        try:
            import pyautogui
        except ImportError:
            self._test_all_lbl.configure(text="✗ pyautogui not installed", fg=Color.DANGER)
            return

        keys       = [k for k, _, _ in IMAGE_DEFS]
        total      = len(keys)
        confidence = self._get_confidence()
        assets_dir = self._assets_dir()
        root       = self.winfo_toplevel()

        self._test_all_lbl.configure(text="Starting in 3s…", fg=Color.TEXT_MUTED)

        def run():
            import time
            time.sleep(3)

            try:
                root.after(0, root.iconify)
            except Exception:
                pass
            time.sleep(0.6)

            results = {}
            for idx, key in enumerate(keys):
                root.after(0, lambda i=idx: self._test_all_lbl.configure(
                    text=f"Testing {i + 1}/{total}…", fg=Color.TEXT_MUTED))

                row      = self._image_rows.get(key)
                filename = row["filename_var"].get() if row else DEFAULT_FILENAMES.get(key, "")
                img_path = os.path.join(assets_dir, filename)

                found = False
                if os.path.exists(img_path):
                    for conf in [0.90, 0.80, 0.70]:
                        try:
                            if pyautogui.locateOnScreen(img_path, confidence=conf, grayscale=True):
                                found = True
                                break
                        except Exception:
                            pass

                results[key] = found
                color = Color.SUCCESS if found else Color.DANGER
                text  = "✓ Found" if found else "✗ Not found"
                root.after(0, lambda k=key, t=text, c=color: self._set_result(k, t, c))
                time.sleep(0.3)

            root.after(0, root.deiconify)

            found_count = sum(1 for v in results.values() if v)
            miss_count  = total - found_count

            def summary():
                if miss_count == 0:
                    self._test_all_lbl.configure(
                        text=f"✓ All {total} images found", fg=Color.SUCCESS)
                else:
                    self._test_all_lbl.configure(
                        text=f"{found_count}/{total} found — {miss_count} missing",
                        fg=Color.DANGER)

            root.after(500, summary)

        threading.Thread(target=run, daemon=True).start()

    # ──────────────────────────────────────────────
    #  SAVE BAR — saves Tally connection settings
    # ──────────────────────────────────────────────

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

        tk.Button(
            bar, text="✓  Save Tally Settings",
            font=Font.BUTTON, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XL, pady=6,
            cursor="hand2", command=self._save_tally,
        ).grid(row=0, column=1)

    def _save_tally(self):
        """Save Tally host/port to ConfigManager and update live state."""
        host = self._v["tally_host"].get().strip() or "localhost"
        port_str = self._v["tally_port"].get().strip()

        if not port_str.isdigit() or not (1 <= int(port_str) <= 65535):
            messagebox.showerror("Error", "Port must be a number between 1 and 65535.")
            return

        port = int(port_str)

        # Save to config.json
        self._cfg.save_tally_config({"host": host, "port": port})

        # Update live state
        self.state.tally.host = host
        self.state.tally.port = port
        if self.state.tally_config:
            self.state.tally_config["host"] = host
            self.state.tally_config["port"] = port

        # Update per-company defaults that are still on default
        for co in self.state.companies.values():
            if co.tally_host == "localhost":
                co.tally_host = host
            if co.tally_port == 9000:
                co.tally_port = port

        self._save_status_lbl.configure(
            text=f"✓ Tally settings saved at {datetime.now().strftime('%H:%M:%S')}",
            fg=Color.SUCCESS)
        self.after(4000, lambda: self._save_status_lbl.configure(text=""))

    # ──────────────────────────────────────────────
    #  HELPERS
    # ──────────────────────────────────────────────

    def _load_thumbnail(self, img_path, size=(60, 40)):
        """Load a PNG as a small Tkinter PhotoImage thumbnail. Returns None if unavailable."""
        if not HAS_PIL:
            return None
        try:
            img = Image.open(img_path)
            img.thumbnail(size, Image.LANCZOS)
            return ImageTk.PhotoImage(img)
        except Exception:
            return None

    def _assets_dir(self):
        return os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            "assets"
        )

    def _get_confidence(self):
        try:
            return float(self._v.get("confidence", tk.StringVar(value="0.80")).get())
        except ValueError:
            return 0.80

    def _set_result(self, key, text, color):
        row = self._image_rows.get(key)
        if row:
            row["result_lbl"].configure(text=text, fg=color)

    def _card(self, parent, row, title):
        """Create a styled card with a colored header."""
        outer = tk.Frame(
            parent, bg=Color.BG_CARD,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        outer.grid(row=row, column=0, sticky="ew",
                   padx=Spacing.XL, pady=(0, Spacing.MD))
        outer.columnconfigure(0, weight=1)

        hdr = tk.Frame(outer, bg=Color.PRIMARY, pady=Spacing.SM)
        hdr.grid(row=0, column=0, sticky="ew")
        tk.Label(hdr, text=title,
                 font=Font.LABEL_BOLD, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
                 anchor="w", padx=Spacing.LG).pack(fill="x")

        content = tk.Frame(outer, bg=Color.BG_CARD, padx=Spacing.XL, pady=Spacing.LG)
        content.grid(row=1, column=0, sticky="ew")
        content.columnconfigure(2, weight=1)
        return content

    # ──────────────────────────────────────────────
    #  LIFECYCLE — called when page is shown
    # ──────────────────────────────────────────────

    def on_show(self):
        """Reload all values from live state and ConfigManager."""
        # Tally connection
        tally_cfg = self._cfg.get_tally_config()
        self._v["tally_host"].set(self.state.tally.host or tally_cfg.get("host", "localhost"))
        self._v["tally_port"].set(str(self.state.tally.port or tally_cfg.get("port", 9000)))
        self._tally_status_lbl.configure(text="")

        # DB fields (reload from ConfigManager)
        if not self._db_editing:
            db_cfg = self._cfg.get_db_config()
            self._v["db_host"].set(db_cfg.get("host",     "localhost"))
            self._v["db_port"].set(str(db_cfg.get("port", 3306)))
            self._v["db_username"].set(db_cfg.get("username", "root"))
            self._v["db_password"].set(db_cfg.get("password", ""))
            self._v["db_database"].set(db_cfg.get("database", ""))
            for entry in self._db_entries.values():
                entry.configure(state="readonly", bg=Color.BG_TABLE_HEADER)
        self._db_status_lbl.configure(text="")

        # Automation
        self._v["tally_exe"].set(getattr(self.state, 'tally_exe_path', '') or '')
        aut = getattr(self.state, 'automation', None)
        if aut:
            self._v["confidence"].set(str(getattr(aut, 'confidence',       0.80)))
            self._v["click_delay"].set(str(getattr(aut, 'click_delay_ms',  500)))
            self._v["timeout"].set(str(getattr(aut, 'wait_timeout_sec',    30)))
        self._automation_status_lbl.configure(text="")

        # Image filenames + refresh thumbnails
        images = getattr(self.state, 'tally_images', {}) or {}
        for key, row in self._image_rows.items():
            fname = images.get(key, DEFAULT_FILENAMES.get(key, f"tally_{key}.png"))
            row["filename_var"].set(fname)
            row["result_lbl"].configure(text="")

            # Refresh thumbnail in case file changed on disk
            thumb_lbl = row.get("thumb_lbl")
            if thumb_lbl:
                img_path = os.path.join(self._assets_dir(), fname)
                new_thumb = self._load_thumbnail(img_path)
                if new_thumb:
                    self._thumbnails[key] = new_thumb
                    thumb_lbl.configure(image=new_thumb, text="")
                else:
                    thumb_lbl.configure(image="", text="No\npreview")