"""
gui/components/setup_wizard.py
================================
First-run setup wizard — shown automatically when:
  1. config.json does not exist (brand new install)
  2. setup_complete = False (previous setup failed or was cancelled)
  3. DB connection fails on startup (credentials changed / DB moved)

Thread-safety approach
----------------------
The wizard is shown BEFORE root.mainloop() is running (root is withdrawn).
This means .after() on ANY tkinter widget called from a background thread
will raise "RuntimeError: main thread is not in main loop" — even if you
capture winfo_toplevel() before the thread starts — because there is no
event loop processing callbacks yet.

Solution: each panel has its own queue.Queue + a _poll() loop.
  - _poll() is started with Toplevel.after() from the MAIN thread (safe).
  - Background workers put results into the queue; never touch tkinter.
  - _poll() reads results on the main thread and updates the UI safely.

Two-step flow:
  Step 1 — DB Configuration
    Test     = connect to MySQL server without DB name (validates credentials)
    Next →   = create DB if needed + create tables → save config → advance

  Step 2 — Tally Configuration
    Fields   = Tally host, port, Tally.exe path (browse button)
    Launch   = open Tally via Win+R with the configured exe path
    Test     = ping Tally XML port → confirm TDL server is responding
    Finish   = save tally config (host/port) + save exe path to DB tally_settings
"""

import os
import queue
import subprocess
import threading
import tkinter as tk
from tkinter import filedialog, messagebox

from gui.config_manager import ConfigManager
from gui.styles         import Color, Font, Spacing

# Poll interval for result queues (ms)
_POLL_MS = 50


# ─────────────────────────────────────────────────────────────────────────────
#  SetupWizard — Toplevel container
# ─────────────────────────────────────────────────────────────────────────────
class SetupWizard(tk.Toplevel):
    """
    Modal wizard window.
    After both steps complete → self.completed = True
    If user closes/cancels   → self.completed = False
    """

    def __init__(self, parent, config_manager: ConfigManager, error_msg: str = ""):
        super().__init__(parent)
        self.title("Tally Sync Manager — Setup")
        self.resizable(False, False)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._on_cancel)

        self.config_manager = config_manager
        self.completed      = False
        self._error_msg     = error_msg

        # db_engine is stored here after Step 1 so Step 2 can save to tally_settings
        self._db_engine     = None

        self._center(parent)
        self._show_step1()

    # ─────────────────────────────────────────────────────────────────────────
    def _center(self, parent):
        self.update_idletasks()
        w, h = 540, 560
        try:
            px = parent.winfo_rootx() + parent.winfo_width()  // 2
            py = parent.winfo_rooty() + parent.winfo_height() // 2
        except Exception:
            px = self.winfo_screenwidth()  // 2
            py = self.winfo_screenheight() // 2
        self.geometry(f"{w}x{h}+{px - w // 2}+{py - h // 2}")

    def _clear(self):
        for w in self.winfo_children():
            w.destroy()

    def _show_step1(self):
        self._clear()
        self.title("Setup  [1/2]  —  Database Connection")
        Step1Panel(
            self, self.config_manager, self._error_msg,
            on_next   = self._on_step1_done,
            on_cancel = self._on_cancel,
        )

    def _on_step1_done(self, db_engine):
        """Called by Step1Panel after DB is ready. Passes engine to Step 2."""
        self._db_engine = db_engine
        self._clear()
        self.title("Setup  [2/2]  —  Tally Connection")
        Step2Panel(
            self, self.config_manager,
            db_engine = db_engine,
            on_finish = self._on_finish,
            on_back   = self._show_step1,
            on_cancel = self._on_cancel,
        )

    def _on_finish(self):
        self.config_manager.mark_setup_complete()
        self.completed = True
        self.destroy()

    def _on_cancel(self):
        if messagebox.askyesno(
            "Cancel Setup",
            "Setup is not complete.\n\nAre you sure you want to exit?",
            parent=self,
        ):
            self.completed = False
            self.destroy()


# ─────────────────────────────────────────────────────────────────────────────
#  Step 1 Panel — Database Configuration  (unchanged from original)
# ─────────────────────────────────────────────────────────────────────────────
class Step1Panel(tk.Frame):

    def __init__(self, parent, config_manager: ConfigManager,
                 error_msg: str, on_next, on_cancel):
        super().__init__(parent, bg=Color.BG_CARD)
        self.pack(fill="both", expand=True)
        self._cfg       = config_manager
        self._on_next   = on_next   # now called as on_next(db_engine)
        self._on_cancel = on_cancel
        self._vars      = {}
        self._busy      = False
        self._animating = False
        self._anim_pos  = 0
        self._q: queue.Queue = queue.Queue()

        self._build(error_msg)
        self.master.after(_POLL_MS, self._poll)

    # ── Queue polling ─────────────────────────────────────────────────────────
    def _poll(self):
        try:
            while True:
                msg = self._q.get_nowait()
                self._handle_msg(msg)
        except queue.Empty:
            pass
        try:
            self.master.after(_POLL_MS, self._poll)
        except tk.TclError:
            pass

    def _handle_msg(self, msg: tuple):
        kind = msg[0]

        if kind == "test_result":
            _, ok, detail = msg
            self._set_busy(False)
            if ok:
                self._set_feedback(f"✓ {detail}", Color.SUCCESS)
            else:
                self._set_feedback(f"✗ {detail}", Color.DANGER)

        elif kind == "setup_result":
            _, ok, detail, cfg, engine = msg
            self._set_busy(False)
            if ok:
                self._cfg.save_db_config(cfg)
                self._set_feedback("✓ Database connected and ready!", Color.SUCCESS)
                try:
                    self.master.after(800, lambda: self._on_next(engine))
                except tk.TclError:
                    pass
            else:
                self._set_feedback(f"✗ Setup failed: {detail}", Color.DANGER)

    # ── Build UI ──────────────────────────────────────────────────────────────
    def _build(self, error_msg: str):
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=36, pady=28)
        pad.pack(fill="both", expand=True)

        tk.Label(
            pad, text="🗄️  Database Connection",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 4))

        tk.Label(
            pad,
            text="Enter your MySQL / MariaDB connection details.\n"
                 "The database will be created automatically if it doesn't exist.",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            justify="left",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 16))

        tk.Frame(pad, bg=Color.BORDER, height=1).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(0, 16),
        )

        existing = self._cfg.get_db_config()
        fields = [
            ("Host",     "host",     existing.get("host",     "localhost"), False),
            ("Port",     "port",     existing.get("port",     3306),        False),
            ("Username", "username", existing.get("username", "root"),      False),
            ("Password", "password", existing.get("password", ""),          True),
            ("Database", "database", existing.get("database", "tally_db"),  False),
        ]

        for i, (label, key, default, secret) in enumerate(fields):
            r = i + 3
            tk.Label(
                pad, text=f"{label}:",
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                anchor="w", width=10,
            ).grid(row=r, column=0, sticky="w", pady=5)

            var = tk.StringVar(value=str(default))
            self._vars[key] = var
            tk.Entry(
                pad, textvariable=var,
                font=Font.BODY, width=30,
                bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
                show="●" if secret else "",
            ).grid(row=r, column=1, sticky="ew", pady=5, padx=(10, 0))

        pad.columnconfigure(1, weight=1)

        self._feedback = tk.Label(
            pad, text=error_msg,
            font=Font.BODY_SM, bg=Color.BG_CARD,
            fg=Color.DANGER if error_msg else Color.TEXT_MUTED,
            wraplength=440, justify="left",
        )
        self._feedback.grid(row=8, column=0, columnspan=2, sticky="w", pady=(12, 0))

        # Indeterminate progress bar
        self._prog_frame = tk.Frame(pad, bg=Color.BG_CARD)
        self._prog_frame.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(4, 0))
        self._prog_canvas = tk.Canvas(
            self._prog_frame, height=4,
            bg=Color.PROGRESS_BG, highlightthickness=0, bd=0,
        )
        self._prog_bar_id = None
        self._prog_frame.grid_remove()

        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(20, 0))

        tk.Button(
            btn_row, text="Cancel",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=12, pady=5, cursor="hand2",
            command=self._on_cancel,
        ).pack(side="left")

        tk.Button(
            btn_row, text="🔌  Test Connection",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=12, pady=5, cursor="hand2",
            command=self._on_test,
        ).pack(side="right", padx=(8, 0))

        self._next_btn = tk.Button(
            btn_row, text="Next  →",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=16, pady=5, cursor="hand2",
            command=self._on_next_click,
        )
        self._next_btn.pack(side="right")

        tk.Label(
            pad, text="Step 1 of 2",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).grid(row=11, column=0, columnspan=2, pady=(12, 0))

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _collect(self) -> dict:
        return {k: v.get().strip() for k, v in self._vars.items()}

    def _validate(self) -> tuple:
        d = self._collect()
        if not d.get("host"):
            return False, "Host is required."
        if not d.get("database"):
            return False, "Database name is required."
        if not d.get("username"):
            return False, "Username is required."
        try:
            port = int(d.get("port", "3306"))
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            return False, "Port must be a number between 1 and 65535."
        return True, ""

    def _set_feedback(self, msg: str, color: str = None):
        self._feedback.configure(text=msg, fg=color or Color.TEXT_MUTED)

    def _set_busy(self, busy: bool):
        self._busy = busy
        if busy:
            self._prog_frame.grid()
            self._prog_canvas.pack(fill="x")
            self._next_btn.configure(state="disabled")
            self._animating = True
            self._anim_pos  = 0
            self._animate()
        else:
            self._animating = False
            self._prog_frame.grid_remove()
            self._next_btn.configure(state="normal")

    def _animate(self):
        if not self._animating:
            return
        try:
            self._prog_canvas.update_idletasks()
            w = self._prog_canvas.winfo_width() or 460
            if self._prog_bar_id:
                self._prog_canvas.delete(self._prog_bar_id)
            bar_w = w // 3
            x1    = self._anim_pos % (w + bar_w) - bar_w
            x2    = x1 + bar_w
            self._prog_bar_id = self._prog_canvas.create_rectangle(
                x1, 0, x2, 4, fill=Color.PRIMARY, width=0,
            )
            self._anim_pos += 8
            self.master.after(30, self._animate)
        except tk.TclError:
            pass

    # ── Test connection (credentials only, no DB name) ────────────────────────
    def _on_test(self):
        if self._busy:
            return
        ok, err = self._validate()
        if not ok:
            self._set_feedback(f"✗ {err}", Color.DANGER)
            return
        self._set_feedback("Testing connection...", Color.TEXT_MUTED)
        self._set_busy(True)
        cfg = self._collect()

        def worker():
            result, detail = self._do_test(cfg)
            self._q.put(("test_result", result, detail))

        threading.Thread(target=worker, daemon=True).start()

    def _do_test(self, cfg: dict) -> tuple:
        try:
            import pymysql
            conn = pymysql.connect(
                host=cfg["host"], port=int(cfg.get("port", 3306)),
                user=cfg["username"], password=cfg["password"],
                connect_timeout=8,
            )
            conn.close()
            return True, "MySQL server reachable — credentials OK!"
        except Exception as e:
            return False, str(e)

    # ── Next → create DB + tables ─────────────────────────────────────────────
    def _on_next_click(self):
        if self._busy:
            return
        ok, err = self._validate()
        if not ok:
            self._set_feedback(f"✗ {err}", Color.DANGER)
            return
        self._set_feedback("Connecting and setting up database...", Color.TEXT_MUTED)
        self._set_busy(True)
        cfg = self._collect()

        def worker():
            result, detail, engine = self._do_setup(cfg)
            self._q.put(("setup_result", result, detail, cfg, engine))

        threading.Thread(target=worker, daemon=True).start()

    def _do_setup(self, cfg: dict) -> tuple:
        try:
            from database.db_connector import DatabaseConnector
            conn = DatabaseConnector(
                username=cfg["username"], password=cfg["password"],
                host=cfg["host"],         port=int(cfg.get("port", 3306)),
                database=cfg["database"],
            )
            conn.create_database_if_not_exists()
            conn.create_tables()
            engine = conn.get_engine()
            return True, "Database ready.", engine
        except Exception as e:
            return False, str(e), None


# ─────────────────────────────────────────────────────────────────────────────
#  Step 2 Panel — Tally Configuration  (UPGRADED)
#
#  New in this version:
#    • Tally.exe path field  +  Browse button  +  path validation
#    • "🚀 Launch Tally" button  →  Win+R → type path → Enter
#    • "🔌 Test Tally" button  →  ping XML port → show result
#    • Finish  →  saves host/port to config.json  +  saves exe_path to DB
# ─────────────────────────────────────────────────────────────────────────────
class Step2Panel(tk.Frame):

    def __init__(self, parent, config_manager: ConfigManager,
                 db_engine, on_finish, on_back, on_cancel):
        super().__init__(parent, bg=Color.BG_CARD)
        self.pack(fill="both", expand=True)
        self._cfg       = config_manager
        self._engine    = db_engine       # SQLAlchemy engine from Step 1
        self._on_finish = on_finish
        self._on_back   = on_back
        self._on_cancel = on_cancel
        self._vars      = {}
        self._busy      = False

        self._q: queue.Queue = queue.Queue()

        self._build()
        self.master.after(_POLL_MS, self._poll)

    # ── Queue polling ─────────────────────────────────────────────────────────
    def _poll(self):
        try:
            while True:
                msg = self._q.get_nowait()
                self._handle_msg(msg)
        except queue.Empty:
            pass
        try:
            self.master.after(_POLL_MS, self._poll)
        except tk.TclError:
            pass

    def _handle_msg(self, msg: tuple):
        kind = msg[0]

        if kind == "test_result":
            _, ok, detail = msg
            self._set_busy(False)
            self._test_btn.configure(state="normal", text="🔌  Test Tally")
            if ok:
                self._set_feedback(f"✓ {detail}", Color.SUCCESS)
            else:
                self._set_feedback(f"✗ {detail}", Color.DANGER)

        elif kind == "launch_result":
            _, ok, detail = msg
            self._launch_btn.configure(state="normal", text="🚀  Launch Tally")
            if ok:
                self._set_feedback(
                    "✓ Tally launch command sent. Wait a few seconds then click Test Tally.",
                    Color.SUCCESS,
                )
            else:
                self._set_feedback(f"✗ {detail}", Color.DANGER)

        elif kind == "save_result":
            _, ok, detail = msg
            self._set_busy(False)
            if ok:
                self._set_feedback("✓ Configuration saved!", Color.SUCCESS)
                try:
                    self.master.after(600, self._on_finish)
                except tk.TclError:
                    pass
            else:
                self._set_feedback(f"✗ Save failed: {detail}", Color.DANGER)

    # ── Build UI ──────────────────────────────────────────────────────────────
    def _build(self):
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=36, pady=24)
        pad.pack(fill="both", expand=True)
        pad.columnconfigure(1, weight=1)

        # ── Header ────────────────────────────────────────────────────────────
        tk.Label(
            pad, text="⚡  Tally Connection",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).grid(row=0, column=0, columnspan=3, sticky="w", pady=(0, 4))

        tk.Label(
            pad,
            text=(
                "Configure how this app connects to Tally.\n"
                "Set the host/port for XML API, and the Tally.exe path for auto-launch."
            ),
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            justify="left",
        ).grid(row=1, column=0, columnspan=3, sticky="w", pady=(0, 12))

        tk.Frame(pad, bg=Color.BORDER, height=1).grid(
            row=2, column=0, columnspan=3, sticky="ew", pady=(0, 16),
        )

        # ── Tally Host ────────────────────────────────────────────────────────
        existing = self._cfg.get_tally_config()

        tk.Label(
            pad, text="Tally Host:",
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            anchor="w", width=14,
        ).grid(row=3, column=0, sticky="w", pady=6)

        host_var = tk.StringVar(value=str(existing.get("host", "localhost")))
        self._vars["host"] = host_var
        tk.Entry(
            pad, textvariable=host_var,
            font=Font.BODY, width=22,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY, relief="solid", bd=1,
        ).grid(row=3, column=1, columnspan=2, sticky="ew", pady=6, padx=(10, 0))

        # ── Tally Port ────────────────────────────────────────────────────────
        tk.Label(
            pad, text="Tally Port:",
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            anchor="w", width=14,
        ).grid(row=4, column=0, sticky="w", pady=6)

        port_var = tk.StringVar(value=str(existing.get("port", 9000)))
        self._vars["port"] = port_var
        tk.Entry(
            pad, textvariable=port_var,
            font=Font.BODY, width=10,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY, relief="solid", bd=1,
        ).grid(row=4, column=1, columnspan=2, sticky="w", pady=6, padx=(10, 0))

        # ── Divider ───────────────────────────────────────────────────────────
        tk.Frame(pad, bg=Color.BORDER_LIGHT, height=1).grid(
            row=5, column=0, columnspan=3, sticky="ew", pady=(8, 12),
        )

        # ── Tally.exe Path ────────────────────────────────────────────────────
        tk.Label(
            pad, text="Tally.exe Path:",
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            anchor="w", width=14,
        ).grid(row=6, column=0, sticky="w", pady=6)

        # Load existing exe path from DB if engine is available
        saved_exe = self._load_saved_exe_path()
        exe_var = tk.StringVar(value=saved_exe)
        self._vars["exe_path"] = exe_var

        exe_entry = tk.Entry(
            pad, textvariable=exe_var,
            font=Font.BODY, width=26,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY, relief="solid", bd=1,
        )
        exe_entry.grid(row=6, column=1, sticky="ew", pady=6, padx=(10, 4))

        tk.Button(
            pad, text="📂  Browse",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=8, pady=3, cursor="hand2",
            command=self._browse_exe,
        ).grid(row=6, column=2, sticky="w", pady=6)

        # ── Hint under exe path ───────────────────────────────────────────────
        tk.Label(
            pad,
            text="e.g.  C:\\TallyPrime\\tally.exe",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            anchor="w",
        ).grid(row=7, column=1, columnspan=2, sticky="w", padx=(10, 0))

        # ── Launch + Test buttons row ─────────────────────────────────────────
        action_row = tk.Frame(pad, bg=Color.BG_CARD)
        action_row.grid(row=8, column=0, columnspan=3, sticky="ew", pady=(14, 0))

        self._launch_btn = tk.Button(
            action_row, text="🚀  Launch Tally",
            font=Font.BUTTON_SM, bg=Color.PRIMARY_LIGHT, fg=Color.PRIMARY,
            relief="solid", bd=1, padx=14, pady=5, cursor="hand2",
            command=self._on_launch,
        )
        self._launch_btn.pack(side="left")

        self._test_btn = tk.Button(
            action_row, text="🔌  Test Tally",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=14, pady=5, cursor="hand2",
            command=self._on_test,
        )
        self._test_btn.pack(side="left", padx=(10, 0))

        # ── Info box ──────────────────────────────────────────────────────────
        info = tk.Frame(
            pad, bg=Color.INFO_BG,
            highlightthickness=1, highlightbackground=Color.ACCENT,
            padx=12, pady=8,
        )
        info.grid(row=9, column=0, columnspan=3, sticky="ew", pady=(14, 0))
        tk.Label(
            info,
            text=(
                "ℹ️  To enable Tally TDL Server:\n"
                "  Tally → F1 (Help) → TDL & Add-On → Enable TDL Server\n"
                "  Then set port to 9000 (or your chosen port)"
            ),
            font=Font.BODY_SM, bg=Color.INFO_BG, fg=Color.INFO_FG,
            justify="left", anchor="w",
        ).pack(fill="x")

        # ── Feedback label ────────────────────────────────────────────────────
        self._feedback = tk.Label(
            pad, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            wraplength=460, justify="left",
        )
        self._feedback.grid(row=10, column=0, columnspan=3, sticky="w", pady=(10, 0))

        # ── Bottom buttons ────────────────────────────────────────────────────
        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.grid(row=11, column=0, columnspan=3, sticky="ew", pady=(16, 0))

        tk.Button(
            btn_row, text="←  Back",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=12, pady=5, cursor="hand2",
            command=self._on_back,
        ).pack(side="left")

        self._finish_btn = tk.Button(
            btn_row, text="✓  Finish Setup",
            font=Font.BUTTON_SM, bg=Color.SUCCESS, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=16, pady=5, cursor="hand2",
            command=self._on_finish_click,
        )
        self._finish_btn.pack(side="right")

        tk.Label(
            pad, text="Step 2 of 2",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).grid(row=12, column=0, columnspan=3, pady=(10, 0))

    # ── Load existing exe path from tally_settings table ─────────────────────
    def _load_saved_exe_path(self) -> str:
        """Read exe_path from tally_settings table if DB engine is available."""
        if not self._engine:
            return ""
        try:
            from sqlalchemy.orm import sessionmaker
            from database.models.tally_settings import TallySettings
            Session = sessionmaker(bind=self._engine)
            db = Session()
            try:
                ts = db.query(TallySettings).filter_by(id=1).first()
                return ts.exe_path or "" if ts else ""
            finally:
                db.close()
        except Exception:
            return ""

    # ── Browse for Tally.exe ──────────────────────────────────────────────────
    def _browse_exe(self):
        path = filedialog.askopenfilename(
            title="Select Tally.exe",
            filetypes=[("Executable", "*.exe"), ("All files", "*.*")],
            initialdir=r"C:\TallyPrime",
        )
        if path:
            # Normalize to Windows path separators
            path = os.path.normpath(path)
            self._vars["exe_path"].set(path)
            self._set_feedback(f"Path set: {path}", Color.TEXT_SECONDARY)

    # ── Launch Tally via Win+R ────────────────────────────────────────────────
    def _on_launch(self):
        exe_path = self._vars["exe_path"].get().strip()

        if not exe_path:
            self._set_feedback(
                "✗ Please set the Tally.exe path first (Browse or type it).",
                Color.DANGER,
            )
            return

        if not os.path.exists(exe_path):
            self._set_feedback(
                f"✗ File not found: {exe_path}\n"
                "Please check the path and try again.",
                Color.DANGER,
            )
            return

        self._launch_btn.configure(state="disabled", text="Launching...")
        self._set_feedback("Opening Tally via Win+R...", Color.TEXT_MUTED)

        def worker():
            ok, detail = self._do_launch(exe_path)
            self._q.put(("launch_result", ok, detail))

        threading.Thread(target=worker, daemon=True).start()

    def _do_launch(self, exe_path: str) -> tuple:
        """
        Launch Tally using Win+R shortcut:
          1. Press Win+R  →  Run dialog opens
          2. Type the full exe path
          3. Press Enter

        Falls back to direct subprocess.Popen if pyautogui is unavailable.
        """
        try:
            import pyautogui
            import time

            # Press Win+R to open Run dialog
            pyautogui.hotkey('win', 'r')
            time.sleep(0.8)  # wait for Run dialog to appear

            # Clear any existing text and type the exe path
            pyautogui.hotkey('ctrl', 'a')
            pyautogui.typewrite(exe_path, interval=0.04)
            time.sleep(0.3)

            # Press Enter to run
            pyautogui.press('enter')
            time.sleep(1)

            return True, "Launch command sent via Win+R"

        except ImportError:
            # pyautogui not installed — use subprocess directly
            try:
                subprocess.Popen(
                    [exe_path],
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                    if hasattr(subprocess, 'CREATE_NEW_PROCESS_GROUP') else 0,
                )
                return True, "Tally launched via subprocess"
            except Exception as e:
                return False, f"Could not launch Tally: {e}"

        except Exception as e:
            return False, f"Win+R launch failed: {e}"

    # ── Test Tally XML connection ─────────────────────────────────────────────
    def _on_test(self):
        ok, err = self._validate()
        if not ok:
            self._set_feedback(f"✗ {err}", Color.DANGER)
            return

        cfg = self._collect()
        self._set_feedback("Testing Tally connection...", Color.TEXT_MUTED)
        self._test_btn.configure(state="disabled", text="Testing...")
        self._set_busy(True)

        def worker():
            result, detail = self._do_test(cfg)
            self._q.put(("test_result", result, detail))

        threading.Thread(target=worker, daemon=True).start()

    def _do_test(self, cfg: dict) -> tuple:
        try:
            from services.tally_connector import TallyConnector
            tc = TallyConnector(host=cfg["host"], port=int(cfg["port"]))
            # Try ping first, fall back to status check
            if hasattr(tc, 'ping'):
                ok = tc.ping()
            else:
                ok = tc.status == "Connected"

            if ok:
                return True, f"Tally is reachable at {cfg['host']}:{cfg['port']} ✓"
            return False, "Tally did not respond. Is Tally running with TDL Server enabled?"
        except Exception as e:
            return False, f"Cannot reach Tally: {e}"

    # ── Finish — save config.json + save exe_path to DB ──────────────────────
    def _on_finish_click(self):
        if self._busy:
            return
        ok, err = self._validate()
        if not ok:
            self._set_feedback(f"✗ {err}", Color.DANGER)
            return

        cfg      = self._collect()
        exe_path = self._vars["exe_path"].get().strip()

        # Validate exe path if provided (it's optional — user may launch Tally manually)
        if exe_path and not os.path.exists(exe_path):
            if not messagebox.askyesno(
                "Path Not Found",
                f"Tally.exe not found at:\n{exe_path}\n\n"
                "You can still save and fix this later in Settings.\n\n"
                "Continue anyway?",
                parent=self.master,
            ):
                return

        self._set_feedback("Saving configuration...", Color.TEXT_MUTED)
        self._set_busy(True)

        def worker():
            ok, detail = self._do_save(cfg, exe_path)
            self._q.put(("save_result", ok, detail))

        threading.Thread(target=worker, daemon=True).start()

    def _do_save(self, cfg: dict, exe_path: str) -> tuple:
        """
        1. Save host/port to config.json
        2. Save exe_path to tally_settings table in DB
        """
        try:
            # Save host + port to config.json
            self._cfg.save_tally_config({
                "host": cfg["host"],
                "port": int(cfg["port"]),
            })

            # Save exe_path to DB tally_settings table (id=1 row)
            if self._engine and exe_path:
                self._save_exe_to_db(exe_path)

            return True, "Saved."
        except Exception as e:
            return False, str(e)

    def _save_exe_to_db(self, exe_path: str):
        """Upsert exe_path into tally_settings table (id=1)."""
        from sqlalchemy.orm import sessionmaker
        from database.models.tally_settings import TallySettings

        Session = sessionmaker(bind=self._engine)
        db = Session()
        try:
            ts = db.query(TallySettings).filter_by(id=1).first()
            if ts:
                ts.exe_path = exe_path
            else:
                ts = TallySettings(id=1, exe_path=exe_path)
                db.add(ts)
            db.commit()
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _collect(self) -> dict:
        return {
            "host": self._vars["host"].get().strip() or "localhost",
            "port": self._vars["port"].get().strip() or "9000",
        }

    def _validate(self) -> tuple:
        d = self._collect()
        try:
            port = int(d["port"])
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            return False, "Port must be a number between 1 and 65535."
        return True, ""

    def _set_feedback(self, msg: str, color: str = None):
        self._feedback.configure(text=msg, fg=color or Color.TEXT_MUTED)

    def _set_busy(self, busy: bool):
        self._busy = busy
        state = "disabled" if busy else "normal"
        try:
            self._finish_btn.configure(state=state)
        except tk.TclError:
            pass