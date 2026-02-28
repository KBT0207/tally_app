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
    Test     = optional ping (Tally may be offline during setup)
    Finish   = save config → mark setup complete
"""

import queue
import threading
import tkinter as tk
from tkinter import messagebox

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

        self._center(parent)
        self._show_step1()

    # ─────────────────────────────────────────────────────────────────────────
    def _center(self, parent):
        self.update_idletasks()
        w, h = 520, 500
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
            on_next   = self._show_step2,
            on_cancel = self._on_cancel,
        )

    def _show_step2(self):
        self._clear()
        self.title("Setup  [2/2]  —  Tally Connection")
        Step2Panel(
            self, self.config_manager,
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
#  Step 1 Panel — Database Configuration
# ─────────────────────────────────────────────────────────────────────────────
class Step1Panel(tk.Frame):

    def __init__(self, parent, config_manager: ConfigManager,
                 error_msg: str, on_next, on_cancel):
        super().__init__(parent, bg=Color.BG_CARD)
        self.pack(fill="both", expand=True)
        self._cfg       = config_manager
        self._on_next   = on_next
        self._on_cancel = on_cancel
        self._vars      = {}
        self._busy      = False
        self._animating = False
        self._anim_pos  = 0

        # Queue for thread → main-thread communication
        # Workers put results here; _poll() reads on main thread
        self._q: queue.Queue = queue.Queue()

        self._build(error_msg)
        # Start polling — called from main thread so safe
        self.master.after(_POLL_MS, self._poll)

    # ─────────────────────────────────────────────────────────────────────────
    #  Queue polling — runs exclusively on main thread
    # ─────────────────────────────────────────────────────────────────────────
    def _poll(self):
        try:
            while True:
                msg = self._q.get_nowait()
                self._handle_msg(msg)
        except queue.Empty:
            pass
        # Reschedule via Toplevel (self.master) — always safe from main thread
        try:
            self.master.after(_POLL_MS, self._poll)
        except tk.TclError:
            pass  # wizard was destroyed

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
            _, ok, detail, cfg = msg
            self._set_busy(False)
            if ok:
                self._cfg.save_db_config(cfg)
                self._set_feedback("✓ Database connected and ready!", Color.SUCCESS)
                # Delay advance — safe: called from main thread via _poll
                try:
                    self.master.after(800, self._on_next)
                except tk.TclError:
                    pass
            else:
                self._set_feedback(f"✗ Setup failed: {detail}", Color.DANGER)

    # ─────────────────────────────────────────────────────────────────────────
    #  Build UI
    # ─────────────────────────────────────────────────────────────────────────
    def _build(self, error_msg: str):
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=36, pady=28)
        pad.pack(fill="both", expand=True)

        # Header
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

        # Fields — pre-filled from saved config
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

        # Feedback label
        self._feedback = tk.Label(
            pad, text=error_msg,
            font=Font.BODY_SM, bg=Color.BG_CARD,
            fg=Color.DANGER if error_msg else Color.TEXT_MUTED,
            wraplength=420, justify="left",
        )
        self._feedback.grid(row=8, column=0, columnspan=2, sticky="w", pady=(12, 0))

        # Progress bar (hidden until busy)
        self._prog_frame = tk.Frame(pad, bg=Color.BG_CARD)
        self._prog_frame.grid(row=9, column=0, columnspan=2, sticky="ew", pady=(4, 0))
        self._prog_canvas = tk.Canvas(
            self._prog_frame, height=4,
            bg=Color.PROGRESS_BG, highlightthickness=0, bd=0,
        )
        self._prog_bar_id = None
        self._prog_frame.grid_remove()

        # Buttons
        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.grid(row=10, column=0, columnspan=2, sticky="ew", pady=(20, 0))
        btn_row.columnconfigure(0, weight=1)

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

    # ─────────────────────────────────────────────────────────────────────────
    def _collect(self) -> dict:
        return {k: v.get().strip() for k, v in self._vars.items()}

    def _validate(self) -> tuple[bool, str]:
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
        """Indeterminate progress bar — runs on main thread only."""
        if not self._animating:
            return
        try:
            self._prog_canvas.update_idletasks()
            w = self._prog_canvas.winfo_width() or 420
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

    # ─────────────────────────────────────────────────────────────────────────
    #  Test Connection
    # ─────────────────────────────────────────────────────────────────────────
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
            # ONLY interaction with tkinter from this thread: put into queue
            self._q.put(("test_result", result, detail))

        threading.Thread(target=worker, daemon=True).start()

    def _do_test(self, cfg: dict) -> tuple[bool, str]:
        """
        Connect to MySQL WITHOUT the database name to test credentials.
        Avoids '1049 Unknown database' when DB doesn't exist yet.
        """
        try:
            import pymysql
            conn = pymysql.connect(
                host            = cfg["host"],
                port            = int(cfg.get("port", 3306)),
                user            = cfg["username"],
                password        = cfg["password"],
                connect_timeout = 8,
            )
            conn.close()
            return True, "MySQL server reachable — credentials OK!"
        except Exception as e:
            return False, str(e)

    # ─────────────────────────────────────────────────────────────────────────
    #  Next → (create DB + tables)
    # ─────────────────────────────────────────────────────────────────────────
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
            result, detail = self._do_setup(cfg)
            self._q.put(("setup_result", result, detail, cfg))

        threading.Thread(target=worker, daemon=True).start()

    def _do_setup(self, cfg: dict) -> tuple[bool, str]:
        try:
            from database.db_connector import DatabaseConnector
            conn = DatabaseConnector(
                username = cfg["username"],
                password = cfg["password"],
                host     = cfg["host"],
                port     = int(cfg.get("port", 3306)),
                database = cfg["database"],
            )
            conn.create_database_if_not_exists()
            conn.create_tables()
            conn.close()
            return True, "Database ready."
        except Exception as e:
            return False, str(e)


# ─────────────────────────────────────────────────────────────────────────────
#  Step 2 Panel — Tally Configuration
# ─────────────────────────────────────────────────────────────────────────────
class Step2Panel(tk.Frame):

    def __init__(self, parent, config_manager: ConfigManager,
                 on_finish, on_back, on_cancel):
        super().__init__(parent, bg=Color.BG_CARD)
        self.pack(fill="both", expand=True)
        self._cfg       = config_manager
        self._on_finish = on_finish
        self._on_back   = on_back
        self._on_cancel = on_cancel
        self._vars      = {}

        self._q: queue.Queue = queue.Queue()

        self._build()
        self.master.after(_POLL_MS, self._poll)

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
            self._test_btn.configure(state="normal", text="🔌  Test Tally")
            if ok:
                self._set_feedback(f"✓ {detail}", Color.SUCCESS)
            else:
                self._set_feedback(f"✗ {detail}", Color.DANGER)

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        pad = tk.Frame(self, bg=Color.BG_CARD, padx=36, pady=28)
        pad.pack(fill="both", expand=True)

        tk.Label(
            pad, text="⚡  Tally Connection",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 4))

        tk.Label(
            pad,
            text=(
                "Enter the Tally ERP connection details.\n"
                "Make sure Tally is running and TDL Server is enabled.\n\n"
                "Default: Host = localhost, Port = 9000\n"
                "These are global defaults — you can override per-company later.\n\n"
                "You can skip the test and click Finish if Tally isn't running yet."
            ),
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            justify="left",
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 16))

        tk.Frame(pad, bg=Color.BORDER, height=1).grid(
            row=2, column=0, columnspan=2, sticky="ew", pady=(0, 20),
        )

        existing = self._cfg.get_tally_config()
        fields = [
            ("Tally Host", "host", existing.get("host", "localhost")),
            ("Tally Port", "port", existing.get("port", 9000)),
        ]

        for i, (label, key, default) in enumerate(fields):
            r = i + 3
            tk.Label(
                pad, text=f"{label}:",
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                anchor="w", width=12,
            ).grid(row=r, column=0, sticky="w", pady=8)

            var = tk.StringVar(value=str(default))
            self._vars[key] = var
            tk.Entry(
                pad, textvariable=var,
                font=Font.BODY, width=20,
                bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
            ).grid(row=r, column=1, sticky="w", pady=8, padx=(10, 0))

        pad.columnconfigure(1, weight=1)

        # Info box
        info = tk.Frame(
            pad, bg=Color.INFO_BG,
            highlightthickness=1, highlightbackground=Color.ACCENT,
            padx=12, pady=10,
        )
        info.grid(row=5, column=0, columnspan=2, sticky="ew", pady=(16, 0))
        tk.Label(
            info,
            text=(
                "ℹ️  How to enable Tally TDL Server:\n"
                "  Tally → F1 (Help) → TDL & Add-On → Enable TDL Server\n"
                "  Then set port to 9000 (or your chosen port)"
            ),
            font=Font.BODY_SM, bg=Color.INFO_BG, fg=Color.INFO_FG,
            justify="left", anchor="w",
        ).pack(fill="x")

        # Feedback
        self._feedback = tk.Label(
            pad, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
            wraplength=420, justify="left",
        )
        self._feedback.grid(row=6, column=0, columnspan=2, sticky="w", pady=(12, 0))

        # Buttons
        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.grid(row=7, column=0, columnspan=2, sticky="ew", pady=(20, 0))
        btn_row.columnconfigure(0, weight=1)

        tk.Button(
            btn_row, text="←  Back",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=12, pady=5, cursor="hand2",
            command=self._on_back,
        ).pack(side="left")

        self._test_btn = tk.Button(
            btn_row, text="🔌  Test Tally",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=12, pady=5, cursor="hand2",
            command=self._on_test,
        )
        self._test_btn.pack(side="right", padx=(8, 0))

        tk.Button(
            btn_row, text="✓  Finish Setup",
            font=Font.BUTTON_SM, bg=Color.SUCCESS, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=16, pady=5, cursor="hand2",
            command=self._on_finish_click,
        ).pack(side="right")

        tk.Label(
            pad, text="Step 2 of 2",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).grid(row=8, column=0, columnspan=2, pady=(12, 0))

    # ─────────────────────────────────────────────────────────────────────────
    def _collect(self) -> dict:
        return {
            "host": self._vars["host"].get().strip() or "localhost",
            "port": self._vars["port"].get().strip() or "9000",
        }

    def _validate(self) -> tuple[bool, str]:
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

    def _on_test(self):
        ok, err = self._validate()
        if not ok:
            self._set_feedback(f"✗ {err}", Color.DANGER)
            return

        cfg = self._collect()
        self._set_feedback("Testing Tally connection...", Color.TEXT_MUTED)
        self._test_btn.configure(state="disabled", text="Testing...")

        def worker():
            result, detail = self._do_test(cfg)
            self._q.put(("test_result", result, detail))

        threading.Thread(target=worker, daemon=True).start()

    def _do_test(self, cfg: dict) -> tuple[bool, str]:
        try:
            from services.tally_connector import TallyConnector
            tc = TallyConnector(host=cfg["host"], port=int(cfg["port"]))
            ok = tc.ping()
            if ok:
                return True, "Tally is reachable!"
            return False, "Tally did not respond. Is Tally running with TDL Server enabled?"
        except Exception as e:
            return False, f"Cannot reach Tally: {e}"

    def _on_finish_click(self):
        ok, err = self._validate()
        if not ok:
            self._set_feedback(f"✗ {err}", Color.DANGER)
            return

        cfg = self._collect()
        self._cfg.save_tally_config({
            "host": cfg["host"],
            "port": int(cfg["port"]),
        })
        self._on_finish()