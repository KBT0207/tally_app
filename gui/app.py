"""
gui/app.py
==========
Root application window — the entry point for the entire GUI.

Phase 1 changes:
  - Replaced .env file with ConfigManager (AppData/TallySyncManager/config.json)
  - Added SetupWizard shown automatically on first run or if DB fails
  - db_config and tally_config now stored on AppState
  - Main window only shown AFTER setup is complete and DB is ready
  - Scheduler jobstore now correctly uses db_config from AppState

Usage (from run_gui.py):
    from gui.app import TallySyncApp
    app = TallySyncApp()
    app.run()
"""

import threading
import queue
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

try:
    import ttkbootstrap as tb
    from ttkbootstrap.constants import *
    HAS_TTKBOOTSTRAP = True
except ImportError:
    HAS_TTKBOOTSTRAP = False

from gui.state          import AppState, CompanyState, CompanyStatus
from gui.config_manager import ConfigManager
from gui.styles import (
    Color, Font, Spacing, Layout,
    NAV_ITEMS, APP_TITLE, APP_VERSION,
    BOOTSTRAP_THEME, STATUS_STYLE,
)


# ─────────────────────────────────────────────────────────────────────────────
#  Main Application Class
# ─────────────────────────────────────────────────────────────────────────────
class TallySyncApp:

    def __init__(self):
        self.state          = AppState()
        self._q             = queue.Queue()
        self._frames        = {}
        self._active_page   = None
        self._config        = ConfigManager()   # ← Phase 1: replaces .env

        # ── Build root window (hidden until setup complete) ──
        self._build_root()

        # ── Phase 1: Run setup wizard if needed BEFORE showing main UI ──
        if not self._run_setup_if_needed():
            # User cancelled setup — exit cleanly
            self.root.destroy()
            return

        # ── Setup complete — load config into AppState ────────
        self._apply_config_to_state()

        # ── Build the main UI ─────────────────────────────────
        self._build_layout()
        self._build_sidebar()
        self._build_header()
        self._build_content_area()
        self._load_pages()

        # Show main window now
        self.root.deiconify()

        # Listen for sync_finished to detect post-snapshot completion
        self.state.on("sync_finished", self._on_sync_finished_app)
        self._snapshot_celebrated: set = set()

        # ── Start background startup sequence ─────────────────
        self._start_startup_sequence()
        self._poll_queue()

    # ─────────────────────────────────────────────────────────────────────────
    #  Root window
    # ─────────────────────────────────────────────────────────────────────────
    def _build_root(self):
        if HAS_TTKBOOTSTRAP:
            self.root = tb.Window(themename=BOOTSTRAP_THEME)
        else:
            self.root = tk.Tk()

        self.root.title(f"{APP_TITLE}  {APP_VERSION}")
        self.root.geometry(f"{Layout.MIN_WIDTH}x{Layout.MIN_HEIGHT}")
        self.root.minsize(Layout.MIN_WIDTH, Layout.MIN_HEIGHT)
        self.root.configure(bg=Color.BG_ROOT)

        # Hide main window until setup is done
        self.root.withdraw()

        # Center on screen
        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x  = (sw - Layout.MIN_WIDTH)  // 2
        y  = (sh - Layout.MIN_HEIGHT) // 2
        self.root.geometry(f"{Layout.MIN_WIDTH}x{Layout.MIN_HEIGHT}+{x}+{y}")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ─────────────────────────────────────────────────────────────────────────
    #  Phase 1: Setup wizard gate
    # ─────────────────────────────────────────────────────────────────────────
    def _run_setup_if_needed(self) -> bool:
        """
        Check if setup is needed. If yes, show the wizard.
        Returns True if setup is complete (or was just completed).
        Returns False if user cancelled — app should exit.

        Cases that trigger the wizard:
          1. First run — config.json doesn't exist yet
          2. setup_complete = False — previous run failed or was cancelled
          3. DB connection fails with existing config — credentials changed
        """
        # Case 1 & 2: No config or setup not marked complete
        if not self._config.is_setup_complete():
            return self._show_setup_wizard(error_msg="")

        # Case 3: Config exists and setup_complete=True, but DB might be unreachable
        db_cfg = self._config.get_db_config()
        ok, detail = self._test_db_connection(db_cfg)
        if not ok:
            # Mark incomplete so next launch also shows wizard
            self._config.mark_setup_incomplete()
            error = (
                f"Could not connect to the database with saved settings:\n\n"
                f"{detail}\n\n"
                f"Please reconfigure your database connection."
            )
            return self._show_setup_wizard(error_msg=error)

        return True   # All good — existing config works

    def _show_setup_wizard(self, error_msg: str = "") -> bool:
        """Show the setup wizard. Returns True if completed, False if cancelled."""
        from gui.components.setup_wizard import SetupWizard

        wizard = SetupWizard(self.root, self._config, error_msg=error_msg)
        self.root.wait_window(wizard)
        return wizard.completed

    def _test_db_connection(self, db_cfg: dict) -> tuple[bool, str]:
        """Quick connection test. Returns (success, error_detail)."""
        try:
            from database.db_connector import DatabaseConnector
            conn = DatabaseConnector(
                username = db_cfg.get("username", "root"),
                password = db_cfg.get("password", ""),
                host     = db_cfg.get("host",     "localhost"),
                port     = int(db_cfg.get("port", 3306)),
                database = db_cfg.get("database", "tally_db"),
            )
            ok = conn.test_connection()
            conn.close()
            if ok:
                return True, ""
            return False, "Connection test returned False — check credentials."
        except Exception as e:
            return False, str(e)

    # ─────────────────────────────────────────────────────────────────────────
    #  Phase 1: Apply config to AppState
    # ─────────────────────────────────────────────────────────────────────────
    def _apply_config_to_state(self):
        """
        Load config from ConfigManager into AppState.
        Called once after setup is confirmed complete.
        """
        db_cfg    = self._config.get_db_config()
        tally_cfg = self._config.get_tally_config()

        # Store on AppState so all controllers can access
        self.state.db_config    = db_cfg
        self.state.tally_config = tally_cfg

        # Apply tally defaults to TallyConnectionState
        self.state.tally.host = tally_cfg.get("host", "localhost")
        self.state.tally.port = int(tally_cfg.get("port", 9000))

    # ─────────────────────────────────────────────────────────────────────────
    #  Main layout
    # ─────────────────────────────────────────────────────────────────────────
    def _build_layout(self):
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(0, weight=1)

        self.sidebar_frame = tk.Frame(
            self.root,
            bg=Color.BG_SIDEBAR,
            width=Layout.SIDEBAR_WIDTH,
        )
        self.sidebar_frame.grid(row=0, column=0, sticky="nsew")
        self.sidebar_frame.grid_propagate(False)

        self.main_frame = tk.Frame(self.root, bg=Color.BG_ROOT)
        self.main_frame.grid(row=0, column=1, sticky="nsew")
        self.main_frame.rowconfigure(1, weight=1)
        self.main_frame.columnconfigure(0, weight=1)

    # ─────────────────────────────────────────────────────────────────────────
    #  Sidebar
    # ─────────────────────────────────────────────────────────────────────────
    def _build_sidebar(self):
        f = self.sidebar_frame

        brand = tk.Frame(f, bg=Color.BG_SIDEBAR, height=Layout.HEADER_HEIGHT)
        brand.pack(fill="x")
        brand.pack_propagate(False)

        tk.Label(
            brand,
            text="⚡ Tally Sync",
            font=Font.SIDEBAR_TITLE,
            bg=Color.BG_SIDEBAR,
            fg=Color.SIDEBAR_TEXT,
            anchor="w",
            padx=Spacing.LG,
        ).pack(fill="x", expand=True)

        tk.Frame(f, bg=Color.SIDEBAR_HOVER_BG, height=1).pack(fill="x")

        self._nav_buttons = {}
        nav_container = tk.Frame(f, bg=Color.BG_SIDEBAR)
        nav_container.pack(fill="x", pady=(Spacing.SM, 0))

        for item in NAV_ITEMS:
            btn = self._make_nav_button(nav_container, item)
            self._nav_buttons[item["page"]] = btn

        bottom = tk.Frame(f, bg=Color.BG_SIDEBAR)
        bottom.pack(side="bottom", fill="x", padx=Spacing.MD, pady=Spacing.LG)

        tk.Frame(f, bg=Color.SIDEBAR_HOVER_BG, height=1).pack(side="bottom", fill="x")

        self._tally_status_lbl = tk.Label(
            bottom,
            text="● Tally: Checking...",
            font=Font.BODY_SM,
            bg=Color.BG_SIDEBAR,
            fg=Color.SIDEBAR_TEXT_MUTED,
            anchor="w",
        )
        self._tally_status_lbl.pack(fill="x", pady=(0, Spacing.XS))

        tk.Label(
            bottom,
            text=APP_VERSION,
            font=Font.BODY_SM,
            bg=Color.BG_SIDEBAR,
            fg=Color.SIDEBAR_TEXT_MUTED,
            anchor="w",
        ).pack(fill="x")

    def _make_nav_button(self, parent, item: dict) -> tk.Frame:
        container = tk.Frame(parent, bg=Color.BG_SIDEBAR, cursor="hand2")
        container.pack(fill="x")

        inner = tk.Frame(container, bg=Color.BG_SIDEBAR, padx=Spacing.LG, pady=Spacing.MD)
        inner.pack(fill="x")

        icon_lbl = tk.Label(
            inner, text=item["icon"],
            font=Font.BODY, bg=Color.BG_SIDEBAR, fg=Color.SIDEBAR_TEXT, width=2,
        )
        icon_lbl.pack(side="left")

        text_lbl = tk.Label(
            inner, text=item["label"],
            font=Font.SIDEBAR_ITEM, bg=Color.BG_SIDEBAR,
            fg=Color.SIDEBAR_TEXT, anchor="w",
        )
        text_lbl.pack(side="left", padx=(Spacing.SM, 0))

        page_key = item["page"]
        widgets  = [container, inner, icon_lbl, text_lbl]

        def on_enter(e):
            if self._active_page != page_key:
                for w in widgets: w.configure(bg=Color.SIDEBAR_HOVER_BG)
        def on_leave(e):
            if self._active_page != page_key:
                for w in widgets: w.configure(bg=Color.BG_SIDEBAR)
        def on_click(e):
            self.navigate(page_key)

        for w in widgets:
            w.bind("<Enter>",    on_enter)
            w.bind("<Leave>",    on_leave)
            w.bind("<Button-1>", on_click)

        container._widgets  = widgets
        container._page_key = page_key
        return container

    def _set_active_nav(self, page_key: str):
        for key, btn in self._nav_buttons.items():
            is_active = (key == page_key)
            bg = Color.SIDEBAR_ACTIVE_BG if is_active else Color.BG_SIDEBAR
            for w in btn._widgets:
                w.configure(bg=bg)

    # ─────────────────────────────────────────────────────────────────────────
    #  Header bar
    # ─────────────────────────────────────────────────────────────────────────
    def _build_header(self):
        header = tk.Frame(
            self.main_frame,
            bg=Color.BG_HEADER,
            height=Layout.HEADER_HEIGHT,
            relief="flat",
        )
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False)
        header.columnconfigure(0, weight=1)

        self._header_title = tk.Label(
            header, text="Companies",
            font=Font.HEADING_4, bg=Color.BG_HEADER,
            fg=Color.TEXT_PRIMARY, anchor="w", padx=Spacing.XL,
        )
        self._header_title.grid(row=0, column=0, sticky="ew")

        right = tk.Frame(header, bg=Color.BG_HEADER)
        right.grid(row=0, column=1, padx=Spacing.XL)

        self._db_status_lbl = tk.Label(
            right, text="● DB: Connecting...",
            font=Font.BODY_SM, bg=Color.BG_HEADER, fg=Color.MUTED,
        )
        self._db_status_lbl.pack(side="left", padx=(0, Spacing.LG))

        self._clock_lbl = tk.Label(
            right, text="",
            font=Font.BODY_SM, bg=Color.BG_HEADER, fg=Color.TEXT_SECONDARY,
        )
        self._clock_lbl.pack(side="left")
        self._update_clock()

        # ⚙ Settings button — opens setup wizard to let user change DB/Tally config
        tk.Button(
            right, text="⚙",
            font=Font.BODY, bg=Color.BG_HEADER, fg=Color.TEXT_SECONDARY,
            relief="flat", bd=0, padx=6, cursor="hand2",
            command=self.open_db_settings,
        ).pack(side="left", padx=(Spacing.MD, 0))

        tk.Frame(self.main_frame, bg=Color.BORDER, height=1).grid(
            row=0, column=0, sticky="sew"
        )

    def _update_clock(self):
        now = datetime.now().strftime("%d %b %Y  %H:%M:%S")
        self._clock_lbl.configure(text=now)
        self.root.after(1000, self._update_clock)

    # ─────────────────────────────────────────────────────────────────────────
    #  Content area
    # ─────────────────────────────────────────────────────────────────────────
    def _build_content_area(self):
        self.content_frame = tk.Frame(self.main_frame, bg=Color.BG_ROOT)
        self.content_frame.grid(row=1, column=0, sticky="nsew")
        self.content_frame.rowconfigure(0, weight=1)
        self.content_frame.columnconfigure(0, weight=1)

    # ─────────────────────────────────────────────────────────────────────────
    #  Page management
    # ─────────────────────────────────────────────────────────────────────────
    def _load_pages(self):
        from gui.pages.home_page      import HomePage
        from gui.pages.sync_page      import SyncPage
        from gui.pages.scheduler_page import SchedulerPage
        from gui.pages.logs_page      import LogsPage
        from gui.pages.settings_page  import SettingsPage

        page_classes = {
            "home":      HomePage,
            "sync":      SyncPage,
            "scheduler": SchedulerPage,
            "logs":      LogsPage,
            "settings":  SettingsPage,
        }

        for key, PageClass in page_classes.items():
            frame = PageClass(
                parent   = self.content_frame,
                state    = self.state,
                navigate = self.navigate,
                app      = self,
            )
            frame.grid(row=0, column=0, sticky="nsew")
            self._frames[key] = frame

        self.navigate("home")

    def navigate(self, page_key: str):
        if page_key not in self._frames:
            return
        self._frames[page_key].tkraise()
        self._active_page = page_key

        labels = {
            "home":      "Companies",
            "sync":      "Sync",
            "scheduler": "Scheduler",
            "logs":      "Logs",
            "settings":  "Settings",
        }
        self._header_title.configure(text=labels.get(page_key, ""))
        self._set_active_nav(page_key)

        page = self._frames[page_key]
        if hasattr(page, "on_show"):
            page.on_show()

    # ─────────────────────────────────────────────────────────────────────────
    #  Phase 1: Startup sequence — uses ConfigManager, not .env
    # ─────────────────────────────────────────────────────────────────────────
    def _start_startup_sequence(self):
        threading.Thread(target=self._startup_worker, daemon=True).start()

    def _startup_worker(self):
        """
        Background startup after setup wizard completes.
        Config is already validated at this point — just connect and load.
        """
        # ── Step 1: Connect to database ──────────────────
        try:
            db_cfg = self._config.get_db_config()    # ← ConfigManager, not .env
            engine = self._create_engine(db_cfg)
            self.state.db_engine = engine
            self._q.put(("db_status", True, "Connected"))
        except Exception as e:
            self._q.put(("db_status", False, str(e)))
            return

        # ── Step 2: Load companies from DB ───────────────
        try:
            self._load_companies_from_db(engine)
        except Exception as e:
            self._q.put(("error", f"Failed to load companies: {e}"))

        # ── Step 3: Load scheduler config from DB ────────
        try:
            from gui.controllers.company_controller import CompanyController
            CompanyController(self.state).load_scheduler_config()
        except Exception as e:
            from logging_config import logger
            logger.warning(f"[App] Could not load scheduler config: {e}")

        self._q.put(("companies_loaded", None))

        # ── Step 4: Start APScheduler ─────────────────────
        try:
            from gui.controllers.scheduler_controller import SchedulerController
            self._scheduler_controller = SchedulerController(self.state, self._q)
            self._scheduler_controller.start()
        except Exception as e:
            from logging_config import logger
            logger.warning(f"[App] Could not start scheduler: {e}")

        # ── Step 5: Ping Tally ────────────────────────────
        try:
            from services.tally_connector import TallyConnector
            tally     = TallyConnector(
                host=self.state.tally.host,
                port=self.state.tally.port,
            )
            connected = (tally.status == "Connected")
            self.state.tally.connected  = connected
            self.state.tally.last_check = datetime.now()
            self._q.put(("tally_status", connected))
        except Exception:
            self._q.put(("tally_status", False))

    @staticmethod
    def _create_engine(cfg: dict):
        """Create SQLAlchemy engine from config dict."""
        from database.db_connector import DatabaseConnector

        connector = DatabaseConnector(
            username = cfg.get("username", "root"),
            password = cfg.get("password", ""),
            host     = cfg.get("host",     "localhost"),
            port     = int(cfg.get("port", 3306)),
            database = cfg.get("database", "tally_db"),
        )
        connector.create_database_if_not_exists()
        connector.create_tables()
        return connector.get_engine()

    # ─────────────────────────────────────────────────────────────────────────
    #  Load companies (unchanged from original)
    # ─────────────────────────────────────────────────────────────────────────
    def _load_companies_from_db(self, engine):
        from sqlalchemy.orm import sessionmaker
        from database.models.company    import Company
        from database.models.sync_state import SyncState

        Session = sessionmaker(bind=engine)
        db      = Session()
        try:
            db_companies = {co.name: co for co in db.query(Company).all()}

            for name, co in db_companies.items():
                states = db.query(SyncState).filter_by(company_name=name).all()

                last_sync  = None
                last_alter = 0
                is_initial = False
                last_month = None

                if states:
                    times = [s.last_sync_time for s in states if s.last_sync_time]
                    if times:
                        last_sync = max(times)
                    last_alter = max(s.last_alter_id for s in states)
                    # is_initial_done: True only when at least one SyncState row exists
                    # AND every row that exists has is_initial_done=True.
                    # Using `all()` on an empty list returns True (wrong); we guard with `bool(states)`.
                    is_initial = bool(states) and all(s.is_initial_done for s in states)
                    months     = [s.last_synced_month for s in states if s.last_synced_month]
                    last_month = max(months) if months else None

                from_str  = None
                books_str = None
                if co.starting_from:
                    from_str = str(co.starting_from).replace("-", "")[:8]
                if hasattr(co, 'books_from') and co.books_from:
                    books_str = str(co.books_from).replace("-", "")[:8]

                cs = CompanyState(
                    name              = name,
                    guid              = co.guid or "",
                    status            = CompanyStatus.CONFIGURED,
                    last_sync_time    = last_sync,
                    last_alter_id     = last_alter,
                    last_synced_month = last_month,
                    is_initial_done   = is_initial,
                    starting_from     = from_str,
                    books_from        = books_str,
                    tally_host        = getattr(co, 'tally_host', 'localhost') or 'localhost',
                    tally_port        = int(getattr(co, 'tally_port', 9000) or 9000),
                    tally_open        = False,
                )
                self.state.companies[name] = cs
        finally:
            db.close()

        # Fetch live Tally companies
        tally_companies = []
        try:
            from services.tally_connector import TallyConnector
            tally = TallyConnector(
                host=self.state.tally.host,
                port=self.state.tally.port,
            )
            if tally.status == "Connected":
                tally_companies = tally.fetch_all_companies()
        except Exception as e:
            from logging_config import logger
            logger.warning(f"[App] Could not fetch Tally company list: {e}")

        tally_names = set()
        for tc in tally_companies:
            name = (tc.get("name") or "").strip()
            if not name:
                continue
            tally_names.add(name)

            raw_from  = tc.get("starting_from", "")
            raw_books = tc.get("books_from", "")
            from_str  = str(raw_from).replace("-", "")[:8]  if raw_from  else None
            books_str = str(raw_books).replace("-", "")[:8] if raw_books else None

            if name in self.state.companies:
                self.state.companies[name].tally_open = True
                if not self.state.companies[name].books_from and books_str:
                    self.state.companies[name].books_from = books_str
            else:
                cs = CompanyState(
                    name          = name,
                    guid          = tc.get("guid", ""),
                    status        = CompanyStatus.NOT_CONFIGURED,
                    starting_from = from_str,
                    books_from    = books_str,
                    tally_open    = True,
                )
                self.state.companies[name] = cs

    def save_company_to_db(self, company_name: str, guid: str,
                           starting_from: str, books_from: str = None):
        from sqlalchemy.orm import sessionmaker
        from database.models.company import Company

        engine = self.state.db_engine
        if not engine:
            return False, "No DB connection"

        Session = sessionmaker(bind=engine)
        db      = Session()
        try:
            existing = db.query(Company).filter_by(name=company_name).first()
            if existing:
                existing.guid          = guid
                existing.starting_from = starting_from
                if books_from:
                    existing.books_from = books_from
            else:
                co = Company(
                    name          = company_name,
                    guid          = guid,
                    starting_from = starting_from,
                )
                if books_from and hasattr(Company, 'books_from'):
                    co.books_from = books_from
                db.add(co)
            db.commit()
            return True, "Saved"
        except Exception as e:
            db.rollback()
            return False, str(e)
        finally:
            db.close()

    # ─────────────────────────────────────────────────────────────────────────
    #  Queue polling
    # ─────────────────────────────────────────────────────────────────────────
    def _poll_queue(self):
        try:
            while True:
                msg = self._q.get_nowait()
                self._handle_queue_msg(msg)
        except queue.Empty:
            pass
        finally:
            self.root.after(100, self._poll_queue)

    def _handle_queue_msg(self, msg: tuple):
        event = msg[0]

        if event == "db_status":
            _, ok, detail = msg
            if ok:
                self._db_status_lbl.configure(text="● DB: Connected", fg=Color.SUCCESS)
            else:
                self._db_status_lbl.configure(text="● DB: Error", fg=Color.DANGER)
                # Offer to reconfigure
                if messagebox.askyesno(
                    "Database Connection Error",
                    f"{detail}\n\nWould you like to reconfigure the database connection?",
                ):
                    self.open_db_settings()

        elif event == "tally_status":
            _, connected = msg
            if connected:
                self._tally_status_lbl.configure(text="● Tally: Online",  fg=Color.SUCCESS)
            else:
                self._tally_status_lbl.configure(text="● Tally: Offline", fg=Color.DANGER)

        elif event == "companies_loaded":
            home = self._frames.get("home")
            if home and hasattr(home, "refresh_companies"):
                home.refresh_companies()
            sched = self._frames.get("scheduler")
            if sched and hasattr(sched, "refresh_companies"):
                sched.refresh_companies()

        elif event == "error":
            _, msg_text = msg
            messagebox.showerror("Error", msg_text)

        elif event == "sync_log":
            _, line = msg
            logs_page = self._frames.get("logs")
            if logs_page and hasattr(logs_page, "append_log"):
                logs_page.append_log(line)

        elif event == "company_progress":
            _, name, pct, label = msg
            self.state.set_company_progress(name, pct, label)

        elif event == "sync_done":
            self.state.sync_active = False
            self.state.emit("sync_finished")

        elif event == "scheduler_updated":
            _, company_name = msg
            self.state.emit("scheduler_updated", company=company_name)

        elif event == "scheduler_sync_done":
            _, company_name = msg
            self.state.emit("scheduler_updated", company=company_name)

        elif event == "scheduler_job_error":
            _, company_name, err = msg
            self.state.set_company_status(company_name, CompanyStatus.SYNC_ERROR)

    def post(self, *args):
        self._q.put(args)

    # ─────────────────────────────────────────────────────────────────────────
    #  Post-snapshot celebration
    # ─────────────────────────────────────────────────────────────────────────
    def _on_sync_finished_app(self):
        self._check_post_snapshot_companies()

    def _check_post_snapshot_companies(self):
        if not hasattr(self, "_snapshot_celebrated"):
            self._snapshot_celebrated: set = set()

        newly_done = [
            co for co in self.state.companies.values()
            if co.is_initial_done
            and co.name not in self._snapshot_celebrated
            and co.last_sync_time is not None
        ]

        for co in newly_done:
            self._snapshot_celebrated.add(co.name)
            self._show_post_snapshot_dialog(co)

    def _show_post_snapshot_dialog(self, co):
        from gui.components.initial_snapshot_dialog import PostSnapshotDialog
        dialog = PostSnapshotDialog(self.root, co)
        self.root.wait_window(dialog)
        if dialog.result == "schedule":
            self.state.selected_companies = [co.name]
            self.navigate("scheduler")

    # ─────────────────────────────────────────────────────────────────────────
    #  Open DB/Tally settings (from header ⚙ button or after DB error)
    # ─────────────────────────────────────────────────────────────────────────
    def open_db_settings(self):
        """
        Re-open the setup wizard so user can change DB or Tally config.
        After saving, reconnects the DB engine with new credentials.
        """
        from gui.components.setup_wizard import SetupWizard

        # Temporarily mark setup incomplete so wizard shows both steps
        self._config.mark_setup_incomplete()

        wizard = SetupWizard(self.root, self._config)
        self.root.wait_window(wizard)

        if wizard.completed:
            # Re-apply new config to state
            self._apply_config_to_state()

            # Reconnect DB with new credentials
            try:
                if self.state.db_engine:
                    self.state.db_engine.dispose()
                db_cfg = self._config.get_db_config()
                self.state.db_engine = self._create_engine(db_cfg)
                self._db_status_lbl.configure(text="● DB: Connected", fg=Color.SUCCESS)

                # Reload companies with new DB
                self._load_companies_from_db(self.state.db_engine)
                home = self._frames.get("home")
                if home and hasattr(home, "refresh_companies"):
                    home.refresh_companies()

            except Exception as e:
                messagebox.showerror("Reconnect Failed", str(e))
        else:
            # Wizard cancelled — restore setup_complete so app continues
            self._config.mark_setup_complete()

    # ─────────────────────────────────────────────────────────────────────────
    #  Shutdown
    # ─────────────────────────────────────────────────────────────────────────
    def _on_close(self):
        if self.state.sync_active:
            if not messagebox.askyesno(
                "Sync in Progress",
                "A sync is currently running.\n\nAre you sure you want to quit?",
            ):
                return

        sched_ctrl = getattr(self, '_scheduler_controller', None)
        if sched_ctrl and hasattr(sched_ctrl, 'shutdown'):
            try:
                sched_ctrl.shutdown()
            except Exception:
                pass

        self.root.destroy()

    # ─────────────────────────────────────────────────────────────────────────
    #  Entry point
    # ─────────────────────────────────────────────────────────────────────────
    def run(self):
        self.root.mainloop()