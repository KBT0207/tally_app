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

import logging
import threading
import queue
import ctypes
import webbrowser
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

import requests

logging.getLogger("PIL").setLevel(logging.WARNING)
logging.getLogger("PIL.Image").setLevel(logging.WARNING)
logging.getLogger("PIL.PngImagePlugin").setLevel(logging.WARNING)

try:
    import ttkbootstrap as tb
    from ttkbootstrap.constants import *
    HAS_TTKBOOTSTRAP = False
except ImportError:
    HAS_TTKBOOTSTRAP = False

from gui.state          import AppState, CompanyState, CompanyStatus
from gui.config_manager import ConfigManager
from gui.styles import (
    Color, Font, Spacing, Layout,
    NAV_ITEMS, APP_TITLE, APP_VERSION, GIST_VERSION_URL,
    BOOTSTRAP_THEME, STATUS_STYLE,
)
from gui.tray_manager import TrayManager

class TallySyncApp:

    def __init__(self):
        self.state          = AppState()
        self._q             = queue.Queue()
        self._frames        = {}
        self._active_page   = None
        self._config        = ConfigManager()
        self._build_root()

        if not self._run_setup_if_needed():
            self.root.destroy()
            return

        self._apply_config_to_state()

        self._build_layout()
        self._build_sidebar()
        self._build_header()
        self._build_content_area()
        self._load_pages()

        self.root.deiconify()

        self.state.on("sync_finished", self._on_sync_finished_app)
        self._snapshot_celebrated: set = set()

        self._syncs_paused = False
        self._tray = TrayManager(
            root             = self.root,
            state            = self.state,
            on_open          = self._show_window,
            on_pause_toggle  = self._toggle_pause,
            on_exit          = self._quit_app,
        )
        self._tray.start()

        self._start_startup_sequence()
        self._poll_queue()

    def _build_root(self):
        try:
            ctypes.windll.shcore.SetProcessDpiAwareness(2)
        except Exception:
            try:
                ctypes.windll.user32.SetProcessDPIAware()
            except Exception:
                pass

        if HAS_TTKBOOTSTRAP:
            self.root = tb.Window(themename=BOOTSTRAP_THEME)
        else:
            self.root = tk.Tk()

        self.root.title(f"{APP_TITLE}  {APP_VERSION}")
        self.root.geometry(f"{Layout.MIN_WIDTH}x{Layout.MIN_HEIGHT}")
        self.root.minsize(Layout.MIN_WIDTH, Layout.MIN_HEIGHT)
        self.root.configure(bg=Color.BG_ROOT)

        self.root.withdraw()

        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x  = (sw - Layout.MIN_WIDTH)  // 2
        y  = (sh - Layout.MIN_HEIGHT) // 2
        self.root.geometry(f"{Layout.MIN_WIDTH}x{Layout.MIN_HEIGHT}+{x}+{y}")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    def _run_setup_if_needed(self) -> bool:
        """
        Check if setup is needed. If yes, show the wizard.
        Returns True if setup is complete (or was just completed).
        Returns False if user cancelled — app should exit.

        Cases that trigger the wizard:
          1. First run — config.json doesn't exist yet
             → Step A: mandatory admin password dialog
             → Step B: DB setup wizard
          2. setup_complete = False — previous run failed or was cancelled
          3. DB connection fails with existing config — credentials changed
        """
        if not self._config.is_setup_complete():
            # ── First run: require admin password before DB setup ─────────────
            if self._config.is_first_run():
                if not self._show_first_run_password():
                    return False   # wrong password / closed → exit app
            return self._show_setup_wizard(error_msg="")

        db_cfg = self._config.get_db_config()
        ok, detail = self._test_db_connection(db_cfg)
        if not ok:
            self._config.mark_setup_incomplete()
            error = (
                f"Could not connect to the database with saved settings:\n\n"
                f"{detail}\n\n"
                f"Please reconfigure your database connection."
            )
            return self._show_setup_wizard(error_msg=error)

        return True

    def _show_first_run_password(self) -> bool:
        """
        Show mandatory admin password dialog on very first launch.
        Returns True if correct password entered, False if closed/cancelled.
        """
        from gui.components.first_run_password_dialog import FirstRunPasswordDialog
        dlg = FirstRunPasswordDialog(self.root)
        self.root.wait_window(dlg)
        return dlg.verified

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

    def _apply_config_to_state(self):
        """
        Load config from ConfigManager into AppState.
        Called once after setup is confirmed complete.
        """
        db_cfg    = self._config.get_db_config()
        tally_cfg = self._config.get_tally_config()

        self.state.db_config    = db_cfg
        self.state.tally_config = tally_cfg

        self.state.tally.host = tally_cfg.get("host", "localhost")
        self.state.tally.port = int(tally_cfg.get("port", 9000))

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

        tk.Frame(f, bg=Color.SIDEBAR_HOVER_BG, height=1).pack(side="bottom", fill="x")

        self._update_btn = tk.Label(
            f,
            text="↑ Check for Updates",
            font=Font.BODY_SM,
            bg=Color.BG_SIDEBAR,
            fg=Color.SIDEBAR_TEXT_MUTED,
            anchor="w",
            cursor="hand2",
            padx=Spacing.MD,
            pady=Spacing.SM,
        )
        self._update_btn.pack(side="bottom", fill="x")
        self._update_btn.bind("<Button-1>", lambda e: self._check_for_updates())
        self._update_btn.bind("<Enter>", lambda e: self._update_btn.configure(fg=Color.SIDEBAR_TEXT))
        self._update_btn.bind("<Leave>", lambda e: self._update_btn.configure(fg=Color.SIDEBAR_TEXT_MUTED))

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

        tk.Button(
            right, text="⚙",
            font=Font.BODY, bg=Color.BG_HEADER, fg=Color.TEXT_SECONDARY,
            relief="flat", bd=0, padx=6, cursor="hand2",
            command=self._protected_db_settings,
        ).pack(side="left", padx=(Spacing.MD, 0))

        tk.Frame(self.main_frame, bg=Color.BORDER, height=1).grid(
            row=0, column=0, sticky="sew"
        )

    def _update_clock(self):
        now = datetime.now().strftime("%d %b %Y  %H:%M:%S")
        self._clock_lbl.configure(text=now)
        self.root.after(1000, self._update_clock)

    def _build_content_area(self):
        self.content_frame = tk.Frame(self.main_frame, bg=Color.BG_ROOT)
        self.content_frame.grid(row=1, column=0, sticky="nsew")
        self.content_frame.rowconfigure(0, weight=1)
        self.content_frame.columnconfigure(0, weight=1)

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

        if self._active_page and self._active_page != page_key:
            prev = self._frames.get(self._active_page)
            if prev and hasattr(prev, "on_hide"):
                prev.on_hide()

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

    def _start_startup_sequence(self):
        threading.Thread(target=self._startup_worker, daemon=True).start()

    def _startup_worker(self):
        """
        Background startup after setup wizard completes.
        Config is already validated at this point — just connect and load.

        ── Correct startup order ──────────────────────────────────────────────
        Step 1  Connect to database
        Step 2  Load companies from DB
        Step 3  Load scheduler config from DB
        Step 4  Load TallySettings + AutomationSettings
        Step 5  Notify GUI — companies ready (home + scheduler pages refresh)
        Step 6  Start SyncQueueController  ← MUST be before scheduler
        Step 7  Run MissedSyncChecker      ← MUST be before scheduler starts
                                             so missed companies enter the queue
                                             BEFORE APScheduler fires any jobs.
                                             If scheduler starts first, it fires
                                             jobs that set _round_active=True,
                                             and MissedSyncChecker's enqueue()
                                             calls get silently skipped by Rule 1.
        Step 8  Start SchedulerController  ← LAST — APScheduler only fires
                                             after missed syncs are queued.
        Step 9  Notify GUI — scheduler ready (scheduler page wires up controller)
        Step 10 Ping Tally
        ──────────────────────────────────────────────────────────────────────
        """
        from logging_config import logger

        try:
            db_cfg = self._config.get_db_config()
            engine = self._create_engine(db_cfg)
            self.state.db_engine = engine
            self._q.put(("db_status", True, "Connected"))
        except Exception as e:
            self._q.put(("db_status", False, str(e)))
            return

        try:
            self._load_companies_from_db(engine)
        except Exception as e:
            self._q.put(("error", f"Failed to load companies: {e}"))

        try:
            from gui.controllers.company_controller import CompanyController
            CompanyController(self.state).load_scheduler_config()
        except Exception as e:
            logger.warning(f"[App] Could not load scheduler config: {e}")

        try:
            self._load_automation_settings(engine)
        except Exception as e:
            logger.warning(f"[App] Could not load automation settings: {e}")

        self._q.put(("companies_loaded", None))

        try:
            from gui.controllers.sync_queue_controller import SyncQueueController
            self._sync_queue_controller = SyncQueueController(self.state, self._q)
            self._sync_queue_controller.start()
            logger.info("[App] SyncQueueController started ✓")
        except Exception as e:
            logger.warning(f"[App] Could not start SyncQueueController: {e}")
            self._sync_queue_controller = None

        if self._sync_queue_controller:
            try:
                from gui.controllers.missed_sync_checker import MissedSyncChecker
                checker = MissedSyncChecker(
                    state                 = self.state,
                    sync_queue_controller = self._sync_queue_controller,
                    app_queue             = self._q,
                )
                checker.check_and_enqueue()
                logger.info("[App] MissedSyncChecker completed ✓")
            except Exception as e:
                logger.warning(f"[App] Missed sync check failed: {e}")

        try:
            from gui.controllers.scheduler_controller import SchedulerController
            self._scheduler_controller = SchedulerController(
                self.state, self._q,
                sync_queue_ctrl = self._sync_queue_controller,
            )
            self._scheduler_controller.start()
            logger.info("[App] SchedulerController started ✓")
        except Exception as e:
            logger.warning(f"[App] Could not start scheduler: {e}")
            self._scheduler_controller = None

        self._q.put(("scheduler_ready", None))

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

    def _load_automation_settings(self, engine):
        """
        Load TallySettings (exe path + image names) and AutomationSettings
        (confidence, delays, timeouts) from DB into AppState.
        Creates default rows if they don't exist yet.
        """
        from sqlalchemy.orm import sessionmaker
        from database.models.tally_settings      import TallySettings
        from database.models.automation_settings import AutomationSettings
        from gui.state import AutomationConfig

        Session = sessionmaker(bind=engine)
        db      = Session()
        try:
            ts = db.query(TallySettings).filter_by(id=1).first()
            if not ts:
                ts = TallySettings(id=1)
                db.add(ts)
                db.commit()
            self.state.tally_exe_path = ts.exe_path or ""

            self.state.tally_images = {
                "gateway":      ts.image_gateway      or "tally_gateway_screen.png",
                "search_box":   ts.image_search_box   or "tally_company_search_box.png",
                "username":     ts.image_username     or "tally_username_field.png",
                "password":     ts.image_password     or "tally_password_field.png",
                "select_title": ts.image_select_title or "tally_select_company_title.png",
                "change_path":  ts.image_change_path  or "tally_change_path_btn.png",
                "remote_tab":   ts.image_remote_tab   or "tally_remote_tab.png",
                "tds_field":    ts.image_tds_field    or "tally_tds_field.png",
                "data_server":  getattr(ts, 'image_data_server', None) or "tally_dataserver_image.png",
                "local_path":   getattr(ts, 'image_local_path',  None) or "tally_local_path_image.png",
                "change_period": getattr(ts, 'image_change_period', None) or "tally_change_period.png",
            }

            aut = db.query(AutomationSettings).filter_by(id=1).first()
            if not aut:
                aut = AutomationSettings(id=1)
                db.add(aut)
                db.commit()

            self.state.automation = AutomationConfig(
                confidence       = float(aut.confidence       or 0.80),
                click_delay_ms   = int(aut.click_delay_ms     or 500),
                wait_timeout_sec = int(aut.wait_timeout_sec   or 30),
                retry_attempts   = int(aut.retry_attempts     or 3),
            )

            from logging_config import logger
            logger.info("[App] Automation settings loaded from DB")

        except Exception as e:
            from logging_config import logger
            logger.error(f"[App] Failed to load automation settings: {e}")
        finally:
            db.close()

    def _load_companies_from_db(self, engine):
        from sqlalchemy.orm import sessionmaker
        from database.models.company          import Company
        from database.models.sync_state       import SyncState
        from database.models.scheduler_config import CompanySchedulerConfig
        from collections import defaultdict

        Session = sessionmaker(bind=engine)
        db      = Session()
        try:
            db_companies = {co.name: co for co in db.query(Company).all()}

            all_states = db.query(SyncState).all()
            states_by_company = defaultdict(list)
            for s in all_states:
                states_by_company[s.company_name].append(s)

            all_sched_configs = {
                sc.company_name: sc
                for sc in db.query(CompanySchedulerConfig).all()
            }

            for name, co in db_companies.items():
                states = states_by_company.get(name, [])

                last_sync  = None
                last_alter = 0
                is_initial = False
                last_month = None

                if states:
                    times      = [s.last_sync_time for s in states if s.last_sync_time]
                    last_alter = max(s.last_alter_id for s in states)
                    sched_cfg  = all_sched_configs.get(name)
                    if sched_cfg and sched_cfg.last_sync_time:
                        last_sync = sched_cfg.last_sync_time.replace(tzinfo=None)
                    elif times:
                        last_sync = max(times)
                    core_types = {'sales', 'purchase', 'credit_note', 'debit_note'}
                    done_types = {s.voucher_type for s in states if s.is_initial_done}
                    is_initial = core_types.issubset(done_types)
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
                    status            = CompanyStatus.CONFIGURED if co.tally_username is not None else CompanyStatus.NOT_CONFIGURED,
                    last_sync_time    = last_sync,
                    last_alter_id     = last_alter,
                    last_synced_month = last_month,
                    is_initial_done   = is_initial,
                    starting_from     = from_str,
                    books_from        = books_str,
                    formal_name       = getattr(co, 'formal_name',    None) or None,
                    company_number    = getattr(co, 'company_number',  None) or None,
                    audited_upto      = str(co.audited_upto).replace("-", "")[:8] if getattr(co, 'audited_upto', None) else None,
                    tally_host        = getattr(co, 'tally_host', 'localhost') or 'localhost',
                    tally_port        = int(getattr(co, 'tally_port', 9000) or 9000),
                    tally_open        = False,
                    tally_username    = getattr(co, 'tally_username', '') or '',
                    tally_password    = getattr(co, 'tally_password', '') or '',
                    company_type      = getattr(co, 'company_type',  'local') or 'local',
                    data_path         = getattr(co, 'data_path',     '') or '',
                    tds_path          = getattr(co, 'tds_path',      '') or '',
                    drive_letter      = getattr(co, 'drive_letter',  '') or '',
                    material_centre   = getattr(co, 'material_centre',  '') or '',
                    default_currency  = getattr(co, 'default_currency', 'INR') or 'INR',
                )
                self.state.companies[name] = cs
        finally:
            db.close()

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
                if tc.get('formal_name'):
                    self.state.companies[name].formal_name    = tc['formal_name']
                if tc.get('company_number'):
                    self.state.companies[name].company_number = tc['company_number']
                if tc.get('audited_upto'):
                    self.state.companies[name].audited_upto   = tc['audited_upto']
            else:
                cs = CompanyState(
                    name           = name,
                    guid           = tc.get("guid", ""),
                    status         = CompanyStatus.NOT_CONFIGURED,
                    starting_from  = from_str,
                    books_from     = books_str,
                    formal_name    = tc.get("formal_name",    None) or None,
                    company_number = tc.get("company_number", None) or None,
                    audited_upto   = tc.get("audited_upto",   None) or None,
                    tally_open     = True,
                )
                self.state.companies[name] = cs

    def save_company_to_db(self, company_name: str, guid: str,
                           starting_from: str, books_from: str = None,
                           tally_username: str = "", tally_password: str = "",
                           tally_host: str = "localhost", tally_port: int = 9000,
                           company_type: str = "local", data_path: str = "",
                           tds_path: str = "", drive_letter: str = "",
                           material_centre: str = "", default_currency: str = "INR",
                           formal_name: str = "", company_number: str = "",
                           audited_upto: str = ""):
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
                existing.guid           = guid
                existing.starting_from  = starting_from
                existing.tally_username = tally_username
                existing.tally_password = tally_password
                if hasattr(existing, 'tally_host'):
                    existing.tally_host = tally_host or "localhost"
                if hasattr(existing, 'tally_port'):
                    existing.tally_port = int(tally_port or 9000)
                existing.company_type   = company_type
                existing.data_path      = data_path or None
                existing.tds_path       = tds_path  or None
                existing.drive_letter   = drive_letter or None
                if hasattr(existing, 'material_centre'):
                    existing.material_centre  = material_centre or None
                if hasattr(existing, 'default_currency'):
                    existing.default_currency = default_currency or 'INR'
                if books_from:
                    existing.books_from = books_from
                if formal_name and hasattr(existing, 'formal_name'):
                    existing.formal_name    = formal_name or None
                if company_number and hasattr(existing, 'company_number'):
                    existing.company_number = company_number or None
                if audited_upto and hasattr(existing, 'audited_upto'):
                    from database.database_processor import _parse_date_str
                    existing.audited_upto = _parse_date_str(audited_upto)
            else:
                co = Company(
                    name             = company_name,
                    guid             = guid,
                    starting_from    = starting_from,
                    tally_username   = tally_username,
                    tally_password   = tally_password,
                    company_type     = company_type,
                    data_path        = data_path        or None,
                    tds_path         = tds_path         or None,
                    drive_letter     = drive_letter     or None,
                    material_centre  = material_centre  or None,
                    default_currency = default_currency or 'INR',
                    formal_name      = formal_name      or None,
                    company_number   = company_number   or None,
                )
                if audited_upto and hasattr(Company, 'audited_upto'):
                    from database.database_processor import _parse_date_str
                    co.audited_upto = _parse_date_str(audited_upto)
                if hasattr(Company, 'tally_host'):
                    co.tally_host = tally_host or "localhost"
                if hasattr(Company, 'tally_port'):
                    co.tally_port = int(tally_port or 9000)
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
            if hasattr(self, '_tray'):
                self._tray.update_tooltip()

        elif event == "scheduler_ready":
            sched = self._frames.get("scheduler")
            if sched and hasattr(sched, "on_scheduler_ready"):
                sched.on_scheduler_ready()

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

        elif event == "sync_queue_done":
            _, company_name, success = msg
            self.state.emit("scheduler_updated", company=company_name)

        elif event == "sync_queue_log":
            _, company_name, text, level = msg
            logs_page = self._frames.get("logs")
            if logs_page and hasattr(logs_page, "append_log"):
                logs_page.append_log(f"[{company_name}] {text}")

        elif event == "sync_queue_progress":
            _, company_name, pct, label = msg
            self.state.set_company_progress(company_name, pct, label)

        elif event == "sync_queue_started":
            _, company_name = msg
            self.state.set_company_status(company_name, CompanyStatus.SYNCING)

        elif event == "queue_updated":
            home = self._frames.get("home")
            if home and hasattr(home, "_refresh_cards_queue_state"):
                home.after(0, home._refresh_cards_queue_state)
            elif home and hasattr(home, "refresh_companies"):
                home.refresh_companies()
            sched = self._frames.get("scheduler")
            if sched and hasattr(sched, "refresh_queue_status"):
                sched.refresh_queue_status()

        elif event == "missed_syncs_found":
            _, company_names = msg
            self._show_missed_sync_banner(company_names)

        elif event == "sync_overrun_detected":
            _, companies, elapsed_min, interval_min, suggested_min = msg
            self._show_overrun_banner(companies, elapsed_min, interval_min, suggested_min)

    def post(self, *args):
        self._q.put(args)

    def _show_missed_sync_banner(self, company_names: list):
        """
        Show a non-blocking notification banner at the top of the main window
        when missed syncs are detected on startup.

        The banner auto-dismisses after 8 seconds or when the user clicks ✖.
        It does NOT block the UI — it's a thin strip above the content area.

        Example:
            ⚠  2 scheduled syncs were missed while the app was closed —
               running them now:  CompanyA, CompanyB          [✖ Dismiss]
        """
        if not company_names:
            return

        count     = len(company_names)
        names_str = ",  ".join(company_names)
        msg_text  = (
            f"⚠  {count} scheduled sync{'s' if count > 1 else ''} missed while "
            f"app was closed — running now:  {names_str}"
        )

        from gui.styles import Color, Font, Spacing
        import tkinter as tk

        banner = tk.Frame(
            self.main_frame,
            bg     = Color.WARNING_BG,
            relief = "flat",
            bd     = 0,
        )
        self.content_frame.grid(row=2, column=0, sticky="nsew")
        self.main_frame.rowconfigure(1, weight=0)
        self.main_frame.rowconfigure(2, weight=1)
        banner.grid(row=1, column=0, sticky="ew")

        tk.Frame(banner, bg=Color.WARNING_FG, height=1).pack(fill="x")

        inner = tk.Frame(banner, bg=Color.WARNING_BG, padx=Spacing.LG, pady=Spacing.SM)
        inner.pack(fill="x")
        inner.columnconfigure(0, weight=1)

        tk.Label(
            inner,
            text      = msg_text,
            font      = Font.BODY_SM,
            bg        = Color.WARNING_BG,
            fg        = Color.WARNING_FG,
            anchor    = "w",
            wraplength= 800,
            justify   = "left",
        ).grid(row=0, column=0, sticky="w")

        def _dismiss():
            try:
                banner.destroy()
                self.content_frame.grid(row=1, column=0, sticky="nsew")
                self.main_frame.rowconfigure(1, weight=1)
                self.main_frame.rowconfigure(2, weight=0)
            except Exception:
                pass

        tk.Button(
            inner,
            text    = "✖  Dismiss",
            font    = Font.BODY_SM,
            bg      = Color.WARNING_BG,
            fg      = Color.WARNING_FG,
            relief  = "flat",
            bd      = 0,
            cursor  = "hand2",
            command = _dismiss,
        ).grid(row=0, column=1, sticky="e", padx=(Spacing.LG, 0))

        tk.Frame(banner, bg=Color.BORDER, height=1).pack(fill="x")

        self.root.after(8000, _dismiss)

        from logging_config import logger
        logger.info(f"[App] Missed sync banner shown for: {company_names}")

    def _show_overrun_banner(
        self,
        companies:    list,
        elapsed_min:  float,
        interval_min: float,
        suggested_min: int,
    ):
        """
        Shown when a sync round takes longer than the scheduled interval.

        Example:
          ⚠  Sync round took 75 min but interval is 60 min (overrun: 15 min).
             Consider increasing your interval to at least 90 min.
                                                        [Go to Scheduler] [✖]
        Auto-dismisses after 12 seconds. Clicking "Go to Scheduler" navigates
        to the scheduler page so the user can fix their interval.
        """
        import tkinter as tk
        from gui.styles import Color, Font, Spacing

        overrun_min = elapsed_min - interval_min
        msg_text = (
            f"⚠  Sync round took {elapsed_min:.0f} min "
            f"but interval is {interval_min:.0f} min "
            f"(overrun: {overrun_min:.0f} min).  "
            f"Consider increasing your interval to at least {suggested_min} min."
        )

        banner = tk.Frame(
            self.main_frame,
            bg     = Color.WARNING_BG,
            relief = "flat",
            bd     = 0,
        )
        self.content_frame.grid(row=2, column=0, sticky="nsew")
        self.main_frame.rowconfigure(1, weight=0)
        self.main_frame.rowconfigure(2, weight=1)
        banner.grid(row=1, column=0, sticky="ew")

        tk.Frame(banner, bg=Color.WARNING_FG, height=1).pack(fill="x")

        inner = tk.Frame(banner, bg=Color.WARNING_BG, padx=Spacing.LG, pady=Spacing.SM)
        inner.pack(fill="x")
        inner.columnconfigure(0, weight=1)

        tk.Label(
            inner,
            text       = msg_text,
            font       = Font.BODY_SM,
            bg         = Color.WARNING_BG,
            fg         = Color.WARNING_FG,
            anchor     = "w",
            wraplength = 750,
            justify    = "left",
        ).grid(row=0, column=0, sticky="w")

        btn_frame = tk.Frame(inner, bg=Color.WARNING_BG)
        btn_frame.grid(row=0, column=1, sticky="e", padx=(Spacing.LG, 0))

        def _dismiss():
            try:
                banner.destroy()
                self.content_frame.grid(row=1, column=0, sticky="nsew")
                self.main_frame.rowconfigure(1, weight=1)
                self.main_frame.rowconfigure(2, weight=0)
            except Exception:
                pass

        tk.Button(
            btn_frame,
            text    = "⚙ Fix Schedule",
            font    = Font.BODY_SM,
            bg      = Color.WARNING_FG,
            fg      = Color.BG_CARD,
            relief  = "flat",
            bd      = 0,
            padx    = 8,
            cursor  = "hand2",
            command = lambda: [_dismiss(), self.navigate("scheduler")],
        ).pack(side="left", padx=(0, Spacing.SM))

        tk.Button(
            btn_frame,
            text    = "✖",
            font    = Font.BODY_SM,
            bg      = Color.WARNING_BG,
            fg      = Color.WARNING_FG,
            relief  = "flat",
            bd      = 0,
            cursor  = "hand2",
            command = _dismiss,
        ).pack(side="left")

        tk.Frame(banner, bg=Color.BORDER, height=1).pack(fill="x")

        self.root.after(12_000, _dismiss)

        from logging_config import logger
        logger.info(
            f"[App] Overrun banner shown — "
            f"{elapsed_min:.0f}min round vs {interval_min:.0f}min interval"
        )

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

    def _protected_db_settings(self):
        """
        Gate DB settings behind admin password + OTP.
        Asked every single time ⚙ is clicked — no session memory.
        """
        from gui.components.protected_access_dialog import ProtectedAccessDialog
        ProtectedAccessDialog(self.root, self._config, callback=self.open_db_settings)

    def open_db_settings(self):
        """
        Re-open the setup wizard so user can change DB or Tally config.
        After saving, reconnects the DB engine with new credentials.
        """
        from gui.components.setup_wizard import SetupWizard

        self._config.mark_setup_incomplete()

        wizard = SetupWizard(self.root, self._config)
        self.root.wait_window(wizard)

        if wizard.completed:
            self._apply_config_to_state()

            try:
                if self.state.db_engine:
                    self.state.db_engine.dispose()
                db_cfg = self._config.get_db_config()
                self.state.db_engine = self._create_engine(db_cfg)
                self._db_status_lbl.configure(text="● DB: Connected", fg=Color.SUCCESS)

                self._load_companies_from_db(self.state.db_engine)
                home = self._frames.get("home")
                if home and hasattr(home, "refresh_companies"):
                    home.refresh_companies()

            except Exception as e:
                messagebox.showerror("Reconnect Failed", str(e))
        else:
            self._config.mark_setup_complete()

    def _show_window(self):
        """Bring the main window back from the tray."""
        self._tray.show_window()

    def _toggle_pause(self) -> bool:
        """
        Pause or resume all scheduled syncs.
        Returns new paused state (True = paused).
        Called from tray menu — runs on main thread via root.after().
        """
        self._syncs_paused = not self._syncs_paused

        sched = getattr(self, '_scheduler_controller', None)
        if sched:
            try:
                if self._syncs_paused:
                    sched.pause_all()
                else:
                    sched.resume_all()
            except Exception:
                pass

        status = "paused" if self._syncs_paused else "resumed"
        from logging_config import logger
        logger.info(f"[App] Scheduled syncs {status} via tray")

        self._tray.update_tooltip()
        return self._syncs_paused

    def _quit_app(self):
        """
        Truly exit — called from tray 'Exit TallySync' menu item.
        Shuts down scheduler + queue then destroys the window.
        """
        self._do_shutdown()

    def _do_shutdown(self):
        """Shared shutdown logic used by both tray exit and direct close."""
        sched_ctrl = getattr(self, '_scheduler_controller', None)
        if sched_ctrl and hasattr(sched_ctrl, 'shutdown'):
            try:
                sched_ctrl.shutdown()
            except Exception:
                pass

        sync_q_ctrl = getattr(self, '_sync_queue_controller', None)
        if sync_q_ctrl and hasattr(sync_q_ctrl, 'shutdown'):
            try:
                sync_q_ctrl.shutdown()
            except Exception:
                pass

        self._tray.stop()
        self.root.destroy()

    def _check_for_updates(self):
        """Check public gist for a newer version and notify the user."""
        if not GIST_VERSION_URL:
            messagebox.showinfo(
                "Not Configured",
                "Update check is not configured yet.\n(GIST_VERSION_URL is empty in styles.py)",
                parent=self.root,
            )
            return

        self._update_btn.configure(text="Checking...", fg=Color.SIDEBAR_TEXT_MUTED)
        self._update_btn.unbind("<Button-1>")

        def _fetch():
            try:
                resp = requests.get(GIST_VERSION_URL, timeout=8)
                resp.raise_for_status()
                data        = resp.json()
                latest      = data.get("version", "").strip()
                download_url = data.get("download_url", "")
                if not latest:
                    self.root.after(0, lambda: _show_result(None, None, "bad_gist"))
                    return
                self.root.after(0, lambda: _show_result(latest, download_url, None))
            except Exception as exc:
                err = str(exc)
                self.root.after(0, lambda: _show_result(None, None, err))

        def _show_result(latest, download_url, error):
            self._update_btn.configure(text="↑ Check for Updates")
            self._update_btn.bind("<Button-1>", lambda e: self._check_for_updates())

            if error == "bad_gist":
                messagebox.showerror(
                    "Update Check Failed",
                    "version.json in the gist is missing the 'version' field.",
                    parent=self.root,
                )
                return

            if error:
                messagebox.showerror(
                    "Update Check Failed",
                    f"Could not fetch version info:\n{error}",
                    parent=self.root,
                )
                return

            def _normalize(v):
                return v.lstrip("v").strip()

            if _normalize(latest) == _normalize(APP_VERSION):
                messagebox.showinfo(
                    "Up to Date",
                    f"You are running the latest version ({APP_VERSION}).",
                    parent=self.root,
                )
            else:
                answer = messagebox.askyesno(
                    "Update Available",
                    f"A new version is available!\n\n"
                    f"  Current : {APP_VERSION}\n"
                    f"  Latest  : {latest}\n\n"
                    f"Open the download page?",
                    parent=self.root,
                )
                if answer:
                    self._prompt_update_password(download_url)

        threading.Thread(target=_fetch, daemon=True).start()

    def _prompt_update_password(self, release_url: str):
        """
        Show a password dialog before allowing the update download.
        Password is read from .env (update_pass key) — never stored locally.
        """
        if not self._config.get_update_password():
            messagebox.showwarning(
                "Not Configured",
                "Update password is not set.\nAdd 'update_pass' to your .env file.",
                parent=self.root,
            )
            return

        dialog = tk.Toplevel(self.root)
        dialog.title("Admin Password Required")
        dialog.resizable(False, False)
        dialog.grab_set()
        dialog.configure(bg=Color.BG_CARD)

        self.root.update_idletasks()
        x = self.root.winfo_x() + (self.root.winfo_width()  // 2) - 175
        y = self.root.winfo_y() + (self.root.winfo_height() // 2) - 90
        dialog.geometry(f"350x180+{x}+{y}")

        tk.Label(
            dialog, text="Enter admin password to proceed:",
            font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).pack(padx=Spacing.LG, pady=(Spacing.LG, Spacing.SM), anchor="w")

        pwd_var = tk.StringVar()
        entry = tk.Entry(
            dialog, textvariable=pwd_var, show="●",
            font=Font.BODY, bg=Color.BG_INPUT,
            fg=Color.TEXT_PRIMARY, relief="solid", bd=1,
        )
        entry.pack(fill="x", padx=Spacing.LG)
        entry.focus_set()

        error_lbl = tk.Label(
            dialog, text="", font=Font.BODY_SM,
            bg=Color.BG_CARD, fg=Color.DANGER,
        )
        error_lbl.pack(padx=Spacing.LG, pady=(Spacing.XS, 0), anchor="w")

        def _attempt():
            if self._config.verify_update_password(pwd_var.get()):
                dialog.destroy()
                webbrowser.open(release_url)
            else:
                error_lbl.configure(text="Incorrect password. Try again.")
                entry.delete(0, "end")
                entry.focus_set()

        btn_frame = tk.Frame(dialog, bg=Color.BG_CARD)
        btn_frame.pack(fill="x", padx=Spacing.LG, pady=Spacing.MD)

        tk.Button(
            btn_frame, text="Confirm", command=_attempt,
            bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            font=Font.BODY, relief="flat", padx=Spacing.MD, cursor="hand2",
        ).pack(side="right")

        tk.Button(
            btn_frame, text="Cancel", command=dialog.destroy,
            bg=Color.BG_ROOT, fg=Color.TEXT_SECONDARY,
            font=Font.BODY, relief="flat", padx=Spacing.MD, cursor="hand2",
        ).pack(side="right", padx=(0, Spacing.SM))

        entry.bind("<Return>", lambda e: _attempt())
        dialog.bind("<Escape>", lambda e: dialog.destroy())

    def _on_close(self):
        """
        Called when user clicks ✖ on the main window.

        Phase 1 behaviour:
          - If tray is available → hide window, keep running in tray
          - If tray is NOT available (pystray not installed) → ask and quit
        """
        if not self._tray.available:
            if self.state.sync_active:
                if not messagebox.askyesno(
                    "Sync in Progress",
                    "A sync is currently running.\n\nAre you sure you want to quit?",
                ):
                    return
            self._do_shutdown()
            return

        if self.state.sync_active:
            if not messagebox.askyesno(
                "Sync in Progress",
                "A sync is currently running.\n\n"
                "Hide to tray and let it finish in the background?",
            ):
                return

        self._tray.hide_to_tray()

    def run(self):
        self.root.mainloop()