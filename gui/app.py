"""
gui/app.py
==========
Root application window — the entry point for the entire GUI.

Responsibilities:
  - Create the main Tk window with ttkbootstrap theme
  - Build the persistent sidebar + header
  - Manage page switching (show/hide frames)
  - Initialize AppState and pass it to all pages
  - Handle app startup (DB connect, Tally ping) in background thread
  - Handle clean shutdown (stop scheduler, close DB)

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

from gui.state  import AppState, CompanyState, CompanyStatus
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
        self.state   = AppState()
        self._q      = queue.Queue()          # background → GUI communication
        self._frames = {}                     # page_key → Frame instance
        self._active_page = None

        self._build_root()
        self._build_layout()
        self._build_sidebar()
        self._build_header()
        self._build_content_area()
        self._load_pages()
        # Listen for sync_finished to detect post-snapshot completion
        self.state.on("sync_finished", self._on_sync_finished_app)
        self._snapshot_celebrated: set = set()

        self._start_startup_sequence()
        self._poll_queue()                    # start queue polling loop

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

        # Center on screen
        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x  = (sw - Layout.MIN_WIDTH)  // 2
        y  = (sh - Layout.MIN_HEIGHT) // 2
        self.root.geometry(f"{Layout.MIN_WIDTH}x{Layout.MIN_HEIGHT}+{x}+{y}")

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

    # ─────────────────────────────────────────────────────────────────────────
    #  Main layout — 2 columns: sidebar | main
    # ─────────────────────────────────────────────────────────────────────────
    def _build_layout(self):
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(0, weight=1)

        # Left sidebar column
        self.sidebar_frame = tk.Frame(
            self.root,
            bg=Color.BG_SIDEBAR,
            width=Layout.SIDEBAR_WIDTH,
        )
        self.sidebar_frame.grid(row=0, column=0, sticky="nsew")
        self.sidebar_frame.grid_propagate(False)

        # Right main column
        self.main_frame = tk.Frame(self.root, bg=Color.BG_ROOT)
        self.main_frame.grid(row=0, column=1, sticky="nsew")
        self.main_frame.rowconfigure(1, weight=1)
        self.main_frame.columnconfigure(0, weight=1)

    # ─────────────────────────────────────────────────────────────────────────
    #  Sidebar
    # ─────────────────────────────────────────────────────────────────────────
    def _build_sidebar(self):
        f = self.sidebar_frame

        # ── Logo / Brand ─────────────────────────────────
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

        # Separator
        tk.Frame(f, bg=Color.SIDEBAR_HOVER_BG, height=1).pack(fill="x")

        # ── Nav Items ────────────────────────────────────
        self._nav_buttons = {}

        nav_container = tk.Frame(f, bg=Color.BG_SIDEBAR)
        nav_container.pack(fill="x", pady=(Spacing.SM, 0))

        for item in NAV_ITEMS:
            btn = self._make_nav_button(nav_container, item)
            self._nav_buttons[item["page"]] = btn

        # ── Bottom — version + tally status ──────────────
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
        """Create a sidebar nav item that looks like a button."""
        container = tk.Frame(parent, bg=Color.BG_SIDEBAR, cursor="hand2")
        container.pack(fill="x")

        inner = tk.Frame(container, bg=Color.BG_SIDEBAR, padx=Spacing.LG, pady=Spacing.MD)
        inner.pack(fill="x")

        icon_lbl = tk.Label(
            inner,
            text=item["icon"],
            font=Font.BODY,
            bg=Color.BG_SIDEBAR,
            fg=Color.SIDEBAR_TEXT,
            width=2,
        )
        icon_lbl.pack(side="left")

        text_lbl = tk.Label(
            inner,
            text=item["label"],
            font=Font.SIDEBAR_ITEM,
            bg=Color.BG_SIDEBAR,
            fg=Color.SIDEBAR_TEXT,
            anchor="w",
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
            w.bind("<Enter>",   on_enter)
            w.bind("<Leave>",   on_leave)
            w.bind("<Button-1>",on_click)

        # Store widget refs for active state toggling
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
    #  Header bar (top of main area)
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

        # Page title (updated on navigate)
        self._header_title = tk.Label(
            header,
            text="Companies",
            font=Font.HEADING_4,
            bg=Color.BG_HEADER,
            fg=Color.TEXT_PRIMARY,
            anchor="w",
            padx=Spacing.XL,
        )
        self._header_title.grid(row=0, column=0, sticky="ew", pady=0)

        # Right side — DB status + time
        right = tk.Frame(header, bg=Color.BG_HEADER)
        right.grid(row=0, column=1, padx=Spacing.XL)

        self._db_status_lbl = tk.Label(
            right,
            text="● DB: Connecting...",
            font=Font.BODY_SM,
            bg=Color.BG_HEADER,
            fg=Color.MUTED,
        )
        self._db_status_lbl.pack(side="left", padx=(0, Spacing.LG))

        self._clock_lbl = tk.Label(
            right,
            text="",
            font=Font.BODY_SM,
            bg=Color.BG_HEADER,
            fg=Color.TEXT_SECONDARY,
        )
        self._clock_lbl.pack(side="left")
        self._update_clock()

        # ⚙ DB Settings button
        tk.Button(
            right, text="⚙",
            font=Font.BODY, bg=Color.BG_HEADER, fg=Color.TEXT_SECONDARY,
            relief="flat", bd=0, padx=6, cursor="hand2",
            command=self.open_db_settings,
        ).pack(side="left", padx=(Spacing.MD, 0))

        # Bottom border
        tk.Frame(self.main_frame, bg=Color.BORDER, height=1).grid(
            row=0, column=0, sticky="sew"
        )

    def _update_clock(self):
        now = datetime.now().strftime("%d %b %Y  %H:%M:%S")
        self._clock_lbl.configure(text=now)
        self.root.after(1000, self._update_clock)

    # ─────────────────────────────────────────────────────────────────────────
    #  Content area — pages stack here
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
        """
        Import and instantiate all pages.
        Each page is a Frame that fills the content_area.
        They are stacked (grid) and only the active one is raised.
        """
        # Import here (not at top) to avoid circular imports
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

        # Show home page first
        self.navigate("home")

    def navigate(self, page_key: str):
        """Switch to the given page."""
        if page_key not in self._frames:
            return

        # Raise the target page
        self._frames[page_key].tkraise()
        self._active_page = page_key

        # Update header title
        labels = {
            "home":      "Companies",
            "sync":      "Sync",
            "scheduler": "Scheduler",
            "logs":      "Logs",
            "settings":  "Settings",
        }
        self._header_title.configure(text=labels.get(page_key, ""))

        # Update sidebar active state
        self._set_active_nav(page_key)

        # Notify the page it's being shown (if it has on_show)
        page = self._frames[page_key]
        if hasattr(page, "on_show"):
            page.on_show()

    # ─────────────────────────────────────────────────────────────────────────
    #  DB config — load from file or prompt user
    # ─────────────────────────────────────────────────────────────────────────
    _ENV_FILE = ".env"   # sits next to run_gui.py

    def _load_db_config(self) -> dict:
        """
        Load DB credentials exclusively from .env file.

        Expected keys (case-insensitive):
            DB_USERNAME   — MySQL username
            DB_PASSWORD   — MySQL password
            DB_HOST       — host (default: localhost)
            DB_PORT       — port (default: 3306)
            DB_NAME       — database name

        Raises RuntimeError with a clear message if the file is missing
        or any required key is absent, which the startup worker will
        catch and display as an error dialog.
        """
        import os

        env_path = self._ENV_FILE

        # ── Locate .env ───────────────────────────────────
        if not os.path.exists(env_path):
            # Also check one directory up (in case run from a sub-folder)
            alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
            if os.path.exists(alt):
                env_path = alt
            else:
                raise RuntimeError(
                    f".env file not found.\n\n"
                    f"Please create a file named  .env  next to run_gui.py  with:\n\n"
                    f"  DB_USERNAME=root\n"
                    f"  DB_PASSWORD=yourpassword\n"
                    f"  DB_HOST=localhost\n"
                    f"  DB_PORT=3306\n"
                    f"  DB_NAME=tally_db"
                )

        # ── Parse .env (simple key=value, ignore comments/blanks) ─
        env: dict[str, str] = {}
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" not in line:
                    continue
                key, _, val = line.partition("=")
                # Strip optional surrounding quotes  'val'  or  "val"
                key = key.strip().upper()
                val = val.strip().strip("'\"")
                env[key] = val

        # ── Validate required keys ────────────────────────
        missing = [k for k in ("DB_USERNAME", "DB_NAME") if not env.get(k)]
        if missing:
            raise RuntimeError(
                f"Missing required keys in .env: {', '.join(missing)}\n\n"
                f"Your .env must contain at minimum:\n"
                f"  DB_USERNAME=root\n"
                f"  DB_PASSWORD=yourpassword\n"
                f"  DB_HOST=localhost\n"
                f"  DB_PORT=3306\n"
                f"  DB_NAME=tally_db"
            )

        return {
            "username": env.get("DB_USERNAME", "root"),
            "password": env.get("DB_PASSWORD", ""),
            "host":     env.get("DB_HOST",     "localhost"),
            "port":     env.get("DB_PORT",     "3306"),
            "database": env.get("DB_NAME",     "tally_db"),
        }

    @staticmethod
    def _create_engine(cfg: dict):
        """Create SQLAlchemy engine from config dict using DatabaseConnector."""
        from database.db_connector import DatabaseConnector
        from database.models.scheduler_config import Base as SchedBase

        connector = DatabaseConnector(
            username = cfg.get("username", "root"),
            password = cfg.get("password", ""),
            host     = cfg.get("host",     "localhost"),
            port     = int(cfg.get("port", 3306)),
            database = cfg.get("database", "tally_sync"),
        )
        connector.create_database_if_not_exists()
        connector.create_tables()
        engine = connector.get_engine()
        # Create company_scheduler_config table if not yet present
        SchedBase.metadata.create_all(engine, checkfirst=True)
        return engine

    # ─────────────────────────────────────────────────────────────────────────
    #  Startup sequence (background thread)
    # ─────────────────────────────────────────────────────────────────────────
    def _start_startup_sequence(self):
        """
        Connect to DB and Tally in background.
        Results are posted to self._q for the GUI thread to handle.
        """
        threading.Thread(target=self._startup_worker, daemon=True).start()

    def _startup_worker(self):
        # ── Step 1: Connect to database ──────────────────
        try:
            cfg    = self._load_db_config()
            engine = self._create_engine(cfg)
            self.state.db_engine      = engine
            self.state.db_config      = cfg        # store for reconnect
            self._q.put(("db_status", True, "Connected"))
        except Exception as e:
            self._q.put(("db_status", False, str(e)))
            return   # Can't do anything without DB

        # ── Step 2: Load companies from DB ───────────────
        try:
            self._load_companies_from_db(engine)
        except Exception as e:
            self._q.put(("error", f"Failed to load companies: {e}"))

        # ── Step 3: Load scheduler config from DB ────────
        # IMPORTANT: must run AFTER _load_companies_from_db so that the
        # CompanyState objects already exist and can receive schedule fields.
        try:
            from gui.controllers.company_controller import CompanyController
            CompanyController(self.state).load_scheduler_config()
        except Exception as e:
            from logging_config import logger
            logger.warning(f"[App] Could not load scheduler config: {e}")

        # companies_loaded triggers home + scheduler page refresh
        self._q.put(("companies_loaded", None))

        # ── Step 3: Ping Tally ────────────────────────────
        try:
            from services.tally_connector import TallyConnector
            tally = TallyConnector(
                host=self.state.tally.host,
                port=self.state.tally.port,
            )
            connected = (tally.status == "Connected")
            self.state.tally.connected  = connected
            self.state.tally.last_check = datetime.now()
            self._q.put(("tally_status", connected))
        except Exception as e:
            self._q.put(("tally_status", False))

    def _load_companies_from_db(self, engine):
        """
        Read companies from DB and merge with live Tally company list.

        Strategy:
          - DB companies  → always shown as Configured (or Sync Done etc.)
          - Tally-only    → shown as Not Configured, with a Configure button
          - DB-only       → shown as Configured but flagged tally_open=False
          - Both          → Configured, tally_open=True
        """
        from sqlalchemy.orm import sessionmaker
        from database.models.company    import Company
        from database.models.sync_state import SyncState

        # ── Step 1: Load DB companies ─────────────────────
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
                    is_initial = all(s.is_initial_done for s in states)
                    months     = [s.last_synced_month for s in states if s.last_synced_month]
                    last_month = max(months) if months else None

                from_str  = None
                books_str = None
                if co.starting_from:
                    from_str  = str(co.starting_from).replace("-", "")[:8]
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
                )
                # Mark as not open in Tally until we check below
                cs.tally_open = False
                self.state.companies[name] = cs
        finally:
            db.close()

        # ── Step 2: Fetch live Tally companies ────────────
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

        # ── Step 3: Merge Tally into state ────────────────
        tally_names = set()
        for tc in tally_companies:
            name = (tc.get("name") or "").strip()
            if not name:
                continue
            tally_names.add(name)

            # Normalize Tally starting_from (YYYYMMDD)
            raw_from  = tc.get("starting_from", "")
            raw_books = tc.get("books_from", "")
            from_str  = str(raw_from).replace("-", "")[:8]  if raw_from  else None
            books_str = str(raw_books).replace("-", "")[:8] if raw_books else None

            if name in self.state.companies:
                # Already in DB — just mark as open in Tally
                self.state.companies[name].tally_open = True
                # Fill in books_from if not already set
                if not self.state.companies[name].books_from and books_str:
                    self.state.companies[name].books_from = books_str
            else:
                # New — exists only in Tally, not in DB
                cs = CompanyState(
                    name          = name,
                    guid          = tc.get("guid", ""),
                    status        = CompanyStatus.NOT_CONFIGURED,
                    starting_from = from_str,
                    books_from    = books_str,
                )
                cs.tally_open = True
                self.state.companies[name] = cs

        # ── Step 4: Mark DB companies not currently open in Tally ─
        for name, cs in self.state.companies.items():
            if not hasattr(cs, 'tally_open'):
                cs.tally_open = False

    def save_company_to_db(self, company_name: str, guid: str,
                           starting_from: str, books_from: str = None):
        """
        Insert or update a company record in the DB.
        Called after the user fills in the Configure dialog.
        """
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
    #  Queue polling — safely update GUI from background threads
    # ─────────────────────────────────────────────────────────────────────────
    def _poll_queue(self):
        """Called every 100ms to drain the queue and update the GUI."""
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
                self._db_status_lbl.configure(
                    text=f"● DB: Connected",
                    fg=Color.SUCCESS,
                )
            else:
                self._db_status_lbl.configure(
                    text=f"● DB: Error",
                    fg=Color.DANGER,
                )
                messagebox.showerror(
                    "Database Configuration Error",
                    detail,
                )

        elif event == "tally_status":
            _, connected = msg
            if connected:
                self._tally_status_lbl.configure(
                    text="● Tally: Online",
                    fg=Color.SUCCESS,
                )
            else:
                self._tally_status_lbl.configure(
                    text="● Tally: Offline",
                    fg=Color.DANGER,
                )

        elif event == "companies_loaded":
            # Refresh the home page with the newly loaded companies
            home = self._frames.get("home")
            if home and hasattr(home, "refresh_companies"):
                home.refresh_companies()

            # Refresh the scheduler page so rows reflect current schedule config
            # and next-run times are recalculated from the live APScheduler state.
            sched_page = self._frames.get("scheduler")
            if sched_page and hasattr(sched_page, "refresh_companies"):
                sched_page.refresh_companies()

        elif event == "error":
            _, msg_text = msg
            messagebox.showerror("Error", msg_text)

        elif event == "sync_log":
            # Forward log lines to logs page
            _, line = msg
            logs_page = self._frames.get("logs")
            if logs_page and hasattr(logs_page, "append_log"):
                logs_page.append_log(line)

        elif event == "company_progress":
            # Forward to home page progress bars
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

    # ─────────────────────────────────────────────────────────────────────────
    #  Public helper — post to queue from any thread
    # ─────────────────────────────────────────────────────────────────────────
    def post(self, *args):
        """Safe way for background threads to communicate with GUI."""
        self._q.put(args)

    # ─────────────────────────────────────────────────────────────────────────
    #  Shutdown
    # ─────────────────────────────────────────────────────────────────────────
    # ─────────────────────────────────────────────────────────────────────────
    #  Post-snapshot detection — show schedule prompt after first full sync
    # ─────────────────────────────────────────────────────────────────────────
    def _on_sync_finished_app(self):
        """Called via state event after any sync finishes (on main thread via emit)."""
        self._check_post_snapshot_companies()

    def _check_post_snapshot_companies(self):
        """
        After any sync completes, check if any company just had its initial
        snapshot finish (is_initial_done True + last_sync_time just set).
        If so, show PostSnapshotDialog offering to set up a schedule.
        Only shown once per company (tracked via _snapshot_celebrated set).
        """
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
        """Show PostSnapshotDialog for a company and handle result."""
        from gui.components.initial_snapshot_dialog import PostSnapshotDialog

        dialog = PostSnapshotDialog(self.root, co)
        self.root.wait_window(dialog)

        if dialog.result == "schedule":
            # Navigate to scheduler page with this company pre-selected
            self.state.selected_companies = [co.name]
            self.navigate("scheduler")

    def _on_close(self):
        if self.state.sync_active:
            if not messagebox.askyesno(
                "Sync in Progress",
                "A sync is currently running.\n\n"
                "Are you sure you want to quit? The sync will be interrupted.",
            ):
                return

        # Stop scheduler if running
        sched_ctrl = getattr(self, '_scheduler_controller', None)
        if sched_ctrl and hasattr(sched_ctrl, 'shutdown'):
            try:
                sched_ctrl.shutdown()
            except Exception:
                pass

        self.root.destroy()

    # ─────────────────────────────────────────────────────────────────────────
    #  DB Settings dialog (re-open from header ⚙ button)
    # ─────────────────────────────────────────────────────────────────────────
    def open_db_settings(self):
        """
        Open DB settings dialog.
        Reads current values from .env (via _load_db_config),
        shows the dialog, then writes changes back to .env.
        """
        import os

        # Pre-fill from current .env (or fall back to defaults if missing)
        defaults = {
            "host": "localhost", "port": "3306",
            "username": "root",  "password": "",
            "database": "tally_db",
        }
        try:
            loaded = self._load_db_config()
            defaults.update(loaded)
        except Exception:
            pass   # .env missing or malformed — dialog opens with defaults

        dialog = DBConfigDialog(self.root, defaults)
        self.root.wait_window(dialog)

        if dialog.result:
            res = dialog.result
            env_path = self._ENV_FILE

            # Resolve absolute path the same way _load_db_config does
            if not os.path.isabs(env_path):
                alt = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), env_path
                )
                if os.path.exists(alt) or not os.path.exists(env_path):
                    env_path = alt

            lines = [
                f"DB_USERNAME={res.get('username', 'root')}",
                f"DB_PASSWORD={res.get('password', '')}",
                f"DB_HOST={res.get('host', 'localhost')}",
                f"DB_PORT={res.get('port', '3306')}",
                f"DB_NAME={res.get('database', 'tally_db')}",
            ]
            with open(env_path, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")

            if messagebox.askyesno(
                "Restart Required",
                "DB settings saved.\n\nRestart the app for changes to take effect. Restart now?"
            ):
                self.root.destroy()

    # ─────────────────────────────────────────────────────────────────────────
    #  Entry point
    # ─────────────────────────────────────────────────────────────────────────
    def run(self):
        self.root.mainloop()


# ─────────────────────────────────────────────────────────────────────────────
#  DB Config Dialog
# ─────────────────────────────────────────────────────────────────────────────
class DBConfigDialog(tk.Toplevel):
    """
    Modal dialog to collect MySQL connection details.
    Sets self.result = dict on OK, or None on Cancel.
    """

    def __init__(self, parent, defaults: dict):
        super().__init__(parent)
        self.title("Database Connection Settings")
        self.resizable(False, False)
        self.grab_set()              # modal
        self.result = None

        self._vars = {}
        self._build(defaults)

        # Center over parent
        self.update_idletasks()
        pw = parent.winfo_rootx() + parent.winfo_width()  // 2
        ph = parent.winfo_rooty() + parent.winfo_height() // 2
        self.geometry(f"+{pw - 210}+{ph - 180}")

    def _build(self, defaults: dict):
        from gui.styles import Color, Font, Spacing

        pad = tk.Frame(self, bg=Color.BG_CARD, padx=30, pady=24)
        pad.pack(fill="both", expand=True)

        tk.Label(
            pad, text="MySQL / MariaDB Connection",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 16))

        fields = [
            ("Host",     "host",     False),
            ("Port",     "port",     False),
            ("Username", "username", False),
            ("Password", "password", True),
            ("Database", "database", False),
        ]

        for i, (label, key, secret) in enumerate(fields, start=1):
            tk.Label(
                pad, text=f"{label}:",
                font=Font.BODY, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                anchor="w", width=10,
            ).grid(row=i, column=0, sticky="w", pady=4)

            var = tk.StringVar(value=defaults.get(key, ""))
            self._vars[key] = var

            entry = tk.Entry(
                pad, textvariable=var,
                font=Font.BODY, width=26,
                bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
                show="●" if secret else "",
            )
            entry.grid(row=i, column=1, sticky="ew", pady=4, padx=(8, 0))

        # Test connection feedback
        self._feedback = tk.Label(
            pad, text="", font=Font.BODY_SM,
            bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._feedback.grid(row=len(fields)+1, column=0, columnspan=2,
                            sticky="w", pady=(8, 0))

        # Buttons
        btn_row = tk.Frame(pad, bg=Color.BG_CARD)
        btn_row.grid(row=len(fields)+2, column=0, columnspan=2,
                     sticky="ew", pady=(16, 0))

        tk.Button(
            btn_row, text="Test Connection",
            font=Font.BUTTON_SM, bg=Color.BG_ROOT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=10, pady=4, cursor="hand2",
            command=self._on_test,
        ).pack(side="left")

        tk.Button(
            btn_row, text="Cancel",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=10, pady=4, cursor="hand2",
            command=self.destroy,
        ).pack(side="right", padx=(8, 0))

        tk.Button(
            btn_row, text="Save & Connect",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=14, pady=4, cursor="hand2",
            command=self._on_save,
        ).pack(side="right")

        # Enter key submits
        self.bind("<Return>", lambda e: self._on_save())
        self.bind("<Escape>", lambda e: self.destroy())

    def _collect(self) -> dict:
        return {k: v.get().strip() for k, v in self._vars.items()}

    def _on_test(self):
        self._feedback.configure(text="Testing...", fg=Color.TEXT_MUTED if True else "")
        self.update_idletasks()
        try:
            from gui.styles import Color
            from database.db_connector import DatabaseConnector
            cfg = self._collect()
            conn = DatabaseConnector(
                username=cfg["username"], password=cfg["password"],
                host=cfg["host"],        port=int(cfg["port"]),
                database=cfg["database"],
            )
            ok = conn.test_connection()
            if ok:
                self._feedback.configure(text="✓ Connection successful!", fg=Color.SUCCESS)
            else:
                self._feedback.configure(text="✗ Connection failed — check credentials.", fg=Color.DANGER)
        except Exception as e:
            from gui.styles import Color
            self._feedback.configure(text=f"✗ {e}", fg=Color.DANGER)

    def _on_save(self):
        cfg = self._collect()
        if not cfg.get("host") or not cfg.get("database"):
            from gui.styles import Color
            self._feedback.configure(text="Host and Database are required.", fg=Color.DANGER)
            return
        self.result = cfg
        self.destroy()