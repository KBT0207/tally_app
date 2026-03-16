"""
gui/state.py
============
Central application state — single source of truth for the entire GUI.
All pages read from and write to this shared AppState instance.
Never import pages here — only data structures.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional
import threading


# ─────────────────────────────────────────────
#  Company status constants
# ─────────────────────────────────────────────
class CompanyStatus:
    CONFIGURED     = "Configured"
    NOT_CONFIGURED = "Not Configured"
    SYNCING        = "Syncing"
    SYNC_DONE      = "Sync Done"
    SYNC_ERROR     = "Sync Error"
    TALLY_OFFLINE  = "Tally Offline"
    SCHEDULED      = "Scheduled"


# ─────────────────────────────────────────────
#  Sync mode constants
# ─────────────────────────────────────────────
class SyncMode:
    INCREMENTAL = "incremental"   # CDC — use alter_id
    SNAPSHOT    = "snapshot"      # Full date range pull


# ─────────────────────────────────────────────
#  Per-company runtime state
# ─────────────────────────────────────────────
@dataclass
class CompanyState:
    name:              str
    guid:              str                  = ""
    status:            str                  = CompanyStatus.NOT_CONFIGURED
    last_sync_time:    Optional[datetime]   = None
    last_alter_id:     int                  = 0
    last_synced_month: Optional[str]        = None
    is_initial_done:   bool                 = False
    starting_from:     Optional[str]        = None   # YYYYMMDD string
    books_from:        Optional[str]        = None
    tally_host:        str                  = "localhost"
    tally_port:        int                  = 9000
    tally_open:        bool                 = False  # ← FIXED: was missing, caused pill to never show
    tally_username:    str                  = ""
    tally_password:    str                  = ""
    # ── Per-company Tally data location (Phase 1) ─────────────────────────────
    company_type:      str                  = "local"   # local | remote | tds
    data_path:         str                  = ""        # C:\TallyData\CompanyA
    tds_path:          str                  = ""        # 192.168.1.10
    drive_letter:      str                  = ""        # Z:
    material_centre:   str                  = ""        # e.g. Main Location
    default_currency:  str                  = "INR"     # e.g. INR, USD
    # runtime progress (not persisted)
    progress_pct:      float                = 0.0
    progress_label:    str                  = ""
    error_message:     str                  = ""
    syncing:           bool                 = False   # True only while THIS company is syncing
    # scheduler config
    schedule_enabled:  bool                 = False
    schedule_interval: str                  = "hourly"  # hourly | daily | minutes
    schedule_value:    int                  = 1         # e.g. every N hours/minutes
    schedule_time:     str                  = "09:00"   # for daily — HH:MM


# ─────────────────────────────────────────────
#  Voucher selection state
# ─────────────────────────────────────────────
@dataclass
class VoucherSelection:
    ledgers:              bool = True
    items:                bool = True
    sales:                bool = True
    purchase:             bool = True
    credit_note:          bool = True
    debit_note:           bool = True
    receipt:              bool = True
    payment:              bool = True
    journal:              bool = True
    contra:               bool = True
    trial_balance:        bool = True
    outstanding_debtors:  bool = True

    def selected_types(self) -> list:
        """Return list of selected voucher_type strings matching VOUCHER_CONFIG keys."""
        mapping = {
            'ledgers':             'ledger',
            'items':               'items',
            'sales':               'sales',
            'purchase':            'purchase',
            'credit_note':         'credit_note',
            'debit_note':          'debit_note',
            'receipt':             'receipt',
            'payment':             'payment',
            'journal':             'journal',
            'contra':              'contra',
            'trial_balance':       'trial_balance',
            'outstanding_debtors': 'outstanding_debtors',
        }
        return [v for k, v in mapping.items() if getattr(self, k)]

    def all_selected(self) -> bool:
        return all([
            self.ledgers, self.items, self.sales, self.purchase, self.credit_note,
            self.debit_note, self.receipt, self.payment, self.journal,
            self.contra, self.trial_balance, self.outstanding_debtors,
        ])


# ─────────────────────────────────────────────
#  Tally connection state
# ─────────────────────────────────────────────
@dataclass
class TallyConnectionState:
    host:       str             = "localhost"
    port:       int             = 9000
    connected:  bool            = False
    last_check: Optional[datetime] = None


# ─────────────────────────────────────────────
#  Tally automation config (Phase 1)
#  Loaded from DB → AppState.automation at startup
# ─────────────────────────────────────────────
@dataclass
class AutomationConfig:
    confidence:       float = 0.80
    click_delay_ms:   int   = 500
    wait_timeout_sec: int   = 30
    retry_attempts:   int   = 3


# ─────────────────────────────────────────────
#  Central AppState
# ─────────────────────────────────────────────
class AppState:
    """
    Singleton-style state object passed to every page and controller.
    Holds all runtime data for the application session.
    """

    def __init__(self):
        # ── Thread safety ─────────────────────────────────
        # Use RLock (re-entrant) so the same thread can acquire it multiple
        # times without deadlocking (e.g. set_company_status calls emit which
        # calls a listener that reads state again).
        self._lock = threading.RLock()

        # ── Company data ──────────────────────────────────
        self.companies: dict[str, CompanyState] = {}   # keyed by company name
        self.selected_companies: list[str]      = []   # names of ticked companies

        # ── Sync options (set on sync_page, read by sync_controller) ──
        self.sync_mode:         str              = SyncMode.INCREMENTAL
        self.sync_from_date:    Optional[str]    = None   # YYYYMMDD
        self.sync_to_date:      Optional[str]    = None   # YYYYMMDD
        self.voucher_selection: VoucherSelection = VoucherSelection()
        self.batch_sequential:  bool             = True   # True=sequential, False=parallel

        # ── Tally connection ──────────────────────────────
        self.tally: TallyConnectionState        = TallyConnectionState()

        # ── DB engine (set by app.py on startup) ─────────
        self.db_engine                          = None

        # ── DB config dict (set by app.py from ConfigManager) ────────────
        # Shape: {"host": str, "port": int, "username": str,
        #         "password": str, "database": str}
        # Used by: SchedulerController (jobstore URL), settings page
        self.db_config: Optional[dict]          = None

        # ── Tally global config (set by app.py from ConfigManager) ───────
        # Shape: {"host": str, "port": int}
        # Used by: TallyConnector default, per-company override falls back here
        self.tally_config: Optional[dict]       = None

        # ── Phase 1: Tally exe path + automation settings ─────────────────
        # tally_exe_path: full path to Tally.exe — loaded from tally_settings table
        self.tally_exe_path: str                = ""
        # automation: PyAutoGUI runtime controls — loaded from automation_settings table
        self.automation: AutomationConfig       = AutomationConfig()
        # tally_images: image filename map — loaded from tally_settings table
        # Key = short name (e.g. "gateway"), value = filename in assets/ folder
        self.tally_images: dict                 = {
            "gateway":      "tally_gateway_screen.png",
            "search_box":   "tally_company_search_box.png",
            "username":     "tally_username_field.png",
            "password":     "tally_password_field.png",
            "select_title": "tally_select_company_title.png",
            "change_path":  "tally_change_path_btn.png",
            "remote_tab":   "tally_remote_tab.png",
            "tds_field":    "tally_tds_field.png",
        }

        # ── Active sync tracking ──────────────────────────
        self._sync_active:    bool              = False   # use property below
        self._sync_cancelled: bool              = False   # use property below

        # ── Callbacks (pages register listeners here) ─────
        self._listeners: dict[str, list]        = {}

    # ── Thread-safe sync flags ────────────────────────────────────────────────
    @property
    def sync_active(self) -> bool:
        with self._lock:
            return self._sync_active

    @sync_active.setter
    def sync_active(self, value: bool):
        with self._lock:
            self._sync_active = value

    @property
    def sync_cancelled(self) -> bool:
        with self._lock:
            return self._sync_cancelled

    @sync_cancelled.setter
    def sync_cancelled(self, value: bool):
        with self._lock:
            self._sync_cancelled = value

    # ── Event system ─────────────────────────────────────────────────────────
    def on(self, event: str, callback):
        """Register a listener for an event."""
        with self._lock:
            self._listeners.setdefault(event, []).append(callback)

    def off(self, event: str, callback):
        """Remove a specific listener."""
        with self._lock:
            if event in self._listeners:
                try:
                    self._listeners[event].remove(callback)
                except ValueError:
                    pass

    def emit(self, event: str, **kwargs):
        """Fire all listeners for an event (safe — catches exceptions).
        Snapshot the listener list under lock so we don't hold the lock
        during callback execution (avoids deadlocks with re-entrant calls).
        """
        with self._lock:
            callbacks = list(self._listeners.get(event, []))
        for cb in callbacks:
            try:
                cb(**kwargs)
            except Exception as e:
                print(f"[AppState] Event '{event}' listener error: {e}")

    # ── Company helpers ───────────────────────────────────────────────────────
    def get_company(self, name: str) -> Optional[CompanyState]:
        with self._lock:
            return self.companies.get(name)

    def set_company_status(self, name: str, status: str, **kwargs):
        """Update a company's status and optionally other fields, then emit event."""
        with self._lock:
            if name in self.companies:
                self.companies[name].status = status
                for k, v in kwargs.items():
                    if hasattr(self.companies[name], k):
                        setattr(self.companies[name], k, v)
                company = self.companies[name]
            else:
                return
        self.emit("company_updated", name=name, company=company)

    def set_company_progress(self, name: str, pct: float, label: str = ""):
        """Update sync progress for a company."""
        with self._lock:
            if name not in self.companies:
                return
            self.companies[name].progress_pct   = pct
            self.companies[name].progress_label = label
        self.emit("company_progress", name=name, pct=pct, label=label)

    def configured_companies(self) -> list[CompanyState]:
        with self._lock:
            return [c for c in self.companies.values()
                    if c.status != CompanyStatus.NOT_CONFIGURED]

    def not_configured_companies(self) -> list[CompanyState]:
        with self._lock:
            return [c for c in self.companies.values()
                    if c.status == CompanyStatus.NOT_CONFIGURED]

    def get_selected_company_states(self) -> list[CompanyState]:
        with self._lock:
            return [self.companies[n] for n in self.selected_companies
                    if n in self.companies]

    # ── Sync helpers ──────────────────────────────────────────────────────────
    def reset_sync_progress(self):
        """Clear progress on all companies before starting a new sync."""
        with self._lock:
            for c in self.companies.values():
                c.progress_pct   = 0.0
                c.progress_label = ""
                c.error_message  = ""

    def to_date_str(self) -> str:
        """Return sync_to_date or today as YYYYMMDD."""
        if self.sync_to_date:
            return self.sync_to_date
        return datetime.now().strftime('%Y%m%d')

    # ── Config helpers ────────────────────────────────────────────────────────
    def get_tally_host(self) -> str:
        """Global Tally host from config. Falls back to localhost."""
        if self.tally_config:
            return self.tally_config.get("host", "localhost")
        return "localhost"

    def get_tally_port(self) -> int:
        """Global Tally port from config. Falls back to 9000."""
        if self.tally_config:
            return int(self.tally_config.get("port", 9000))
        return 9000