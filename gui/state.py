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
    # runtime progress (not persisted)
    progress_pct:      float                = 0.0
    progress_label:    str                  = ""
    error_message:     str                  = ""
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
    ledgers:       bool = True
    items:         bool = True
    sales:         bool = True
    purchase:      bool = True
    credit_note:   bool = True
    debit_note:    bool = True
    receipt:       bool = True
    payment:       bool = True
    journal:       bool = True
    contra:        bool = True
    trial_balance: bool = True

    def selected_types(self) -> list:
        """Return list of selected voucher_type strings matching VOUCHER_CONFIG keys."""
        mapping = {
            'ledgers':       'ledger',
            'items':         'items',
            'sales':         'sales',
            'purchase':      'purchase',
            'credit_note':   'credit_note',
            'debit_note':    'debit_note',
            'receipt':       'receipt',
            'payment':       'payment',
            'journal':       'journal',
            'contra':        'contra',
            'trial_balance': 'trial_balance',
        }
        return [v for k, v in mapping.items() if getattr(self, k)]

    def all_selected(self) -> bool:
        return all([
            self.ledgers, self.items, self.sales, self.purchase, self.credit_note,
            self.debit_note, self.receipt, self.payment, self.journal,
            self.contra, self.trial_balance
        ])


# ─────────────────────────────────────────────
#  Tally connection state
# ─────────────────────────────────────────────
@dataclass
class TallyConnectionState:
    host:      str  = "localhost"
    port:      int  = 9000
    connected: bool = False
    last_check: Optional[datetime] = None


# ─────────────────────────────────────────────
#  Central AppState
# ─────────────────────────────────────────────
class AppState:
    """
    Singleton-style state object passed to every page and controller.
    Holds all runtime data for the application session.
    """

    def __init__(self):
        # ── Company data ──────────────────────────────────
        self.companies: dict[str, CompanyState] = {}   # keyed by company name
        self.selected_companies: list[str]      = []   # names of ticked companies

        # ── Sync options (set on sync_page, read by sync_controller) ──
        self.sync_mode:        str              = SyncMode.INCREMENTAL
        self.sync_from_date:   Optional[str]    = None   # YYYYMMDD
        self.sync_to_date:     Optional[str]    = None   # YYYYMMDD
        self.voucher_selection: VoucherSelection = VoucherSelection()
        self.batch_sequential: bool             = True   # True=sequential, False=parallel

        # ── Tally connection ──────────────────────────────
        self.tally: TallyConnectionState        = TallyConnectionState()

        # ── DB engine (set by app.py on startup) ─────────
        self.db_engine                          = None

        # ── Active sync tracking ──────────────────────────
        self.sync_active:  bool                 = False
        self.sync_cancelled: bool               = False

        # ── Callbacks (pages register listeners here) ─────
        self._listeners: dict[str, list]        = {}

    # ── Event system ─────────────────────────────────────────────────────────
    def on(self, event: str, callback):
        """Register a listener for an event."""
        self._listeners.setdefault(event, []).append(callback)

    def off(self, event: str, callback):
        """Remove a specific listener."""
        if event in self._listeners:
            try:
                self._listeners[event].remove(callback)
            except ValueError:
                pass

    def emit(self, event: str, **kwargs):
        """Fire all listeners for an event (safe — catches exceptions)."""
        for cb in self._listeners.get(event, []):
            try:
                cb(**kwargs)
            except Exception as e:
                print(f"[AppState] Event '{event}' listener error: {e}")

    # ── Company helpers ───────────────────────────────────────────────────────
    def get_company(self, name: str) -> Optional[CompanyState]:
        return self.companies.get(name)

    def set_company_status(self, name: str, status: str, **kwargs):
        """Update a company's status and optionally other fields, then emit event."""
        if name in self.companies:
            self.companies[name].status = status
            for k, v in kwargs.items():
                if hasattr(self.companies[name], k):
                    setattr(self.companies[name], k, v)
            self.emit("company_updated", name=name, company=self.companies[name])

    def set_company_progress(self, name: str, pct: float, label: str = ""):
        """Update sync progress for a company."""
        if name in self.companies:
            self.companies[name].progress_pct   = pct
            self.companies[name].progress_label = label
            self.emit("company_progress", name=name, pct=pct, label=label)

    def configured_companies(self) -> list[CompanyState]:
        return [c for c in self.companies.values()
                if c.status != CompanyStatus.NOT_CONFIGURED]

    def not_configured_companies(self) -> list[CompanyState]:
        return [c for c in self.companies.values()
                if c.status == CompanyStatus.NOT_CONFIGURED]

    def get_selected_company_states(self) -> list[CompanyState]:
        return [self.companies[n] for n in self.selected_companies
                if n in self.companies]

    # ── Sync helpers ──────────────────────────────────────────────────────────
    def reset_sync_progress(self):
        """Clear progress on all companies before starting a new sync."""
        for c in self.companies.values():
            c.progress_pct   = 0.0
            c.progress_label = ""
            c.error_message  = ""

    def to_date_str(self) -> str:
        """Return sync_to_date or today as YYYYMMDD."""
        if self.sync_to_date:
            return self.sync_to_date
        return datetime.now().strftime('%Y%m%d')