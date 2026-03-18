"""
database/models/__init__.py
============================
Register ALL SQLAlchemy models here so that:
  1. Base.metadata knows about every table
  2. db_connector.create_tables() builds everything in one call
  3. No circular import issues — import Base + models from here

Add every new model to this file when created.
"""

from .base                  import Base

# ── Existing models ───────────────────────────────────────────────────────────
from .company               import Company
from .sync_state            import SyncState
from .ledger                import Ledger
from .item                  import Item
from .inventory_voucher     import SalesVoucher, PurchaseVoucher, CreditNote, DebitNote
from .ledger_voucher        import ReceiptVoucher, PaymentVoucher, JournalVoucher, ContraVoucher
from .trial_balance         import TrialBalance
from .scheduler_config      import CompanySchedulerConfig

# ── Phase 1: New models ───────────────────────────────────────────────────────
from .tally_settings        import TallySettings
from .automation_settings   import AutomationSettings
from .outstanding_models import OutstandingData

__all__ = [
    'Base',
    # existing
    'Company',
    'SyncState',
    'Ledger',
    'Item',
    'SalesVoucher', 'PurchaseVoucher', 'CreditNote', 'DebitNote',
    'ReceiptVoucher', 'PaymentVoucher', 'JournalVoucher', 'ContraVoucher',
    'TrialBalance',
    'CompanySchedulerConfig',

    'OutstandingData',
    # phase 1
    'TallySettings',
    'AutomationSettings',
]