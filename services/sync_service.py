from datetime import datetime, date, timedelta
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import xml.etree.ElementTree as ET
import gc
import psutil
import os

from logging_config import logger
from database.models.sync_state import SyncState

from services.tally_connector import TallyConnector
from services.data_processor import (
    parse_inventory_voucher,
    parse_ledger_voucher,
    parse_ledgers,
    parse_items,
    parse_trial_balance,
    parse_outstanding,
    parse_guids,
    sanitize_xml_content,
)
from database.database_processor import (
    get_sync_state,
    update_sync_state,
    upsert_ledgers,
    upsert_items,
    upsert_and_advance_month,
    upsert_sales_vouchers,
    upsert_purchase_vouchers,
    upsert_credit_notes,
    upsert_debit_notes,
    upsert_receipt_vouchers,
    upsert_payment_vouchers,
    upsert_journal_vouchers,
    upsert_contra_vouchers,
    upsert_trial_balance,
    upsert_outstanding,
    reconcile_deleted_by_guids,
    reconcile_deleted_masters_in_db,
    INVENTORY_MODEL_MAP,
    LEDGER_MODEL_MAP,
    _upsert_inventory_voucher_in_session,
    _upsert_ledger_voucher_in_session,
    _get_session,
)

# ── Tuning constants ──────────────────────────────────────────────────────────
SNAPSHOT_CHUNK_MONTHS = 3
VOUCHER_WORKERS       = 1

# ── Global Tally request semaphore ───────────────────────────────────────────
_TALLY_SEMAPHORE         = threading.Semaphore(1)
_TALLY_SEMAPHORE_TIMEOUT = 120


def _tally_semaphore_acquire(voucher_type: str = "") -> bool:
    acquired = _TALLY_SEMAPHORE.acquire(timeout=_TALLY_SEMAPHORE_TIMEOUT)
    if not acquired:
        logger.error(
            f"[sync_service] Tally semaphore timeout after {_TALLY_SEMAPHORE_TIMEOUT}s"
            + (f" [{voucher_type}]" if voucher_type else "")
            + " — skipping to unblock other syncs"
        )
    return acquired

# ── Per-company SyncState write lock ─────────────────────────────────────────
_SYNC_STATE_LOCKS: dict[str, threading.Lock] = {}
_SYNC_STATE_LOCKS_MUTEX = threading.Lock()

def _get_company_lock(company_name: str) -> threading.Lock:
    with _SYNC_STATE_LOCKS_MUTEX:
        if company_name not in _SYNC_STATE_LOCKS:
            _SYNC_STATE_LOCKS[company_name] = threading.Lock()
        return _SYNC_STATE_LOCKS[company_name]


def cleanup_sync_state(company_name: str = "") -> None:
    """
    Clear accumulated global lock state and force garbage collection.
    Called in a finally block after each company sync to prevent memory growth
    across repeated runs. Zero data impact — only clears in-memory lock dicts.
    """
    prefix = f"[{company_name}] " if company_name else ""
    logger.info(f"{prefix}Cleaning up global state...")

    with _SYNC_STATE_LOCKS_MUTEX:
        sync_lock_count = len(_SYNC_STATE_LOCKS)
        _SYNC_STATE_LOCKS.clear()

    logger.info(f"Cleared {sync_lock_count} sync locks")

    gc.collect()
    logger.info(f"{prefix}Cleanup complete (zero data impact)")


class MemoryMonitor:
    """Tracks RSS memory and warns if growth exceeds a threshold."""

    def __init__(self, company_name: str, warn_mb: float = 500.0):
        self.company_name = company_name
        self.warn_mb      = warn_mb
        self._proc        = psutil.Process(os.getpid())
        self.baseline_mb  = self._rss_mb()

    def _rss_mb(self) -> float:
        return self._proc.memory_info().rss / (1024 * 1024)

    def check(self, label: str = "") -> float:
        current_mb = self._rss_mb()
        growth_mb  = current_mb - self.baseline_mb
        if growth_mb > self.warn_mb:
            logger.warning(
                f"[{self.company_name}] MEMORY ALERT{' ' + label if label else ''}: "
                f"RSS={current_mb:.0f}MB (+{growth_mb:.0f}MB since start)"
            )
        else:
            logger.debug(
                f"[{self.company_name}] Memory{' ' + label if label else ''}: "
                f"RSS={current_mb:.0f}MB (+{growth_mb:.0f}MB)"
            )
        return current_mb


# ── Voucher type routing ──────────────────────────────────────────────────────
VOUCHER_TYPE_NAMES = {
    'sales'       : None,
    'purchase'    : None,
    'credit_note' : None,
    'debit_note'  : None,
    'receipt'     : None,
    'payment'     : None,
    'journal'     : None,
    'contra'      : None,
}
VOUCHER_CONFIG = [
    {
        'voucher_type'    : 'sales',
        'snapshot_fetch'  : 'fetch_sales',
        'cdc_fetch'       : 'fetch_sales_cdc',
        'guid_fetch'      : 'fetch_sales_guids',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_sales_vouchers,
        'parser_type_name': 'Sales Vouchers',
        'kind'            : 'inventory',
        'allowed_types'   : VOUCHER_TYPE_NAMES['sales'],
    },
    {
        'voucher_type'    : 'purchase',
        'snapshot_fetch'  : 'fetch_purchase',
        'cdc_fetch'       : 'fetch_purchase_cdc',
        'guid_fetch'      : 'fetch_purchase_guids',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_purchase_vouchers,
        'parser_type_name': 'Purchase Vouchers',
        'kind'            : 'inventory',
        'allowed_types'   : VOUCHER_TYPE_NAMES['purchase'],
    },
    {
        'voucher_type'    : 'credit_note',
        'snapshot_fetch'  : 'fetch_credit_note',
        'cdc_fetch'       : 'fetch_credit_note_cdc',
        'guid_fetch'      : 'fetch_credit_note_guids',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_credit_notes,
        'parser_type_name': 'Credit Note',
        'kind'            : 'inventory',
        'allowed_types'   : VOUCHER_TYPE_NAMES['credit_note'],
    },
    {
        'voucher_type'    : 'debit_note',
        'snapshot_fetch'  : 'fetch_debit_note',
        'cdc_fetch'       : 'fetch_debit_note_cdc',
        'guid_fetch'      : 'fetch_debit_note_guids',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_debit_notes,
        'parser_type_name': 'Debit Note',
        'kind'            : 'inventory',
        'allowed_types'   : VOUCHER_TYPE_NAMES['debit_note'],
    },
    {
        'voucher_type'    : 'receipt',
        'snapshot_fetch'  : 'fetch_receipt',
        'cdc_fetch'       : 'fetch_receipt_cdc',
        'guid_fetch'      : 'fetch_receipt_guids',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_receipt_vouchers,
        'parser_type_name': 'Receipt Vouchers',
        'kind'            : 'ledger',
        'allowed_types'   : VOUCHER_TYPE_NAMES['receipt'],
    },
    {
        'voucher_type'    : 'payment',
        'snapshot_fetch'  : 'fetch_payment',
        'cdc_fetch'       : 'fetch_payment_cdc',
        'guid_fetch'      : 'fetch_payment_guids',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_payment_vouchers,
        'parser_type_name': 'Payment Vouchers',
        'kind'            : 'ledger',
        'allowed_types'   : VOUCHER_TYPE_NAMES['payment'],
    },
    {
        'voucher_type'    : 'journal',
        'snapshot_fetch'  : 'fetch_journal',
        'cdc_fetch'       : 'fetch_journal_cdc',
        'guid_fetch'      : 'fetch_journal_guids',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_journal_vouchers,
        'parser_type_name': 'Journal Vouchers',
        'kind'            : 'ledger',
        'allowed_types'   : VOUCHER_TYPE_NAMES['journal'],
    },
    {
        'voucher_type'    : 'contra',
        'snapshot_fetch'  : 'fetch_contra',
        'cdc_fetch'       : 'fetch_contra_cdc',
        'guid_fetch'      : 'fetch_contra_guids',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_contra_vouchers,
        'parser_type_name': 'Contra Vouchers',
        'kind'            : 'ledger',
        'allowed_types'   : VOUCHER_TYPE_NAMES['contra'],
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_max_alter_id(rows: list) -> int:
    if not rows:
        return 0
    return max(int(r.get('alter_id', 0)) for r in rows)


def _resolve_from_date(company: dict) -> str:
    starting_from = company.get('starting_from', '')
    if starting_from:
        cleaned = str(starting_from).strip().replace('-', '')
        if len(cleaned) == 8 and cleaned.isdigit():
            return cleaned
    fallback = '20240401'
    logger.warning(
        f"No valid starting_from for '{company.get('name')}' — using fallback {fallback}"
    )
    return fallback


def _generate_chunks(from_date_str: str, to_date_str: str, chunk_months: int = SNAPSHOT_CHUNK_MONTHS):
    """
    Yield (chunk_from, chunk_to, month_str) tuples covering [from_date, to_date]
    in steps of chunk_months months.
    """
    start       = datetime.strptime(from_date_str, '%Y%m%d').date()
    end         = datetime.strptime(to_date_str,   '%Y%m%d').date()
    chunk_start = start

    while chunk_start <= end:
        end_month = chunk_start.month + chunk_months - 1
        end_year  = chunk_start.year + (end_month - 1) // 12
        end_month = (end_month - 1) % 12 + 1
        last_day  = monthrange(end_year, end_month)[1]

        chunk_end  = min(date(end_year, end_month, last_day), end)
        month_str  = chunk_end.strftime('%Y%m')
        chunk_from = chunk_start.strftime('%Y%m%d')
        chunk_to   = chunk_end.strftime('%Y%m%d')

        yield chunk_from, chunk_to, month_str

        if chunk_end >= end:
            break

        next_month  = chunk_end.month + 1 if chunk_end.month < 12 else 1
        next_year   = chunk_end.year      if chunk_end.month < 12 else chunk_end.year + 1
        chunk_start = date(next_year, next_month, 1)


def _mark_chunk_done(company_name: str, voucher_type: str, month_str: str, engine):
    """Persist progress for a chunk that returned no data."""
    lock = _get_company_lock(company_name)
    with lock:
        db = _get_session(engine)
        try:
            state = db.query(SyncState).filter_by(
                company_name=company_name,
                voucher_type=voucher_type,
            ).first()

            if state:
                state.last_synced_month = month_str
                state.last_sync_time    = datetime.utcnow()
            else:
                db.add(SyncState(
                    company_name      = company_name,
                    voucher_type      = voucher_type,
                    last_alter_id     = 0,
                    is_initial_done   = False,
                    last_synced_month = month_str,
                    last_sync_time    = datetime.utcnow(),
                ))

            db.commit()
        except Exception:
            db.rollback()
            logger.exception(f"[{company_name}][{voucher_type}] Failed to mark chunk {month_str} done")
            raise
        finally:
            db.close()


def _advance_alter_id_from_xml(
    xml:          bytes,
    company_name: str,
    voucher_type: str,
    engine,
    lock:         threading.Lock,
):
    """
    Advance last_alter_id from a deleted-only CDC batch where the parser
    returned [] but valid XML was received.
    """
    try:
        xml_str = sanitize_xml_content(xml)
        if not xml_str:
            return

        root = ET.fromstring(xml_str.encode('utf-8'))
        ids  = [
            int(e.text.strip())
            for e in root.iter('ALTERID')
            if e.text and e.text.strip().lstrip('-').isdigit()
        ]
        if not ids:
            return

        new_max = max(ids)
        with lock:
            update_sync_state(
                company_name    = company_name,
                voucher_type    = voucher_type,
                last_alter_id   = new_max,
                engine          = engine,
                is_initial_done = True,
            )
        logger.info(
            f"[{company_name}][{voucher_type}] "
            f"alter_id advanced to {new_max} from deleted-only CDC batch"
        )

    except Exception as e:
        logger.warning(
            f"[{company_name}][{voucher_type}] "
            f"_advance_alter_id_from_xml failed (non-fatal): {e}"
        )


# ── Phase 3 — GUID Reconciliation ────────────────────────────────────────────

def _reconcile_deleted_vouchers(
    company_name: str,
    config:       dict,
    tally:        TallyConnector,
    engine,
    from_date:    str,
    to_date:      str,
):
    voucher_type = config['voucher_type']
    kind         = config['kind']
    guid_fetch   = config['guid_fetch']
    fetch_fn     = getattr(tally, guid_fetch)

    if kind == 'inventory':
        model_class = INVENTORY_MODEL_MAP[voucher_type]
    else:
        model_class = LEDGER_MODEL_MAP[voucher_type]

    total_deleted = 0

    logger.info(
        f"[{company_name}][{voucher_type}] "
        f"Phase 3: GUID reconciliation | {from_date}→{to_date} | "
        f"{SNAPSHOT_CHUNK_MONTHS}-month chunks"
    )

    for chunk_from, chunk_to, month_str in _generate_chunks(from_date, to_date):
        try:
            if not _tally_semaphore_acquire(voucher_type):
                logger.warning(
                    f"[{company_name}][{voucher_type}] "
                    f"Phase 3 chunk {month_str}: semaphore timeout — skipping chunk"
                )
                continue

            try:
                xml = fetch_fn(
                    company_name = company_name,
                    from_date    = chunk_from,
                    to_date      = chunk_to,
                )
            finally:
                _TALLY_SEMAPHORE.release()

            if not xml:
                logger.debug(
                    f"[{company_name}][{voucher_type}] "
                    f"Phase 3 chunk {month_str}: no response from Tally — skipping (safe)"
                )
                continue

            tally_guids = parse_guids(xml)

            if tally_guids is None:
                logger.warning(
                    f"[{company_name}][{voucher_type}] "
                    f"Phase 3 chunk {month_str}: parse_guids failed — skipping chunk (safe)"
                )
                continue

            if len(tally_guids) == 0:
                logger.info(
                    f"[{company_name}][{voucher_type}] "
                    f"Phase 3 chunk {month_str}: Tally returned 0 GUIDs "
                    f"— deleting all orphaned DB rows in this window"
                )

            deleted_count = reconcile_deleted_by_guids(
                company_name = company_name,
                model_class  = model_class,
                tally_guids  = tally_guids,
                from_date    = chunk_from,
                to_date      = chunk_to,
                engine       = engine,
            )
            total_deleted += deleted_count

            if deleted_count > 0:
                logger.info(
                    f"[{company_name}][{voucher_type}] "
                    f"Phase 3 chunk {month_str}: removed {deleted_count} missed deletes ✓"
                )
            else:
                logger.debug(
                    f"[{company_name}][{voucher_type}] "
                    f"Phase 3 chunk {month_str}: no missed deletes ✓"
                )

        except Exception:
            logger.exception(
                f"[{company_name}][{voucher_type}] "
                f"Phase 3 chunk {month_str}: failed (non-fatal, continuing)"
            )

    logger.info(
        f"[{company_name}][{voucher_type}] "
        f"Phase 3: done | total removed={total_deleted}"
    )


# ── Sub-sync functions ────────────────────────────────────────────────────────

# ── Master GUID Reconciliation (items + ledgers) ─────────────────────────────

def _reconcile_deleted_masters(
    company_name: str,
    master_type:  str,
    tally:        TallyConnector,
    engine,
):
    fetch_fn_name = 'fetch_item_guids' if master_type == 'items' else 'fetch_ledger_guids'
    fetch_fn      = getattr(tally, fetch_fn_name)

    logger.info(f"[{company_name}][{master_type}] Master GUID reconciliation starting")

    if not _tally_semaphore_acquire(master_type):
        logger.warning(f"[{company_name}][{master_type}] Master GUID reconciliation: semaphore timeout — skipping")
        return

    try:
        xml = fetch_fn(company_name=company_name)
    finally:
        _TALLY_SEMAPHORE.release()

    if not xml:
        logger.warning(f"[{company_name}][{master_type}] Master GUID reconciliation: no response — skipping (safe)")
        return

    tally_guids = parse_guids(xml)

    if not tally_guids:
        logger.warning(f"[{company_name}][{master_type}] Master GUID reconciliation: 0 GUIDs — skipping (safe)")
        return

    deleted = reconcile_deleted_masters_in_db(
        company_name = company_name,
        master_type  = master_type,
        tally_guids  = tally_guids,
        engine       = engine,
    )
    logger.info(f"[{company_name}][{master_type}] Master GUID reconciliation done | hard-deleted={deleted}")


def _sync_trial_balance(
    company_name:     str,
    tally:            TallyConnector,
    engine,
    from_date:        str,
    to_date:          str,
    progress_cb=None,
    material_centre:  str = '',
    default_currency: str = 'INR',
):
    logger.info(f"[{company_name}] Syncing Trial Balance | {from_date} -> {to_date}")
    try:
        if progress_cb:
            progress_cb(0.0, "Fetching trial balance from Tally...")

        if not _tally_semaphore_acquire():
            raise RuntimeError("Tally semaphore timeout — Tally may be hung")
        try:
            xml = tally.fetch_trial_balance(
                company_name=company_name, from_date=from_date, to_date=to_date
            )
        finally:
            _TALLY_SEMAPHORE.release()

        if not xml:
            logger.warning(f"[{company_name}] No trial balance data from Tally")
            return

        rows = parse_trial_balance(
            xml, company_name, from_date, to_date,
            material_centre=material_centre,
            default_currency=default_currency,
        )
        if not rows:
            logger.warning(f"[{company_name}] Trial balance parsed 0 rows")
            return

        upsert_trial_balance(rows, engine)

        max_alter_id = _get_max_alter_id(rows)
        lock = _get_company_lock(company_name)
        with lock:
            update_sync_state(company_name, 'trial_balance', max_alter_id, engine)

        logger.info(
            f"[{company_name}] Trial Balance done | "
            f"rows={len(rows)} | max_alter_id={max_alter_id}"
        )

    except Exception:
        logger.exception(f"[{company_name}] Trial Balance sync failed")


def _sync_outstanding(
    company_name:     str,
    tally:            TallyConnector,
    engine,
    from_date:        str,
    to_date:          str,
    progress_cb=None,
    material_centre:  str = '',
    default_currency: str = 'INR',
):
    logger.info(f"[{company_name}] Syncing Outstanding Debtors | {from_date} -> {to_date}")
    try:
        if progress_cb:
            progress_cb(0.0, "Fetching outstanding debtors from Tally...")

        if not _tally_semaphore_acquire('outstanding'):
            raise RuntimeError("Tally semaphore timeout — Tally may be hung")
        try:
            xml = tally.fetch_outstanding(
                company_name=company_name, from_date=from_date, to_date=to_date
            )
        finally:
            _TALLY_SEMAPHORE.release()

        if not xml:
            logger.warning(f"[{company_name}] No outstanding debtors data from Tally")
            return

        rows = parse_outstanding(
            xml, company_name,
            material_centre=material_centre,
            default_currency=default_currency,
        )
        if not rows:
            logger.warning(f"[{company_name}] Outstanding debtors parsed 0 rows")
            return

        upsert_outstanding(rows, engine)

        lock = _get_company_lock(company_name)
        with lock:
            update_sync_state(
                company_name    = company_name,
                voucher_type    = 'outstanding',
                last_alter_id   = 0,
                engine          = engine,
                is_initial_done = True,
            )

        logger.info(
            f"[{company_name}] Outstanding Debtors done | rows={len(rows)}"
        )

    except Exception:
        logger.exception(f"[{company_name}] Outstanding Debtors sync failed")


def _sync_items(
    company_name:     str,
    tally:            TallyConnector,
    engine,
    progress_cb=None,
    material_centre:  str = '',
    default_currency: str = 'INR',
):
    logger.info(f"[{company_name}] Syncing Items (StockItem master)")
    lock = _get_company_lock(company_name)
    try:
        if progress_cb:
            progress_cb(0.0, "Fetching items from Tally...")
        state           = get_sync_state(company_name, 'items', engine)
        is_initial_done = state.is_initial_done if state else False
        last_alter_id   = state.last_alter_id   if state else 0

        if is_initial_done:
            logger.info(f"[{company_name}][items] CDC | last_alter_id={last_alter_id}")
            if not _tally_semaphore_acquire():
                raise RuntimeError("Tally semaphore timeout — Tally may be hung")
            try:
                xml = tally.fetch_items_cdc(
                    company_name=company_name, last_alter_id=last_alter_id
                )
            finally:
                _TALLY_SEMAPHORE.release()
            if not xml:
                logger.info(f"[{company_name}][items] CDC: no new/changed items")
                _reconcile_deleted_masters(company_name, 'items', tally, engine)
                return

            rows = parse_items(xml, company_name, material_centre=material_centre, default_currency=default_currency)
            if not rows:
                logger.info(
                    f"[{company_name}][items] CDC: 0 rows "
                    f"(nothing changed since AlterID {last_alter_id})"
                )
                _advance_alter_id_from_xml(xml, company_name, 'items', engine, lock)
                _reconcile_deleted_masters(company_name, 'items', tally, engine)
                return

            new_max = _get_max_alter_id(rows)
            upsert_items(rows, engine)
            with lock:
                update_sync_state(company_name, 'items', new_max, engine, is_initial_done=True)
            logger.info(
                f"[{company_name}][items] CDC done | rows={len(rows)} | new max_alter_id={new_max}"
            )
            _reconcile_deleted_masters(company_name, 'items', tally, engine)
            return

        logger.info(f"[{company_name}][items] SNAPSHOT — fetching all stock items")
        if not _tally_semaphore_acquire():
            raise RuntimeError("Tally semaphore timeout — Tally may be hung")
        try:
            xml = tally.fetch_items(company_name=company_name)
        finally:
            _TALLY_SEMAPHORE.release()
        if not xml:
            logger.warning(f"[{company_name}][items] No item data from Tally")
            return

        rows = parse_items(xml, company_name, material_centre=material_centre, default_currency=default_currency)
        if not rows:
            logger.warning(f"[{company_name}][items] Snapshot parsed 0 rows")
            return

        max_alter_id = _get_max_alter_id(rows)
        upsert_items(rows, engine)
        with lock:
            update_sync_state(company_name, 'items', max_alter_id, engine, is_initial_done=True)
        logger.info(
            f"[{company_name}][items] Snapshot done | "
            f"rows={len(rows)} | max_alter_id={max_alter_id} | CDC enabled from next run"
        )

    except Exception:
        logger.exception(f"[{company_name}] Item sync failed")


def _sync_ledgers(
    company_name:     str,
    tally:            TallyConnector,
    engine,
    progress_cb=None,
    material_centre:  str = '',
    default_currency: str = 'INR',
):
    logger.info(f"[{company_name}] Syncing Ledgers")
    lock = _get_company_lock(company_name)
    try:
        if progress_cb:
            progress_cb(0.0, "Fetching ledgers from Tally...")
        state           = get_sync_state(company_name, 'ledger', engine)
        is_initial_done = state.is_initial_done if state else False
        last_alter_id   = state.last_alter_id   if state else 0

        if is_initial_done:
            logger.info(f"[{company_name}][ledger] CDC | last_alter_id={last_alter_id}")
            if not _tally_semaphore_acquire():
                raise RuntimeError("Tally semaphore timeout — Tally may be hung")
            try:
                xml = tally.fetch_ledger_cdc(
                    company_name=company_name, last_alter_id=last_alter_id
                )
            finally:
                _TALLY_SEMAPHORE.release()
            if not xml:
                logger.info(f"[{company_name}][ledger] CDC: no new/changed ledgers")
                _reconcile_deleted_masters(company_name, 'ledger', tally, engine)
                return

            rows = parse_ledgers(xml, company_name, material_centre=material_centre, default_currency=default_currency)
            if not rows:
                logger.info(
                    f"[{company_name}][ledger] CDC: 0 rows "
                    f"(nothing changed since AlterID {last_alter_id})"
                )
                _advance_alter_id_from_xml(xml, company_name, 'ledger', engine, lock)
                _reconcile_deleted_masters(company_name, 'ledger', tally, engine)
                return

            upsert_ledgers(rows, engine)
            new_max = _get_max_alter_id(rows)
            with lock:
                update_sync_state(company_name, 'ledger', new_max, engine, is_initial_done=True)
            logger.info(
                f"[{company_name}][ledger] CDC done | rows={len(rows)} | new max_alter_id={new_max}"
            )
            _reconcile_deleted_masters(company_name, 'ledger', tally, engine)
            return

        logger.info(f"[{company_name}][ledger] SNAPSHOT — fetching all ledgers")
        if not _tally_semaphore_acquire():
            raise RuntimeError("Tally semaphore timeout — Tally may be hung")
        try:
            xml = tally.fetch_ledgers(company_name=company_name)
        finally:
            _TALLY_SEMAPHORE.release()
        if not xml:
            logger.warning(f"[{company_name}][ledger] No ledger data from Tally")
            return

        rows = parse_ledgers(xml, company_name, material_centre=material_centre, default_currency=default_currency)
        if not rows:
            logger.warning(f"[{company_name}][ledger] Snapshot parsed 0 rows")
            return

        upsert_ledgers(rows, engine)
        max_alter_id = _get_max_alter_id(rows)
        with lock:
            update_sync_state(company_name, 'ledger', max_alter_id, engine, is_initial_done=True)
        logger.info(
            f"[{company_name}][ledger] Snapshot done | "
            f"rows={len(rows)} | max_alter_id={max_alter_id} | CDC enabled from next run"
        )

    except Exception:
        logger.exception(f"[{company_name}] Ledger sync failed")


def _sync_voucher(
    company_name:     str,
    config:           dict,
    tally:            TallyConnector,
    engine,
    from_date:        str,
    to_date:          str,
    full_from:        str,
    progress_cb=None,
    material_centre:  str = '',
    default_currency: str = 'INR',
):
    voucher_type     = config['voucher_type']
    snapshot_fetch   = config['snapshot_fetch']
    cdc_fetch        = config['cdc_fetch']
    parser           = config['parser']
    upsert           = config['upsert']
    parser_type_name = config['parser_type_name']
    kind             = config['kind']
    allowed_types    = config.get('allowed_types')

    lock = _get_company_lock(company_name)
    logger.info(f"[{company_name}][{voucher_type}] Starting")

    try:
        state             = get_sync_state(company_name, voucher_type, engine)
        is_initial_done   = state.is_initial_done   if state else False
        last_alter_id     = state.last_alter_id     if state else 0
        last_synced_month = state.last_synced_month if state else None

        # ── PHASE 2: CDC mode ─────────────────────────────────────────────────
        if is_initial_done:
            logger.info(f"[{company_name}][{voucher_type}] Phase 2: CDC | last_alter_id={last_alter_id}")

            t0       = datetime.now()
            fetch_fn = getattr(tally, cdc_fetch)
            if not _tally_semaphore_acquire():
                raise RuntimeError("Tally semaphore timeout — Tally may be hung")
            try:
                xml = fetch_fn(company_name=company_name, last_alter_id=last_alter_id)
            finally:
                _TALLY_SEMAPHORE.release()
            fetch_ms = int((datetime.now() - t0).total_seconds() * 1000)

            if not xml:
                logger.warning(
                    f"[{company_name}][{voucher_type}] CDC: no response from Tally ({fetch_ms}ms)"
                )
                _reconcile_deleted_vouchers(company_name, config, tally, engine, full_from, to_date)
                return

            rows = parser(
                xml, company_name, parser_type_name,
                allowed_types=allowed_types,
                material_centre=material_centre,
                default_currency=default_currency,
            )

            if not rows:
                logger.info(
                    f"[{company_name}][{voucher_type}] CDC: 0 active rows "
                    f"(possible delete-only batch) | advancing alter_id from XML"
                )
                _advance_alter_id_from_xml(xml, company_name, voucher_type, engine, lock)
                _reconcile_deleted_vouchers(company_name, config, tally, engine, full_from, to_date)
                return

            new_max = _get_max_alter_id(rows)
            t1 = datetime.now()
            upsert(rows, engine)
            upsert_ms = int((datetime.now() - t1).total_seconds() * 1000)
            with lock:
                update_sync_state(company_name, voucher_type, new_max, engine, is_initial_done=True)
            logger.info(
                f"[{company_name}][{voucher_type}] Phase 2: CDC done | "
                f"rows={len(rows)} | new max_alter_id={new_max} | "
                f"fetch={fetch_ms}ms upsert={upsert_ms}ms"
            )

            _reconcile_deleted_vouchers(company_name, config, tally, engine, full_from, to_date)
            return

        # ── PHASE 1: Snapshot mode ────────────────────────────────────────────
        logger.info(
            f"[{company_name}][{voucher_type}] Phase 1: SNAPSHOT "
            f"({SNAPSHOT_CHUNK_MONTHS}-month chunks) | {from_date} → {to_date}"
        )

        if kind == 'inventory':
            model_class = INVENTORY_MODEL_MAP[voucher_type]
            upsert_fn   = _upsert_inventory_voucher_in_session
        else:
            model_class = LEDGER_MODEL_MAP[voucher_type]
            upsert_fn   = _upsert_ledger_voucher_in_session

        fetch_fn      = getattr(tally, snapshot_fetch)
        total_rows    = 0
        chunks_done   = 0
        all_alter_ids = [last_alter_id] if last_alter_id > 0 else []

        _all_chunks  = list(_generate_chunks(from_date, to_date))
        total_chunks = max(len([c for c in _all_chunks
            if not (last_synced_month and c[2] < last_synced_month)]), 1)

        for chunk_from, chunk_to, month_str in _generate_chunks(from_date, to_date):

            if last_synced_month and month_str <= last_synced_month:
                logger.debug(
                    f"[{company_name}][{voucher_type}] Skipping already-done chunk {month_str}"
                )
                continue

            if progress_cb:
                progress_cb(
                    (chunks_done / max(total_chunks, 1)) * 100,
                    f"Syncing {parser_type_name} — {month_str}",
                )

            logger.info(
                f"[{company_name}][{voucher_type}] Chunk {month_str} | {chunk_from} → {chunk_to}"
            )

            if not _tally_semaphore_acquire():
                raise RuntimeError("Tally semaphore timeout — Tally may be hung")
            try:
                xml = fetch_fn(company_name=company_name, from_date=chunk_from, to_date=chunk_to)
            finally:
                _TALLY_SEMAPHORE.release()
            if not xml:
                logger.info(
                    f"[{company_name}][{voucher_type}] Chunk {month_str}: empty, advancing"
                )
                _mark_chunk_done(company_name, voucher_type, month_str, engine)
                chunks_done += 1
                continue

            rows = parser(
                xml, company_name, parser_type_name,
                allowed_types=allowed_types,
                material_centre=material_centre,
                default_currency=default_currency,
            )

            if not rows:
                logger.info(
                    f"[{company_name}][{voucher_type}] Chunk {month_str}: 0 rows, advancing"
                )
                _mark_chunk_done(company_name, voucher_type, month_str, engine)
                chunks_done += 1
                continue

            chunk_max = max((int(r.get('alter_id', 0)) for r in rows), default=0)
            try:
                upsert_and_advance_month(
                    rows               = rows,
                    model_class        = model_class,
                    upsert_fn          = upsert_fn,
                    company_name       = company_name,
                    voucher_type       = voucher_type,
                    month_str          = month_str,
                    engine             = engine,
                    chunk_max_alter_id = chunk_max,
                )

                all_alter_ids.extend(int(r.get('alter_id', 0)) for r in rows)
                total_rows  += len(rows)
                chunks_done += 1
            finally:
                del xml
                del rows
                gc.collect()

        final_alter_id = max(all_alter_ids) if all_alter_ids else 0
        with lock:
            update_sync_state(
                company_name      = company_name,
                voucher_type      = voucher_type,
                last_alter_id     = final_alter_id,
                engine            = engine,
                last_synced_month = to_date[:6],
                is_initial_done   = True,
            )

        logger.info(
            f"[{company_name}][{voucher_type}] Phase 1: Snapshot complete | "
            f"chunks={chunks_done} | total_rows={total_rows} | max_alter_id={final_alter_id}"
        )

    except Exception:
        logger.exception(
            f"[{company_name}][{voucher_type}] Sync failed — "
            f"will resume from last committed chunk on next run"
        )


# ── Public API ────────────────────────────────────────────────────────────────

def sync_company(
    company:               dict,
    tally:                 TallyConnector,
    engine,
    to_date:               str,
    manual_from_date:      str  = None,
    parallel_company_mode: bool = False,
    voucher_selection:     set  = None,
):
    """
    Sync a single company — all 3 phases.

    Phase 1 (Snapshot): runs on first sync, chunked 3-month fetches
    Phase 2 (CDC):      runs on every subsequent sync, alter_id gated
    Phase 3 (Reconcile): runs after every CDC sync, full-history GUID diff

    default_currency is read from the company dict (set by sync_controller
    from CompanyState.default_currency, which is loaded from DB).
    It flows to every parser so plain amounts with no currency symbol are
    attributed to the correct base currency for this company.
    """
    comp_name        = company.get('name', '').strip()
    mat_centre       = company.get('material_centre', '') or ''
    default_currency = company.get('default_currency', 'INR') or 'INR'
    from_date        = manual_from_date if manual_from_date else _resolve_from_date(company)
    inner_workers    = 1 if parallel_company_mode else VOUCHER_WORKERS

    sel = set(voucher_selection) if voucher_selection is not None else None

    def _selected(name: str) -> bool:
        return sel is None or name in sel

    logger.info('=' * 60)
    logger.info(f'Syncing company  : {comp_name}')
    logger.info(f'Date range       : {from_date} -> {to_date}')
    logger.info(f'Default currency : {default_currency}')
    logger.info(f'Chunk size       : {SNAPSHOT_CHUNK_MONTHS} months per API call')
    logger.info(f'Voucher workers  : {inner_workers} (parallel_company_mode={parallel_company_mode})')
    logger.info(f'Voucher filter   : {sorted(sel) if sel is not None else "all"}')
    logger.info('=' * 60)

    start_time = datetime.now()

    try:
        if _selected('ledger'):
            _sync_ledgers(comp_name, tally, engine, material_centre=mat_centre, default_currency=default_currency)
        else:
            logger.info(f"[{comp_name}] Skipping ledgers (not selected)")

        if _selected('items'):
            _sync_items(comp_name, tally, engine, material_centre=mat_centre, default_currency=default_currency)
        else:
            logger.info(f"[{comp_name}] Skipping items (not selected)")

        if _selected('trial_balance'):
            _sync_trial_balance(comp_name, tally, engine, from_date, to_date, material_centre=mat_centre, default_currency=default_currency)
        else:
            logger.info(f"[{comp_name}] Skipping trial_balance (not selected)")

        if _selected('outstanding'):
            _sync_outstanding(comp_name, tally, engine, from_date, to_date, material_centre=mat_centre, default_currency=default_currency)
        else:
            logger.info(f"[{comp_name}] Skipping outstanding (not selected)")

        active_configs = [
            config for config in VOUCHER_CONFIG
            if _selected(config['voucher_type'])
        ]

        skipped = [c['voucher_type'] for c in VOUCHER_CONFIG if c not in active_configs]
        if skipped:
            logger.info(f"[{comp_name}] Skipping voucher types: {skipped}")

        if active_configs:
            logger.info(f"[{comp_name}] Launching {len(active_configs)} voucher syncs ...")
            full_from = _resolve_from_date(company)
            with ThreadPoolExecutor(max_workers=inner_workers) as executor:
                futures = {
                    executor.submit(
                        _sync_voucher,
                        company_name     = comp_name,
                        config           = config,
                        tally            = tally,
                        engine           = engine,
                        from_date        = from_date,
                        to_date          = to_date,
                        full_from        = full_from,
                        material_centre  = mat_centre,
                        default_currency = default_currency,
                    ): config['voucher_type']
                    for config in active_configs
                }

                for future in as_completed(futures):
                    vt = futures[future]
                    try:
                        future.result()
                        logger.info(f"[{comp_name}][{vt}] Thread finished OK")
                    except Exception:
                        logger.error(
                            f"[{comp_name}][{vt}] Thread raised an exception (other types continue)"
                        )
        else:
            logger.info(f"[{comp_name}] No voucher types to run after filtering")

        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{comp_name}] Sync completed in {elapsed:.1f}s")

    finally:
        cleanup_sync_state(comp_name)


def sync_all_companies(
    companies:        list,
    tally:            TallyConnector,
    engine,
    to_date:          str,
    manual_from_date: str = None,
):
    """Sequential company sync."""
    if not companies:
        logger.warning("sync_all_companies: empty company list")
        return

    invalid_names = {'', 'N/A', 'NA', 'NONE'}
    valid   = [c for c in companies if c.get('name', '').strip().upper() not in invalid_names]
    skipped = len(companies) - len(valid)
    logger.info(f"Syncing {len(valid)} companies sequentially (skipped {skipped} invalid entries)")

    for company in valid:
        sync_company(
            company               = company,
            tally                 = tally,
            engine                = engine,
            to_date               = to_date,
            manual_from_date      = manual_from_date,
            parallel_company_mode = False,
        )


def sync_all_companies_parallel(
    companies:           list,
    tally:               TallyConnector,
    engine,
    to_date:             str,
    manual_from_date:    str = None,
    max_company_workers: int = 3,
):
    """
    Parallel company sync.
    All HTTP calls serialised via _TALLY_SEMAPHORE regardless of parallelism.
    """
    if not companies:
        logger.warning("sync_all_companies_parallel: empty company list")
        return

    invalid_names = {'', 'N/A', 'NA', 'NONE'}
    valid   = [c for c in companies if c.get('name', '').strip().upper() not in invalid_names]
    skipped = len(companies) - len(valid)
    logger.info(
        f"Syncing {len(valid)} companies in parallel "
        f"(workers={max_company_workers}, skipped {skipped} invalid)"
    )

    with ThreadPoolExecutor(max_workers=max_company_workers) as executor:
        futures = {
            executor.submit(
                sync_company,
                company               = company,
                tally                 = tally,
                engine                = engine,
                to_date               = to_date,
                manual_from_date      = manual_from_date,
                parallel_company_mode = True,
            ): company.get('name', '?')
            for company in valid
        }

        for future in as_completed(futures):
            name = futures[future]
            try:
                future.result()
                logger.info(f"Company '{name}' sync thread finished ✓")
            except Exception:
                logger.error(f"Company '{name}' sync thread raised an exception")


# ── Deep Reconcile ────────────────────────────────────────────────────────────

def deep_reconcile_company(
    company:     dict,
    tally:       TallyConnector,
    engine,
    to_date:     str,
    progress_cb  = None,
) -> dict:
    """
    Re-run Phase 3 GUID reconciliation for the full company history on demand.
    Exposed for the UI "Deep Reconcile" button.
    Returns dict: { voucher_type: deleted_count, ... }
    """
    comp_name = company.get('name', '').strip()
    from_date = _resolve_from_date(company)
    results   = {}
    total     = len(VOUCHER_CONFIG)

    logger.info('=' * 60)
    logger.info(f'[{comp_name}] DEEP RECONCILE START')
    logger.info(f'  Range : {from_date} → {to_date} | {SNAPSHOT_CHUNK_MONTHS}-month chunks')
    logger.info(f'  Voucher types : {total}')
    logger.info('=' * 60)

    for idx, config in enumerate(VOUCHER_CONFIG):
        vt = config['voucher_type']

        if progress_cb:
            progress_cb(
                (idx / total) * 100,
                f"Deep reconcile: {vt} ({idx + 1}/{total})...",
            )

        state = get_sync_state(comp_name, vt, engine)
        if not state or not state.is_initial_done:
            logger.info(f"[{comp_name}][{vt}] Deep reconcile: skipped (snapshot not done)")
            results[vt] = 0
            continue

        before = _count_active_rows(comp_name, config, engine, from_date, to_date)

        _reconcile_deleted_vouchers(
            company_name = comp_name,
            config       = config,
            tally        = tally,
            engine       = engine,
            from_date    = from_date,
            to_date      = to_date,
        )

        after        = _count_active_rows(comp_name, config, engine, from_date, to_date)
        results[vt]  = max(0, before - after)

    if progress_cb:
        progress_cb(100.0, "Deep reconcile complete")

    total_deleted = sum(results.values())
    logger.info(
        f"[{comp_name}] DEEP RECONCILE DONE | "
        f"total deleted={total_deleted} | by type={results}"
    )
    return results


def _count_active_rows(
    company_name: str,
    config:       dict,
    engine,
    from_date:    str,
    to_date:      str,
) -> int:
    """Count active (is_deleted=No) DB rows in the given date range."""
    try:
        kind = config['kind']
        vt   = config['voucher_type']
        model_class = (INVENTORY_MODEL_MAP if kind == 'inventory' else LEDGER_MODEL_MAP)[vt]

        fd = datetime.strptime(from_date, '%Y%m%d').date()
        td = datetime.strptime(to_date,   '%Y%m%d').date()

        db = _get_session(engine)
        try:
            return db.query(model_class).filter(
                model_class.company_name == company_name,
                model_class.date         >= fd,
                model_class.date         <= td,
                model_class.is_deleted   == 'No',
            ).count()
        finally:
            db.close()
    except Exception:
        return 0
