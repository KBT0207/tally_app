from datetime import datetime, date
from calendar import monthrange
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from logging_config import logger
from database.models.sync_state import SyncState

from services.tally_connector import TallyConnector
from services.data_processor import (
    parse_inventory_voucher,
    parse_ledger_voucher,
    parse_ledgers,
    parse_items,
    parse_trial_balance,
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
    INVENTORY_MODEL_MAP,
    LEDGER_MODEL_MAP,
    _upsert_inventory_voucher_in_session,
    _upsert_ledger_voucher_in_session,
    _get_session,
)

# ── Tuning constants ──────────────────────────────────────────────────────────
SNAPSHOT_CHUNK_MONTHS = 3   # months fetched per Tally API call during snapshot
VOUCHER_WORKERS       = 2   # parallel threads for voucher sync within ONE company

# ── Global Tally request semaphore ───────────────────────────────────────────
# Tally Prime is single-user/single-connection — even when multiple companies
# run in parallel, all HTTP requests must be serialised so Tally does not get
# overwhelmed or return garbled XML.  A semaphore with limit=1 achieves this
# without removing the benefit of parallel DB writes between requests.
#
# Increase to 2-3 ONLY if you are on Tally Server (multi-user edition) AND
# have verified it handles concurrent connections reliably.
_TALLY_SEMAPHORE = threading.Semaphore(1)

# Max seconds to wait for the semaphore before giving up (prevents deadlock
# when Tally hangs indefinitely on a request).
_TALLY_SEMAPHORE_TIMEOUT = 120


def _tally_semaphore_acquire(voucher_type: str = "") -> bool:
    """
    Try to acquire _TALLY_SEMAPHORE within _TALLY_SEMAPHORE_TIMEOUT seconds.
    Returns True on success, False on timeout.
    """
    acquired = _TALLY_SEMAPHORE.acquire(timeout=_TALLY_SEMAPHORE_TIMEOUT)
    if not acquired:
        logger.error(
            f"[sync_service] Tally semaphore timeout after {_TALLY_SEMAPHORE_TIMEOUT}s"
            + (f" [{voucher_type}]" if voucher_type else "")
            + " — skipping to unblock other syncs"
        )
    return acquired

# ── Per-company SyncState write lock ─────────────────────────────────────────
# Prevents two threads (voucher workers within the same company) from updating
# the same SyncState row simultaneously, which would cause a lost-update
# on last_synced_month / last_alter_id.
_SYNC_STATE_LOCKS: dict[str, threading.Lock] = {}
_SYNC_STATE_LOCKS_MUTEX = threading.Lock()

def _get_company_lock(company_name: str) -> threading.Lock:
    """Return (creating if needed) a per-company threading.Lock."""
    with _SYNC_STATE_LOCKS_MUTEX:
        if company_name not in _SYNC_STATE_LOCKS:
            _SYNC_STATE_LOCKS[company_name] = threading.Lock()
        return _SYNC_STATE_LOCKS[company_name]

# ── Voucher configuration table ───────────────────────────────────────────────
VOUCHER_CONFIG = [
    {
        'voucher_type'    : 'sales',
        'snapshot_fetch'  : 'fetch_sales',
        'cdc_fetch'       : 'fetch_sales_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_sales_vouchers,
        'parser_type_name': 'Sales Vouchers',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'purchase',
        'snapshot_fetch'  : 'fetch_purchase',
        'cdc_fetch'       : 'fetch_purchase_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_purchase_vouchers,
        'parser_type_name': 'Purchase Vouchers',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'credit_note',
        'snapshot_fetch'  : 'fetch_credit_note',
        'cdc_fetch'       : 'fetch_credit_note_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_credit_notes,
        'parser_type_name': 'Credit Note',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'debit_note',
        'snapshot_fetch'  : 'fetch_debit_note',
        'cdc_fetch'       : 'fetch_debit_note_cdc',
        'parser'          : parse_inventory_voucher,
        'upsert'          : upsert_debit_notes,
        'parser_type_name': 'Debit Note',
        'kind'            : 'inventory',
    },
    {
        'voucher_type'    : 'receipt',
        'snapshot_fetch'  : 'fetch_receipt',
        'cdc_fetch'       : 'fetch_receipt_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_receipt_vouchers,
        'parser_type_name': 'Receipt Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'payment',
        'snapshot_fetch'  : 'fetch_payment',
        'cdc_fetch'       : 'fetch_payment_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_payment_vouchers,
        'parser_type_name': 'Payment Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'journal',
        'snapshot_fetch'  : 'fetch_journal',
        'cdc_fetch'       : 'fetch_journal_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_journal_vouchers,
        'parser_type_name': 'Journal Vouchers',
        'kind'            : 'ledger',
    },
    {
        'voucher_type'    : 'contra',
        'snapshot_fetch'  : 'fetch_contra',
        'cdc_fetch'       : 'fetch_contra_cdc',
        'parser'          : parse_ledger_voucher,
        'upsert'          : upsert_contra_vouchers,
        'parser_type_name': 'Contra Vouchers',
        'kind'            : 'ledger',
    },
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_max_alter_id(rows: list) -> int:
    if not rows:
        return 0
    return max(int(r.get('alter_id', 0)) for r in rows)


def _resolve_from_date(company: dict) -> str:
    """Return YYYYMMDD start date for a company; fall back to a safe default."""
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

        chunk_end = min(date(end_year, end_month, last_day), end)
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
    """Persist progress for a chunk that returned no data so we can skip it on restart."""
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


# ── Sub-sync functions ────────────────────────────────────────────────────────

def _sync_trial_balance(
    company_name: str,
    tally:        TallyConnector,
    engine,
    from_date:    str,
    to_date:      str,
    progress_cb=None,
):
    logger.info(f"[{company_name}] Syncing Trial Balance | {from_date} -> {to_date}")
    try:
        if progress_cb:
            progress_cb(0.0, "Fetching trial balance from Tally...")
        state          = get_sync_state(company_name, 'trial_balance', engine)
        saved_alter_id = state.last_alter_id if state else 0

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

        rows = parse_trial_balance(xml, company_name, from_date, to_date)
        if not rows:
            logger.warning(f"[{company_name}] Trial balance parsed 0 rows")
            return

        max_alter_id = _get_max_alter_id(rows)

        if max_alter_id == saved_alter_id and saved_alter_id > 0:
            logger.info(
                f"[{company_name}] Trial Balance SKIPPED — "
                f"max_alter_id unchanged ({max_alter_id}), no changes in Tally"
            )
            return

        upsert_trial_balance(rows, engine)

        lock = _get_company_lock(company_name)
        with lock:
            update_sync_state(company_name, 'trial_balance', max_alter_id, engine)

        logger.info(
            f"[{company_name}] Trial Balance done | "
            f"rows={len(rows)} | max_alter_id={max_alter_id} (was {saved_alter_id})"
        )

    except Exception:
        logger.exception(f"[{company_name}] Trial Balance sync failed")


def _sync_items(company_name: str, tally: TallyConnector, engine, progress_cb=None):
    """
    Sync the StockItem master for *company_name*.

    • First run  → full snapshot  (no date range needed for masters)
    • Subsequent → CDC using stored alter_id
    """
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
                return

            rows = parse_items(xml, company_name)
            if not rows:
                logger.info(
                    f"[{company_name}][items] CDC: 0 rows "
                    f"(nothing changed since AlterID {last_alter_id})"
                )
                return

            upsert_items(rows, engine)
            new_max = _get_max_alter_id(rows)
            with lock:
                update_sync_state(company_name, 'items', new_max, engine, is_initial_done=True)
            logger.info(
                f"[{company_name}][items] CDC done | rows={len(rows)} | new max_alter_id={new_max}"
            )
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

        rows = parse_items(xml, company_name)
        if not rows:
            logger.warning(f"[{company_name}][items] Snapshot parsed 0 rows")
            return

        upsert_items(rows, engine)
        max_alter_id = _get_max_alter_id(rows)
        with lock:
            update_sync_state(company_name, 'items', max_alter_id, engine, is_initial_done=True)
        logger.info(
            f"[{company_name}][items] Snapshot done | "
            f"rows={len(rows)} | max_alter_id={max_alter_id} | CDC enabled from next run"
        )

    except Exception:
        logger.exception(f"[{company_name}] Item sync failed")


def _sync_ledgers(company_name: str, tally: TallyConnector, engine, progress_cb=None):
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
                return

            rows = parse_ledgers(xml, company_name)
            if not rows:
                logger.info(
                    f"[{company_name}][ledger] CDC: 0 rows "
                    f"(nothing changed since AlterID {last_alter_id})"
                )
                return

            upsert_ledgers(rows, engine)
            new_max = _get_max_alter_id(rows)
            with lock:
                update_sync_state(company_name, 'ledger', new_max, engine, is_initial_done=True)
            logger.info(
                f"[{company_name}][ledger] CDC done | rows={len(rows)} | new max_alter_id={new_max}"
            )
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

        rows = parse_ledgers(xml, company_name)
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
    company_name: str,
    config:       dict,
    tally:        TallyConnector,
    engine,
    from_date:    str,
    to_date:      str,
    progress_cb=None,
):
    voucher_type     = config['voucher_type']
    snapshot_fetch   = config['snapshot_fetch']
    cdc_fetch        = config['cdc_fetch']
    parser           = config['parser']
    upsert           = config['upsert']
    parser_type_name = config['parser_type_name']
    kind             = config['kind']

    lock = _get_company_lock(company_name)
    logger.info(f"[{company_name}][{voucher_type}] Starting")

    try:
        state             = get_sync_state(company_name, voucher_type, engine)
        is_initial_done   = state.is_initial_done   if state else False
        last_alter_id     = state.last_alter_id     if state else 0
        last_synced_month = state.last_synced_month if state else None

        # ── CDC mode (after first full snapshot) ──────────────────────────────
        if is_initial_done:
            logger.info(f"[{company_name}][{voucher_type}] CDC | last_alter_id={last_alter_id}")

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
                return

            rows = parser(xml, company_name, parser_type_name)
            if not rows:
                logger.info(
                    f"[{company_name}][{voucher_type}] CDC: nothing changed "
                    f"(fetch={fetch_ms}ms, 0 rows)"
                )
                return

            t1 = datetime.now()
            upsert(rows, engine)
            upsert_ms = int((datetime.now() - t1).total_seconds() * 1000)

            new_max = _get_max_alter_id(rows)
            with lock:
                update_sync_state(company_name, voucher_type, new_max, engine, is_initial_done=True)
            logger.info(
                f"[{company_name}][{voucher_type}] CDC done | "
                f"rows={len(rows)} | new max_alter_id={new_max} | "
                f"fetch={fetch_ms}ms upsert={upsert_ms}ms"
            )
            return

        # ── Snapshot mode (first run — chunked) ───────────────────────────────
        logger.info(
            f"[{company_name}][{voucher_type}] SNAPSHOT "
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

        # Pre-calculate chunks for accurate progress reporting
        _all_chunks = list(_generate_chunks(from_date, to_date))
        total_chunks = max(len([c for c in _all_chunks
            if not (last_synced_month and c[2] < last_synced_month)]), 1)

        for chunk_from, chunk_to, month_str in _generate_chunks(from_date, to_date):

            if last_synced_month and month_str < last_synced_month:
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

            rows = parser(xml, company_name, parser_type_name)

            if not rows:
                logger.info(
                    f"[{company_name}][{voucher_type}] Chunk {month_str}: 0 rows, advancing"
                )
                _mark_chunk_done(company_name, voucher_type, month_str, engine)
                chunks_done += 1
                continue

            chunk_max = max((int(r.get('alter_id', 0)) for r in rows), default=0)
            # upsert_and_advance_month internally acquires the per-company lock
            # for its SyncState write so we do NOT hold the lock around the
            # (potentially slow) DB upsert of the voucher rows themselves.
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
            f"[{company_name}][{voucher_type}] Snapshot complete | "
            f"chunks={chunks_done} | total_rows={total_rows} | max_alter_id={final_alter_id}"
        )

    except Exception:
        logger.exception(
            f"[{company_name}][{voucher_type}] Sync failed — "
            f"will resume from last committed chunk on next run"
        )


# ── Public API ────────────────────────────────────────────────────────────────

def sync_company(
    company:              dict,
    tally:                TallyConnector,
    engine,
    to_date:              str,
    manual_from_date:     str  = None,
    parallel_company_mode: bool = False,
):
    """
    Sync a single company.

    parallel_company_mode=True  → called from sync_all_companies_parallel;
        inner voucher ThreadPoolExecutor is capped at 1 worker because Tally
        requests are already serialised by _TALLY_SEMAPHORE, and adding more
        threads just wastes memory with no throughput benefit.

    parallel_company_mode=False → sequential call; use full VOUCHER_WORKERS.
    """
    comp_name   = company.get('name', '').strip()
    from_date   = manual_from_date if manual_from_date else _resolve_from_date(company)
    # When multiple companies run in parallel the inner executor must stay at
    # 1 worker — extra threads would all block on _TALLY_SEMAPHORE anyway.
    inner_workers = 1 if parallel_company_mode else VOUCHER_WORKERS

    logger.info('=' * 60)
    logger.info(f'Syncing company  : {comp_name}')
    logger.info(f'Date range       : {from_date} → {to_date}')
    logger.info(f'Chunk size       : {SNAPSHOT_CHUNK_MONTHS} months per API call')
    logger.info(f'Voucher workers  : {inner_workers} (parallel_company_mode={parallel_company_mode})')
    logger.info('=' * 60)

    start_time = datetime.now()

    # Ledgers, items and trial balance are sequential (fast master syncs)
    _sync_ledgers(comp_name, tally, engine)
    _sync_items(comp_name, tally, engine)
    _sync_trial_balance(comp_name, tally, engine, from_date, to_date)

    logger.info(f"[{comp_name}] Launching {len(VOUCHER_CONFIG)} voucher syncs …")
    with ThreadPoolExecutor(max_workers=inner_workers) as executor:
        futures = {
            executor.submit(
                _sync_voucher,
                company_name = comp_name,
                config       = config,
                tally        = tally,
                engine       = engine,
                from_date    = from_date,
                to_date      = to_date,
            ): config['voucher_type']
            for config in VOUCHER_CONFIG
        }

        for future in as_completed(futures):
            vt = futures[future]
            try:
                future.result()
                logger.info(f"[{comp_name}][{vt}] Thread finished ✓")
            except Exception:
                logger.error(
                    f"[{comp_name}][{vt}] Thread raised an exception (other types continue)"
                )

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"[{comp_name}] Sync completed in {elapsed:.1f}s")


def sync_all_companies(
    companies:        list,
    tally:            TallyConnector,
    engine,
    to_date:          str,
    manual_from_date: str = None,
):
    """Sequential company sync (original behaviour, unchanged)."""
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
    companies:         list,
    tally:             TallyConnector,
    engine,
    to_date:           str,
    manual_from_date:  str = None,
    max_company_workers: int = 3,
):
    """
    Parallel company sync.

    All HTTP calls to Tally are serialised via _TALLY_SEMAPHORE so Tally is
    never hit concurrently regardless of how many company threads are running.
    DB writes to different companies are fully independent and run in parallel.

    max_company_workers: how many companies to process at once.
        Keep ≤ 3 for Tally Prime (single-user); can raise for Tally Server.
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