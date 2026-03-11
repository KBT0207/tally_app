from datetime import datetime, timedelta
from sqlalchemy.orm import sessionmaker
import threading

from database.models.company import Company
from database.models.sync_state import SyncState
from database.models.ledger import Ledger
from database.models.item import Item
from database.models.inventory_voucher import SalesVoucher, PurchaseVoucher, CreditNote, DebitNote
from database.models.ledger_voucher import ReceiptVoucher, PaymentVoucher, JournalVoucher, ContraVoucher
from database.models.trial_balance import TrialBalance
from database.models.outstanding_models import DebtorOutstanding

from logging_config import logger

# ── Per-company SyncState write locks ────────────────────────────────────────
# Imported and shared with sync_service via the same module-level dict.
# sync_service calls _get_db_company_lock() to retrieve the same lock object.
_DB_COMPANY_LOCKS: dict[str, threading.Lock] = {}
_DB_COMPANY_LOCKS_MUTEX = threading.Lock()

def _get_db_company_lock(company_name: str) -> threading.Lock:
    """Return (creating if needed) a per-company threading.Lock for DB writes."""
    with _DB_COMPANY_LOCKS_MUTEX:
        if company_name not in _DB_COMPANY_LOCKS:
            _DB_COMPANY_LOCKS[company_name] = threading.Lock()
        return _DB_COMPANY_LOCKS[company_name]

def _get_session(engine):
    return sessionmaker(bind=engine)()

def _log_result(label, inserted, updated, unchanged, skipped, deleted=0):
    logger.info(
        f"{label} completed | "
        f"Inserted: {inserted} | "
        f"Updated: {updated} | "
        f"Unchanged: {unchanged} | "
        f"Deleted: {deleted} | "
        f"Skipped: {skipped}"
    )

def _log_changes(label, existing, update_fields, new_row):
    changes = []
    for field in update_fields:
        old_val = getattr(existing, field, None)
        new_val = new_row.get(field)
        if str(old_val) != str(new_val):
            changes.append(f"  {field}: [{old_val}] → [{new_val}]")
    if changes:
        logger.debug(
            f"{label} | guid={getattr(existing, 'guid', '?')} | "
            f"{len(changes)} field(s) changed:\n" + "\n".join(changes)
        )

def _t(value, max_len):
    if value is None:
        return None
    value = str(value).strip()
    if len(value) > max_len:
        logger.debug(f"Truncating value of length {len(value)} to {max_len}: {value[:30]}...")
        return value[:max_len]
    return value

def _insert_voucher_rows(rows, model_class, db, set_total_amt=True):
    """
    Insert a list of pre-parsed voucher rows for ONE voucher (same voucherkey/guid).
    Sorts by item_name first so idx==0 is always the same item across re-syncs.
    Enforces total_amt only on idx==0 — all other rows get 0.
    is_deleted is always 'No' — deleted vouchers are physically removed, never inserted.
    """
    rows_sorted = sorted(rows, key=lambda r: r.get('item_name', ''))
    for idx, row in enumerate(rows_sorted):
        db.add(model_class(
            company_name     = row.get('company_name'),
            date             = row.get('date'),
            voucher_number   = row.get('voucher_number'),
            reference        = row.get('reference'),
            voucher_type     = row.get('voucher_type'),
            party_name       = row.get('party_name'),
            gst_number       = row.get('gst_number'),
            e_invoice_number = row.get('e_invoice_number'),
            eway_bill        = row.get('eway_bill'),
            item_name        = row.get('item_name'),
            quantity         = row.get('quantity', 0.0),
            unit             = row.get('unit'),
            alt_qty          = row.get('alt_qty', 0.0),
            alt_unit         = row.get('alt_unit'),
            batch_no         = row.get('batch_no'),
            mfg_date         = row.get('mfg_date'),
            exp_date         = row.get('exp_date'),
            hsn_code         = row.get('hsn_code'),
            gst_rate         = row.get('gst_rate', 0.0),
            rate             = row.get('rate', 0.0),
            amount           = row.get('amount', 0.0),
            discount         = row.get('discount', 0.0),
            cgst_amt         = row.get('cgst_amt', 0.0),
            sgst_amt         = row.get('sgst_amt', 0.0),
            igst_amt         = row.get('igst_amt', 0.0),
            freight_amt      = row.get('freight_amt', 0.0),
            dca_amt          = row.get('dca_amt', 0.0),
            cf_amt           = row.get('cf_amt', 0.0),
            other_amt        = row.get('other_amt', 0.0),
            # total_amt is a voucher-level value — store only on first item row
            total_amt        = row.get('total_amt', 0.0) if idx == 0 else 0.0,
            currency         = row.get('currency', 'INR'),
            exchange_rate    = row.get('exchange_rate', 1.0),
            narration        = row.get('narration'),
            guid             = row.get('guid'),
            voucherkey       = row.get('voucherkey', ''),
            alter_id         = row.get('alter_id', 0),
            master_id        = row.get('master_id'),
            change_status    = row.get('change_status'),
            is_deleted       = 'No',    # always No — deleted rows are hard deleted, never inserted
        ))


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 1 + 2 — Inventory voucher upsert  (Sales / Purchase / Credit / Debit)
#
#  Strategy (replaces old Retired/Deleted soft-delete approach):
#    DELETE signal  → hard DELETE all existing rows for that voucherkey
#    New voucher    → INSERT fresh rows (is_deleted='No' always)
#    Altered voucher→ hard DELETE old rows + INSERT fresh rows (clean replace)
#    Unchanged      → skip
#
#  Used by BOTH snapshot chunks (Phase 1) and CDC (Phase 2).
#  Snapshot calls this via upsert_and_advance_month().
#  CDC calls this via _upsert_inventory_voucher().
# ─────────────────────────────────────────────────────────────────────────────
def _upsert_inventory_voucher_in_session(rows, model_class, db):
    inserted = updated = unchanged = skipped = deleted = 0

    from collections import defaultdict
    groups: dict[str, list] = defaultdict(list)
    for row in rows:
        if not row.get('guid'):
            skipped += 1
            continue
        key = row.get('voucherkey') or row.get('guid', '')
        groups[key].append(row)

    for key, group_rows in groups.items():
        first        = group_rows[0]
        guid         = first.get('guid', '')
        company_name = first.get('company_name', '')
        # MAX alter_id across all rows in the group — Tally CDC can return
        # items of the same voucher with different alter_ids in one response.
        new_alter_id = max(int(r.get('alter_id', 0)) for r in group_rows)
        is_del       = first.get('is_deleted', 'No')

        # ── Find existing active rows ──────────────────────────────────────
        lookup_voucherkey = first.get('voucherkey', '')
        if lookup_voucherkey:
            existing_rows = db.query(model_class).filter(
                model_class.company_name == company_name,
                model_class.voucherkey   == lookup_voucherkey,
                model_class.is_deleted   == 'No',
            ).all()
        else:
            existing_rows = db.query(model_class).filter(
                model_class.company_name == company_name,
                model_class.guid         == guid,
                model_class.is_deleted   == 'No',
            ).all()

        # ── PHASE 2: CDC delete signal → hard DELETE from DB ──────────────
        # Tally sets is_deleted='Yes' in CDC response when a voucher is deleted.
        # We physically remove the rows — no soft-delete flag, no history.
        if is_del == 'Yes':
            for rec in existing_rows:
                db.delete(rec)
            deleted += len(existing_rows)
            logger.debug(
                f"[{company_name}] Hard deleted {len(existing_rows)} rows "
                f"for voucherkey={lookup_voucherkey or guid} (CDC delete)"
            )
            continue

        # ── No existing rows → fresh INSERT ───────────────────────────────
        # Covers: new voucher in CDC, or first time this chunk runs in snapshot.
        if not existing_rows:
            _insert_voucher_rows(group_rows, model_class, db)
            inserted += len(group_rows)
            continue

        # ── Existing rows found — check alter_id to decide action ─────────
        # MAX across all existing rows — handles mixed alter_ids from
        # previous partial syncs under the same voucherkey.
        old_alter_id = max(int(r.alter_id or 0) for r in existing_rows)

        if new_alter_id > old_alter_id:
            # Voucher was altered in Tally.
            # Hard DELETE old rows first, then INSERT fresh.
            # This is safe because: if commit fails after DELETE but before
            # INSERT, the next CDC will re-fetch this voucher (alter_id still
            # > last saved alter_id) and re-insert it correctly.
            for rec in existing_rows:
                db.delete(rec)
            _insert_voucher_rows(group_rows, model_class, db)
            updated += len(group_rows)
            logger.debug(
                f"[{company_name}] Replaced {len(existing_rows)} → {len(group_rows)} rows "
                f"for voucherkey={lookup_voucherkey or guid} "
                f"(alter_id {old_alter_id} → {new_alter_id})"
            )
        else:
            # alter_id unchanged — voucher not modified since last sync
            unchanged += len(existing_rows)

    return inserted, updated, unchanged, skipped, deleted


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 2 — Ledger voucher upsert  (Receipt / Payment / Journal / Contra)
#
#  Ledger vouchers are single-row per ledger entry (not multi-row like inventory).
#  Strategy:
#    DELETE signal → hard DELETE matching rows
#    Existing row  → update fields in-place if alter_id is higher
#    No existing   → INSERT fresh
#    Unchanged     → skip
#
#  Key fixes vs old version:
#    1. Lookup now filters is_deleted='No' — avoids matching already-deleted rows
#    2. Delete is now hard DELETE, not soft is_deleted='Yes'
# ─────────────────────────────────────────────────────────────────────────────
def _upsert_ledger_voucher_in_session(rows, model_class, db):
    """
    Upsert ledger voucher rows (Receipt / Payment / Journal / Contra).

    VOUCHER-NUMBER RENUMBERING FIX
    ─────────────────────────────────────────────────────────────────────────
    When a voucher is deleted in Tally, Tally auto-renumbers the remaining
    vouchers.  Example: vouchers 1, 2, 3 → delete 2 → Tally renames 3 to 2.

    The CDC response contains:
      • GUID_2  is_deleted=Yes  (the deleted voucher)
      • GUID_3  alter_id bumped, voucher_number now = "2"  (renumbered)

    Old strategy (field-level UPDATE per row) had two failure modes:
      1. CDC batch order not guaranteed — if GUID_3 update arrives before
         GUID_2 delete in the same batch, both voucher_number='2' rows exist
         in DB simultaneously, risking a unique-constraint violation or
         wrong data in reports until the delete is processed.
      2. Per-row lookup by (guid + ledger_name) could miss rows if the
         same GUID had multiple ledger entries with varying alter_ids from
         a previous partial sync.

    NEW STRATEGY — mirrors inventory voucher upsert:
      1. Group all incoming rows by GUID.
      2. Process ALL DELETES first in a single pass, before any inserts/updates.
         This ensures the old voucher_number='2' row is gone before we write
         the renumbered voucher_number='2' for a different GUID.
      3. For altered vouchers: hard DELETE all existing rows for that GUID,
         then INSERT fresh rows.  This is safe because:
         - If the commit fails after DELETE but before INSERT, the next CDC
           will re-fetch (alter_id still > saved) and re-insert correctly.
         - No stale field values can survive — the full row is replaced.
      4. For new vouchers: plain INSERT.
      5. For unchanged (alter_id same): skip.
    ─────────────────────────────────────────────────────────────────────────
    """
    from collections import defaultdict

    inserted = updated = unchanged = skipped = deleted = 0

    # ── Step 1: group rows by GUID ─────────────────────────────────────────
    groups: dict[str, list] = defaultdict(list)
    for row in rows:
        if not row.get('guid'):
            skipped += 1
            continue
        groups[row['guid']].append(row)

    # ── Step 2: process ALL DELETES first ─────────────────────────────────
    # Critical for renumber safety: removes old voucher_number rows before
    # we insert/update the renumbered ones.
    delete_guids = {
        guid for guid, group in groups.items()
        if group[0].get('is_deleted', 'No') == 'Yes'
    }
    for guid in delete_guids:
        company_name = groups[guid][0].get('company_name', '')
        affected = db.query(model_class).filter(
            model_class.guid         == guid,
            model_class.company_name == company_name,
            model_class.is_deleted   == 'No',
        ).all()
        for rec in affected:
            db.delete(rec)
        deleted += len(affected)
        logger.debug(
            f"[{company_name}] Hard deleted {len(affected)} ledger rows "
            f"for guid={guid} (CDC delete — renumber-safe pass)"
        )

    # ── Step 3: process INSERTS / UPDATES ─────────────────────────────────
    for guid, group_rows in groups.items():
        if guid in delete_guids:
            continue   # already handled above

        first        = group_rows[0]
        company_name = first.get('company_name', '')
        new_alter_id = max(int(r.get('alter_id', 0)) for r in group_rows)

        # Find all existing active rows for this GUID (any ledger_name)
        existing_rows = db.query(model_class).filter(
            model_class.guid         == guid,
            model_class.company_name == company_name,
            model_class.is_deleted   == 'No',
        ).all()

        if not existing_rows:
            # Brand new voucher → INSERT all ledger-entry rows
            for row in group_rows:
                db.add(model_class(
                    company_name   = company_name,
                    date           = row.get('date'),
                    voucher_type   = row.get('voucher_type'),
                    voucher_number = row.get('voucher_number'),
                    reference      = row.get('reference'),
                    ledger_name    = row.get('ledger_name'),
                    amount         = row.get('amount', 0.0),
                    amount_type    = row.get('amount_type'),
                    currency       = row.get('currency', 'INR'),
                    exchange_rate  = row.get('exchange_rate', 1.0),
                    narration      = row.get('narration'),
                    guid           = guid,
                    alter_id       = row.get('alter_id', 0),
                    master_id      = row.get('master_id'),
                    change_status  = row.get('change_status'),
                    is_deleted     = 'No',
                ))
            inserted += len(group_rows)
            continue

        old_alter_id = max(int(r.alter_id or 0) for r in existing_rows)

        if new_alter_id > old_alter_id:
            # Voucher altered (includes renumber) → hard DELETE old rows,
            # INSERT fresh.  Guarantees voucher_number is always current.
            logger.debug(
                f"[{company_name}] Replacing {len(existing_rows)} ledger rows "
                f"for guid={guid} "
                f"(alter_id {old_alter_id} → {new_alter_id}, voucher_number renumber-safe)"
            )
            for rec in existing_rows:
                db.delete(rec)
            for row in group_rows:
                db.add(model_class(
                    company_name   = company_name,
                    date           = row.get('date'),
                    voucher_type   = row.get('voucher_type'),
                    voucher_number = row.get('voucher_number'),
                    reference      = row.get('reference'),
                    ledger_name    = row.get('ledger_name'),
                    amount         = row.get('amount', 0.0),
                    amount_type    = row.get('amount_type'),
                    currency       = row.get('currency', 'INR'),
                    exchange_rate  = row.get('exchange_rate', 1.0),
                    narration      = row.get('narration'),
                    guid           = guid,
                    alter_id       = row.get('alter_id', 0),
                    master_id      = row.get('master_id'),
                    change_status  = row.get('change_status'),
                    is_deleted     = 'No',
                ))
            updated += len(group_rows)
        else:
            # alter_id unchanged — voucher not modified since last sync
            unchanged += len(existing_rows)

    return inserted, updated, unchanged, skipped, deleted


# ─────────────────────────────────────────────────────────────────────────────
#  PHASE 3 — GUID Reconciliation (catch deletes CDC missed)
#
#  Called after CDC completes for each voucher type.
#  Compares GUIDs in DB (last 90 days) vs GUIDs currently in Tally.
#  Any GUID present in DB but absent from Tally → physically deleted in Tally
#  → hard DELETE from DB.
#
#  SAFE FAILURE CONTRACT:
#    tally_guids=None   → Tally fetch failed     → skip, return 0
#    tally_guids=set()  → Tally has 0 vouchers   → skip, return 0
#    Both cases: NEVER delete when uncertain.
# ─────────────────────────────────────────────────────────────────────────────
def reconcile_deleted_by_guids(
    company_name: str,
    model_class,
    tally_guids: dict,   # {guid: voucher_number} — from parse_guids()
    from_date:   str,    # YYYYMMDD
    to_date:     str,    # YYYYMMDD
    engine,
) -> int:
    """
    Phase 3 reconciliation — two passes for the given date window:

    PASS 1 — DELETE:
      Hard delete DB rows whose GUID is absent from Tally entirely.
      These are vouchers deleted in Tally that CDC missed.

    PASS 2 — RENUMBER FIX:
      For vouchers whose GUID still exists in Tally but voucher_number
      in DB doesn't match what Tally reports, update all DB rows for
      that GUID to the correct voucher_number.

      This catches the case where:
        - Tally renumbered a voucher (deleted another → auto-renumber)
        - The app was offline / CDC missed the alter_id bump
        - GUID still exists in both DB and Tally, so Pass 1 skips it
        - But DB has stale voucher_number that will never self-correct

      Only applies when tally_guids contains actual voucher_number values
      (i.e. the GUID template fetches VOUCHERNUMBER). If voucher_number
      is '' in the dict, the renumber check is skipped for that GUID.

    Returns: total count of rows deleted (Pass 1 only — updates not counted).
    tally_guids must be a non-empty dict — caller enforces this.
    """
    from datetime import datetime
    db = _get_session(engine)
    try:
        fd = datetime.strptime(from_date, '%Y%m%d').date()
        td = datetime.strptime(to_date,   '%Y%m%d').date()

        # Fetch all active rows in the window
        db_rows = db.query(model_class).filter(
            model_class.company_name == company_name,
            model_class.date         >= fd,
            model_class.date         <= td,
            model_class.is_deleted   == 'No',
        ).all()

        if not db_rows:
            logger.debug(
                f"[{company_name}][{model_class.__tablename__}] "
                f"GUID reconciliation: no DB rows in window {from_date}→{to_date}"
            )
            return 0

        tally_guid_set = set(tally_guids.keys())

        # ── PASS 1: hard delete rows whose GUID is gone from Tally ────────
        to_delete = [r for r in db_rows if r.guid not in tally_guid_set]
        for rec in to_delete:
            db.delete(rec)

        deleted_count = len(to_delete)
        if deleted_count:
            logger.info(
                f"[{company_name}][{model_class.__tablename__}] "
                f"GUID reconciliation Pass 1: hard deleted {deleted_count} rows "
                f"not found in Tally | window {from_date}→{to_date}"
            )

        # ── PASS 2: fix stale voucher_numbers for renumbered vouchers ─────
        # Group surviving DB rows by GUID (exclude just-deleted ones)
        surviving_guids = {r.guid for r in db_rows if r.guid in tally_guid_set}

        renumber_count = 0
        from collections import defaultdict
        rows_by_guid: dict = defaultdict(list)
        for r in db_rows:
            if r.guid in surviving_guids:
                rows_by_guid[r.guid].append(r)

        for guid, rows in rows_by_guid.items():
            tally_vnum = tally_guids.get(guid, '')
            if not tally_vnum:
                # Template didn't fetch VOUCHERNUMBER — skip renumber check
                continue

            # Check if any row has a different voucher_number than Tally reports
            stale_rows = [r for r in rows if r.voucher_number != tally_vnum]
            if stale_rows:
                for r in stale_rows:
                    old_num = r.voucher_number
                    r.voucher_number = tally_vnum
                renumber_count += len(stale_rows)
                logger.info(
                    f"[{company_name}][{model_class.__tablename__}] "
                    f"GUID reconciliation Pass 2: fixed voucher_number "
                    f"guid={guid} | '{old_num}' → '{tally_vnum}' "
                    f"({len(stale_rows)} rows)"
                )

        db.commit()

        if deleted_count == 0 and renumber_count == 0:
            logger.debug(
                f"[{company_name}][{model_class.__tablename__}] "
                f"GUID reconciliation: all {len(db_rows)} DB rows confirmed correct in Tally ✓"
            )
        elif renumber_count:
            logger.info(
                f"[{company_name}][{model_class.__tablename__}] "
                f"GUID reconciliation Pass 2: fixed {renumber_count} stale voucher_number rows ✓"
            )

        return deleted_count

    except Exception:
        db.rollback()
        logger.exception(
            f"[{company_name}][{model_class.__tablename__}] "
            f"GUID reconciliation DB error"
        )
        return 0
    finally:
        db.close()


# ─────────────────────────────────────────────────────────────────────────────
#  Snapshot chunk helper — called from sync_service._sync_voucher() snapshot path
# ─────────────────────────────────────────────────────────────────────────────
def upsert_and_advance_month(
    rows, model_class, upsert_fn,
    company_name, voucher_type, month_str, engine,
    chunk_max_alter_id=0,
):
    """
    Upsert voucher rows for one snapshot chunk then atomically advance SyncState.

    The SyncState update is protected by a per-company lock so that two
    voucher worker threads for the same company cannot overwrite each other's
    last_synced_month / last_alter_id. The (slow) voucher upsert itself runs
    outside the lock so other voucher types are not blocked.

    upsert_fn is _upsert_inventory_voucher_in_session or
    _upsert_ledger_voucher_in_session — both now do hard delete+reinsert,
    so snapshot chunks are automatically clean (no Retired rows).
    """
    db = _get_session(engine)
    try:
        inserted, updated, unchanged, skipped, deleted = upsert_fn(rows, model_class, db)
        db.commit()
    except Exception:
        db.rollback()
        logger.exception(f"[{company_name}] [{voucher_type}] Month {month_str} voucher upsert ROLLED BACK")
        db.close()
        raise
    else:
        db.close()

    # Update SyncState in a separate session, protected by the company lock.
    lock = _get_db_company_lock(company_name)
    with lock:
        db2 = _get_session(engine)
        try:
            state = db2.query(SyncState).filter_by(
                company_name = company_name,
                voucher_type = voucher_type,
            ).first()

            if state:
                state.last_synced_month = month_str
                state.last_sync_time    = datetime.now()
                if chunk_max_alter_id > (state.last_alter_id or 0):
                    state.last_alter_id = chunk_max_alter_id
            else:
                db2.add(SyncState(
                    company_name      = company_name,
                    voucher_type      = voucher_type,
                    last_alter_id     = chunk_max_alter_id,
                    is_initial_done   = False,
                    last_synced_month = month_str,
                    last_sync_time    = datetime.now(),
                ))

            db2.commit()
            logger.info(
                f"[{company_name}] [{voucher_type}] Month {month_str} committed | "
                f"ins={inserted} upd={updated} unch={unchanged} del={deleted} skip={skipped}"
            )
            return inserted, updated, unchanged, skipped, deleted

        except Exception:
            db2.rollback()
            logger.exception(f"[{company_name}] [{voucher_type}] Month {month_str} SyncState ROLLED BACK")
            raise
        finally:
            db2.close()


def get_sync_state(company_name, voucher_type, engine):
    db = _get_session(engine)
    try:
        return db.query(SyncState).filter_by(
            company_name = company_name,
            voucher_type = voucher_type,
        ).first()
    finally:
        db.close()


def update_sync_state(
    company_name, voucher_type, last_alter_id, engine,
    last_synced_month=None, is_initial_done=True,
):
    db = _get_session(engine)
    try:
        state = db.query(SyncState).filter_by(
            company_name = company_name,
            voucher_type = voucher_type,
        ).first()

        if state:
            state.last_alter_id   = last_alter_id
            state.is_initial_done = is_initial_done
            state.last_sync_time  = datetime.now()
            if last_synced_month is not None:
                state.last_synced_month = last_synced_month
        else:
            db.add(SyncState(
                company_name      = company_name,
                voucher_type      = voucher_type,
                last_alter_id     = last_alter_id,
                is_initial_done   = is_initial_done,
                last_synced_month = last_synced_month,
                last_sync_time    = datetime.now(),
            ))

        db.commit()
        logger.info(
            f"SyncState finalised | company={company_name} | "
            f"type={voucher_type} | alter_id={last_alter_id}"
        )

    except Exception:
        db.rollback()
        logger.exception("Error updating sync state")
        raise
    finally:
        db.close()


def _upsert_inventory(rows, model_class, unique_fields, update_fields, engine):
    if not rows:
        logger.warning(f"No rows to upsert for {model_class.__tablename__}")
        return 0, 0, 0, 0

    db = _get_session(engine)
    inserted = updated = unchanged = skipped = 0

    try:
        for row in rows:
            if not row.get('guid'):
                skipped += 1
                continue

            filter_kwargs = {f: row.get(f) for f in unique_fields}
            existing = db.query(model_class).filter_by(**filter_kwargs).first()

            if existing:
                if int(row.get('alter_id', 0)) > int(existing.alter_id or 0):
                    _log_changes("inventory UPDATE", existing, update_fields, row)
                    for field in update_fields:
                        setattr(existing, field, row.get(field))
                    updated += 1
                else:
                    unchanged += 1
            else:
                db.add(model_class(**{
                    f: row.get(f)
                    for f in update_fields + unique_fields + ['guid', 'alter_id', 'master_id', 'change_status', 'company_name']
                }))
                inserted += 1

        db.commit()

    except Exception:
        db.rollback()
        logger.exception(f"Error upserting {model_class.__tablename__}")
        raise
    finally:
        db.close()

    return inserted, updated, unchanged, skipped


def _upsert_inventory_voucher(rows, model_class, engine):
    if not rows:
        logger.warning(f"No rows to upsert for {model_class.__tablename__}")
        return 0, 0, 0, 0, 0
    db = _get_session(engine)
    try:
        result = _upsert_inventory_voucher_in_session(rows, model_class, db)
        db.commit()
        return result
    except Exception:
        db.rollback()
        logger.exception(f"Error upserting {model_class.__tablename__}")
        raise
    finally:
        db.close()


def _upsert_ledger_voucher(rows, model_class, engine):
    if not rows:
        logger.warning(f"No rows to upsert for {model_class.__tablename__}")
        return 0, 0, 0, 0, 0
    db = _get_session(engine)
    try:
        result = _upsert_ledger_voucher_in_session(rows, model_class, db)
        db.commit()
        return result
    except Exception:
        db.rollback()
        logger.exception(f"Error upserting {model_class.__tablename__}")
        raise
    finally:
        db.close()


def upsert_sales_vouchers(rows, engine):
    i, u, unch, s, d = _upsert_inventory_voucher(rows, SalesVoucher, engine)
    _log_result("Sales vouchers upsert", i, u, unch, s, d)

def upsert_purchase_vouchers(rows, engine):
    i, u, unch, s, d = _upsert_inventory_voucher(rows, PurchaseVoucher, engine)
    _log_result("Purchase vouchers upsert", i, u, unch, s, d)

def upsert_credit_notes(rows, engine):
    i, u, unch, s, d = _upsert_inventory_voucher(rows, CreditNote, engine)
    _log_result("Credit notes upsert", i, u, unch, s, d)

def upsert_debit_notes(rows, engine):
    i, u, unch, s, d = _upsert_inventory_voucher(rows, DebitNote, engine)
    _log_result("Debit notes upsert", i, u, unch, s, d)

def upsert_receipt_vouchers(rows, engine):
    i, u, unch, s, d = _upsert_ledger_voucher(rows, ReceiptVoucher, engine)
    _log_result("Receipt vouchers upsert", i, u, unch, s, d)

def upsert_payment_vouchers(rows, engine):
    i, u, unch, s, d = _upsert_ledger_voucher(rows, PaymentVoucher, engine)
    _log_result("Payment vouchers upsert", i, u, unch, s, d)

def upsert_journal_vouchers(rows, engine):
    i, u, unch, s, d = _upsert_ledger_voucher(rows, JournalVoucher, engine)
    _log_result("Journal vouchers upsert", i, u, unch, s, d)

def upsert_contra_vouchers(rows, engine):
    i, u, unch, s, d = _upsert_ledger_voucher(rows, ContraVoucher, engine)
    _log_result("Contra vouchers upsert", i, u, unch, s, d)


def upsert_trial_balance(rows, engine):
    if not rows:
        logger.warning("No rows to upsert for trial balance")
        return

    db = _get_session(engine)
    inserted = updated = unchanged = skipped = 0

    update_fields = [
        'parent_group', 'opening_balance', 'net_transactions',
        'closing_balance', 'start_date', 'end_date',
        'alter_id', 'master_id',
    ]

    try:
        for row in rows:
            if not row.get('guid'):
                skipped += 1
                continue

            existing = db.query(TrialBalance).filter_by(
                guid         = row['guid'],
                company_name = row['company_name'],
                start_date   = row.get('start_date'),
                end_date     = row.get('end_date'),
            ).first()

            if existing:
                if int(row.get('alter_id', 0)) > int(existing.alter_id or 0):
                    _log_changes("trial_balance UPDATE", existing, update_fields, row)
                    for field in update_fields:
                        setattr(existing, field, row.get(field))
                    updated += 1
                else:
                    unchanged += 1
            else:
                db.add(TrialBalance(
                    company_name     = row.get('company_name'),
                    ledger_name      = row.get('ledger_name'),
                    parent_group     = row.get('parent_group'),
                    opening_balance  = row.get('opening_balance', 0.0),
                    net_transactions = row.get('net_transactions', 0.0),
                    closing_balance  = row.get('closing_balance', 0.0),
                    start_date       = row.get('start_date'),
                    end_date         = row.get('end_date'),
                    guid             = row.get('guid'),
                    alter_id         = row.get('alter_id', 0),
                    master_id        = row.get('master_id'),
                ))
                inserted += 1

        db.commit()
        _log_result("Trial balance upsert", inserted, updated, unchanged, skipped)

    except Exception:
        db.rollback()
        logger.exception("Error upserting trial balance")
        raise
    finally:
        db.close()


INVENTORY_MODEL_MAP = {
    'sales'       : SalesVoucher,
    'purchase'    : PurchaseVoucher,
    'credit_note' : CreditNote,
    'debit_note'  : DebitNote,
}

LEDGER_MODEL_MAP = {
    'receipt' : ReceiptVoucher,
    'payment' : PaymentVoucher,
    'journal' : JournalVoucher,
    'contra'  : ContraVoucher,
}


def upsert_items(rows, engine):
    if not rows:
        logger.warning("No rows to upsert for items")
        return

    db = _get_session(engine)
    inserted = updated = unchanged = skipped = 0

    update_fields = [
        'item_name', 'parent_group', 'category',
        'base_units', 'gst_type_of_supply',
        'opening_balance', 'opening_rate', 'opening_value',
        'entered_by', 'is_deleted',
        'guid', 'remote_alt_guid', 'alter_id',
    ]

    def _safe(row):
        return {
            'company_name'       : _t(row.get('company_name'),        255),
            'item_name'          : _t(row.get('item_name'),           500),
            'parent_group'       : _t(row.get('parent_group'),        255),
            'category'           : _t(row.get('category'),            255),
            'base_units'         : _t(row.get('base_units'),          100),
            'gst_type_of_supply' : _t(row.get('gst_type_of_supply'),  100),
            'opening_balance'    : row.get('opening_balance',  0.0),
            'opening_rate'       : row.get('opening_rate',     0.0),
            'opening_value'      : row.get('opening_value',    0.0),
            'entered_by'         : _t(row.get('entered_by'),          255),
            'is_deleted'         : _t(row.get('is_deleted'),           10),
            'guid'               : _t(row.get('guid'),                100),
            'remote_alt_guid'    : _t(row.get('remote_alt_guid'),     100),
            'alter_id'           : row.get('alter_id', 0),
        }

    try:
        for row in rows:
            if not row.get('guid'):
                skipped += 1
                continue

            safe = _safe(row)

            existing = db.query(Item).filter_by(
                guid         = safe['guid'],
                company_name = safe['company_name'],
            ).first()

            if existing:
                if int(safe['alter_id']) > int(existing.alter_id or 0):
                    _log_changes(
                        "item UPDATE", existing,
                        [f for f in safe if f not in ('guid', 'company_name')],
                        safe,
                    )
                    for field, value in safe.items():
                        if field not in ('guid', 'company_name'):
                            setattr(existing, field, value)
                    updated += 1
                else:
                    unchanged += 1
            else:
                db.add(Item(**safe))
                inserted += 1

        db.commit()
        _log_result("Items upsert", inserted, updated, unchanged, skipped)

    except Exception:
        db.rollback()
        logger.exception("Error upserting items")
        raise
    finally:
        db.close()


def _parse_date_str(val) -> "date | None":
    """Parse YYYYMMDD string → date object. Returns None on failure."""
    from datetime import date as date_type
    try:
        return datetime.strptime(str(val)[:8], '%Y%m%d').date()
    except Exception:
        return None


def company_import_db(data, engine):
    db = _get_session(engine)
    try:
        logger.info("Starting company import process")

        date_cols = ['starting_from', 'books_from', 'audited_upto']
        fields    = ["name", "formal_name", "company_number", "starting_from", "books_from", "audited_upto"]
        inserted = updated = unchanged = skipped = 0

        valid_rows = [r for r in data if r.get('name') and str(r['name']).strip()]
        logger.info(f"Records after name filtering: {len(valid_rows)}")

        for row in valid_rows:
            if not row.get("guid"):
                skipped += 1
                logger.warning("Skipped record due to missing GUID")
                continue

            for col in date_cols:
                if col in row and row[col]:
                    row[col] = _parse_date_str(row[col])

            existing = db.query(Company).filter_by(guid=row["guid"]).first()

            if existing:
                is_changed = False
                changes = []
                for field in fields:
                    old_val = getattr(existing, field)
                    new_val = row.get(field)
                    if old_val != new_val:
                        changes.append(f"  {field}: [{old_val}] → [{new_val}]")
                        setattr(existing, field, new_val)
                        is_changed = True
                if is_changed:
                    logger.debug(
                        f"company UPDATE | guid={row['guid']} | "
                        f"{len(changes)} field(s) changed:\n" + "\n".join(changes)
                    )
                    updated += 1
                else:
                    unchanged += 1
            else:
                db.add(Company(
                    guid           = row["guid"],
                    name           = row.get("name"),
                    formal_name    = row.get("formal_name"),
                    company_number = row.get("company_number"),
                    starting_from  = row.get("starting_from"),
                    books_from     = row.get("books_from"),
                    audited_upto   = row.get("audited_upto"),
                ))
                inserted += 1

        db.commit()
        _log_result("Company import", inserted, updated, unchanged, skipped)

    except Exception:
        db.rollback()
        logger.exception("Error occurred during company import")
        raise
    finally:
        db.close()


def upsert_ledgers(rows, engine):
    if not rows:
        logger.warning("No rows to upsert for ledgers")
        return

    db = _get_session(engine)
    inserted = updated = unchanged = skipped = 0

    def _safe(row):
        return {
            'company_name'          : _t(row.get('company_name'),           255),
            'ledger_name'           : _t(row.get('ledger_name'),            255),
            'alias'                 : _t(row.get('alias'),                  255),
            'alias_2'               : _t(row.get('alias_2'),                255),
            'alias_3'               : _t(row.get('alias_3'),                255),
            'parent_group'          : _t(row.get('parent_group'),           255),
            'contact_person'        : _t(row.get('contact_person'),         255),
            'email'                 : _t(row.get('email'),                  255),
            'phone'                 : _t(row.get('phone'),                  100),
            'mobile'                : _t(row.get('mobile'),                 100),
            'fax'                   : _t(row.get('fax'),                    100),
            'website'               : _t(row.get('website'),                500),
            'address_line_1'        : row.get('address_line_1'),
            'address_line_2'        : row.get('address_line_2'),
            'address_line_3'        : row.get('address_line_3'),
            'pincode'               : _t(row.get('pincode'),                100),
            'state'                 : _t(row.get('state'),                  255),
            'country'               : _t(row.get('country'),                255),
            'opening_balance'       : _t(row.get('opening_balance'),        100),
            'credit_limit'          : _t(row.get('credit_limit'),           100),
            'bill_credit_period'    : _t(row.get('bill_credit_period'),     100),
            'pan'                   : _t(row.get('pan'),                    100),
            'gstin'                 : _t(row.get('gstin'),                  100),
            'gst_registration_type' : _t(row.get('gst_registration_type'),  255),
            'vat_tin'               : _t(row.get('vat_tin'),                100),
            'sales_tax_number'      : _t(row.get('sales_tax_number'),       100),
            'bank_account_holder'   : _t(row.get('bank_account_holder'),    255),
            'ifsc_code'             : _t(row.get('ifsc_code'),              100),
            'bank_branch'           : _t(row.get('bank_branch'),            255),
            'swift_code'            : _t(row.get('swift_code'),             100),
            'bank_iban'             : _t(row.get('bank_iban'),              100),
            'export_import_code'    : _t(row.get('export_import_code'),     100),
            'msme_reg_number'       : _t(row.get('msme_reg_number'),        100),
            'is_bill_wise_on'       : _t(row.get('is_bill_wise_on'),         10),
            'is_deleted'            : _t(row.get('is_deleted'),              10),
            'created_date'          : _t(row.get('created_date'),            20),
            'altered_on'            : _t(row.get('altered_on'),              20),
            'guid'                  : _t(row.get('guid'),                   255),
            'alter_id'              : row.get('alter_id', 0),
        }

    try:
        for row in rows:
            if not row.get('guid'):
                skipped += 1
                continue

            safe = _safe(row)

            existing = db.query(Ledger).filter_by(
                guid         = safe['guid'],
                company_name = safe['company_name'],
            ).first()

            if existing:
                if int(safe['alter_id']) > int(existing.alter_id or 0):
                    _log_changes("ledger UPDATE", existing, [f for f in safe if f not in ('guid', 'company_name')], safe)
                    for field, value in safe.items():
                        if field not in ('guid', 'company_name'):
                            setattr(existing, field, value)
                    updated += 1
                else:
                    unchanged += 1
            else:
                db.add(Ledger(**safe))
                inserted += 1

        db.commit()
        _log_result("Ledgers upsert", inserted, updated, unchanged, skipped)

    except Exception:
        db.rollback()
        logger.exception("Error upserting ledgers")
        raise
    finally:
        db.close()


def upsert_debtor_outstanding(rows, engine):
    """
    Full-replace for debtor outstanding — point-in-time snapshot.
    Deletes all existing rows for the company then bulk-inserts fresh ones.
    Both in one transaction — if insert fails, delete rolls back too.
    Paid/closed bills are automatically removed on each sync.
    """
    if not rows:
        logger.warning("No rows to upsert for debtor outstanding")
        return

    company_name = rows[0].get('company_name', '')
    db = _get_session(engine)
    inserted = 0
    try:
        db.query(DebtorOutstanding).filter_by(
            company_name=company_name
        ).delete(synchronize_session='fetch')

        for row in rows:
            if not row.get('voucher_number'):
                continue
            db.add(DebtorOutstanding(
                company_name   = row.get('company_name'),
                party_name     = row.get('party_name'),
                voucher_number = row.get('voucher_number'),
                voucher_type   = row.get('voucher_type'),
                bill_name      = row.get('bill_name'),
                bill_type      = row.get('bill_type'),
                date           = row.get('date'),
                bill_date      = row.get('bill_date'),
                due_date       = row.get('due_date'),
                reference      = row.get('reference'),
                currency       = row.get('currency', 'INR'),
                exchange_rate  = row.get('exchange_rate', 1.0),
                amount         = row.get('amount', 0.0),
                narration      = row.get('narration'),
            ))
            inserted += 1

        db.commit()
        logger.info(f"Debtor outstanding full-replace | company={company_name} | inserted={inserted}")

    except Exception:
        db.rollback()
        logger.exception("Error upserting debtor outstanding")
        raise
    finally:
        db.close()
