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
from database.models.outstanding_models import OutstandingData

from logging_config import logger

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
    value = str(value)
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
            total_amt        = row.get('total_amt', 0.0) if idx == 0 else 0.0,
            currency         = row.get('currency', 'INR'),
            exchange_rate    = row.get('exchange_rate', 1.0),
            narration        = row.get('narration'),
            guid             = row.get('guid'),
            voucherkey       = row.get('voucherkey', ''),
            alter_id         = row.get('alter_id', 0),
            master_id        = row.get('master_id'),
            change_status    = row.get('change_status'),
            is_deleted       = 'No',
            material_centre  = row.get('material_centre', ''),
        ))

def _upsert_inventory_voucher_in_session(rows, model_class, db):
    """
    Upsert inventory voucher rows with strict 3-phase commit order:

    PHASE 1 — DELETE:  Hard delete all rows marked is_deleted=Yes.
                        Flush immediately so keys are free.
    PHASE 2 — UPDATE:  For existing vouchers with a higher alter_id,
                        delete old rows then flush, then insert new rows, then flush.
    PHASE 3 — INSERT:  Insert brand-new vouchers.

    Phases never overlap — no interleaved DELETE+INSERT in the same flush.
    This prevents parallel-run race conditions and FK conflicts.
    """
    from collections import defaultdict

    inserted = updated = unchanged = skipped = deleted = 0

    groups: dict[str, list] = defaultdict(list)
    for row in rows:
        if not row.get('guid'):
            skipped += 1
            continue
        key = row.get('voucherkey') or row.get('guid', '')
        groups[key].append(row)

    all_keys        = list(groups.keys())
    company_name_0  = next(iter(groups.values()))[0].get('company_name', '') if groups else ''

    existing_map: dict[str, list] = defaultdict(list)
    if all_keys:
        has_voucherkey = hasattr(model_class, 'voucherkey')
        if has_voucherkey:
            db_rows = db.query(model_class).filter(
                model_class.company_name.in_(
                    list({g[0].get('company_name', '') for g in groups.values()})
                ),
                model_class.is_deleted == 'No',
            ).all()
            for rec in db_rows:
                _rec_vkey = getattr(rec, 'voucherkey', None) or getattr(rec, 'guid', '')
                _rec_co   = getattr(rec, 'company_name', '')
                existing_map[f"{_rec_co}|{_rec_vkey}"].append(rec)
        else:
            db_rows = db.query(model_class).filter(
                model_class.company_name.in_(
                    list({g[0].get('company_name', '') for g in groups.values()})
                ),
                model_class.is_deleted == 'No',
            ).all()
            for rec in db_rows:
                _rec_co = getattr(rec, 'company_name', '')
                existing_map[f"{_rec_co}|{getattr(rec, 'guid', '')}"].append(rec)

    def _map_key(row: dict) -> str:
        """Return the same compound key used to build existing_map."""
        co  = row.get('company_name', '')
        key = row.get('voucherkey') or row.get('guid', '')
        return f"{co}|{key}"

    guid_map: dict[str, list] = defaultdict(list)
    if all_keys:
        for rec in db_rows:
            _rec_co   = getattr(rec, 'company_name', '')
            _rec_guid = getattr(rec, 'guid', '')
            if _rec_guid:
                guid_map[f"{_rec_co}|{_rec_guid}"].append(rec)

    def _guid_key(row: dict) -> str:
        """Compound key for guid_map lookup."""
        return f"{row.get('company_name', '')}|{row.get('guid', '')}"

    def _resolve_existing(row: dict) -> list:
        """
        Return existing DB rows for this incoming row.
        Primary:   match by voucherkey  (normal case)
        Fallback:  match by GUID        (Tally re-saved → new voucherkey, same GUID)
        """
        rows_by_vkey = existing_map.get(_map_key(row), [])
        if rows_by_vkey:
            return rows_by_vkey
        rows_by_guid = guid_map.get(_guid_key(row), [])
        if rows_by_guid:
            logger.debug(
                f"[{row.get('company_name','')}] voucherkey changed for guid={row.get('guid','')} "
                f"— resolved via GUID fallback (old voucherkey={getattr(rows_by_guid[0],'voucherkey','')} "
                f"new voucherkey={row.get('voucherkey','')})"
            )
        return rows_by_guid

    delete_keys = set()
    for key, group_rows in groups.items():
        if group_rows[0].get('is_deleted', 'No') == 'Yes':
            delete_keys.add(key)
            existing_rows = _resolve_existing(group_rows[0])
            for rec in existing_rows:
                db.delete(rec)
                deleted += 1
            company_name = group_rows[0].get('company_name', '')
            logger.debug(
                f"[{company_name}] PHASE1 hard deleted "
                f"{len(existing_rows)} rows for key={key}"
            )
    if delete_keys:
        db.flush()

    update_keys = set()
    for key, group_rows in groups.items():
        if key in delete_keys:
            continue
        existing_rows = _resolve_existing(group_rows[0])
        if not existing_rows:
            continue
        company_name = group_rows[0].get('company_name', '')
        new_alter_id = max(int(r.get('alter_id', 0)) for r in group_rows)
        old_alter_id = max(int(getattr(r, 'alter_id', 0) or 0) for r in existing_rows)
        if new_alter_id > old_alter_id:
            update_keys.add(key)
            for rec in existing_rows:
                db.delete(rec)
            logger.debug(
                f"[{company_name}] PHASE2 deleted {len(existing_rows)} old rows "
                f"for key={key} alter_id {old_alter_id}→{new_alter_id}"
            )
        else:
            unchanged += len(existing_rows)

    if update_keys:
        db.flush()
        for key in update_keys:
            group_rows = groups[key]
            _insert_voucher_rows(group_rows, model_class, db)
            updated += len(group_rows)
        db.flush()

    for key, group_rows in groups.items():
        if key in delete_keys or key in update_keys:
            continue
        if _resolve_existing(group_rows[0]):
            unchanged += len(group_rows)
            continue
        _insert_voucher_rows(group_rows, model_class, db)
        inserted += len(group_rows)

    return inserted, updated, unchanged, skipped, deleted

def _upsert_ledger_voucher_in_session(rows, model_class, db):
    """
    Upsert ledger voucher rows with strict 3-phase commit order:

    PHASE 1 — DELETE:  Hard delete all rows marked is_deleted=Yes.
                        Flush immediately so GUIDs are free.
    PHASE 2 — UPDATE:  For existing GUIDs with higher alter_id,
                        delete old rows → flush → insert new rows → flush.
    PHASE 3 — INSERT:  Insert brand-new GUIDs.

    Phases never overlap.  This is safe for parallel runs because each
    voucher_type runs in its own thread with its own session and each phase
    is flushed before the next begins — no interleaved DELETE+INSERT.
    """
    from collections import defaultdict

    inserted = updated = unchanged = skipped = deleted = 0

    groups: dict[str, list] = defaultdict(list)
    for row in rows:
        if not row.get('guid'):
            skipped += 1
            continue
        groups[row['guid']].append(row)

    if not groups:
        return inserted, updated, unchanged, skipped, deleted

    all_companies = list({g[0].get('company_name', '') for g in groups.values()})

    existing_map: dict[str, list] = defaultdict(list)
    db_rows = db.query(model_class).filter(
        model_class.company_name.in_(all_companies),
        model_class.is_deleted == 'No',
    ).all()
    for rec in db_rows:
        existing_map[rec.guid].append(rec)

    delete_guids = set()
    for guid, group_rows in groups.items():
        if group_rows[0].get('is_deleted', 'No') == 'Yes':
            delete_guids.add(guid)
            company_name = group_rows[0].get('company_name', '')
            affected = existing_map.get(guid, [])
            for rec in affected:
                db.delete(rec)
            deleted += len(affected)
            logger.debug(
                f"[{company_name}] PHASE1 hard deleted {len(affected)} "
                f"ledger rows for guid={guid}"
            )
    if delete_guids:
        db.flush()

    update_guids = set()
    for guid, group_rows in groups.items():
        if guid in delete_guids:
            continue
        existing_rows = existing_map.get(guid, [])
        if not existing_rows:
            continue
        company_name = group_rows[0].get('company_name', '')
        new_alter_id = max(int(r.get('alter_id', 0)) for r in group_rows)
        old_alter_id = max(int(getattr(r, 'alter_id', 0) or 0) for r in existing_rows)
        if new_alter_id > old_alter_id:
            update_guids.add(guid)
            for rec in existing_rows:
                db.delete(rec)
            logger.debug(
                f"[{company_name}] PHASE2 deleted {len(existing_rows)} ledger rows "
                f"for guid={guid} alter_id {old_alter_id}→{new_alter_id}"
            )
        else:
            unchanged += len(existing_rows)

    if update_guids:
        db.flush()
        for guid in update_guids:
            group_rows = groups[guid]
            company_name = group_rows[0].get('company_name', '')
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
                    voucherkey     = row.get('voucherkey', ''),
                    alter_id       = row.get('alter_id', 0),
                    master_id      = row.get('master_id'),
                    change_status  = row.get('change_status'),
                    is_deleted     = 'No',
                    material_centre= row.get('material_centre', ''),
                ))
            updated += len(group_rows)
        db.flush()

    for guid, group_rows in groups.items():
        if guid in delete_guids or guid in update_guids:
            continue
        if existing_map.get(guid):
            continue
        company_name = group_rows[0].get('company_name', '')
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
                voucherkey     = row.get('voucherkey', ''),
                alter_id       = row.get('alter_id', 0),
                master_id      = row.get('master_id'),
                change_status  = row.get('change_status'),
                is_deleted     = 'No',
                material_centre= row.get('material_centre', ''),
            ))
        inserted += len(group_rows)

    return inserted, updated, unchanged, skipped, deleted

def _auto_dedup_by_guid_voucherkey(
    company_name:        str,
    model_class,
    db,
    tally_guid_vkey_map: dict,
    from_date=None,
    to_date=None,
) -> int:
    """
    PASS 0 — Precise GUID + VoucherKey dedup using Tally as source of truth.

    WHY THIS IS BETTER THAN BLIND alter_id COMPARISON
    ──────────────────────────────────────────────────
    When Tally re-saves a voucher it keeps the same GUID but issues a brand-new
    VOUCHERKEY.  The sync inserts a second DB row (new voucherkey) without
    removing the first (old voucherkey) — creating a silent duplicate.

    The correct fix is NOT to blindly keep the highest alter_id row, because:
      • alter_id is a Tally internal counter and can be unreliable.
      • The row that *should* survive is the one whose VOUCHERKEY matches
        what Tally currently reports for that GUID — that is the live record.

    ALGORITHM
    ─────────
    For every DB row whose (GUID, voucherkey) pair is NOT in Tally's current
    guid→voucherkey map, delete it.  This catches:
      1. Old voucherkey rows left behind after a Tally re-save.
      2. Any other stale duplicates sharing the same GUID.

    The row that carries the GUID's current voucherkey is untouched.

    tally_guid_vkey_map must be {guid: voucherkey} built from the GUID-list
    XML response (the same XML already fetched for reconcile_deleted_by_guids).

    Returns: count of rows deleted.
    """
    from collections import defaultdict

    if not tally_guid_vkey_map:
        logger.debug(
            f"[{company_name}][{model_class.__tablename__}] "
            f"PASS 0 GUID+VKey dedup skipped — tally_guid_vkey_map empty"
        )
        return 0

    query = db.query(model_class).filter(
        model_class.company_name == company_name,
        model_class.is_deleted   == 'No',
    )
    if from_date:
        query = query.filter(model_class.date >= from_date)
    if to_date:
        query = query.filter(model_class.date <= to_date)

    all_rows = query.all()

    guid_groups: dict[str, list] = defaultdict(list)
    for rec in all_rows:
        guid = getattr(rec, 'guid', None)
        if guid:
            guid_groups[guid].append(rec)

    dedup_deleted = 0
    for guid, records in guid_groups.items():

        if guid not in tally_guid_vkey_map:
            continue

        tally_vkey = str(tally_guid_vkey_map[guid])

        stale = [
            r for r in records
            if str(getattr(r, 'voucherkey', '')) != tally_vkey
        ]

        if not stale:
            continue

        live = [r for r in records if str(getattr(r, 'voucherkey', '')) == tally_vkey]

        logger.warning(
            f"[{company_name}][{model_class.__tablename__}] "
            f"PASS 0 GUID+VKey DEDUP | guid={guid} | "
            f"Tally voucherkey={tally_vkey} | "
            f"live rows in DB={len(live)} | "
            f"stale rows to delete={len(stale)} "
            f"stale_voucherkeys={[getattr(r,'voucherkey','?') for r in stale]} "
            f"— Tally re-saved this voucher with a new voucherkey"
        )

        for rec in stale:
            db.delete(rec)
            dedup_deleted += 1

    if dedup_deleted:
        db.flush()
        logger.info(
            f"[{company_name}][{model_class.__tablename__}] "
            f"PASS 0 GUID+VKey dedup complete: {dedup_deleted} stale row(s) removed ✓"
        )

    return dedup_deleted

def reconcile_deleted_by_guids(
    company_name: str,
    model_class,
    tally_guids: dict,
    from_date:   str,
    to_date:     str,
    engine,
    tally_guid_vkey_map: dict = None,
) -> int:
    """
    Phase 3 reconciliation — three passes for the given date window.

    PASS 0 — GUID + VOUCHERKEY DEDUP (precise, Tally-authoritative):
      Requires tally_guid_vkey_map = {guid: voucherkey} from parse_guids_vkey().
      Any DB row whose voucherkey doesn't match Tally's current voucherkey for
      that GUID is a stale duplicate and is hard-deleted automatically.
      This is the precise fix for Tally re-saving a voucher with a new
      voucherkey — no blind alter_id guessing, Tally is the source of truth.
      Skipped silently if tally_guid_vkey_map is not provided.

    PASS 1 — DELETE:
      Hard delete DB rows whose GUID is absent from Tally entirely.
      Uses tally_guids = {guid: voucher_number}.

    PASS 2 — RENUMBER FIX:
      For vouchers whose GUID still exists but voucher_number in DB
      doesn't match Tally, update all DB rows for that GUID.
      Uses tally_guids = {guid: voucher_number}.

    Returns: count of rows deleted (Pass 0 + Pass 1 combined).
    tally_guids must be a non-empty dict — caller enforces this.
    """
    from datetime import datetime
    from collections import defaultdict
    db = _get_session(engine)
    try:
        fd = datetime.strptime(from_date, '%Y%m%d').date()
        td = datetime.strptime(to_date,   '%Y%m%d').date()

        dedup_deleted = _auto_dedup_by_guid_voucherkey(
            company_name, model_class, db,
            tally_guid_vkey_map=tally_guid_vkey_map or {},
            from_date=fd, to_date=td,
        )

        db_rows = db.query(model_class).filter(
            model_class.company_name == company_name,
            model_class.date         >= fd,
            model_class.date         <= td,
            model_class.is_deleted   == 'No',
        ).all()

        if not db_rows:
            db.commit()
            logger.debug(
                f"[{company_name}][{model_class.__tablename__}] "
                f"GUID reconciliation: no DB rows in window {from_date}→{to_date}"
            )
            return dedup_deleted

        tally_guid_set = set(tally_guids.keys())

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

        surviving_guids = {r.guid for r in db_rows if r.guid in tally_guid_set}

        renumber_count = 0
        rows_by_guid: dict = defaultdict(list)
        for r in db_rows:
            if r.guid in surviving_guids:
                rows_by_guid[r.guid].append(r)

        for guid, rows in rows_by_guid.items():
            tally_vnum = tally_guids.get(guid, '')
            if not tally_vnum:
                continue

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

        total_deleted = dedup_deleted + deleted_count
        if total_deleted == 0 and renumber_count == 0:
            logger.debug(
                f"[{company_name}][{model_class.__tablename__}] "
                f"GUID reconciliation: all {len(db_rows)} DB rows confirmed correct in Tally ✓"
            )
        elif renumber_count:
            logger.info(
                f"[{company_name}][{model_class.__tablename__}] "
                f"GUID reconciliation Pass 2: fixed {renumber_count} stale voucher_number rows ✓"
            )

        return total_deleted

    except Exception:
        db.rollback()
        logger.exception(
            f"[{company_name}][{model_class.__tablename__}] "
            f"GUID reconciliation DB error"
        )
        return 0
    finally:
        db.close()

def upsert_and_advance_month(
    rows, model_class, upsert_fn,
    company_name, voucher_type, month_str, engine,
    chunk_max_alter_id=0,
):
    """
    Upsert voucher rows for one snapshot chunk then atomically advance SyncState.
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
            if last_alter_id > (state.last_alter_id or 0):
                state.last_alter_id = last_alter_id
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
    """
    Generic master upsert for Item / Ledger (alter_id-gated field update).
    FIX: now returns 5-tuple (ins, upd, unch, skp, 0) to be consistent with
    all other upsert functions — was returning 4-tuple which caused an unpack
    error if the caller ever used the 5-value form.
    """
    if not rows:
        logger.warning(f"No rows to upsert for {model_class.__tablename__}")
        return 0, 0, 0, 0, 0

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

    return inserted, updated, unchanged, skipped, 0

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
    """
    Full delete + full re-insert for trial balance.

    Trial balance is a point-in-time report — balances change every time any
    voucher is posted in Tally. There is no reliable alter_id per row to gate
    incremental updates on.

    Strategy (mirrors upsert_outstanding):
      1. DELETE all existing rows for this company + date window in one shot.
      2. INSERT all fresh rows from Tally.
      Both steps in a single transaction — if insert fails, delete rolls back.

    This guarantees balances are always current and removes stale ledger rows
    (e.g. ledgers deleted in Tally that would otherwise linger in the DB).
    """
    if not rows:
        logger.warning("No rows to upsert for trial balance")
        return

    company_name = rows[0].get('company_name', '')
    start_date   = rows[0].get('start_date')
    end_date     = rows[0].get('end_date')

    db = _get_session(engine)
    inserted = 0
    try:
        deleted = db.query(TrialBalance).filter_by(
            company_name = company_name,
            start_date   = start_date,
            end_date     = end_date,
        ).delete(synchronize_session='fetch')

        skipped = 0
        for row in rows:
            if not row.get('guid'):
                skipped += 1
                continue
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
                material_centre  = row.get('material_centre', ''),
            ))
            inserted += 1

        db.commit()
        logger.info(
            f"Trial balance full-replace | company={company_name} | "
            f"window={start_date}→{end_date} | "
            f"deleted={deleted} inserted={inserted} skipped={skipped}"
        )

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

def _propagate_ledger_rename(
    db,
    company_name: str,
    old_name:     str,
    new_name:     str,
) -> int:
    """
    Cascade a ledger rename to EVERY table that stores a ledger name as text.

    Called INSIDE the upsert_ledgers session so the propagation is atomic with
    the ledger master update — if anything rolls back, all voucher table updates
    roll back too.

    Column mapping (derived from models):
      party_name  column:
        • SalesVoucher        — customer/supplier ledger on inventory vouchers
        • PurchaseVoucher
        • CreditNote
        • DebitNote
        • OutstandingData   — party ledger on outstanding receivables

      ledger_name column:
        • ReceiptVoucher      — every entry row (party + non-party)
        • PaymentVoucher
        • JournalVoucher
        • ContraVoucher
        • TrialBalance        — ledger row in trial balance snapshot

    Returns the total number of rows updated across all tables.

    ISOLATION GUARANTEE:
      Filtered strictly by company_name + ledger name (exact, case-sensitive).
      Only rows belonging to THIS company with THIS exact old name are updated.
      Another company's ledger with the same name is never touched.
    """
    if not old_name or not new_name or old_name == new_name:
        return 0
    total = 0

    for model, col_attr in (
        (SalesVoucher,      SalesVoucher.party_name),
        (PurchaseVoucher,   PurchaseVoucher.party_name),
        (CreditNote,        CreditNote.party_name),
        (DebitNote,         DebitNote.party_name),
        (OutstandingData, OutstandingData.party_name),
    ):
        filters = [
            model.company_name == company_name,
            col_attr           == old_name,
        ]
        if hasattr(model, 'is_deleted'):
            filters.append(model.is_deleted == 'No')

        count = (
            db.query(model)
            .filter(*filters)
            .update(
                {'company_name': company_name, 'party_name': new_name},
                synchronize_session='fetch',
            )
        )
        if count:
            logger.info(
                f"[{company_name}] ledger rename cascade | "
                f"{model.__tablename__}.party_name | "
                f"'{old_name}' → '{new_name}' | rows={count}"
            )
        total += count

    for model, col_attr in (
        (ReceiptVoucher, ReceiptVoucher.ledger_name),
        (PaymentVoucher, PaymentVoucher.ledger_name),
        (JournalVoucher, JournalVoucher.ledger_name),
        (ContraVoucher,  ContraVoucher.ledger_name),
        (TrialBalance,   TrialBalance.ledger_name),
    ):
        filters = [
            model.company_name == company_name,
            col_attr           == old_name,
        ]
        if hasattr(model, 'is_deleted'):
            filters.append(model.is_deleted == 'No')

        count = (
            db.query(model)
            .filter(*filters)
            .update(
                {'company_name': company_name, 'ledger_name': new_name},
                synchronize_session='fetch',
            )
        )
        if count:
            logger.info(
                f"[{company_name}] ledger rename cascade | "
                f"{model.__tablename__}.ledger_name | "
                f"'{old_name}' → '{new_name}' | rows={count}"
            )
        total += count

    return total

def _propagate_item_rename(db, company_name: str, old_name: str, new_name: str) -> int:
    if not old_name or not new_name or old_name == new_name:
        return 0
    total = 0
    for model in (SalesVoucher, PurchaseVoucher, CreditNote, DebitNote):
        count = (
            db.query(model)
            .filter(
                model.company_name == company_name,
                model.item_name    == old_name,
                model.is_deleted   == 'No',
            )
            .update({'item_name': new_name}, synchronize_session='fetch')
        )
        if count:
            logger.info(
                f"[{company_name}] item rename cascade | "
                f"{model.__tablename__}.item_name | "
                f"'{old_name}' \u2192 '{new_name}' | rows={count}"
            )
        total += count
    return total

def _detect_company_migration(rows: list, company_name: str, db) -> tuple[bool, str, str]:
    new_raguid = ''
    for row in rows:
        v = (row.get('remote_alt_guid') or '').strip()
        if v:
            new_raguid = v
            break
    if not new_raguid:
        for row in rows:
            guid = (row.get('guid') or '').strip()
            if guid and guid.count('-') >= 5:
                new_raguid = guid.rsplit('-', 1)[0]
                break
    if not new_raguid:
        return False, '', ''
    sample = db.query(Item).filter(
        Item.company_name    == company_name,
        Item.remote_alt_guid != '',
    ).first()
    if not sample:
        return False, '', new_raguid
    old_raguid = (sample.remote_alt_guid or '').strip()
    if old_raguid and old_raguid != new_raguid:
        return True, old_raguid, new_raguid
    return False, old_raguid, new_raguid

def upsert_items(rows, engine):
    if not rows:
        logger.warning("No rows to upsert for items")
        return False

    db = _get_session(engine)
    inserted = updated = unchanged = skipped = 0

    def _safe(row):
        return {
            'company_name'        : _t(row.get('company_name'),              255),
            'item_name'           : _t(row.get('item_name'),                 500),
            'parent_group'        : _t(row.get('parent_group'),              255),
            'category'            : _t(row.get('category'),                  255),
            'base_units'          : _t(row.get('base_units'),                100),
            'gst_type_of_supply'  : _t(row.get('gst_type_of_supply'),        100),
            'hsn_code'            : _t(row.get('hsn_code', ''),               20),
            'gst_applicable_from' : _t(row.get('gst_applicable_from', ''),    20),
            'taxability'          : _t(row.get('taxability', ''),              50),
            'cgst_rate'           : row.get('cgst_rate',        0.0),
            'sgst_rate'           : row.get('sgst_rate',        0.0),
            'igst_rate'           : row.get('igst_rate',        0.0),
            'cess_rate'           : row.get('cess_rate',        0.0),
            'opening_balance'     : row.get('opening_balance',  0.0),
            'opening_rate'        : row.get('opening_rate',     0.0),
            'opening_value'       : row.get('opening_value',    0.0),
            'entered_by'          : _t(row.get('entered_by'),                255),
            'is_deleted'          : _t(row.get('is_deleted'),                 10),
            'guid'                : _t(row.get('guid'),                      100),
            'remote_alt_guid'     : _t(row.get('remote_alt_guid'),           100),
            'alter_id'            : row.get('alter_id', 0),
            'material_centre'     : _t(row.get('material_centre'),           255),
        }

    try:
        company_name = rows[0].get('company_name', '') if rows else ''

        is_migration, old_raguid, new_raguid = _detect_company_migration(rows, company_name, db)

        if is_migration:
            logger.warning(
                f"[{company_name}] TALLY COMPANY MIGRATION DETECTED | "
                f"old={old_raguid} | new={new_raguid} | wiping old rows and re-inserting."
            )
            deleted_count = (
                db.query(Item)
                .filter(Item.company_name == company_name)
                .delete(synchronize_session='fetch')
            )
            db.flush()
            logger.info(f"[{company_name}] Deleted {deleted_count} stale item rows (old: {old_raguid})")

        for row in rows:
            if not row.get('guid'):
                skipped += 1
                continue

            safe = _safe(row)
            name_changed = False

            existing = db.query(Item).filter_by(
                item_name    = safe['item_name'],
                company_name = safe['company_name'],
            ).first()

            if not existing:
                existing = db.query(Item).filter_by(
                    guid         = safe['guid'],
                    company_name = safe['company_name'],
                ).first()
                if existing:
                    old_item_name = existing.item_name
                    new_item_name = safe['item_name']
                    name_changed = (
                        bool(old_item_name)
                        and bool(new_item_name)
                        and old_item_name.strip() != new_item_name.strip()
                    )
                    if name_changed:
                        renamed = _propagate_item_rename(
                            db           = db,
                            company_name = existing.company_name,
                            old_name     = old_item_name,
                            new_name     = new_item_name,
                        )
                        logger.info(
                            f"[{company_name}] Item RENAMED | "
                            f"'{old_item_name}' \u2192 '{new_item_name}' | "
                            f"guid={safe['guid']} | voucher rows updated={renamed}"
                        )

            if existing:
                new_alter_id     = int(safe['alter_id'])
                old_alter_id     = int(existing.alter_id or 0)
                incoming_deleted = safe.get('is_deleted') == 'Yes'

                guid_changed = existing.guid != safe['guid']
                if guid_changed:
                    old_guid = existing.guid
                    existing.guid = safe['guid']
                    logger.info(
                        f"[{company_name}] Item GUID updated | "
                        f"'{safe['item_name']}' | {old_guid} \u2192 {safe['guid']}"
                    )

                if (
                    new_alter_id > old_alter_id
                    or (incoming_deleted and existing.is_deleted != 'Yes')
                    or name_changed
                    or guid_changed
                ):
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
        if is_migration:
            logger.warning(
                f"[{company_name}] Migration complete | "
                f"{old_raguid} \u2192 {new_raguid} | inserted={inserted} skipped={skipped}"
            )
        else:
            _log_result("Items upsert", inserted, updated, unchanged, skipped)

        return is_migration

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
    """
    Upsert Ledger master rows.

    FIX: CDC delete handling — when Tally marks a ledger is_deleted='Yes'
    via CDC, the is_deleted field must be written even when alter_id hasn't
    changed (same fix as items).  Also added ledger_name update support:
    a ledger rename arrives as an altered row with the same GUID but a new
    NAME — the old code correctly overwrites ledger_name because it's in the
    safe dict, but only when alter_id > old.  This is correct; no change
    needed here for the rename case.  The only new guard is for the delete
    flag exactly mirroring the item fix above.
    """
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
            'material_centre'       : _t(row.get('material_centre'), 255),
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
                new_alter_id     = int(safe['alter_id'])
                old_alter_id     = int(existing.alter_id or 0)
                incoming_deleted = safe.get('is_deleted') == 'Yes'

                old_ledger_name = existing.ledger_name
                new_ledger_name = safe.get('ledger_name', '')
                name_changed = (
                    old_ledger_name
                    and new_ledger_name
                    and old_ledger_name.lower().strip() != new_ledger_name.lower().strip()
                    and not incoming_deleted
                )
                if name_changed:
                    renamed = _propagate_ledger_rename(
                        db           = db,
                        company_name = existing.company_name,
                        old_name     = old_ledger_name,
                        new_name     = new_ledger_name,
                    )
                    logger.info(
                        f"[{existing.company_name}] Ledger rename detected | "
                        f"guid={safe['guid']} | "
                        f"'{old_ledger_name}' → '{new_ledger_name}' | "
                        f"voucher rows updated={renamed}"
                    )

                if new_alter_id > old_alter_id or (incoming_deleted and existing.is_deleted != 'Yes') or name_changed:
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

def reconcile_deleted_masters_in_db(
    company_name: str,
    master_type:  str,
    tally_guids:  dict,
    engine,
) -> int:
    """
    Hard delete master rows (Item or Ledger) whose GUID is no longer in Tally.

    Physically removes the row — same strategy as voucher reconciliation.
    Called after every CDC sync for items and ledgers.

    SAFE FAILURE CONTRACT:
      tally_guids must be non-empty — caller enforces this.
      Returns 0 on any DB error (never raises).
    """
    model_class = Item if master_type == 'items' else Ledger
    db = _get_session(engine)
    try:
        tally_guid_set = set(tally_guids.keys())

        active_rows = db.query(model_class).filter(
            model_class.company_name == company_name,
        ).all()

        hard_deleted = 0
        for row in active_rows:
            if row.guid and row.guid not in tally_guid_set:
                db.delete(row)
                hard_deleted  += 1
                logger.info(
                    f"[{company_name}][{master_type}] "
                    f"Hard deleted master guid={row.guid} "
                    f"name={getattr(row, 'item_name', None) or getattr(row, 'ledger_name', None)} "
                    f"(not found in Tally GUID list)"
                )

        if hard_deleted:
            db.commit()
            logger.info(
                f"[{company_name}][{master_type}] "
                f"Master GUID reconciliation: hard deleted {hard_deleted} rows not in Tally ✓"
            )
        else:
            logger.debug(
                f"[{company_name}][{master_type}] "
                f"Master GUID reconciliation: all {len(active_rows)} rows confirmed in Tally ✓"
            )

        return hard_deleted

    except Exception:
        db.rollback()
        logger.exception(
            f"[{company_name}][{master_type}] reconcile_deleted_masters_in_db error"
        )
        return 0
    finally:
        db.close()

def upsert_outstanding(rows, engine):
    """
    Full-replace for outstanding — point-in-time snapshot.
    Deletes all existing rows for the company then bulk-inserts fresh ones.
    Both in one transaction — if insert fails, delete rolls back too.

    parent_group is enriched from the Ledger table at upsert time:
      (company_name, ledger_name) → parent_group

    Compound key (company_name + ledger_name) ensures that the same ledger
    name across different companies never collides in the lookup map.
    Matching is case-insensitive and whitespace-tolerant on both sides.
    Safe for 40+ companies running sequentially — single worker thread,
    one engine shared, each call fully isolated by company_name filter.
    """
    if not rows:
        logger.warning("No rows to upsert for outstanding")
        return

    company_name = rows[0].get('company_name', '')
    db = _get_session(engine)
    inserted = 0
    try:
        party_names = {r['party_name'] for r in rows if r.get('party_name')}

        ledger_rows = (
            db.query(Ledger.company_name, Ledger.ledger_name, Ledger.parent_group)
            .filter(Ledger.company_name == company_name)
            .all()
        )

        if not ledger_rows:
            logger.warning(
                f"[{company_name}] No ledger rows found with exact company_name match — "
                f"falling back to case-insensitive search"
            )
            ledger_rows = (
                db.query(Ledger.company_name, Ledger.ledger_name, Ledger.parent_group)
                .filter(Ledger.company_name.ilike(company_name.strip()))
                .all()
            )

        parent_group_map: dict[tuple, str] = {
            (row.company_name.strip().lower(), row.ledger_name.strip().lower()): (row.parent_group or '')
            for row in ledger_rows
        }

        company_key = company_name.strip().lower()
        matched   = sum(1 for p in party_names if (company_key, p.strip().lower()) in parent_group_map)
        unmatched = [p for p in party_names if (company_key, p.strip().lower()) not in parent_group_map]

        logger.info(
            f"Outstanding upsert | company={company_name} | "
            f"ledger parent_group lookup: {len(party_names)} parties → "
            f"matched={matched} unmatched={len(unmatched)}"
        )
        if unmatched:
            logger.warning(
                f"Unmatched parties (no ledger master) [{company_name}]: {unmatched}"
            )

        db.query(OutstandingData).filter_by(
            company_name=company_name
        ).delete(synchronize_session='fetch')

        for row in rows:
            if not row.get('party_name'):
                continue
            party = row.get('party_name', '')
            db.add(OutstandingData(
                company_name    = row.get('company_name'),
                party_name      = party,
                parent_group    = parent_group_map.get((company_key, party.strip().lower()), ''),
                bill_name       = row.get('bill_name'),
                bill_id         = row.get('bill_id'),
                bill_date       = row.get('bill_date'),
                due_date        = row.get('due_date'),
                currency        = row.get('currency', 'INR'),
                exchange_rate   = row.get('exchange_rate', 1.0),
                amount          = row.get('amount', 0.0),
                material_centre = row.get('material_centre', ''),
            ))
            inserted += 1

        db.commit()
        logger.info(f"Outstanding full-replace | company={company_name} | inserted={inserted}")

    except Exception:
        db.rollback()
        logger.exception("Error upserting outstanding")
        raise
    finally:
        db.close()

def cleanup_db_locks(company_name: str = '') -> None:
    with _DB_COMPANY_LOCKS_MUTEX:
        count = len(_DB_COMPANY_LOCKS)
        _DB_COMPANY_LOCKS.clear()
    if count:
        prefix = f"[{company_name}] " if company_name else ""
        logger.debug(f"{prefix}cleanup_db_locks: cleared {count} DB company lock(s)")

def save_company_last_sync(company_name: str, engine) -> None:
    from database.models.scheduler_config import CompanySchedulerConfig
    db = _get_session(engine)
    try:
        row = db.query(CompanySchedulerConfig).filter_by(
            company_name=company_name
        ).first()
        if row:
            row.last_sync_time = datetime.utcnow()
            db.commit()
            logger.info(f"[{company_name}] scheduler last_sync_time saved (UTC): {row.last_sync_time}")
        else:
            logger.warning(f"[{company_name}] save_company_last_sync: no scheduler config row found")
    except Exception:
        db.rollback()
        logger.exception(f"[{company_name}] Failed to save scheduler last_sync_time")
    finally:
        db.close()

def get_company_last_sync(company_name: str, engine) -> datetime | None:
    from database.models.scheduler_config import CompanySchedulerConfig
    db = _get_session(engine)
    try:
        row = db.query(CompanySchedulerConfig).filter_by(
            company_name=company_name
        ).first()
        return row.last_sync_time if row else None
    except Exception:
        logger.exception(f"[{company_name}] Failed to read scheduler last_sync_time")
        return None
    finally:
        db.close()

def validate_sync_count(
    company_name:   str,
    voucher_type:   str,
    expected_count: int,
    engine,
    from_date:      str = None,
    to_date:        str = None,
) -> dict:
    """
    Compare active DB row count against an expected count from Tally.

    Call this after each upsert to verify 'na zyada, na kam'.
    Returns dict: {db_count, expected_count, match, delta}.

    Note: expected_count should be UNIQUE voucher count (GUID level),
    not total rows — one Sales voucher can have N item rows in DB.
    """
    model_class = {**INVENTORY_MODEL_MAP, **LEDGER_MODEL_MAP}.get(voucher_type)
    if model_class is None:
        return {'error': f'Unknown voucher_type: {voucher_type}'}

    db = _get_session(engine)
    try:
        query = db.query(model_class).filter(
            model_class.company_name == company_name,
            model_class.is_deleted   == 'No',
        )
        if from_date:
            from datetime import datetime as _dt2
            fd = _dt2.strptime(from_date, '%Y%m%d').date()
            query = query.filter(model_class.date >= fd)
        if to_date:
            from datetime import datetime as _dt2
            td = _dt2.strptime(to_date, '%Y%m%d').date()
            query = query.filter(model_class.date <= td)

        db_count = query.count()
        match    = (db_count == expected_count)
        delta    = db_count - expected_count

        if not match:
            logger.warning(
                f"[{company_name}][{voucher_type}] COUNT MISMATCH | "
                f"db={db_count} expected={expected_count} delta={delta:+d} | "
                f"range={from_date}→{to_date}"
            )
        else:
            logger.debug(
                f"[{company_name}][{voucher_type}] count OK | "
                f"db={db_count} | range={from_date}→{to_date}"
            )

        return {
            'company_name'  : company_name,
            'voucher_type'  : voucher_type,
            'db_count'      : db_count,
            'expected_count': expected_count,
            'match'         : match,
            'delta'         : delta,
            'from_date'     : from_date,
            'to_date'       : to_date,
        }

    except Exception:
        logger.exception(f"[{company_name}][{voucher_type}] validate_sync_count error")
        return {'error': 'validation failed'}
    finally:
        db.close()

def resync_company(company_name: str, engine) -> dict:
    """
    Full resync for a single company.

    Steps
    -----
    1. Delete every data row that belongs to ``company_name`` across all
       tables that carry a ``company_name`` column:
           Ledger, Item,
           SalesVoucher, PurchaseVoucher, CreditNote, DebitNote,
           ReceiptVoucher, PaymentVoucher, JournalVoucher, ContraVoucher,
           TrialBalance, OutstandingData
    2. Reset (delete) all SyncState rows for this company so that the next
       sync treats everything as a fresh initial snapshot (is_initial_done=False,
       last_alter_id=0).
    3. Leave the Company master row intact — the company record in ``companies``
       table is NOT deleted because the caller still needs its config
       (credentials, data_path, schedule, etc.).

    Returns a dict with per-table deleted counts and a 'success' bool.
    """
    db = _get_session(engine)
    counts: dict[str, int] = {}
    try:
        logger.info(f"[{company_name}] RESYNC START — deleting all company data")

        data_models = [
            ("Ledger",          Ledger),
            ("Item",            Item),
            ("SalesVoucher",    SalesVoucher),
            ("PurchaseVoucher", PurchaseVoucher),
            ("CreditNote",      CreditNote),
            ("DebitNote",       DebitNote),
            ("ReceiptVoucher",  ReceiptVoucher),
            ("PaymentVoucher",  PaymentVoucher),
            ("JournalVoucher",  JournalVoucher),
            ("ContraVoucher",   ContraVoucher),
            ("TrialBalance",    TrialBalance),
            ("OutstandingData", OutstandingData),
        ]

        for label, model in data_models:
            n = (
                db.query(model)
                .filter(model.company_name == company_name)
                .delete(synchronize_session="fetch")
            )
            counts[label] = n
            logger.info(f"[{company_name}] Deleted {n:>6} rows from {label}")

        n_state = (
            db.query(SyncState)
            .filter(SyncState.company_name == company_name)
            .delete(synchronize_session="fetch")
        )
        counts["SyncState"] = n_state
        logger.info(f"[{company_name}] Deleted {n_state} SyncState rows — next sync will be a full snapshot")

        db.commit()

        total = sum(counts.values())
        logger.info(
            f"[{company_name}] RESYNC COMPLETE — "
            f"total rows deleted: {total} | breakdown: {counts}"
        )
        return {"success": True, "company_name": company_name, "deleted": counts}

    except Exception:
        db.rollback()
        logger.exception(f"[{company_name}] RESYNC FAILED — rolled back all changes")
        raise
    finally:
        db.close()
