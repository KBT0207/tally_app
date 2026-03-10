from datetime import datetime
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

import pandas as pd
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

def _upsert_inventory_voucher_in_session(rows, model_class, db):
    inserted = updated = unchanged = skipped = deleted = 0

    update_fields = [
        'date', 'voucher_number', 'reference', 'voucher_type',
        'party_name', 'gst_number', 'e_invoice_number', 'eway_bill',
        'item_name', 'quantity', 'unit', 'alt_qty', 'alt_unit',
        'batch_no', 'mfg_date', 'exp_date', 'hsn_code', 'gst_rate',
        'rate', 'amount', 'discount',
        'cgst_amt', 'sgst_amt', 'igst_amt',
        'freight_amt', 'dca_amt', 'cf_amt', 'other_amt', 'total_amt',
        'currency', 'exchange_rate', 'narration',
        'alter_id', 'master_id', 'change_status', 'is_deleted',
    ]

    for row in rows:
        if not row.get('guid'):
            skipped += 1
            continue

        is_deleted_flag = row.get('is_deleted', 'No')

        if is_deleted_flag == 'Yes':
            affected = db.query(model_class).filter_by(
                guid         = row['guid'],
                company_name = row['company_name'],
            ).all()
            for record in affected:
                record.is_deleted    = 'Yes'
                record.change_status = row.get('change_status', 'Deleted')
                record.alter_id      = row.get('alter_id', record.alter_id)
            deleted += len(affected)
            continue

        existing = db.query(model_class).filter_by(
            guid         = row['guid'],
            company_name = row['company_name'],
            item_name    = row.get('item_name', ''),
            batch_no     = row.get('batch_no', ''),
        ).first()

        if existing:
            if int(row.get('alter_id', 0)) > int(existing.alter_id or 0):
                _log_changes("inventory_voucher UPDATE", existing, update_fields, row)
                for field in update_fields:
                    setattr(existing, field, row.get(field))
                updated += 1
            else:
                unchanged += 1
        else:
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
                total_amt        = row.get('total_amt', 0.0),
                currency         = row.get('currency', 'INR'),
                exchange_rate    = row.get('exchange_rate', 1.0),
                narration        = row.get('narration'),
                guid             = row.get('guid'),
                alter_id         = row.get('alter_id', 0),
                master_id        = row.get('master_id'),
                change_status    = row.get('change_status'),
                is_deleted       = row.get('is_deleted', 'No'),
            ))
            inserted += 1

    return inserted, updated, unchanged, skipped, deleted

def _upsert_ledger_voucher_in_session(rows, model_class, db):
    inserted = updated = unchanged = skipped = deleted = 0

    update_fields = [
        'date', 'voucher_type', 'voucher_number', 'reference',
        'amount', 'amount_type', 'currency', 'exchange_rate',
        'narration', 'alter_id', 'master_id', 'change_status', 'is_deleted',
    ]

    for row in rows:
        if not row.get('guid'):
            skipped += 1
            continue

        is_deleted_flag = row.get('is_deleted', 'No')

        if is_deleted_flag == 'Yes':
            affected = db.query(model_class).filter_by(
                guid         = row['guid'],
                company_name = row['company_name'],
            ).all()
            for record in affected:
                record.is_deleted    = 'Yes'
                record.change_status = row.get('change_status', 'Deleted')
                record.alter_id      = row.get('alter_id', record.alter_id)
            deleted += len(affected)
            continue

        existing = db.query(model_class).filter_by(
            guid         = row['guid'],
            company_name = row['company_name'],
            ledger_name  = row.get('ledger_name', ''),
        ).first()

        if existing:
            if int(row.get('alter_id', 0)) > int(existing.alter_id or 0):
                _log_changes("ledger_voucher UPDATE", existing, update_fields, row)
                for field in update_fields:
                    setattr(existing, field, row.get(field))
                updated += 1
            else:
                unchanged += 1
        else:
            db.add(model_class(
                company_name   = row.get('company_name'),
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
                guid           = row.get('guid'),
                alter_id       = row.get('alter_id', 0),
                master_id      = row.get('master_id'),
                change_status  = row.get('change_status'),
                is_deleted     = row.get('is_deleted', 'No'),
            ))
            inserted += 1

    return inserted, updated, unchanged, skipped, deleted

def upsert_and_advance_month(rows, model_class, upsert_fn, company_name, voucher_type, month_str, engine, chunk_max_alter_id=0):
    """
    Upsert voucher rows for one chunk then atomically advance SyncState.

    The SyncState update is protected by a per-company lock so that two
    voucher worker threads for the same company cannot overwrite each other's
    last_synced_month / last_alter_id.  The (slow) voucher upsert itself runs
    outside the lock so other voucher types are not blocked.
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

    # Now update SyncState in a separate session, protected by the company lock.
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
                state.last_sync_time    = datetime.utcnow()
                if chunk_max_alter_id > (state.last_alter_id or 0):
                    state.last_alter_id = chunk_max_alter_id
            else:
                db2.add(SyncState(
                    company_name      = company_name,
                    voucher_type      = voucher_type,
                    last_alter_id     = chunk_max_alter_id,
                    is_initial_done   = False,
                    last_synced_month = month_str,
                    last_sync_time    = datetime.utcnow(),
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

def update_sync_state(company_name, voucher_type, last_alter_id, engine, last_synced_month=None, is_initial_done=True):
    db = _get_session(engine)
    try:
        state = db.query(SyncState).filter_by(
            company_name = company_name,
            voucher_type = voucher_type,
        ).first()

        if state:
            state.last_alter_id   = last_alter_id
            state.is_initial_done = is_initial_done
            state.last_sync_time  = datetime.utcnow()
            if last_synced_month is not None:
                state.last_synced_month = last_synced_month
        else:
            db.add(SyncState(
                company_name      = company_name,
                voucher_type      = voucher_type,
                last_alter_id     = last_alter_id,
                is_initial_done   = is_initial_done,
                last_synced_month = last_synced_month,
                last_sync_time    = datetime.utcnow(),
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
        db.close()   # ← always close, even on exception

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


def company_import_db(data, engine):
    db = _get_session(engine)
    try:
        logger.info("Starting company import process")
        df = pd.DataFrame(data)
        df = df[df['name'].notna() & (df['name'].str.strip() != '')]
        logger.info(f"Records after name filtering: {len(df)}")

        date_cols = ['starting_from', 'books_from', 'audited_upto']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], format="%Y%m%d", errors="coerce").dt.date

        inserted = updated = unchanged = skipped = 0
        fields   = ["name", "formal_name", "company_number", "starting_from", "books_from", "audited_upto"]

        for _, row in df.iterrows():
            if not row.get("guid"):
                skipped += 1
                logger.warning("Skipped record due to missing GUID")
                continue

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
    Full-replace upsert for Sundry Debtors outstanding.

    Outstanding is a point-in-time snapshot (not a voucher ledger), so the
    strategy is:
      1. Delete all existing rows for this company + date range.
      2. Bulk-insert the freshly parsed rows.

    This avoids complex key matching on bills that may be partially paid,
    renamed, or closed since the last sync.

    Unique natural key used for existence check:
        company_name + party_name + voucher_number + bill_name
    """
    if not rows:
        logger.warning("No rows to upsert for debtor outstanding")
        return

    db = _get_session(engine)
    inserted = updated = unchanged = skipped = 0

    update_fields = [
        'party_name', 'voucher_type', 'bill_type',
        'date', 'bill_date', 'due_date',
        'reference', 'currency', 'exchange_rate', 'amount', 'narration',
    ]

    try:
        for row in rows:
            # Natural key: company + voucher_number + bill_name identify one bill line
            if not row.get('voucher_number'):
                skipped += 1
                continue

            existing = db.query(DebtorOutstanding).filter_by(
                company_name   = row['company_name'],
                voucher_number = row['voucher_number'],
                bill_name      = row.get('bill_name', ''),
            ).first()

            if existing:
                changed = any(
                    str(getattr(existing, f, None)) != str(row.get(f))
                    for f in update_fields
                )
                if changed:
                    for f in update_fields:
                        setattr(existing, f, row.get(f))
                    updated += 1
                else:
                    unchanged += 1
            else:
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
        _log_result("Debtor outstanding upsert", inserted, updated, unchanged, skipped)

    except Exception:
        db.rollback()
        logger.exception("Error upserting debtor outstanding")
        raise
    finally:
        db.close()