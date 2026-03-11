import xml.etree.ElementTree as ET
import re
import traceback
import time
from datetime import datetime
from logging_config import logger


# ──────────────────────────────────────────────────────────────────────────────
# Timer utility
# ──────────────────────────────────────────────────────────────────────────────

class ProcessingTimer:
    def __init__(self, process_name):
        self.process_name = process_name
        self.start_time   = None
        self.end_time     = None

    def __enter__(self):
        self.start_time = time.time()
        logger.info(f"Started: {self.process_name}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        elapsed = self.end_time - self.start_time
        logger.info(f"Completed: {self.process_name} - Time taken: {elapsed:.2f} seconds")


# ──────────────────────────────────────────────────────────────────────────────
# Text / XML helpers
# ──────────────────────────────────────────────────────────────────────────────

def clean_text(text):
    if not text:
        return ""
    text = str(text).replace('&#13;&#10;', ' ').replace('&#13;', ' ').replace('&#10;', ' ')
    text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
    return re.sub(r'\s+', ' ', text).strip()


def sanitize_xml_content(content):
    if content is None:
        logger.error("XML content is None")
        return ""

    if isinstance(content, bytes):
        try:
            content = content.decode('utf-8')
        except UnicodeDecodeError:
            content = content.decode('latin-1')

    content = str(content)
    content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', content)
    content = re.sub(r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;)', '&amp;', content)
    return content


def convert_to_float(value):
    if value is None or value == "":
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except Exception:
        return 0.0


# ──────────────────────────────────────────────────────────────────────────────
# Date helpers
# ──────────────────────────────────────────────────────────────────────────────

def parse_tally_date_formatted(date_str):
    if not date_str or date_str.strip() == "":
        return None
    try:
        return datetime.strptime(date_str.strip(), '%Y%m%d').strftime('%Y-%m-%d')
    except Exception:
        return None


def parse_expiry_date(exp_date_text):
    if not exp_date_text or exp_date_text.strip() == "":
        return ""
    exp_date_text = str(exp_date_text).strip()
    try:
        for fmt in ['%d-%b-%y', '%d-%b-%Y']:
            try:
                return datetime.strptime(exp_date_text, fmt).strftime('%Y-%m-%d')
            except Exception:
                continue
        return exp_date_text
    except Exception:
        return exp_date_text


# ──────────────────────────────────────────────────────────────────────────────
# FCY-aware amount / rate parsers
# ──────────────────────────────────────────────────────────────────────────────
#
# Tally FCY strings look like:
#   RATE  : "$14.00 = ? 84.5/$ = ? 1183/box"
#           "$14.00 = ? 1/$ = ? 14.00/box"
#   AMOUNT: "$61600.00 @ ? 1/$ = ? 61600.00"
#           "-$61600.00 @ ? 1/$ = -? 61600.00"   ← ledger (negative) side
#
# Strategy
# ─────────
#   • Rate         → take the number immediately AFTER the currency symbol
#   • Amount       → take the FIRST numeric value (the FCY amount Tally prints first)
#   • Exchange rate → look for the "? <n>/<symbol>" pattern Tally embeds
# ──────────────────────────────────────────────────────────────────────────────

# Currency symbols Tally uses in FCY strings
_CURRENCY_SYMBOLS = r'[\$€£¥₹₨₩₱₽₺₪₦฿₫]'

# Map of symbol → ISO code
_SYMBOL_TO_ISO = {
    '$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY',
    '₹': 'INR', '₨': 'INR', '₩': 'KRW', '₱': 'PHP',
    '₽': 'RUB', '₺': 'TRY', '₪': 'ILS', '₦': 'NGN',
    '฿': 'THB', '₫': 'VND',
}


def _detect_currency(text: str) -> str:
    """Return ISO currency code from a Tally amount/rate string. Defaults to INR."""
    if not text:
        return 'INR'
    for sym, iso in _SYMBOL_TO_ISO.items():
        if sym in text and iso != 'INR':
            return iso
    return 'INR'


def _parse_fcy_rate(raw: str) -> float:
    """
    Extract per-unit FCY rate from Tally RATE field.

    Tally FCY rate string formats:
      "$14.00/box"                      → 14.0
      "$14.00 = ? 1/$ = ? 14.00/box"   → 14.0  (exchange rate = 1)
      "$14.00 = ? 84.5/$ = ? 1183/box" → 14.0  (exchange rate = 84.5, home = 1183)
      "14.00/box"                       → 14.0  (plain INR rate)

    BUG FIXED: The old regex r'(-?[\d,]+\.?\d*)\s*/\s*\w' matched "1183/box" on
    FCY strings like "$14.00 = ? 84.5/$ = ? 1183/box", returning the INR
    home-rate (1183) instead of the correct FCY per-unit rate ($14.00).
    Root cause: \w skips '$' (not a word char), so the regex found the last
    /alphabetic match instead of the first /currency match.

    Fix strategy:
      1. FCY  → take the number immediately after the currency symbol ($14.00 → 14.0)
      2. INR  → take the number immediately before /alphabetic-unit (14.00/box → 14.0)
      3. Fallback → first number in string
    """
    if not raw:
        return 0.0
    raw = str(raw).strip()

    # Step 1: FCY — number immediately after currency symbol
    #   "$14.00 = ? 84.5/$ = ? 1183/box"  →  grabs 14.0  ✓
    #   "$14.00/box"                        →  grabs 14.0  ✓
    m = re.search(_CURRENCY_SYMBOLS + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return abs(convert_to_float(m.group(1)))

    # Step 2: INR — number immediately before /alphabetic-unit
    #   "14.00/box"  →  14.0  ✓
    m = re.search(r'([\d,]+\.?\d*)\s*/\s*[a-zA-Z]', raw)
    if m:
        return abs(convert_to_float(m.group(1)))

    # Fallback: first number in string
    m = re.search(r'([\d,]+\.?\d*)', raw)
    if m:
        return abs(convert_to_float(m.group(1)))

    return 0.0


def _parse_fcy_amount(raw: str) -> float:
    """
    Extract the FCY (foreign currency) amount from a Tally AMOUNT field.
    Always returns an absolute value — sign is handled by the caller.

    "$61600.00 @ ? 1/$ = ? 61600.00"    → 61600.0
    "-$61600.00 @ ? 1/$ = -? 61600.00"  → 61600.0
    "61600.00"                           → 61600.0
    """
    if not raw:
        return 0.0
    raw = str(raw).strip()

    # First number after optional '-' and optional currency symbol
    m = re.search(r'-?\s*' + _CURRENCY_SYMBOLS + r'?\s*([\d,]+\.?\d*)', raw)
    if m:
        return abs(convert_to_float(m.group(1)))

    # Fallback: just first number
    m = re.search(r'([\d,]+\.?\d*)', raw)
    if m:
        return abs(convert_to_float(m.group(1)))

    return 0.0


def _parse_fcy_exchange_rate(raw_amount: str) -> float:
    """
    Extract exchange rate embedded in Tally FCY amount/rate string.

    "$61600.00 @ ? 1/$ = ? 61600.00"       → 1.0
    "$615.00 @ ? 84.5/$ = ? 51997.50"      → 84.5
    "$14.00 = ? 84.5/$ = ? 1183/box"       → 84.5
    Returns 1.0 if not found.
    """
    if not raw_amount:
        return 1.0
    # Pattern: ? <number>/<currency-symbol>
    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/' + _CURRENCY_SYMBOLS, raw_amount)
    if m:
        rate = convert_to_float(m.group(1))
        return rate if rate > 0 else 1.0
    return 1.0


def _is_fcy_string(text: str) -> bool:
    """True if the text contains a foreign-currency symbol (not INR/?)."""
    if not text:
        return False
    for sym in _SYMBOL_TO_ISO:
        if sym in text and _SYMBOL_TO_ISO[sym] != 'INR':
            return True
    return False


def extract_numeric_amount(text):
    """
    Legacy helper kept for backward-compat (used in trial-balance / total_amt).
    For FCY vouchers, prefer _parse_fcy_amount() directly.
    """
    if not text:
        return "0"
    text = str(text)

    # Tally FCY: prefer the FIRST numeric value (foreign amount)
    if _is_fcy_string(text):
        m = re.search(r'-?\s*' + _CURRENCY_SYMBOLS + r'?\s*([\d,]+\.?\d*)', text)
        if m:
            return m.group(1)

    # For plain / INR: take value after '= ?' pattern (base currency)
    m = re.search(r'=\s*[?]?\s*[-]?(\d+\.?\d*)', text)
    if m:
        return m.group(1)

    m = re.search(r'[-]?(\d+\.?\d*)', text)
    if m:
        return m.group(1)

    return "0"


def extract_currency_and_values(rate_text=None, amount_text=None, discount_text=None):
    """
    Unified FCY-aware extractor for rate, amount, discount, currency, exchange_rate.
    Works for both INR (plain) and FCY (foreign currency) vouchers.
    """
    result = {
        'currency'     : 'INR',
        'rate'         : 0.0,
        'amount'       : 0.0,
        'discount'     : 0.0,
        'exchange_rate': 1.0,
    }

    # Detect currency from either field
    detected = 'INR'
    for txt in (amount_text, rate_text):
        c = _detect_currency(txt or '')
        if c != 'INR':
            detected = c
            break
    result['currency'] = detected

    if detected == 'INR':
        # Plain INR voucher — use legacy numeric extraction
        result['exchange_rate'] = 1.0
        if rate_text:
            result['rate'] = abs(convert_to_float(extract_numeric_amount(rate_text)))
        if amount_text:
            result['amount'] = abs(convert_to_float(extract_numeric_amount(amount_text)))
        if discount_text:
            result['discount'] = abs(convert_to_float(extract_numeric_amount(discount_text)))
    else:
        # FCY voucher — use dedicated FCY parsers
        if amount_text:
            result['amount']        = _parse_fcy_amount(amount_text)
            result['exchange_rate'] = _parse_fcy_exchange_rate(amount_text)

        if rate_text:
            result['rate'] = _parse_fcy_rate(rate_text)  # ← fixed function

            # If exchange rate still at default, try deriving from rate string
            if result['exchange_rate'] == 1.0:
                exr = _parse_fcy_exchange_rate(rate_text)
                if exr != 1.0:
                    result['exchange_rate'] = exr

        if discount_text:
            result['discount'] = _parse_fcy_amount(discount_text)

    return result


def extract_unit_from_rate(rate_text):
    """Extract unit string from rate text e.g. '14.00/box' → 'box'."""
    if not rate_text:
        return ""
    match = re.search(r'/\s*(\w+)\s*$', str(rate_text))
    return match.group(1) if match else ""


def parse_quantity_with_unit(qty_text):
    """Parse '4400 box' → (4400.0, 'box')."""
    if not qty_text:
        return (0.0, "")
    qty_text = str(qty_text).strip()
    match = re.match(r'[-]?(\d+\.?\d*)\s*(\w*)', qty_text)
    if match:
        return (convert_to_float(match.group(1)), match.group(2) if match.group(2) else "")
    return (0.0, "")


# ──────────────────────────────────────────────────────────────────────────────
# Ledger voucher parser  (Receipt / Payment / Journal / Contra)
# ──────────────────────────────────────────────────────────────────────────────

def parse_ledger_voucher(xml_content, company_name: str, voucher_type_name: str = 'ledger') -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning(f"Empty or None XML content for {voucher_type_name}")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning(f"Empty XML after sanitization for {voucher_type_name}")
            return []

        root     = ET.fromstring(xml_content.encode('utf-8'))
        vouchers = root.findall('.//VOUCHER')
        logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")

        if not vouchers:
            return []

        all_rows = []

        for voucher in vouchers:
            guid           = voucher.findtext('GUID', '')
            alter_id       = voucher.findtext('ALTERID', '0')
            master_id      = voucher.findtext('MASTERID', '')
            voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
            voucher_type   = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
            date           = clean_text(voucher.findtext('DATE', ''))
            reference      = clean_text(voucher.findtext('REFERENCE', ''))
            narration      = clean_text(voucher.findtext('NARRATION', ''))

            action          = voucher.get('ACTION', 'Unknown')
            is_deleted      = voucher.findtext('ISDELETED', 'No')
            change_status   = 'Deleted' if is_deleted == 'Yes' else action
            is_deleted_flag = 'Yes' if change_status in ('Deleted', 'Delete') else 'No'

            ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
            if not ledger_entries:
                ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')

            # Deleted vouchers from CDC arrive with no entries — emit a stub row
            if not ledger_entries and is_deleted_flag == 'Yes':
                all_rows.append({
                    'company_name'  : company_name,
                    'date'          : parse_tally_date_formatted(date),
                    'voucher_type'  : voucher_type,
                    'voucher_number': voucher_number,
                    'reference'     : reference,
                    'ledger_name'   : '',
                    'amount'        : 0.0,
                    'amount_type'   : None,
                    'currency'      : 'INR',
                    'exchange_rate' : 1.0,
                    'narration'     : narration,
                    'guid'          : guid,
                    'alter_id'      : int(alter_id) if alter_id else 0,
                    'master_id'     : master_id,
                    'change_status' : change_status,
                    'is_deleted'    : 'Yes',
                })
                continue

            # Detect voucher-level FCY from any entry that has it
            voucher_exchange_rate = 1.0
            voucher_currency      = 'INR'
            for ledger in ledger_entries:
                amount_text = clean_text(ledger.findtext('AMOUNT', '0'))
                temp        = extract_currency_and_values(None, amount_text)
                if temp['currency'] != 'INR':
                    voucher_currency      = temp['currency']
                    voucher_exchange_rate = temp['exchange_rate']
                    break

            for ledger in ledger_entries:
                ledger_name   = clean_text(ledger.findtext('LEDGERNAME', ''))
                amount_text   = clean_text(ledger.findtext('AMOUNT', '0'))
                currency_info = extract_currency_and_values(None, amount_text)

                # Propagate voucher-level FCY if this entry didn't resolve its own
                if currency_info['currency'] == 'INR' and voucher_currency != 'INR':
                    currency_info['currency']      = voucher_currency
                    currency_info['exchange_rate'] = voucher_exchange_rate

                # Determine Dr/Cr from raw amount sign
                raw_sign    = str(amount_text).strip()
                is_negative = raw_sign.startswith('-')
                amount_type = 'Debit' if is_negative else 'Credit'

                all_rows.append({
                    'company_name'  : company_name,
                    'date'          : parse_tally_date_formatted(date),
                    'voucher_type'  : voucher_type,
                    'voucher_number': voucher_number,
                    'reference'     : reference,
                    'ledger_name'   : ledger_name,
                    'amount'        : currency_info['amount'],
                    'amount_type'   : amount_type,
                    'currency'      : currency_info['currency'],
                    'exchange_rate' : currency_info['exchange_rate'],
                    'narration'     : narration,
                    'guid'          : guid,
                    'alter_id'      : int(alter_id) if alter_id else 0,
                    'master_id'     : master_id,
                    'change_status' : change_status,
                    'is_deleted'    : is_deleted_flag,
                })

        logger.info(f"Parsed {len(all_rows)} rows for {voucher_type_name} [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in {voucher_type_name}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing {voucher_type_name}: {e}")
        logger.error(traceback.format_exc())
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Inventory voucher parser  (Sales / Purchase / Credit Note / Debit Note)
# ──────────────────────────────────────────────────────────────────────────────

def parse_inventory_voucher(xml_content, company_name: str, voucher_type_name: str = 'inventory') -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning(f"Empty or None XML content for {voucher_type_name}")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning(f"Empty XML after sanitization for {voucher_type_name}")
            return []

        root     = ET.fromstring(xml_content.encode('utf-8'))
        vouchers = root.findall('.//VOUCHER')
        logger.info(f"Found {len(vouchers)} {voucher_type_name} vouchers")

        if not vouchers:
            return []

        all_rows = []

        for voucher in vouchers:
            guid           = voucher.findtext('GUID', '')
            alter_id       = voucher.findtext('ALTERID', '0')
            master_id      = voucher.findtext('MASTERID', '')
            voucherkey     = clean_text(voucher.findtext('VOUCHERKEY', ''))
            voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
            voucher_type   = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
            date           = clean_text(voucher.findtext('DATE', ''))
            party_name     = clean_text(voucher.findtext('PARTYNAME', ''))
            reference      = clean_text(voucher.findtext('REFERENCE', ''))
            narration      = clean_text(voucher.findtext('NARRATION', ''))
            party_gstin    = clean_text(voucher.findtext('PARTYGSTIN', ''))
            irn_number     = clean_text(voucher.findtext('IRNACKNO', ''))
            eway_bill      = clean_text(voucher.findtext('TEMPGSTEWAYBILLNUMBER', ''))

            action          = voucher.get('ACTION', 'Unknown')
            is_deleted      = voucher.findtext('ISDELETED', 'No')
            change_status   = 'Deleted' if is_deleted == 'Yes' else action
            is_deleted_flag = 'Yes' if change_status in ('Deleted', 'Delete') else 'No'

            ledger_entries    = (voucher.findall('.//ALLLEDGERENTRIES.LIST') or
                                 voucher.findall('.//LEDGERENTRIES.LIST'))
            inventory_entries = (voucher.findall('.//ALLINVENTORYENTRIES.LIST') or
                                 voucher.findall('.//INVENTORYENTRIES.LIST'))

            # Deleted CDC stub — emit a single row so DB can mark it deleted
            if is_deleted_flag == 'Yes' and not ledger_entries and not inventory_entries:
                all_rows.append(_deleted_inventory_stub(
                    company_name, date, voucher_number, reference, voucher_type,
                    party_name, party_gstin, irn_number, eway_bill,
                    narration, guid, voucherkey, alter_id, master_id, change_status,
                ))
                continue

            # ── Detect voucher-level FCY ───────────────────────────────────────
            voucher_currency      = 'INR'
            voucher_exchange_rate = 1.0

            # Check ledger entries first (party ledger has the total amount)
            for ledger in ledger_entries:
                amt_txt = clean_text(ledger.findtext('AMOUNT', '0'))
                tmp     = extract_currency_and_values(None, amt_txt)
                if tmp['currency'] != 'INR':
                    voucher_currency      = tmp['currency']
                    voucher_exchange_rate = tmp['exchange_rate']
                    break

            # Fall back to inventory entries if ledger didn't reveal it
            if voucher_currency == 'INR':
                for inv in inventory_entries:
                    rate_elem   = inv.find('RATE')
                    amount_elem = inv.find('AMOUNT')
                    r_txt = clean_text(rate_elem.text   if rate_elem   is not None and rate_elem.text   else '0')
                    a_txt = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else '0')
                    tmp   = extract_currency_and_values(r_txt, a_txt)
                    if tmp['currency'] != 'INR':
                        voucher_currency      = tmp['currency']
                        voucher_exchange_rate = tmp['exchange_rate']
                        break

            # ── Total amount from party ledger (for total_amt column) ──────────
            total_amt_from_xml = 0.0
            for ledger in ledger_entries:
                if clean_text(ledger.findtext('ISPARTYLEDGER', 'No')) == 'Yes':
                    total_amt_from_xml = _parse_fcy_amount(
                        clean_text(ledger.findtext('AMOUNT', '0'))
                    )
                    break

            # ── Aggregate GST / charges from ledger entries ────────────────────
            voucher_gst_data = {
                'cgst_total': 0.0, 'sgst_total': 0.0, 'igst_total': 0.0,
                'cgst_rate' : 0.0, 'sgst_rate' : 0.0, 'igst_rate' : 0.0,
            }
            voucher_charges = {
                'freight_amt': 0.0, 'dca_amt': 0.0,
                'cf_amt'     : 0.0, 'other_amt': 0.0,
            }

            for ledger in ledger_entries:
                ledger_name_raw   = clean_text(ledger.findtext('LEDGERNAME', ''))
                ledger_name_lower = ledger_name_raw.lower()
                amt_text          = clean_text(ledger.findtext('AMOUNT', '0'))
                # For GST / charge amounts always use the absolute INR-equivalent
                amount            = abs(convert_to_float(extract_numeric_amount(amt_text)))

                if re.search(r'cgst|c\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                    voucher_gst_data['cgst_total'] += amount
                    m = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name_raw)
                    if m and voucher_gst_data['cgst_rate'] == 0.0:
                        voucher_gst_data['cgst_rate'] = convert_to_float(m.group(1))

                elif re.search(r'sgst|s\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                    voucher_gst_data['sgst_total'] += amount
                    m = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name_raw)
                    if m and voucher_gst_data['sgst_rate'] == 0.0:
                        voucher_gst_data['sgst_rate'] = convert_to_float(m.group(1))

                elif re.search(r'igst|i\.gst', ledger_name_lower) and re.search(r'input|output', ledger_name_lower):
                    voucher_gst_data['igst_total'] += amount
                    m = re.search(r'@\s*(\d+\.?\d*)\s*%?', ledger_name_raw)
                    if m and voucher_gst_data['igst_rate'] == 0.0:
                        voucher_gst_data['igst_rate'] = convert_to_float(m.group(1))

                elif re.search(r'freight', ledger_name_lower):
                    voucher_charges['freight_amt'] += amount

                elif re.search(r'\bdca\b', ledger_name_lower):
                    voucher_charges['dca_amt'] += amount

                elif re.search(r'clearing\s*[&]\s*forwarding|clearing\s+forwarding', ledger_name_lower):
                    voucher_charges['cf_amt'] += amount

                elif clean_text(ledger.findtext('ISPARTYLEDGER', 'No')) != 'Yes':
                    # Unknown non-GST, non-party ledger → other charges
                    voucher_charges['other_amt'] += amount

            # ── Inventory line items ───────────────────────────────────────────
            has_real_inventory = any(
                clean_text(inv.findtext('STOCKITEMNAME', ''))
                and _parse_fcy_amount(
                    clean_text((inv.find('AMOUNT').text if inv.find('AMOUNT') is not None else ''))
                ) > 0.01
                for inv in inventory_entries
            )

            temp_item_data    = []
            total_item_amount = 0.0

            if has_real_inventory:
                for inv in inventory_entries:
                    item_name = clean_text(inv.findtext('STOCKITEMNAME', ''))
                    if not item_name:
                        continue

                    # Raw text from XML elements
                    qty_elem        = inv.find('ACTUALQTY')
                    billed_qty_elem = inv.find('BILLEDQTY')
                    rate_elem       = inv.find('RATE')
                    amount_elem     = inv.find('AMOUNT')
                    discount_elem   = inv.find('DISCOUNT')

                    qty_txt      = clean_text(qty_elem.text        if qty_elem        is not None and qty_elem.text        else '0')
                    billed_txt   = clean_text(billed_qty_elem.text if billed_qty_elem is not None and billed_qty_elem.text else '0')
                    rate_txt     = clean_text(rate_elem.text        if rate_elem       is not None and rate_elem.text        else '0')
                    amount_txt   = clean_text(amount_elem.text      if amount_elem     is not None and amount_elem.text      else '0')
                    discount_txt = clean_text(discount_elem.text    if discount_elem   is not None and discount_elem.text    else '0')

                    # FCY-aware parsers (rate now correctly returns FCY per-unit rate)
                    currency_data = extract_currency_and_values(rate_txt, amount_txt, discount_txt)

                    # Propagate voucher-level FCY if item-level didn't resolve
                    if currency_data['currency'] == 'INR' and voucher_currency != 'INR':
                        currency_data['currency']      = voucher_currency
                        currency_data['exchange_rate'] = voucher_exchange_rate

                    # Quantity & unit
                    qty_numeric        = convert_to_float(re.search(r'[\d,]+\.?\d*', qty_txt).group() if re.search(r'[\d,]+\.?\d*', qty_txt) else '0')
                    alt_qty, alt_unit  = parse_quantity_with_unit(billed_txt)
                    unit               = extract_unit_from_rate(rate_txt) or alt_unit

                    # Batch / MFG / EXP
                    batch_no = mfg_date = exp_date = ''
                    batch_allocations = inv.findall('.//BATCHALLOCATIONS.LIST')
                    if batch_allocations:
                        batch    = batch_allocations[0]
                        batch_no = clean_text(batch.findtext('BATCHNAME', ''))
                        mfg_raw  = clean_text(batch.findtext('MFDON', ''))
                        mfg_date = parse_tally_date_formatted(mfg_raw) or ''
                        exp_elem = batch.find('EXPIRYPERIOD')
                        if exp_elem is not None:
                            if exp_elem.text:
                                exp_date = parse_expiry_date(exp_elem.text)
                            exp_jd = exp_elem.get('JD', '')
                            if exp_jd and not exp_date:
                                exp_date = parse_tally_date_formatted(exp_jd) or ''

                    # HSN code
                    hsn_code = ''
                    for acc in inv.findall('.//ACCOUNTINGALLOCATIONS.LIST'):
                        hsn_code = clean_text(acc.findtext('GSTHSNSACCODE', ''))
                        if hsn_code:
                            break

                    item_amount        = currency_data['amount']
                    total_item_amount += item_amount

                    temp_item_data.append({
                        'item_name'    : item_name,
                        'quantity'     : qty_numeric,
                        'unit'         : unit,
                        'alt_qty'      : alt_qty,
                        'alt_unit'     : alt_unit,
                        'batch_no'     : batch_no,
                        'mfg_date'     : mfg_date,
                        'exp_date'     : exp_date,
                        'hsn_code'     : hsn_code,
                        'rate'         : currency_data['rate'],      # ✅ FCY per-unit rate
                        'amount'       : item_amount,                # ✅ FCY amount
                        'discount'     : currency_data['discount'],
                        'currency'     : currency_data['currency'],
                        'exchange_rate': currency_data['exchange_rate'],
                    })

            # ── Build output rows ──────────────────────────────────────────────
            gst_rate = (voucher_gst_data['cgst_rate']
                        + voucher_gst_data['sgst_rate']
                        + voucher_gst_data['igst_rate'])

            base = {
                'company_name'    : company_name,
                'date'            : parse_tally_date_formatted(date),
                'voucher_number'  : voucher_number,
                'reference'       : reference,
                'voucher_type'    : voucher_type,
                'party_name'      : party_name,
                'gst_number'      : party_gstin,
                'e_invoice_number': irn_number,
                'eway_bill'       : eway_bill,
                'change_status'   : change_status,
                'is_deleted'      : is_deleted_flag,
                'narration'       : narration,
                'guid'            : guid,
                'voucherkey'      : voucherkey,
                'alter_id'        : int(alter_id) if alter_id else 0,
                'master_id'       : master_id,
            }

            # Sort items by name — makes idx==0 deterministic across re-syncs
            temp_item_data.sort(key=lambda x: x['item_name'])

            # total_amt fallback: if party ledger returned 0, compute from line items
            if total_amt_from_xml == 0 and temp_item_data:
                total_amt_from_xml = round(
                    sum(i['amount'] for i in temp_item_data)
                    + voucher_gst_data['cgst_total']
                    + voucher_gst_data['sgst_total']
                    + voucher_gst_data['igst_total']
                    + voucher_charges['freight_amt']
                    + voucher_charges['dca_amt']
                    + voucher_charges['cf_amt']
                    + voucher_charges['other_amt'],
                    2,
                )

            if not temp_item_data or total_item_amount == 0:
                # No inventory items parsed — emit a summary row
                all_rows.append({
                    **base,
                    'item_name'    : 'No Item',
                    'quantity'     : 0.0,
                    'unit'         : 'No Unit',
                    'alt_qty'      : 0.0,
                    'alt_unit'     : '',
                    'batch_no'     : '',
                    'mfg_date'     : '',
                    'exp_date'     : '',
                    'hsn_code'     : '',
                    'gst_rate'     : gst_rate,
                    'rate'         : 0.0,
                    'amount'       : 0.0,
                    'discount'     : 0.0,
                    'cgst_amt'     : voucher_gst_data['cgst_total'],
                    'sgst_amt'     : voucher_gst_data['sgst_total'],
                    'igst_amt'     : voucher_gst_data['igst_total'],
                    'freight_amt'  : voucher_charges['freight_amt'],
                    'dca_amt'      : voucher_charges['dca_amt'],
                    'cf_amt'       : voucher_charges['cf_amt'],
                    'other_amt'    : voucher_charges['other_amt'],
                    'total_amt'    : total_amt_from_xml,
                    'currency'     : voucher_currency,
                    'exchange_rate': voucher_exchange_rate,
                })
            else:
                for idx, item in enumerate(temp_item_data):
                    proportion = item['amount'] / total_item_amount if total_item_amount else 0
                    all_rows.append({
                        **base,
                        'item_name'    : item['item_name'],
                        'quantity'     : item['quantity'],
                        'unit'         : item['unit'],
                        'alt_qty'      : item['alt_qty'],
                        'alt_unit'     : item['alt_unit'],
                        'batch_no'     : item['batch_no'],
                        'mfg_date'     : item['mfg_date'],
                        'exp_date'     : item['exp_date'],
                        'hsn_code'     : item['hsn_code'],
                        'gst_rate'     : gst_rate,
                        'rate'         : item['rate'],
                        'amount'       : item['amount'],
                        'discount'     : item['discount'],
                        # GST & charges distributed proportionally across items
                        'cgst_amt'     : voucher_gst_data['cgst_total'] * proportion,
                        'sgst_amt'     : voucher_gst_data['sgst_total'] * proportion,
                        'igst_amt'     : voucher_gst_data['igst_total'] * proportion,
                        # Charges only on first item to avoid double-counting
                        'freight_amt'  : voucher_charges['freight_amt'] if idx == 0 else 0.0,
                        'dca_amt'      : voucher_charges['dca_amt']     if idx == 0 else 0.0,
                        'cf_amt'       : voucher_charges['cf_amt']      if idx == 0 else 0.0,
                        'other_amt'    : voucher_charges['other_amt']   if idx == 0 else 0.0,
                        # total_amt only on first item row (it's a voucher-level value)
                        'total_amt'    : total_amt_from_xml if idx == 0 else 0.0,
                        'currency'     : item['currency'],
                        'exchange_rate': item['exchange_rate'],
                    })

        logger.info(f"Parsed {len(all_rows)} rows for {voucher_type_name} [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in {voucher_type_name}: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing {voucher_type_name}: {e}")
        logger.error(traceback.format_exc())
        return []


def _deleted_inventory_stub(
    company_name, date, voucher_number, reference, voucher_type,
    party_name, party_gstin, irn_number, eway_bill,
    narration, guid, voucherkey, alter_id, master_id, change_status,
) -> dict:
    """Return a zeroed stub row used to mark a voucher deleted in the DB."""
    return {
        'company_name'    : company_name,
        'date'            : parse_tally_date_formatted(date),
        'voucher_number'  : voucher_number,
        'reference'       : reference,
        'voucher_type'    : voucher_type,
        'party_name'      : party_name,
        'gst_number'      : party_gstin,
        'e_invoice_number': irn_number,
        'eway_bill'       : eway_bill,
        'item_name'       : '',
        'quantity'        : 0.0,
        'unit'            : '',
        'alt_qty'         : 0.0,
        'alt_unit'        : '',
        'batch_no'        : '',
        'mfg_date'        : '',
        'exp_date'        : '',
        'hsn_code'        : '',
        'gst_rate'        : 0.0,
        'rate'            : 0.0,
        'amount'          : 0.0,
        'discount'        : 0.0,
        'cgst_amt'        : 0.0,
        'sgst_amt'        : 0.0,
        'igst_amt'        : 0.0,
        'freight_amt'     : 0.0,
        'dca_amt'         : 0.0,
        'cf_amt'          : 0.0,
        'other_amt'       : 0.0,
        'total_amt'       : 0.0,
        'currency'        : 'INR',
        'exchange_rate'   : 1.0,
        'narration'       : narration,
        'guid'            : guid,
        'voucherkey'      : voucherkey,
        'alter_id'        : int(alter_id) if alter_id else 0,
        'master_id'       : master_id,
        'change_status'   : change_status,
        'is_deleted'      : 'Yes',
    }


# ──────────────────────────────────────────────────────────────────────────────
# Ledger master parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_ledgers(xml_content, company_name: str) -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning("Empty or None XML content for ledgers")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning("Empty XML after sanitization for ledgers")
            return []

        root    = ET.fromstring(xml_content.encode('utf-8'))
        ledgers = root.findall('.//LEDGER')

        if not ledgers:
            logger.warning("No ledgers found in XML")
            return []

        all_rows = []

        for ledger in ledgers:
            ledger_name    = ledger.get('NAME', '')
            guid           = clean_text(ledger.findtext('GUID', ''))
            alter_id       = clean_text(ledger.findtext('ALTERID', '0'))
            parent         = clean_text(ledger.findtext('PARENT', ''))
            created_date   = clean_text(ledger.findtext('CREATEDDATE', ''))
            altered_on     = clean_text(ledger.findtext('ALTEREDON', ''))
            email          = clean_text(ledger.findtext('EMAIL', ''))
            website        = clean_text(ledger.findtext('WEBSITE', ''))
            phone          = clean_text(ledger.findtext('LEDGERPHONE', ''))
            mobile         = clean_text(ledger.findtext('LEDGERMOBILE', ''))
            fax            = clean_text(ledger.findtext('LEDGERFAX', ''))
            contact_person = clean_text(ledger.findtext('LEDGERCONTACT', ''))

            # Aliases
            aliases      = []
            direct_alias = clean_text(ledger.findtext('ALIAS', ''))
            if direct_alias and direct_alias != ledger_name:
                aliases.append(direct_alias)
            for lang_list in ledger.findall('.//LANGUAGENAME.LIST'):
                for name_list in lang_list.findall('.//NAME.LIST'):
                    for name in name_list.findall('NAME'):
                        alias_text = clean_text(name.text or '')
                        if alias_text and alias_text != ledger_name and alias_text not in aliases:
                            aliases.append(alias_text)

            # Address
            address_lines = []
            for addr_list in ledger.findall('.//ADDRESS.LIST'):
                for address in addr_list.findall('ADDRESS'):
                    addr_text = clean_text(address.text or '')
                    if addr_text:
                        address_lines.append(addr_text)

            all_rows.append({
                'company_name'         : company_name,
                'ledger_name'          : ledger_name,
                'alias'                : aliases[0] if len(aliases) > 0 else '',
                'alias_2'              : aliases[1] if len(aliases) > 1 else '',
                'alias_3'              : aliases[2] if len(aliases) > 2 else '',
                'parent_group'         : parent,
                'contact_person'       : contact_person,
                'email'                : email,
                'phone'                : phone,
                'mobile'               : mobile,
                'fax'                  : fax,
                'website'              : website,
                'address_line_1'       : address_lines[0] if len(address_lines) > 0 else '',
                'address_line_2'       : address_lines[1] if len(address_lines) > 1 else '',
                'address_line_3'       : address_lines[2] if len(address_lines) > 2 else '',
                'pincode'              : clean_text(ledger.findtext('PINCODE', '')),
                'state'                : clean_text(ledger.findtext('STATENAME', '')),
                'country'              : clean_text(ledger.findtext('COUNTRYNAME', '')),
                'opening_balance'      : clean_text(ledger.findtext('OPENINGBALANCE', '0')),
                'credit_limit'         : clean_text(ledger.findtext('CREDITLIMIT', '0')),
                'bill_credit_period'   : clean_text(ledger.findtext('BILLCREDITPERIOD', '')),
                'pan'                  : clean_text(ledger.findtext('INCOMETAXNUMBER', '')),
                'gstin'                : clean_text(ledger.findtext('PARTYGSTIN', '')),
                'gst_registration_type': clean_text(ledger.findtext('GSTREGISTRATIONTYPE', '')),
                'vat_tin'              : clean_text(ledger.findtext('VATTINNUMBER', '')),
                'sales_tax_number'     : clean_text(ledger.findtext('SALESTAXNUMBER', '')),
                'bank_account_holder'  : clean_text(ledger.findtext('BANKACCHOLDERNAME', '')),
                'ifsc_code'            : clean_text(ledger.findtext('IFSCODE', '')),
                'bank_branch'          : clean_text(ledger.findtext('BRANCHNAME', '')),
                'swift_code'           : clean_text(ledger.findtext('SWIFTCODE', '')),
                'bank_iban'            : clean_text(ledger.findtext('BANKIBAN', '')),
                'export_import_code'   : clean_text(ledger.findtext('EXPORTIMPORTCODE', '')),
                'msme_reg_number'      : clean_text(ledger.findtext('MSMEREGNUMBER', '')),
                'is_bill_wise_on'      : clean_text(ledger.findtext('ISBILLWISEON', 'No')),
                'is_deleted'           : clean_text(ledger.findtext('ISDELETED', 'No')),
                'created_date'         : created_date,
                'altered_on'           : altered_on,
                'guid'                 : guid,
                'alter_id'             : int(alter_id) if alter_id else 0,
            })

        logger.info(f"Parsed {len(all_rows)} ledgers [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in ledgers: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing ledgers: {e}")
        logger.error(traceback.format_exc())
        return []


# ──────────────────────────────────────────────────────────────────────────────
# Trial Balance parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_trial_balance(xml_content, company_name: str, start_date: str, end_date: str) -> list:
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning("Empty or None XML content for trial balance")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning("Empty XML after sanitization for trial balance")
            return []

        root         = ET.fromstring(xml_content.encode('utf-8'))
        ledger_nodes = root.findall('.//LEDGER')

        if not ledger_nodes:
            logger.warning("No ledger nodes found in trial balance XML")
            return []

        all_rows = []

        for ledger in ledger_nodes:
            ledger_name = ledger.get('NAME', '') or clean_text(ledger.findtext('LEDGERNAME', ''))
            ledger_name = clean_text(ledger_name)
            if not ledger_name:
                continue

            guid         = clean_text(ledger.findtext('GUID',     ''))
            alter_id     = clean_text(ledger.findtext('ALTERID',  '0'))
            master_id    = clean_text(ledger.findtext('MASTERID', ''))
            parent_group = clean_text(ledger.findtext('PARENT',   ''))

            opening_text     = clean_text(ledger.findtext('OPENINGBALANCE', '0'))
            closing_text     = clean_text(ledger.findtext('CLOSINGBALANCE', '0'))
            # Trial balance values are INR totals — use plain extraction
            opening_val      = abs(convert_to_float(extract_numeric_amount(str(opening_text))))
            closing_val      = abs(convert_to_float(extract_numeric_amount(str(closing_text))))
            net_transactions = closing_val - opening_val

            all_rows.append({
                'company_name'    : company_name,
                'ledger_name'     : ledger_name,
                'parent_group'    : parent_group,
                'opening_balance' : opening_val,
                'net_transactions': net_transactions,
                'closing_balance' : closing_val,
                'start_date'      : start_date,
                'end_date'        : end_date,
                'guid'            : guid,
                'alter_id'        : int(alter_id) if alter_id else 0,
                'master_id'       : master_id,
            })

        logger.info(f"Parsed {len(all_rows)} trial balance rows [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in trial balance: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing trial balance: {e}")
        logger.error(traceback.format_exc())
        return []

# ──────────────────────────────────────────────────────────────────────────────
# Stock Item (Item) master parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_items(xml_content, company_name: str) -> list:
    """
    Parse Tally StockItem XML (full snapshot or CDC).

    Column ordering:
      1. Identity / CDC keys   — company_name, guid, remote_alt_guid, alter_id
      2. Master descriptors    — item_name, parent_group, category
      3. UoM / supply type     — base_units, gst_type_of_supply
      4. Opening-stock values  — opening_balance, opening_rate, opening_value
      5. Audit metadata        — entered_by, is_deleted
    """
    try:
        if not xml_content or (isinstance(xml_content, str) and not xml_content.strip()):
            logger.warning("Empty or None XML content for items")
            return []

        xml_content = sanitize_xml_content(xml_content)
        if not xml_content or not xml_content.strip():
            logger.warning("Empty XML after sanitization for items")
            return []

        root  = ET.fromstring(xml_content.encode('utf-8'))
        items = root.findall('.//STOCKITEM')

        if not items:
            logger.warning(f"No stock items found in XML [{company_name}]")
            return []

        all_rows = []
        skipped  = 0

        for item in items:
            item_name = clean_text(item.get('NAME', '') or item.findtext('NAME', ''))
            guid      = clean_text(item.findtext('GUID', ''))

            if not item_name or not guid:
                skipped += 1
                logger.debug(f"Skipping item with no name/guid (name={item_name!r})")
                continue

            remote_alt_guid = clean_text(item.findtext('REMOTEALTGUID', ''))
            alter_id_raw    = clean_text(item.findtext('ALTERID', '0'))

            parent_group = clean_text(item.findtext('PARENT',          '')).strip()
            category     = clean_text(item.findtext('CATEGORY',        '')).strip()
            base_units   = clean_text(item.findtext('BASEUNITS',       ''))
            gst_type     = clean_text(item.findtext('GSTTYPEOFSUPPLY', ''))

            opening_balance = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGBALANCE', '0')))
            )
            opening_rate    = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGRATE',    '0')))
            )
            opening_value   = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGVALUE',   '0')))
            )

            entered_by = clean_text(item.findtext('ENTEREDBY', ''))

            is_deleted_raw = clean_text(item.findtext('ISDELETED', ''))
            is_deleted     = 'Yes' if is_deleted_raw.lower() in ('yes', 'true', '1') else 'No'

            all_rows.append({
                'company_name'      : company_name,
                'item_name'         : item_name,
                'parent_group'      : parent_group,
                'category'          : category,
                'base_units'        : base_units,
                'gst_type_of_supply': gst_type,
                'opening_balance'   : opening_balance,
                'opening_rate'      : opening_rate,
                'opening_value'     : opening_value,
                'entered_by'        : entered_by,
                'is_deleted'        : is_deleted,
                'guid'              : guid,
                'remote_alt_guid'   : remote_alt_guid,
                'alter_id'          : int(alter_id_raw) if alter_id_raw else 0,
            })

        logger.info(f"Parsed {len(all_rows)} stock items (skipped {skipped} empty nodes) [{company_name}]")
        return all_rows

    except ET.ParseError as e:
        logger.error(f"XML Parse Error in items: {e}")
        return []
    except Exception as e:
        logger.error(f"Error parsing items: {e}")
        logger.error(traceback.format_exc())
        return []

# ──────────────────────────────────────────────────────────────────────────────
# Outstanding — Debtors parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_outstanding_debtors(xml_content, company_name: str) -> list:
    """
    Parse Sundry Debtors (Receivables) outstanding vouchers.

    Returns list[dict] with keys:
        company_name, party_name,
        voucher_number, voucher_type,
        bill_name, bill_type, date, bill_date, due_date,
        reference, narration,
        currency, exchange_rate, amount
    """
    try:
        from lxml import etree as _lxml

        if not xml_content:
            logger.warning(f"Empty XML for outstanding debtors [{company_name}]")
            return []

        raw = xml_content if isinstance(xml_content, bytes) else xml_content.encode('utf-8')

        parser   = _lxml.XMLParser(recover=True, encoding='utf-8')
        root     = _lxml.fromstring(raw, parser=parser)
        vouchers = root.findall('.//VOUCHER')
        logger.info(f"Found {len(vouchers)} debtors vouchers [{company_name}]")

        if not vouchers:
            return []

        all_rows = []

        for voucher in vouchers:
            vnum    = clean_text(voucher.findtext('VOUCHERNUMBER',      ''))
            vtype   = clean_text(voucher.findtext('VOUCHERTYPENAME',    ''))
            party   = clean_text(voucher.findtext('PARTYLEDGERNAME',    ''))
            ref     = clean_text(voucher.findtext('REFERENCE',          ''))
            narr    = clean_text(voucher.findtext('NARRATION',          ''))
            raw_dt  = clean_text(voucher.findtext('DATE',               ''))
            due_raw = clean_text(voucher.findtext('BASICDUEDATEOFPYMT', ''))

            if not vnum:
                continue

            txn_date = parse_tally_date_formatted(raw_dt)
            due_date = (
                parse_tally_date_formatted(due_raw)
                if due_raw and due_raw not in ('', '000000000') else None
            )

            amount        = 0.0
            exchange_rate = 1.0
            currency      = 'INR'
            bill_name     = vnum
            bill_type     = vtype
            bill_date     = txn_date

            for le in voucher.findall('.//ALLLEDGERENTRIES.LIST'):
                isparty = clean_text(le.findtext('ISPARTYLEDGER', 'No'))
                le_name = clean_text(le.findtext('LEDGERNAME',    ''))
                amt_raw = clean_text(le.findtext('AMOUNT',        '0'))
                bn      = clean_text(le.findtext('NAME',          ''))
                bt      = clean_text(le.findtext('BILLTYPE',      ''))
                bd      = clean_text(le.findtext('BILLDATE',      ''))

                if isparty == 'Yes' and le_name == party:
                    amount        = abs(_parse_fcy_amount(amt_raw))
                    exchange_rate = _parse_fcy_exchange_rate(amt_raw)
                    currency      = _detect_currency(amt_raw)
                    if bn:
                        bill_name = bn
                    if bt:
                        bill_type = bt
                    if bd and bd not in ('', '000000000'):
                        bill_date = parse_tally_date_formatted(bd)
                    break

            all_rows.append({
                'company_name'  : company_name,
                'party_name'    : party,
                'voucher_number': vnum,
                'voucher_type'  : vtype,
                'bill_name'     : bill_name,
                'bill_type'     : bill_type,
                'date'          : txn_date,
                'bill_date'     : bill_date,
                'due_date'      : due_date,
                'reference'     : ref,
                'currency'      : currency,
                'exchange_rate' : exchange_rate,
                'amount'        : amount,
                'narration'     : narr,
            })

        logger.info(f"Parsed {len(all_rows)} debtors rows [{company_name}]")
        return all_rows

    except Exception as e:
        logger.error(f"Error parsing outstanding debtors [{company_name}]: {e}")
        logger.error(traceback.format_exc())
        return []

# ──────────────────────────────────────────────────────────────────────────────
# GUID reconciliation parser  (Phase 3)
# ──────────────────────────────────────────────────────────────────────────────

def parse_guids(xml_content) -> "set | None":
    """
    Parse a GUID-only XML response from Tally (utils/guid/*.xml templates).

    Returns:
        set[str]  — set of GUID strings on success (may be empty if Tally
                    genuinely has 0 vouchers in the requested window)
        None      — on ANY parse failure (network error, bad XML, empty content)

    CRITICAL CONTRACT for callers:
        None  → Tally fetch or parse FAILED → DO NOT delete anything
        set() → Tally confirmed 0 vouchers in window → safe to delete DB rows
    """
    try:
        if not xml_content:
            logger.warning("parse_guids: received empty/None xml_content")
            return None

        xml_str = sanitize_xml_content(xml_content)
        if not xml_str or not xml_str.strip():
            logger.warning("parse_guids: empty XML after sanitization")
            return None

        root = ET.fromstring(xml_str.encode('utf-8'))

        guids = {
            e.text.strip()
            for e in root.iter('GUID')
            if e.text and e.text.strip()
        }

        logger.info(f"parse_guids: parsed {len(guids)} GUIDs from Tally response")
        return guids

    except ET.ParseError as e:
        logger.error(f"parse_guids: XML parse error — {e}")
        return None
    except Exception as e:
        logger.error(f"parse_guids: unexpected error — {e}")
        logger.error(traceback.format_exc())
        return None