import xml.etree.ElementTree as ET
import re
import traceback
import time
from datetime import datetime
from logging_config import logger


# ──────────────────────────────────────────────────────────────────────────────
# Sign transformation rules
# ──────────────────────────────────────────────────────────────────────────────
#
# Tally stores amounts with accounting sign conventions that vary by voucher
# type.  These helpers re-sign parsed rows so all amounts are expressed from
# a consistent reporting perspective before the rows reach the database.
#
# Rules:
#   Sales       → total_amt × -1
#   Purchase    → amount, cgst_amt, sgst_amt, igst_amt,
#                 freight_amt, dca_amt, cf_amt, other_amt  × -1
#   Credit Note → same 8 fields as Purchase  × -1
#   Debit Note  → total_amt × -1
#   Receipt / Payment / Journal / Contra
#               → amount × -1  only when amount_type == 'Debit'

_SIGN_DETAIL_FIELDS = (
    'amount', 'cgst_amt', 'sgst_amt', 'igst_amt',
    'freight_amt', 'dca_amt', 'cf_amt', 'other_amt',
)

# All amount fields that must be rounded to 2 decimal places.
# Excludes: exchange_rate, gst_rate (precision rates), quantity/alt_qty (counts).
_INVENTORY_AMOUNT_FIELDS = (
    'rate', 'amount', 'discount',
    'cgst_amt', 'sgst_amt', 'igst_amt',
    'freight_amt', 'dca_amt', 'cf_amt', 'other_amt',
    'total_amt',
)
_LEDGER_AMOUNT_FIELDS = (
    'amount',
)


def _negate_fields(row: dict, *fields: str) -> None:
    """Multiply each named numeric field by -1 in-place; skips missing/None/zero."""
    for field in fields:
        val = row.get(field)
        if val is not None:
            try:
                fval = float(val)
                if fval != 0:          # ignore zero — never negate a zero value
                    row[field] = -fval
            except (TypeError, ValueError):
                pass


def _round_fields(row: dict, *fields: str) -> None:
    """Round each named numeric field to 2 decimal places in-place; skips missing/None."""
    for field in fields:
        val = row.get(field)
        if val is not None:
            try:
                row[field] = round(float(val), 2)
            except (TypeError, ValueError):
                pass


def _apply_inventory_signs(rows: list, voucher_type_name: str) -> list:
    """
    Apply sign rules + round to 2dp for inventory voucher rows
    (Sales / Purchase / Credit Note / Debit Note).
    Called inside parse_inventory_voucher just before returning all_rows.
    """
    key = voucher_type_name.strip().lower()
    for row in rows:
        if 'sales' in key:
            _negate_fields(row, 'total_amt')
        elif 'purchase' in key or 'credit' in key:
            _negate_fields(row, *_SIGN_DETAIL_FIELDS)
        elif 'debit' in key:
            _negate_fields(row, 'total_amt')
        _round_fields(row, *_INVENTORY_AMOUNT_FIELDS)
    return rows


def _apply_ledger_signs(rows: list) -> list:
    """
    Apply sign rules + round to 2dp for ledger voucher rows
    (Receipt / Payment / Journal / Contra).
    Called inside parse_ledger_voucher just before returning all_rows.
    Negates amount only when amount_type == 'Debit'.
    """
    for row in rows:
        if row.get('amount_type') == 'Debit':
            _negate_fields(row, 'amount')
        _round_fields(row, *_LEDGER_AMOUNT_FIELDS)
    return rows


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

# def clean_text(text):
#     if not text:
#         return ""
#     text = str(text).replace('&#13;&#10;', ' ').replace('&#13;', ' ').replace('&#10;', ' ')
#     text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
#     return re.sub(r'\s+', ' ', text).strip()

def clean_text(text):
    if not text:
        return ""
    text = str(text).replace('&#13;&#10;', ' ').replace('&#13;', ' ').replace('&#10;', ' ')
    text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
    return text.strip()


def clean_narration(text):
    """Clean narration text: replace tabs with a single space, then collapse
    any runs of whitespace down to one space and strip edges."""
    if not text:
        return ""
    text = clean_text(text)          # handle newlines / HTML entities first
    text = text.replace('\t', ' ')   # tabs → single space
    return re.sub(r' {2,}', ' ', text).strip()


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

# Compound dollar prefixes Tally prints for dollar-denominated currencies.
# MUST be checked BEFORE the bare '$' entry in _SYMBOL_TO_ISO, otherwise
# "AU$28.00" is misdetected as USD.
_COMPOUND_DOLLAR_SYMBOLS = [
    ('AU$', 'AUD'),   # Australian Dollar  - "AU$28.00"
    ('NZ$', 'NZD'),   # New Zealand Dollar - "NZ$50.00"
    ('HK$', 'HKD'),   # Hong Kong Dollar   - "HK$100.00"
    ('SG$', 'SGD'),   # Singapore Dollar   - "SG$75.00"
    ('CA$', 'CAD'),   # Canadian Dollar    - "CA$50.00" (alternate Tally format)
]
_COMPOUND_DOLLAR_PATTERN = r'(' + '|'.join(re.escape(s) for s, _ in _COMPOUND_DOLLAR_SYMBOLS) + r')'
_COMPOUND_DOLLAR_MAP = {sym: iso for sym, iso in _COMPOUND_DOLLAR_SYMBOLS}

# Map of symbol -> ISO code  (bare $ is USD -- checked AFTER compound symbols)
_SYMBOL_TO_ISO = {
    '$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY',
    '₹': 'INR', '₨': 'INR', '₩': 'KRW', '₱': 'PHP',
    '₽': 'RUB', '₺': 'TRY', '₪': 'ILS', '₦': 'NGN',
    '฿': 'THB', '₫': 'VND',
}

# ISO text codes Tally prints as prefixes when no symbol is defined
# e.g. "CAD50.00 = ? 47.93/box"  or  "CAD350.00 @ ? 0.9585/CAD = ? 335.48"
_ISO_TEXT_CODES = [
    'CAD', 'AUD', 'NZD', 'SGD', 'HKD', 'CHF', 'SEK', 'NOK', 'DKK',
    'MYR', 'IDR', 'AED', 'SAR', 'QAR', 'KWD', 'BHD', 'OMR',
    'ZAR', 'EGP', 'PKR', 'BDT', 'LKR', 'NPR', 'MMK', 'KES',
    'CNY', 'CNH', 'TWD', 'MXN', 'BRL', 'ARS', 'CLP', 'COP',
    'EUR', 'GBP', 'JPY', 'USD',   # also listed here for text-prefix fallback
]
# Build a regex that matches an ISO code even when immediately followed by digits
# (e.g. "CAD50.00") but NOT when preceded by a word char (e.g. avoids "CANADA").
# The lookahead (?=[\d\s\-]) matches: "CAD50", "CAD 50", "CAD-50", "/CAD "
_ISO_TEXT_PATTERN = r'(?<!\w)(' + '|'.join(_ISO_TEXT_CODES) + r')(?=[\d\s\-/])'


def _detect_currency(text: str, default_currency: str = 'INR') -> str:
    """
    Return ISO currency code from a Tally amount/rate string.
    Falls back to default_currency (per-company base currency) when no
    symbol or ISO code is found — replaces the old hardcoded 'INR' fallback.

    Detection order (most-specific first to avoid false matches):
      1. Compound dollar prefixes: AU$, NZ$, HK$, SG$, CA$  -> AUD, NZD, HKD, SGD, CAD
      2. Single-char symbols: $->USD, EUR, GBP, etc.
      3. ISO text-code prefix: "CAD50.00", "SGD 50"
      4. ISO text-code denominator: "? 0.9585/CAD"
      5. Corrupted EUR: digit immediately followed by '?' or U+FFFD (with optional space)
         "18.00? = ? 1606.10/Box"   -- RATE: no space between digit and corrupt char
         "7429.97 ? @ ? 105.18/..."  -- AMOUNT: space before corrupt char then '@'
         Both arise when Tally's euro sign (Windows-1252 0x80) is lost in XML transit.
      6. No match → return default_currency
    """
    if not text:
        return default_currency
    # 1. Compound dollar symbols (must be before bare '$' check)
    for sym, iso in _COMPOUND_DOLLAR_SYMBOLS:
        if sym in text:
            return iso
    # 2. Single-char symbol match
    for sym, iso in _SYMBOL_TO_ISO.items():
        if sym in text and iso != default_currency:
            return iso
    # 3. ISO text-code as prefix: "CAD50.00" or "CAD 50"
    m = re.search(_ISO_TEXT_PATTERN, text)
    if m:
        return m.group(1)
    # 4. ISO text-code as denominator: "? 0.9585/CAD"
    m = re.search(r'/(' + '|'.join(_ISO_TEXT_CODES) + r')\b', text)
    if m and m.group(1) != default_currency:
        return m.group(1)
    # 5. Corrupted EUR: digit(s) followed (with optional single space) by '?' or U+FFFD
    if re.search(r'\d\s?[?\ufffd]', text):
        return 'EUR'
    return default_currency


def _parse_fcy_rate(raw: str) -> float:
    """
    Extract per-unit FCY rate from Tally RATE field.

    Tally FCY rate string formats:
      "$14.00/box"                      → 14.0
      "$14.00 = ? 1/$ = ? 14.00/box"   → 14.0  (exchange rate = 1)
      "$14.00 = ? 84.5/$ = ? 1183/box" → 14.0  (exchange rate = 84.5, home = 1183)
      "14.00/box"                       → 14.0  (plain INR rate)
      "CAD50.00 = ? 47.93/box"          → 50.0  (CAD text-prefix format)

    BUG FIXED: The old regex r'(-?[\d,]+\.?\d*)\s*/\s*\w' matched "1183/box" on
    FCY strings like "$14.00 = ? 84.5/$ = ? 1183/box", returning the INR
    home-rate (1183) instead of the correct FCY per-unit rate ($14.00).

    Fix strategy:
      1. FCY symbol  → take the number immediately after the currency symbol ($14.00 → 14.0)
      2. FCY ISO text→ take the number immediately after the ISO code (CAD50.00 → 50.0)
      3. INR         → take the number immediately before /alphabetic-unit (14.00/box → 14.0)
      4. Fallback    → first number in string
    """
    if not raw:
        return 0.0
    raw = str(raw).strip()

    # Step 0: Corrupted EUR -- "18.00? = ? 1606.10/Box"
    # When Tally's euro sign is lost in transit it becomes '?' or U+FFFD sitting
    # directly after the FCY amount (no space for RATE strings).
    # Pattern: <digits>?<optional-space>=  ->  grab the digits before the corrupt char.
    m = re.search(r'([\d,]+\.?\d*)[?\ufffd]\s*=', raw)
    if m:
        return convert_to_float(m.group(1))

    # Step 1a: Compound dollar symbol (AU$, NZ$, HK$, SG$, CA$) — check BEFORE bare $
    #   "AU$28.00 = ? 28.00/box"  ->  28.0  (AUD)
    m = re.search(_COMPOUND_DOLLAR_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(2))

    # Step 1b: Single-char FCY symbol -- number immediately after symbol
    #   "$14.00 = ? 84.5/$ = ? 1183/box"  ->  14.0  (USD)
    m = re.search(_CURRENCY_SYMBOLS + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(1))

    # Step 1c: Single-char FCY symbol -- number immediately BEFORE symbol
    #   "30.58 £ = ? 3216.40/Box"  ->  30.58  (GBP RATE: FCY qty precedes the symbol)
    m = re.search(r'([\d,]+\.?\d*)\s*' + _CURRENCY_SYMBOLS, raw)
    if m:
        return convert_to_float(m.group(1))

    # Step 2: FCY ISO text-prefix — number immediately after code
    #   "CAD50.00 = ? 47.93/box"  →  grabs 50.0  ✓
    m = re.search(_ISO_TEXT_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(2))

    # Step 3: INR — number immediately before /alphabetic-unit
    #   "14.00/box"  →  14.0  ✓
    m = re.search(r'([\d,]+\.?\d*)\s*/\s*[a-zA-Z]', raw)
    if m:
        return convert_to_float(m.group(1))

    # Fallback: first number in string
    m = re.search(r'([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(1))

    return 0.0


def _parse_fcy_amount(raw: str) -> float:
    """
    Extract the FCY (foreign currency) amount from a Tally AMOUNT field.
    Returns a signed value — negative if the raw string is negative.

    "$61600.00 @ ? 1/$ = ? 61600.00"       →  61600.0
    "-$61600.00 @ ? 1/$ = -? 61600.00"     → -61600.0
    "CAD350.00 @ ? 0.9585/CAD = ? 335.48"  →  350.0
    "-CAD350.00 @ ? 0.9585/CAD = ? 335.48" → -350.0
    "61600.00"                              →  61600.0
    "-61600.00"                             → -61600.0
    """
    if not raw:
        return 0.0
    raw = str(raw).strip()

    # Preserve Tally's raw sign
    is_negative = raw.startswith('-')

    def signed(value: float) -> float:
        return -abs(value) if is_negative else abs(value)

    # Corrupted EUR -- "7429.97 ? @ ? 105.18/? = ? 112427.03"
    m = re.search(r'-?\s*([\d,]+\.?\d*)[\s]*[?\ufffd]\s*@', raw)
    if m:
        return signed(convert_to_float(m.group(1)))

    # Compound dollar symbol: -?AU$<number>
    m = re.search(r'-?\s*' + _COMPOUND_DOLLAR_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(2)))

    # Single-char symbol-prefixed FCY: -?<symbol><number>
    m = re.search(r'-?\s*' + _CURRENCY_SYMBOLS + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(1)))

    # ISO text-prefix FCY: -?<CODE><number>  e.g. "CAD350.00" or "-CAD350.00"
    m = re.search(r'-?\s*' + _ISO_TEXT_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(2)))

    # Fallback: first number
    m = re.search(r'-?([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(1)))

    return 0.0


def _parse_fcy_exchange_rate(raw_amount: str) -> float:
    """
    Extract exchange rate embedded in Tally FCY amount/rate string.

    "$61600.00 @ ? 1/$ = ? 61600.00"               -> 1.0
    "$615.00 @ ? 84.5/$ = ? 51997.50"              -> 84.5
    "7429.97 £ @ ? 105.18/ £ = ? 781484.24"        -> 105.18  (GBP: space before £)
    "CAD350.00 @ ? 0.9585/CAD = ? 335.48"          -> 0.9585  (ISO text-code denominator)
    "1260.00? @ ? 89.2278/? = ? 112427.03"         -> 89.2278 (EUR corrupt: /? denominator)
    "CAD50.00 = ? 47.93/box"                       -> 1.0     (no /CODE pattern, default)
    Returns 1.0 if not found.

    BUG FIXES applied (discovered from real XML):
      GBP: Tally writes "/ £" with a space before the symbol. Old Pattern 1b used
           r'/' + _CURRENCY_SYMBOLS requiring the symbol immediately after '/'.
           Fix: added \s* between '/' and the symbol.
      EUR: When euro sign is corrupted to '?', the denominator becomes '/?' which
           is not in _CURRENCY_SYMBOLS. New Pattern 3 handles this explicitly.
    """
    if not raw_amount:
        return 1.0

    def _ret(m, group=1):
        rate = convert_to_float(m.group(group))
        return rate if rate > 0 else 1.0

    # Pattern 1a: ? <number>/<compound-dollar>  e.g.  ? 1/AU$
    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/' + _COMPOUND_DOLLAR_PATTERN, raw_amount)
    if m:
        return _ret(m, 1)

    # Pattern 1b: ? <number>/ <single-symbol>   e.g.  ? 84.5/$  or  ? 105.18/ £
    # FIX: \s* added before symbol to handle Tally's "/ £" (space before pound sign)
    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/\s*' + _CURRENCY_SYMBOLS, raw_amount)
    if m:
        return _ret(m, 1)

    # Pattern 2: ? <number>/<ISO-code>          e.g.  ? 0.9585/CAD
    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/' + _ISO_TEXT_PATTERN, raw_amount)
    if m:
        return _ret(m, 1)

    # Pattern 3: EUR corrupt -- denominator is '/?' or '/U+FFFD' (euro lost in transit)
    # "1260.00? @ ? 89.2278/? = ? 112427.03"  ->  89.2278
    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/\s*[?\ufffd]', raw_amount)
    if m:
        return _ret(m, 1)

    return 1.0

def _is_fcy_string(text: str) -> bool:
    """True if the text contains a foreign-currency symbol or ISO text code (not INR)."""
    if not text:
        return False
    # Compound dollar symbols first (AU$, NZ$, etc.)
    for sym in _COMPOUND_DOLLAR_MAP:
        if sym in text:
            return True
    for sym in _SYMBOL_TO_ISO:
        if sym in text and _SYMBOL_TO_ISO[sym] != 'INR':
            return True
    m = re.search(_ISO_TEXT_PATTERN, text)
    if m and m.group(1) not in ('INR',):
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

    is_negative = text.strip().startswith('-')
    prefix = '-' if is_negative else ''

    # Tally FCY: prefer the FIRST numeric value (foreign amount)
    if _is_fcy_string(text):
        m = re.search(r'-?\s*' + _CURRENCY_SYMBOLS + r'?\s*([\d,]+\.?\d*)', text)
        if m:
            return prefix + m.group(1)

    # For plain / INR: take value after '= ?' pattern (base currency)
    m = re.search(r'=\s*[?]?\s*[-]?(\d+\.?\d*)', text)
    if m:
        return prefix + m.group(1)

    m = re.search(r'[-]?(\d+\.?\d*)', text)
    if m:
        return prefix + m.group(1)

    return "0"


def extract_currency_and_values(rate_text=None, amount_text=None, discount_text=None, default_currency: str = 'INR'):
    """
    Unified FCY-aware extractor for rate, amount, discount, currency, exchange_rate.
    Works for both INR (plain) and FCY (foreign currency) vouchers.

    default_currency: per-company base currency (from Company.default_currency).
                      Used as the fallback when no symbol or ISO code is detected.
                      Replaces the old hardcoded 'INR' fallback.
    """
    result = {
        'currency'     : default_currency,
        'rate'         : 0.0,
        'amount'       : 0.0,
        'discount'     : 0.0,
        'exchange_rate': 1.0,
    }

    # Detect currency from either field
    detected = default_currency
    for txt in (amount_text, rate_text):
        c = _detect_currency(txt or '', default_currency=default_currency)
        if c != default_currency:
            detected = c
            break
    result['currency'] = detected

    if detected == default_currency:
        # Plain base-currency voucher — use legacy numeric extraction
        result['exchange_rate'] = 1.0
        if rate_text:
            result['rate'] = convert_to_float(extract_numeric_amount(rate_text))
        if amount_text:
            result['amount'] = convert_to_float(extract_numeric_amount(amount_text))
        if discount_text:
            result['discount'] = convert_to_float(extract_numeric_amount(discount_text))
    else:
        # FCY voucher — use dedicated FCY parsers
        if amount_text:
            result['amount']        = _parse_fcy_amount(amount_text)
            result['exchange_rate'] = _parse_fcy_exchange_rate(amount_text)

        if rate_text:
            result['rate'] = _parse_fcy_rate(rate_text)

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

def parse_ledger_voucher(
    xml_content,
    company_name:     str,
    voucher_type_name: str = 'ledger',
    allowed_types:    set  = None,
    material_centre:  str  = '',
    default_currency: str  = 'INR',
) -> list:
    """
    Parse Receipt / Payment / Journal / Contra XML from Tally.

    default_currency: per-company base currency passed from sync_service.
                      Used as fallback in _detect_currency / extract_currency_and_values
                      so plain amounts with no symbol are attributed to the correct currency.
    """
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
            narration      = clean_narration(voucher.findtext('NARRATION', ''))

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
                    'currency'      : default_currency,
                    'exchange_rate' : 1.0,
                    'narration'     : narration,
                    'guid'          : guid,
                    'alter_id'      : int(alter_id) if alter_id else 0,
                    'master_id'     : master_id,
                    'change_status' : change_status,
                    'is_deleted'    : 'Yes',
                    'material_centre': material_centre,
                })
                continue

            # Detect voucher-level FCY from any entry that has it
            voucher_exchange_rate = 1.0
            voucher_currency      = default_currency
            for ledger in ledger_entries:
                amount_text = clean_text(ledger.findtext('AMOUNT', '0'))
                temp        = extract_currency_and_values(None, amount_text, default_currency=default_currency)
                if temp['currency'] != default_currency:
                    voucher_currency      = temp['currency']
                    voucher_exchange_rate = temp['exchange_rate']
                    break

            for ledger in ledger_entries:
                ledger_name   = ledger.findtext('LEDGERNAME', '') or ''
                amount_text   = clean_text(ledger.findtext('AMOUNT', '0'))
                currency_info = extract_currency_and_values(None, amount_text, default_currency=default_currency)

                # Propagate voucher-level FCY if this entry didn't resolve its own
                if currency_info['currency'] == default_currency and voucher_currency != default_currency:
                    currency_info['currency']      = voucher_currency
                    currency_info['exchange_rate'] = voucher_exchange_rate

                # Determine Dr/Cr from raw amount sign
                raw_sign    = str(amount_text).strip()
                is_negative = raw_sign.startswith('-')
                amount_type = 'Debit' if is_negative else 'Credit'

                parsed_amount           = currency_info['amount']
                voucher_type_name_lower = voucher_type_name.strip().lower()


                all_rows.append({
                    'company_name'  : company_name,
                    'date'          : parse_tally_date_formatted(date),
                    'voucher_type'  : voucher_type,
                    'voucher_number': voucher_number,
                    'reference'     : reference,
                    'ledger_name'   : ledger_name,
                    'amount'        : parsed_amount,
                    'amount_type'   : amount_type,
                    'currency'      : currency_info['currency'],
                    'exchange_rate' : currency_info['exchange_rate'],
                    'narration'     : narration,
                    'guid'          : guid,
                    'alter_id'      : int(alter_id) if alter_id else 0,
                    'master_id'     : master_id,
                    'change_status' : change_status,
                    'is_deleted'    : is_deleted_flag,
                    'material_centre': material_centre,
                })

        _apply_ledger_signs(all_rows)
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

def parse_inventory_voucher(
    xml_content,
    company_name:     str,
    voucher_type_name: str = 'inventory',
    allowed_types:    set  = None,
    material_centre:  str  = '',
    default_currency: str  = 'INR',
) -> list:
    """
    Parse Sales / Purchase / Credit Note / Debit Note XML from Tally.

    default_currency: per-company base currency passed from sync_service.
    """
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
            party_name     = voucher.findtext('PARTYLEDGERNAME', '') or ''
            reference      = clean_text(voucher.findtext('REFERENCE', ''))
            narration      = clean_narration(voucher.findtext('NARRATION', ''))
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

            # ── Accounting Voucher View fallback (Credit Note / Debit Note) ──────
            # Tally's Accounting Voucher View embeds inventory inside a non-party
            # ALLLEDGERENTRIES.LIST using INVENTORYALLOCATIONS.LIST sub-nodes
            # (not ALLINVENTORYENTRIES.LIST). Extract those so item/qty/rate/amount
            # fields are populated instead of falling back to "No Item".
            if not inventory_entries:
                for ledger in ledger_entries:
                    if clean_text(ledger.findtext('ISPARTYLEDGER', 'No')) != 'Yes':
                        nested_inv = (
                            ledger.findall('.//ALLINVENTORYENTRIES.LIST') or
                            ledger.findall('.//INVENTORYENTRIES.LIST') or
                            ledger.findall('.//INVENTORYALLOCATIONS.LIST')
                        )
                        if nested_inv:
                            inventory_entries.extend(nested_inv)   # ← extend, not assign
                if inventory_entries:
                    logger.info(
                        f"[{voucher_type_name}] Accounting Voucher View: "
                        f"collected {len(inventory_entries)} inventory entries across all ledgers"
                    )

            # Deleted CDC stub — emit a single row so DB can mark it deleted
            if is_deleted_flag == 'Yes' and not ledger_entries and not inventory_entries:
                all_rows.append(_deleted_inventory_stub(
                    company_name, date, voucher_number, reference, voucher_type,
                    party_name, party_gstin, irn_number, eway_bill,
                    narration, guid, voucherkey, alter_id, master_id, change_status,
                    default_currency=default_currency,
                ))
                continue

            # ── Detect voucher-level FCY ───────────────────────────────────────
            voucher_currency      = default_currency
            voucher_exchange_rate = 1.0

            # Check ledger entries first (party ledger has the total amount)
            for ledger in ledger_entries:
                amt_txt = clean_text(ledger.findtext('AMOUNT', '0'))
                tmp     = extract_currency_and_values(None, amt_txt, default_currency=default_currency)
                if tmp['currency'] != default_currency:
                    voucher_currency      = tmp['currency']
                    voucher_exchange_rate = tmp['exchange_rate']
                    break

            # Fall back to inventory entries if ledger didn't reveal it
            if voucher_currency == default_currency:
                for inv in inventory_entries:
                    rate_elem   = inv.find('RATE')
                    amount_elem = inv.find('AMOUNT')
                    r_txt = clean_text(rate_elem.text   if rate_elem   is not None and rate_elem.text   else '0')
                    a_txt = clean_text(amount_elem.text if amount_elem is not None and amount_elem.text else '0')
                    tmp   = extract_currency_and_values(r_txt, a_txt, default_currency=default_currency)
                    if tmp['currency'] != default_currency:
                        voucher_currency      = tmp['currency']
                        voucher_exchange_rate = tmp['exchange_rate']
                        break

            # ── Total amount from party ledger (for total_amt column) ──────────
            total_amt_from_xml = 0.0
            # Standardizing the name format
            v_type = voucher_type_name.strip().lower().replace(' ', '_')
            for ledger in ledger_entries:
                if clean_text(ledger.findtext('ISPARTYLEDGER', 'No')) == 'Yes':
                    total_amt_from_xml = _parse_fcy_amount(
                        clean_text(ledger.findtext('AMOUNT', '0'))
                    )



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
                ledger_name_raw   = ledger.findtext('LEDGERNAME', '') or ''
                ledger_name_lower = ledger_name_raw.lower()
                amt_text          = clean_text(ledger.findtext('AMOUNT', '0'))
                amount            = convert_to_float(extract_numeric_amount(amt_text))

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
                    voucher_charges['other_amt'] += amount

            # ── Inventory line items ───────────────────────────────────────────
            real_items = [
                inv for inv in inventory_entries
                if clean_text(inv.findtext('STOCKITEMNAME', ''))
                and (
                    _parse_fcy_amount(
                        clean_text((inv.find('AMOUNT').text if inv.find('AMOUNT') is not None else ''))
                    ) > 0.01
                    or convert_to_float(
                        re.search(r'[\d,]+\.?\d*', clean_text((inv.find('ACTUALQTY').text if inv.find('ACTUALQTY') is not None else '') or '')).group()
                        if re.search(r'[\d,]+\.?\d*', clean_text((inv.find('ACTUALQTY').text if inv.find('ACTUALQTY') is not None else '') or '')) else '0'
                    ) > 0
                )
            ]

            named_items = [
                inv for inv in inventory_entries
                if clean_text(inv.findtext('STOCKITEMNAME', ''))
            ]

            has_real_inventory = bool(named_items)
            inventory_entries = real_items if real_items else named_items

            temp_item_data    = []
            total_item_amount = 0.0

            if has_real_inventory:
                for inv in inventory_entries:
                    item_name = inv.findtext('STOCKITEMNAME', '') or ''
                    if not item_name:
                        continue

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

                    currency_data = extract_currency_and_values(rate_txt, amount_txt, discount_txt, default_currency=default_currency)

                    # Propagate voucher-level FCY if item-level didn't resolve
                    if currency_data['currency'] == default_currency and voucher_currency != default_currency:
                        currency_data['currency']      = voucher_currency
                        currency_data['exchange_rate'] = voucher_exchange_rate

                    qty_numeric        = convert_to_float(re.search(r'[\d,]+\.?\d*', qty_txt).group() if re.search(r'[\d,]+\.?\d*', qty_txt) else '0')
                    alt_qty, alt_unit  = parse_quantity_with_unit(billed_txt)
                    # Unit priority: rate field → billedqty → actualqty
                    # Tally sometimes sends empty RATE and BILLEDQTY (e.g. zero-qty lines),
                    # so fall back to ACTUALQTY as a last resort.
                    _, qty_unit        = parse_quantity_with_unit(qty_txt)
                    unit               = extract_unit_from_rate(rate_txt) or alt_unit or qty_unit

                    batch_no = mfg_date = exp_date = ''
                    batch_allocations = inv.findall('.//BATCHALLOCATIONS.LIST')
                    if batch_allocations:
                        batch    = batch_allocations[0]
                        batch_no = batch.findtext('BATCHNAME', '') or ''
                        mfg_raw  = clean_text(batch.findtext('MFDON', ''))
                        mfg_date = parse_tally_date_formatted(mfg_raw) or ''
                        exp_elem = batch.find('EXPIRYPERIOD')
                        if exp_elem is not None:
                            if exp_elem.text:
                                exp_date = parse_expiry_date(exp_elem.text)
                            exp_jd = exp_elem.get('JD', '')
                            if exp_jd and not exp_date:
                                exp_date = parse_tally_date_formatted(exp_jd) or ''

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
                        'rate'         : currency_data['rate'],
                        'amount'       : item_amount,
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
                'material_centre' : material_centre,
            }

            temp_item_data.sort(key=lambda x: x['item_name'])

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

            if not temp_item_data:
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
                    proportion = item['amount'] / total_item_amount if total_item_amount > 0 else (1.0 / len(temp_item_data))
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
                        'cgst_amt'     : voucher_gst_data['cgst_total'] * proportion,
                        'sgst_amt'     : voucher_gst_data['sgst_total'] * proportion,
                        'igst_amt'     : voucher_gst_data['igst_total'] * proportion,
                        'freight_amt'  : voucher_charges['freight_amt'] if idx == 0 else 0.0,
                        'dca_amt'      : voucher_charges['dca_amt']     if idx == 0 else 0.0,
                        'cf_amt'       : voucher_charges['cf_amt']      if idx == 0 else 0.0,
                        'other_amt'    : voucher_charges['other_amt']   if idx == 0 else 0.0,
                        'total_amt'    : total_amt_from_xml if idx == 0 else 0.0,
                        'currency'     : item['currency'],
                        'exchange_rate': item['exchange_rate'],
                    })

        _apply_inventory_signs(all_rows, voucher_type_name)
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
    default_currency: str = 'INR',
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
        'currency'        : default_currency,
        'exchange_rate'   : 1.0,
        'narration'       : narration,
        'guid'            : guid,
        'voucherkey'      : voucherkey,
        'alter_id'        : int(alter_id) if alter_id else 0,
        'master_id'       : master_id,
        'change_status'   : change_status,
        'is_deleted'      : 'Yes',
        'material_centre' : '',
    }


# ──────────────────────────────────────────────────────────────────────────────
# Ledger master parser
# ──────────────────────────────────────────────────────────────────────────────

def parse_ledgers(xml_content, company_name: str, material_centre: str = '', default_currency: str = 'INR') -> list:
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
            ledger_name    = ledger.get('NAME', '') or ledger.findtext('NAME', '') or ''
            guid           = clean_text(ledger.findtext('GUID', ''))
            alter_id       = clean_text(ledger.findtext('ALTERID', '0'))
            parent         = ledger.findtext('PARENT', '') or ''
            created_date   = clean_text(ledger.findtext('CREATEDDATE', ''))
            altered_on     = clean_text(ledger.findtext('ALTEREDON', ''))
            email          = clean_text(ledger.findtext('EMAIL', ''))
            website        = clean_text(ledger.findtext('WEBSITE', ''))
            phone          = clean_text(ledger.findtext('LEDGERPHONE', ''))
            mobile         = clean_text(ledger.findtext('LEDGERMOBILE', ''))
            fax            = clean_text(ledger.findtext('LEDGERFAX', ''))
            contact_person = clean_text(ledger.findtext('LEDGERCONTACT', ''))

            aliases      = []
            direct_alias = ledger.findtext('ALIAS', '') or ''
            ledger_name_lower = ledger_name.lower().strip()
            if direct_alias:
                direct_alias_lower = direct_alias.lower().strip()
                if direct_alias_lower != ledger_name_lower:
                    aliases.append(direct_alias)
            for lang_list in ledger.findall('.//LANGUAGENAME.LIST'):
                for name_list in lang_list.findall('.//NAME.LIST'):
                    for name in name_list.findall('NAME'):
                        alias_text = clean_text(name.text or '')
                        if alias_text:
                            alias_text_lower = alias_text.lower().strip()
                            existing_aliases_lower = [a.lower().strip() for a in aliases]
                            if (alias_text_lower != ledger_name_lower and 
                                alias_text_lower not in existing_aliases_lower):
                                aliases.append(alias_text)

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
                'is_deleted'           : (
                    'Yes' if (
                        clean_text(ledger.findtext('ISDELETED', 'No')).lower() in ('yes', 'true', '1')
                        or ledger.get('ACTION', '') in ('Delete', 'Deleted')
                    ) else 'No'
                ),
                'created_date'         : created_date,
                'altered_on'           : altered_on,
                'guid'                 : guid,
                'alter_id'             : int(alter_id) if alter_id else 0,
                'material_centre'      : material_centre,
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

def parse_trial_balance(xml_content, company_name: str, start_date: str, end_date: str, material_centre: str = '', default_currency: str = 'INR') -> list:
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
            ledger_name = ledger.get('NAME', '') or ledger.findtext('LEDGERNAME', '') or ''
            if not ledger_name:
                continue

            guid         = clean_text(ledger.findtext('GUID',     ''))
            alter_id     = clean_text(ledger.findtext('ALTERID',  '0'))
            master_id    = clean_text(ledger.findtext('MASTERID', ''))
            parent_group = clean_text(ledger.findtext('PARENT',   ''))

            opening_text     = clean_text(ledger.findtext('OPENINGBALANCE', '0'))
            closing_text     = clean_text(ledger.findtext('CLOSINGBALANCE', '0'))
            # Trial balance values are base-currency totals — use plain extraction
            opening_val      = convert_to_float(extract_numeric_amount(str(opening_text)))
            closing_val      = convert_to_float(extract_numeric_amount(str(closing_text)))
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
                'material_centre' : material_centre,
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

def parse_items(xml_content, company_name: str, material_centre: str = '', default_currency: str = 'INR') -> list:
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
            item_name = item.get('NAME', '') or item.findtext('NAME', '') or ''
            guid      = clean_text(item.findtext('GUID', ''))

            if not item_name or not guid:
                skipped += 1
                logger.debug(f"Skipping item with no name/guid (name={item_name!r})")
                continue

            remote_alt_guid = clean_text(item.findtext('REMOTEALTGUID', ''))
            alter_id_raw    = clean_text(item.findtext('ALTERID', '0'))

            parent_group = item.findtext('PARENT', '') or ''
            category     = item.findtext('CATEGORY', '') or ''
            base_units   = item.findtext('BASEUNITS', '') or ''
            gst_type     = item.findtext('GSTTYPEOFSUPPLY', '') or ''

            opening_balance = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGBALANCE', '0')))
            )
            opening_rate    = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGRATE',    '0')))
            )
            opening_value   = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGVALUE',   '0')))
            )

            entered_by = item.findtext('ENTEREDBY', '') or ''

            is_deleted_raw = clean_text(item.findtext('ISDELETED', ''))
            action         = item.get('ACTION', '')
            is_deleted     = (
                'Yes' if (
                    is_deleted_raw.lower() in ('yes', 'true', '1')
                    or action in ('Delete', 'Deleted')
                ) else 'No'
            )

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
                'material_centre'   : material_centre,
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

def parse_outstanding(xml_content, company_name: str, material_centre: str = '', default_currency: str = 'INR') -> list:
    """
    Parse Sundry Debtors (Receivables) mapping Closing Balance to 'amount'.

    default_currency: per-company base currency passed from sync_service.
    """
    try:
        from lxml import etree as _lxml

        if not xml_content:
            logger.warning(f"Empty XML for outstanding [{company_name}]")
            return []

        raw = xml_content if isinstance(xml_content, bytes) else xml_content.encode('utf-8')
        parser = _lxml.XMLParser(recover=True, encoding='utf-8')
        root = _lxml.fromstring(raw, parser=parser)

        bills = root.findall('.//BILL')
        logger.info(f"Found {len(bills)} bill nodes [{company_name}]")

        all_rows = []

        for bill in bills:
            b_name = bill.get('NAME', '')
            party  = bill.findtext('PARENT', '') or ''
            b_id   = clean_text(bill.findtext('BILLID', '0'))

            raw_dt     = clean_text(bill.findtext('BILLDATE', ''))
            raw_due_dt = clean_text(bill.findtext('BILLDUEDATE', ''))

            closing_raw = clean_text(bill.findtext('CLOSINGBALANCE', '0'))

            amount   = _parse_fcy_amount(closing_raw)
            currency = _detect_currency(closing_raw, default_currency=default_currency)
            rate     = _parse_fcy_exchange_rate(closing_raw)

            all_rows.append({
                'company_name'   : company_name,
                'party_name'     : party,
                'bill_name'      : b_name,
                'bill_id'        : int(b_id) if b_id.isdigit() else None,
                'bill_date'      : parse_tally_date_formatted(raw_dt),
                'due_date'       : parse_tally_date_formatted(raw_due_dt),
                'currency'       : currency,
                'exchange_rate'  : rate,
                'amount'         : amount,
                'material_centre': material_centre,
            })

        logger.info(f"Successfully parsed {len(all_rows)} rows [{company_name}]")
        return all_rows

    except Exception as e:
        logger.error(f"Error parsing outstanding [{company_name}]: {e}")
        logger.error(traceback.format_exc())
        return []


# ──────────────────────────────────────────────────────────────────────────────
# GUID reconciliation parser  (Phase 3)
# ──────────────────────────────────────────────────────────────────────────────

def parse_guids(xml_content) -> "dict | None":
    """
    Parse a GUID XML response from Tally (utils/guid/*.xml templates).

    Returns:
        dict[str, str]  — {guid: voucher_number} on success.
                          voucher_number is '' if the template doesn't fetch it
                          (inventory GUID templates don't include VOUCHERNUMBER).
        None            — on ANY parse failure (network error, bad XML, empty content)

    CRITICAL CONTRACT for callers:
        None       → Tally fetch or parse FAILED → DO NOT delete or update anything
        {}  (empty)→ Tally confirmed 0 vouchers in window → safe to delete DB rows

    WHY dict instead of set:
        Ledger voucher GUID templates (receipt/payment/journal/contra) now also
        fetch VOUCHERNUMBER.  This lets Phase 3 detect not only deleted vouchers
        but also vouchers whose number changed (renumber) that CDC missed — e.g.
        because the app was offline when the deletion/renumber happened.
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

        result = {}
        for voucher in root.iter('VOUCHER'):
            guid_el  = voucher.find('GUID')
            vnum_el  = voucher.find('VOUCHERNUMBER')
            if guid_el is not None and guid_el.text and guid_el.text.strip():
                guid = guid_el.text.strip()
                vnum = (vnum_el.text or '').strip() if vnum_el is not None else ''
                result[guid] = vnum

        if not result:
            for e in root.iter('GUID'):
                if e.text and e.text.strip():
                    result[e.text.strip()] = ''

        logger.info(f"parse_guids: parsed {len(result)} GUIDs from Tally response")
        return result

    except ET.ParseError as e:
        logger.error(f"parse_guids: XML parse error — {e}")
        return None
    except Exception as e:
        logger.error(f"parse_guids: unexpected error — {e}")
        logger.error(traceback.format_exc())
        return None
