import xml.etree.ElementTree as ET
import re
import traceback
import time
from datetime import datetime
from logging_config import logger


def _is_optional_voucher(voucher_elem) -> bool:
    is_optional = voucher_elem.findtext('ISOPTIONAL', 'No').strip()
    return is_optional.lower() in ('yes', 'true', '1')


def _is_deleted_voucher(voucher_elem) -> bool:
    is_deleted = voucher_elem.findtext('ISDELETED', 'No').strip()
    action = voucher_elem.get('ACTION', 'Create').strip()
    return (
        is_deleted.lower() in ('yes', 'true', '1')
        or action in ('Delete', 'Deleted', 'Remove')
    )


def _should_skip_voucher(voucher_elem) -> bool:
    if _is_optional_voucher(voucher_elem):
        return True
    if _is_deleted_voucher(voucher_elem):
        return True
    return False


def get_voucher_guid(voucher_elem) -> str:
    guid_elem = voucher_elem.find('GUID')
    if guid_elem is not None and guid_elem.text:
        return guid_elem.text.strip()
    return ''


def get_voucher_number(voucher_elem) -> str:
    vnum_elem = voucher_elem.find('VOUCHERNUMBER')
    if vnum_elem is not None and vnum_elem.text:
        return vnum_elem.text.strip()
    return ''


def get_voucher_alter_id(voucher_elem) -> int:
    alter_id_elem = voucher_elem.find('ALTERID')
    if alter_id_elem is not None and alter_id_elem.text:
        try:
            return int(alter_id_elem.text.strip())
        except ValueError:
            pass
    return 0


def get_voucher_key(voucher_elem) -> str:
    vchkey = voucher_elem.get('VCHKEY', '').strip()
    if vchkey:
        return vchkey
    vkey_elem = voucher_elem.find('VOUCHERKEY')
    if vkey_elem is not None and vkey_elem.text:
        return vkey_elem.text.strip()
    return ''


def get_voucher_status_info(voucher_elem) -> dict:
    return {
        'guid':           get_voucher_guid(voucher_elem),
        'voucher_number': get_voucher_number(voucher_elem),
        'voucherkey':     get_voucher_key(voucher_elem),
        'alter_id':       get_voucher_alter_id(voucher_elem),
        'is_optional':    _is_optional_voucher(voucher_elem),
        'is_deleted':     _is_deleted_voucher(voucher_elem),
        'action':         voucher_elem.get('ACTION', 'Create').strip(),
        'should_skip':    _should_skip_voucher(voucher_elem),
    }


def detect_deleted_guids(local_guids: set, tally_guids: set) -> set:
    deleted = local_guids - tally_guids
    if deleted:
        logger.info(f"Deletion detection: {len(deleted)} GUIDs not found in Tally")
        for guid in deleted:
            logger.debug(f"  Deleted GUID: {guid}")
    return deleted


_SIGN_DETAIL_FIELDS = (
    'amount', 'cgst_amt', 'sgst_amt', 'igst_amt',
    'freight_amt', 'dca_amt', 'cf_amt', 'other_amt',
)

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
    for field in fields:
        val = row.get(field)
        if val is not None:
            try:
                fval = float(val)
                if fval != 0:
                    row[field] = -fval
            except (TypeError, ValueError):
                pass


def _round_fields(row: dict, *fields: str) -> None:
    for field in fields:
        val = row.get(field)
        if val is not None:
            try:
                row[field] = round(float(val), 2)
            except (TypeError, ValueError):
                pass


def _apply_inventory_signs(rows: list, voucher_type_name: str) -> list:
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
    for row in rows:
        if row.get('amount_type') == 'Debit':
            _negate_fields(row, 'amount')
        _round_fields(row, *_LEDGER_AMOUNT_FIELDS)
    return rows


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


def clean_text(text):
    if not text:
        return ""
    text = str(text).replace('&#13;&#10;', ' ').replace('&#13;', ' ').replace('&#10;', ' ')
    text = text.replace('\r\n', ' ').replace('\r', ' ').replace('\n', ' ')
    return text.strip()


def clean_narration(text):
    if not text:
        return ""
    text = clean_text(text)
    text = text.replace('\t', ' ')
    return re.sub(r' {2,}', ' ', text).strip()



def sanitize_xml_content(content) -> str:
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
    
    def _filter_char_ref(m):
        n = int(m.group(1))
        if (n in (9, 10, 13)
                or (0x20    <= n <= 0xD7FF)
                or (0xE000  <= n <= 0xFFFD)
                or (0x10000 <= n <= 0x10FFFF)):
            return m.group(0)   # valid — keep as-is
        return ''               # invalid — drop it

    content = re.sub(r'&#(\d+);', _filter_char_ref, content)

    content = re.sub(
        r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)',
        '&amp;',
        content,
    )

    used_prefixes = set(re.findall(r'</?([A-Za-z][A-Za-z0-9_]*):', content))
    if used_prefixes:
        def _inject_namespaces(m):
            root_tag = m.group(0)
            for prefix in sorted(used_prefixes):
                attr = f'xmlns:{prefix}'
                if attr not in root_tag:
                    root_tag = root_tag[:-1] + f' {attr}="urn:tally:{prefix.lower()}">'
            return root_tag
        content = re.sub(r'<ENVELOPE\b[^>]*>', _inject_namespaces, content, count=1)

    return content


def convert_to_float(value):
    if value is None or value == "":
        return 0.0
    try:
        return float(str(value).replace(',', '').strip())
    except Exception:
        return 0.0


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


_CURRENCY_SYMBOLS = r'[\$€£¥₹₨₩₱₽₺₪₦฿₫]'

_COMPOUND_DOLLAR_SYMBOLS = [
    ('AU$', 'AUD'),
    ('NZ$', 'NZD'),
    ('HK$', 'HKD'),
    ('SG$', 'SGD'),
    ('CA$', 'CAD'),
]
_COMPOUND_DOLLAR_PATTERN = r'(' + '|'.join(re.escape(s) for s, _ in _COMPOUND_DOLLAR_SYMBOLS) + r')'
_COMPOUND_DOLLAR_MAP = {sym: iso for sym, iso in _COMPOUND_DOLLAR_SYMBOLS}

_SYMBOL_TO_ISO = {
    '$': 'USD', '€': 'EUR', '£': 'GBP', '¥': 'JPY',
    '₹': 'INR', '₨': 'INR', '₩': 'KRW', '₱': 'PHP',
    '₽': 'RUB', '₺': 'TRY', '₪': 'ILS', '₦': 'NGN',
    '฿': 'THB', '₫': 'VND',
}

_ISO_TEXT_CODES = [
    'CAD', 'AUD', 'NZD', 'SGD', 'HKD', 'CHF', 'SEK', 'NOK', 'DKK',
    'MYR', 'IDR', 'AED', 'SAR', 'QAR', 'KWD', 'BHD', 'OMR',
    'ZAR', 'EGP', 'PKR', 'BDT', 'LKR', 'NPR', 'MMK', 'KES',
    'CNY', 'CNH', 'TWD', 'MXN', 'BRL', 'ARS', 'CLP', 'COP',
    'EUR', 'GBP', 'JPY', 'USD',
]
_ISO_TEXT_PATTERN = r'(?<!\w)(' + '|'.join(_ISO_TEXT_CODES) + r')(?=[\d\s\-/])'
# _ISO_TEXT_PATTERN = r'(?<!\w)(' + '|'.join(_ISO_TEXT_CODES) + r')(?![\w])'


def _detect_currency(text: str, default_currency: str = 'INR') -> str:
    if not text:
        return default_currency
    for sym, iso in _COMPOUND_DOLLAR_SYMBOLS:
        if sym in text:
            return iso
    for sym, iso in _SYMBOL_TO_ISO.items():
        if sym in text and iso != default_currency:
            return iso
    m = re.search(_ISO_TEXT_PATTERN, text)
    if m:
        return m.group(1)
    m = re.search(r'/(' + '|'.join(_ISO_TEXT_CODES) + r')\b', text)
    if m and m.group(1) != default_currency:
        return m.group(1)
    if re.search(r'\d\s?[?\ufffd]', text):
        return 'EUR'
    return default_currency


def _parse_fcy_rate(raw: str) -> float:
    if not raw:
        return 0.0
    raw = str(raw).strip()

    m = re.search(r'([\d,]+\.?\d*)[?\ufffd]\s*=', raw)
    if m:
        return convert_to_float(m.group(1))

    m = re.search(_COMPOUND_DOLLAR_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(2))

    m = re.search(_CURRENCY_SYMBOLS + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(1))

    m = re.search(r'([\d,]+\.?\d*)\s*' + _CURRENCY_SYMBOLS, raw)
    if m:
        return convert_to_float(m.group(1))

    m = re.search(_ISO_TEXT_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(2))

    m = re.search(r'([\d,]+\.?\d*)\s*/\s*[a-zA-Z]', raw)
    if m:
        return convert_to_float(m.group(1))

    m = re.search(r'([\d,]+\.?\d*)', raw)
    if m:
        return convert_to_float(m.group(1))

    return 0.0


def _parse_fcy_amount(raw: str) -> float:
    if not raw:
        return 0.0
    raw = str(raw).strip()

    is_negative = raw.startswith('-')

    def signed(value: float) -> float:
        return -abs(value) if is_negative else abs(value)

    m = re.search(r'-?\s*([\d,]+\.?\d*)[\s]*[?\ufffd]\s*@', raw)
    if m:
        return signed(convert_to_float(m.group(1)))

    m = re.search(r'-?\s*' + _COMPOUND_DOLLAR_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(2)))

    m = re.search(r'-?\s*' + _CURRENCY_SYMBOLS + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(1)))

    m = re.search(r'-?\s*' + _ISO_TEXT_PATTERN + r'\s*([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(2)))

    m = re.search(r'-?([\d,]+\.?\d*)', raw)
    if m:
        return signed(convert_to_float(m.group(1)))

    return 0.0


def _parse_fcy_exchange_rate(raw_amount: str) -> float:
    if not raw_amount:
        return 1.0

    def _ret(m, group=1):
        rate = convert_to_float(m.group(group))
        return rate if rate > 0 else 1.0

    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/' + _COMPOUND_DOLLAR_PATTERN, raw_amount)
    if m:
        return _ret(m, 1)

    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/\s*' + _CURRENCY_SYMBOLS, raw_amount)
    if m:
        return _ret(m, 1)

    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/' + _ISO_TEXT_PATTERN, raw_amount)
    if m:
        return _ret(m, 1)

    m = re.search(r'\?\s*([\d,]+\.?\d*)\s*/\s*[?\ufffd]', raw_amount)
    if m:
        return _ret(m, 1)

    return 1.0


def _is_fcy_string(text: str) -> bool:
    if not text:
        return False
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
    if not text:
        return "0"
    text = str(text)

    is_negative = text.strip().startswith('-')
    prefix = '-' if is_negative else ''

    if _is_fcy_string(text):
        m = re.search(r'-?\s*' + _CURRENCY_SYMBOLS + r'?\s*([\d,]+\.?\d*)', text)
        if m:
            return prefix + m.group(1)

    m = re.search(r'=\s*[?]?\s*[-]?(\d+\.?\d*)', text)
    if m:
        return prefix + m.group(1)

    m = re.search(r'[-]?(\d+\.?\d*)', text)
    if m:
        return prefix + m.group(1)

    return "0"


def extract_currency_and_values(rate_text=None, amount_text=None, discount_text=None, default_currency: str = 'INR'):
    result = {
        'currency'     : default_currency,
        'rate'         : 0.0,
        'amount'       : 0.0,
        'discount'     : 0.0,
        'exchange_rate': 1.0,
    }

    detected = default_currency
    for txt in (amount_text, rate_text):
        c = _detect_currency(txt or '', default_currency=default_currency)
        if c != default_currency:
            detected = c
            break
    result['currency'] = detected

    if detected == default_currency:
        result['exchange_rate'] = 1.0
        if rate_text:
            result['rate'] = convert_to_float(extract_numeric_amount(rate_text))
        if amount_text:
            result['amount'] = convert_to_float(extract_numeric_amount(amount_text))
        if discount_text:
            result['discount'] = convert_to_float(extract_numeric_amount(discount_text))
    else:
        if amount_text:
            result['amount']        = _parse_fcy_amount(amount_text)
            result['exchange_rate'] = _parse_fcy_exchange_rate(amount_text)

        if rate_text:
            result['rate'] = _parse_fcy_rate(rate_text)

            if result['exchange_rate'] == 1.0:
                exr = _parse_fcy_exchange_rate(rate_text)
                if exr != 1.0:
                    result['exchange_rate'] = exr

        if discount_text:
            result['discount'] = _parse_fcy_amount(discount_text)

    return result


def extract_unit_from_rate(rate_text):
    if not rate_text:
        return ""
    match = re.search(r'/\s*(\w+)\s*$', str(rate_text))
    return match.group(1) if match else ""


def parse_quantity_with_unit(qty_text):
    if not qty_text:
        return (0.0, "")
    qty_text = str(qty_text).strip()
    match = re.match(r'[-]?(\d+\.?\d*)\s*(\w*)', qty_text)
    if match:
        return (convert_to_float(match.group(1)), match.group(2) if match.group(2) else "")
    return (0.0, "")


def parse_ledger_voucher(
    xml_content,
    company_name:      str,
    voucher_type_name: str = 'ledger',
    allowed_types:     set = None,
    material_centre:   str = '',
    default_currency:  str = 'INR',
) -> list:
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
        skipped_optional = 0

        for voucher in vouchers:
            if _is_optional_voucher(voucher):
                skipped_optional += 1
                logger.debug(
                    f"[{voucher_type_name}] Skipping OPTIONAL voucher: "
                    f"{voucher.findtext('VOUCHERNUMBER', '')} "
                    f"(GUID: {voucher.findtext('GUID', '')})"
                )
                continue

            guid           = voucher.findtext('GUID', '')
            alter_id       = voucher.findtext('ALTERID', '0')
            master_id      = voucher.findtext('MASTERID', '')
            voucherkey     = get_voucher_key(voucher)
            voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
            voucher_type   = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
            date           = clean_text(voucher.findtext('DATE', ''))
            reference      = clean_text(voucher.findtext('REFERENCE', ''))
            narration      = clean_narration(voucher.findtext('NARRATION', ''))

            action          = voucher.get('ACTION', 'Create')
            is_deleted      = voucher.findtext('ISDELETED', 'No')
            change_status   = 'Deleted' if is_deleted == 'Yes' else action
            is_deleted_flag = 'Yes' if change_status in ('Deleted', 'Delete') else 'No'

            ledger_entries = voucher.findall('.//ALLLEDGERENTRIES.LIST')
            if not ledger_entries:
                ledger_entries = voucher.findall('.//LEDGERENTRIES.LIST')

            if not ledger_entries and is_deleted_flag == 'Yes':
                all_rows.append({
                    'company_name'   : company_name,
                    'date'           : parse_tally_date_formatted(date),
                    'voucher_type'   : voucher_type,
                    'voucher_number' : voucher_number,
                    'reference'      : reference,
                    'ledger_name'    : '',
                    'amount'         : 0.0,
                    'amount_type'    : None,
                    'currency'       : default_currency,
                    'exchange_rate'  : 1.0,
                    'narration'      : narration,
                    'guid'           : guid,
                    'voucherkey'     : voucherkey,
                    'alter_id'       : int(alter_id) if alter_id else 0,
                    'master_id'      : master_id,
                    'change_status'  : change_status,
                    'is_deleted'     : 'Yes',
                    'material_centre': material_centre,
                })
                continue

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

                if currency_info['currency'] == default_currency and voucher_currency != default_currency:
                    currency_info['currency']      = voucher_currency
                    currency_info['exchange_rate'] = voucher_exchange_rate
                    if voucher_exchange_rate and voucher_exchange_rate != 0:
                        inr_amt = currency_info['amount']
                        currency_info['amount'] = round(inr_amt / voucher_exchange_rate, 6)

                raw_sign    = str(amount_text).strip()
                is_negative = raw_sign.startswith('-')
                amount_type = 'Debit' if is_negative else 'Credit'

                parsed_amount = currency_info['amount']

                all_rows.append({
                    'company_name'   : company_name,
                    'date'           : parse_tally_date_formatted(date),
                    'voucher_type'   : voucher_type,
                    'voucher_number' : voucher_number,
                    'reference'      : reference,
                    'ledger_name'    : ledger_name,
                    'amount'         : parsed_amount,
                    'amount_type'    : amount_type,
                    'currency'       : currency_info['currency'],
                    'exchange_rate'  : currency_info['exchange_rate'],
                    'narration'      : narration,
                    'guid'           : guid,
                    'voucherkey'     : voucherkey,
                    'alter_id'       : int(alter_id) if alter_id else 0,
                    'master_id'      : master_id,
                    'change_status'  : change_status,
                    'is_deleted'     : is_deleted_flag,
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


def parse_inventory_voucher(
    xml_content,
    company_name:      str,
    voucher_type_name: str = 'inventory',
    allowed_types:     set = None,
    material_centre:   str = '',
    default_currency:  str = 'INR',
) -> list:
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
        skipped_optional = 0

        for voucher in vouchers:
            if _is_optional_voucher(voucher):
                skipped_optional += 1
                logger.debug(
                    f"[{voucher_type_name}] Skipping OPTIONAL voucher: "
                    f"{voucher.findtext('VOUCHERNUMBER', '')} "
                    f"(GUID: {voucher.findtext('GUID', '')})"
                )
                continue

            guid           = voucher.findtext('GUID', '')
            alter_id       = voucher.findtext('ALTERID', '0')
            master_id      = voucher.findtext('MASTERID', '')
            voucherkey     = clean_text(voucher.get('VCHKEY', '') or voucher.findtext('VOUCHERKEY', ''))
            voucher_number = clean_text(voucher.findtext('VOUCHERNUMBER', ''))
            voucher_type   = clean_text(voucher.findtext('VOUCHERTYPENAME', ''))
            date           = clean_text(voucher.findtext('DATE', ''))
            party_name     = voucher.findtext('PARTYLEDGERNAME', '') or ''
            reference      = clean_text(voucher.findtext('REFERENCE', ''))
            narration      = clean_narration(voucher.findtext('NARRATION', ''))
            party_gstin    = clean_text(voucher.findtext('PARTYGSTIN', ''))
            irn_number     = clean_text(voucher.findtext('IRNACKNO', ''))
            eway_bill      = clean_text(voucher.findtext('TEMPGSTEWAYBILLNUMBER', ''))

            action          = voucher.get('ACTION', 'Create')
            is_deleted      = voucher.findtext('ISDELETED', 'No')
            change_status   = 'Deleted' if is_deleted == 'Yes' else action
            is_deleted_flag = 'Yes' if change_status in ('Deleted', 'Delete') else 'No'

            ledger_entries    = (voucher.findall('.//ALLLEDGERENTRIES.LIST') or
                                 voucher.findall('.//LEDGERENTRIES.LIST'))
            inventory_entries = (voucher.findall('.//ALLINVENTORYENTRIES.LIST') or
                                 voucher.findall('.//INVENTORYENTRIES.LIST'))

            if not inventory_entries:
                for ledger in ledger_entries:
                    if clean_text(ledger.findtext('ISPARTYLEDGER', 'No')) != 'Yes':
                        nested_inv = (
                            ledger.findall('.//ALLINVENTORYENTRIES.LIST') or
                            ledger.findall('.//INVENTORYENTRIES.LIST') or
                            ledger.findall('.//INVENTORYALLOCATIONS.LIST')
                        )
                        if nested_inv:
                            inventory_entries.extend(nested_inv)
                if inventory_entries:
                    logger.info(
                        f"[{voucher_type_name}] Accounting Voucher View: "
                        f"collected {len(inventory_entries)} inventory entries across all ledgers"
                    )

            if is_deleted_flag == 'Yes' and not ledger_entries and not inventory_entries:
                all_rows.append(_deleted_inventory_stub(
                    company_name, date, voucher_number, reference, voucher_type,
                    party_name, party_gstin, irn_number, eway_bill,
                    narration, guid, voucherkey, alter_id, master_id, change_status,
                    default_currency=default_currency,
                ))
                continue

            voucher_currency      = default_currency
            voucher_exchange_rate = 1.0

            for ledger in ledger_entries:
                amt_txt = clean_text(ledger.findtext('AMOUNT', '0'))
                tmp     = extract_currency_and_values(None, amt_txt, default_currency=default_currency)
                if tmp['currency'] != default_currency:
                    voucher_currency      = tmp['currency']
                    voucher_exchange_rate = tmp['exchange_rate']
                    break

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

            total_amt_from_xml = 0.0
            v_type = voucher_type_name.strip().lower().replace(' ', '_')
            for ledger in ledger_entries:
                if clean_text(ledger.findtext('ISPARTYLEDGER', 'No')) == 'Yes':
                    party_amt_raw      = clean_text(ledger.findtext('AMOUNT', '0'))
                    total_amt_from_xml = _parse_fcy_amount(party_amt_raw)
                    if (voucher_currency != default_currency
                            and voucher_exchange_rate and voucher_exchange_rate != 0
                            and not _is_fcy_string(party_amt_raw)):
                        total_amt_from_xml = round(total_amt_from_xml / voucher_exchange_rate, 6)

            _ledgers_with_inventory = set()
            for ledger in ledger_entries:
                if clean_text(ledger.findtext('ISPARTYLEDGER', 'No')) != 'Yes':
                    nested_check = (
                        ledger.findall('.//ALLINVENTORYENTRIES.LIST') or
                        ledger.findall('.//INVENTORYENTRIES.LIST') or
                        ledger.findall('.//INVENTORYALLOCATIONS.LIST')
                    )
                    if nested_check:
                        _ledgers_with_inventory.add(
                            ledger.findtext('LEDGERNAME', '') or ''
                        )

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
                    if ledger_name_raw not in _ledgers_with_inventory:
                        voucher_charges['other_amt'] += amount

            real_items = [
                inv for inv in inventory_entries
                if clean_text(inv.findtext('STOCKITEMNAME', ''))
                and (
                    abs(_parse_fcy_amount(
                        clean_text((inv.find('AMOUNT').text if inv.find('AMOUNT') is not None else ''))
                    )) > 0.01
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
            inventory_entries  = real_items if real_items else named_items

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

                    if currency_data['currency'] == default_currency and voucher_currency != default_currency:
                        currency_data['currency']      = voucher_currency
                        currency_data['exchange_rate'] = voucher_exchange_rate
                        if voucher_exchange_rate and voucher_exchange_rate != 0:
                            if currency_data['amount']:
                                currency_data['amount'] = round(currency_data['amount'] / voucher_exchange_rate, 6)
                            if currency_data['rate']:
                                currency_data['rate'] = round(currency_data['rate'] / voucher_exchange_rate, 6)

                    qty_numeric       = convert_to_float(re.search(r'[\d,]+\.?\d*', qty_txt).group() if re.search(r'[\d,]+\.?\d*', qty_txt) else '0')
                    alt_qty, alt_unit = parse_quantity_with_unit(billed_txt)
                    _, qty_unit       = parse_quantity_with_unit(qty_txt)
                    unit              = extract_unit_from_rate(rate_txt) or alt_unit or qty_unit

                    batch_no = mfg_date = exp_date = ''
                    batch_allocations = inv.findall('.//BATCHALLOCATIONS.LIST')
                    if batch_allocations:
                        _batch_names = []
                        _mfg_dates   = []
                        _exp_dates   = []
                        for _batch in batch_allocations:
                            _bn = _batch.findtext('BATCHNAME', '') or ''
                            if _bn and _bn not in _batch_names:
                                _batch_names.append(_bn)
                            _mfg_raw = clean_text(_batch.findtext('MFDON', ''))
                            _md = parse_tally_date_formatted(_mfg_raw) or ''
                            if _md:
                                _mfg_dates.append(_md)
                            _exp_elem = _batch.find('EXPIRYPERIOD')
                            if _exp_elem is not None:
                                _ed = ''
                                if _exp_elem.text:
                                    _ed = parse_expiry_date(_exp_elem.text)
                                if not _ed:
                                    _exp_jd = _exp_elem.get('JD', '')
                                    if _exp_jd:
                                        _ed = parse_tally_date_formatted(_exp_jd) or ''
                                if _ed:
                                    _exp_dates.append(_ed)
                        batch_no = ', '.join(_batch_names)
                        mfg_date = min(_mfg_dates) if _mfg_dates else ''
                        exp_date = min(_exp_dates)  if _exp_dates  else ''

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

            aliases           = []
            direct_alias      = ledger.findtext('ALIAS', '') or ''
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
                            alias_text_lower       = alias_text.lower().strip()
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


def parse_items(xml_content, company_name: str, material_centre: str = '', default_currency: str = 'INR') -> list:
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
            entered_by   = item.findtext('ENTEREDBY', '') or ''

            opening_balance = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGBALANCE', '0')))
            )
            opening_rate    = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGRATE', '0')))
            )
            opening_value   = convert_to_float(
                extract_numeric_amount(clean_text(item.findtext('OPENINGVALUE', '0')))
            )

            is_deleted_raw = clean_text(item.findtext('ISDELETED', ''))
            action         = item.get('ACTION', '')
            is_deleted     = (
                'Yes' if (
                    is_deleted_raw.lower() in ('yes', 'true', '1')
                    or action in ('Delete', 'Deleted')
                ) else 'No'
            )

            hsn_code            = ''
            gst_applicable_from = ''
            taxability          = ''
            cgst_rate           = 0.0
            sgst_rate           = 0.0
            igst_rate           = 0.0
            cess_rate           = 0.0

            gst_details_list = sorted(
                item.findall('.//GSTDETAILS.LIST'),
                key=lambda x: x.findtext('APPLICABLEFROM', '0'),
                reverse=True,
            )

            for gst in gst_details_list:
                hsn = clean_text(gst.findtext('HSNCODE', ''))
                if hsn:
                    hsn_code            = hsn
                    gst_applicable_from = gst.findtext('APPLICABLEFROM', '') or ''
                    taxability          = gst.findtext('TAXABILITY', '') or ''

                    for rate_detail in gst.findall('.//RATEDETAILS.LIST'):
                        duty_head = (rate_detail.findtext('GSTRATEDUTYHEAD', '') or '').strip()
                        rate      = convert_to_float(
                            clean_text(rate_detail.findtext('GSTRATE', '0'))
                        )
                        if duty_head == 'Central Tax':
                            cgst_rate = rate
                        elif duty_head == 'State Tax':
                            sgst_rate = rate
                        elif duty_head == 'Integrated Tax':
                            igst_rate = rate
                        elif 'Cess' in duty_head:
                            cess_rate = rate

                    break

            all_rows.append({
                'company_name'       : company_name,
                'item_name'          : item_name,
                'parent_group'       : parent_group,
                'category'           : category,
                'base_units'         : base_units,
                'gst_type_of_supply' : gst_type,
                'hsn_code'           : hsn_code,
                'gst_applicable_from': gst_applicable_from,
                'taxability'         : taxability,
                'cgst_rate'          : cgst_rate,
                'sgst_rate'          : sgst_rate,
                'igst_rate'          : igst_rate,
                'cess_rate'          : cess_rate,
                'opening_balance'    : opening_balance,
                'opening_rate'       : opening_rate,
                'opening_value'      : opening_value,
                'entered_by'         : entered_by,
                'is_deleted'         : is_deleted,
                'guid'               : guid,
                'remote_alt_guid'    : remote_alt_guid,
                'alter_id'           : int(alter_id_raw) if alter_id_raw else 0,
                'material_centre'    : material_centre,
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


def parse_outstanding(xml_content, company_name: str, material_centre: str = '', default_currency: str = 'INR') -> list:
    try:
        if not xml_content:
            logger.warning(f"Empty XML for outstanding [{company_name}]")
            return []

        try:
            from lxml import etree as _lxml
            raw    = xml_content if isinstance(xml_content, bytes) else xml_content.encode('utf-8')
            parser = _lxml.XMLParser(recover=True, encoding='utf-8')
            root   = _lxml.fromstring(raw, parser=parser)
        except ImportError:
            logger.debug(f"lxml not available for outstanding [{company_name}] — using stdlib ElementTree")
            xml_str = sanitize_xml_content(xml_content)
            if not xml_str or not xml_str.strip():
                logger.warning(f"Empty XML after sanitization for outstanding [{company_name}]")
                return []
            root = ET.fromstring(xml_str.encode('utf-8'))

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


def parse_guids_with_status(xml_content) -> "dict | None":
    try:
        if not xml_content:
            logger.warning("parse_guids_with_status: received empty/None xml_content")
            return None

        xml_str = sanitize_xml_content(xml_content)
        if not xml_str or not xml_str.strip():
            logger.warning("parse_guids_with_status: empty XML after sanitization")
            return None

        root             = ET.fromstring(xml_str.encode('utf-8'))
        result           = {}
        skipped_optional = 0
        skipped_deleted  = 0

        for voucher in root.iter('VOUCHER'):
            guid = get_voucher_guid(voucher)
            if not guid:
                continue

            status = get_voucher_status_info(voucher)

            if status['should_skip']:
                if status['is_optional']:
                    skipped_optional += 1
                    logger.debug(
                        f"Skipping optional voucher: {status['voucher_number']} "
                        f"(GUID: {guid})"
                    )
                elif status['is_deleted']:
                    skipped_deleted += 1
                    logger.debug(
                        f"Skipping deleted voucher: {status['voucher_number']} "
                        f"(GUID: {guid})"
                    )
                continue

            result[guid] = {
                'voucher_number': status['voucher_number'],
                'voucherkey':     status['voucherkey'],
                'alter_id':       status['alter_id'],
                'is_optional':    status['is_optional'],
                'is_deleted':     status['is_deleted'],
            }

        logger.info(
            f"parse_guids_with_status: parsed {len(result)} active GUIDs "
            f"(skipped {skipped_optional} optional, {skipped_deleted} deleted)"
        )
        return result

    except ET.ParseError as e:
        logger.error(f"parse_guids_with_status: XML parse error — {e}")
        return None
    except Exception as e:
        logger.error(f"parse_guids_with_status: unexpected error — {e}")
        logger.error(traceback.format_exc())
        return None


def parse_guids(xml_content) -> "dict | None":
    result_with_status = parse_guids_with_status(xml_content)
    if result_with_status is None:
        return None

    return {
        guid: info['voucher_number']
        for guid, info in result_with_status.items()
    }


def parse_guids_vkey(xml_content) -> "dict | None":
    result_with_status = parse_guids_with_status(xml_content)
    if result_with_status is None:
        return None

    return {
        guid: info['voucherkey']
        for guid, info in result_with_status.items()
    }
