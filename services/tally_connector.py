import re
import sys
import os
import threading
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Optional, Dict, Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from logging_config import logger


# ─────────────────────────────────────────────────────────────────────────────
#  EXE-safe resource path  (dev: relative to project root, EXE: _MEIPASS)
# ─────────────────────────────────────────────────────────────────────────────
def _resource_path(relative: str) -> str:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        base = sys._MEIPASS  # type: ignore[attr-defined]
    else:
        # services/tally_connector.py  →  one folder up = project root
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, relative.replace("/", os.sep))


class TallyConnector:
    """
    HTTP connector to a locally-running Tally Prime instance.

    Handles:
      • XML template loading & caching
      • Request preparation (date range, CDC AlterID substitution)
      • Response sanitisation for safe XML parsing
      • Debug file saving
      • All fetch methods for every voucher / master type
    """

    _xml_template_cache: Dict[str, ET.Element] = {}
    _xml_template_cache_lock = threading.Lock()  # guards concurrent writes to the cache

    def __init__(self, host='localhost', port=9000, timeout=(60, 1800), max_retries=3):
        self.host    = host
        self.port    = port
        self.url     = f'http://{host}:{port}'
        self.header  = {'Content-Type': 'text/xml; charset=utf-8'}
        self.status  = 'Disconnected'
        self.timeout = timeout
        self.session = self._create_session(max_retries)
        logger.info(f'Initializing TallyConnector → {self.url}')
        self.connect()

    # ── Session / connection ──────────────────────────────────────────────────

    def _create_session(self, max_retries: int) -> requests.Session:
        session = requests.Session()
        retry_strategy = Retry(
            total            = max_retries,
            backoff_factor   = 1,
            status_forcelist = [429, 500, 502, 503, 504],
            allowed_methods  = ["POST"],
        )
        adapter = HTTPAdapter(
            max_retries      = retry_strategy,
            pool_connections = 10,
            pool_maxsize     = 20,
        )
        session.mount("http://",  adapter)
        session.mount("https://", adapter)
        return session

    def connect(self) -> bool:
        try:
            logger.info(f'Connecting to Tally at {self.url} …')
            response = self.session.post(url=self.url, headers=self.header, timeout=60)
            if response.status_code == 200:
                self.status = 'Connected'
                logger.info('Connected to Tally ✓')
                return True
            self.status = 'Disconnected'
            logger.warning(f'Tally returned status {response.status_code}')
            return False
        except Exception as e:
            self.status = 'Disconnected'
            logger.error(f'Cannot connect to Tally: {e}', exc_info=True)
            return False

    def ping(self) -> bool:
        """
        Quick connectivity check — used by setup wizard and settings page.
        Returns True if Tally responds, False otherwise.
        Does NOT update self.status — non-destructive.
        Uses a short 8s timeout so the UI doesn't freeze.
        """
        try:
            response = self.session.post(
                url=self.url, headers=self.header, timeout=8
            )
            return response.status_code == 200
        except Exception:
            return False

    # ── XML template management ───────────────────────────────────────────────

    @classmethod
    def _load_xml_template(cls, template_path: str) -> ET.Element:
        # Double-checked locking: fast path (no lock) for already-cached entries,
        # slow path (with lock) for the first load of each template.
        if template_path not in cls._xml_template_cache:
            with cls._xml_template_cache_lock:
                if template_path not in cls._xml_template_cache:
                    tree = ET.parse(template_path)
                    cls._xml_template_cache[template_path] = tree.getroot()
                    logger.debug(f'Cached XML template: {template_path}')
        return ET.fromstring(ET.tostring(cls._xml_template_cache[template_path]))

    def _prepare_xml_request(
        self,
        template_path: str,
        company_name:  str,
        from_date:     Optional[str] = None,
        to_date:       Optional[str] = None,
        alter_id:      Optional[int] = None,
    ) -> bytes:
        root = self._load_xml_template(template_path)

        for elem in root.iter('SVCURRENTCOMPANY'):
            elem.text = company_name
        if from_date:
            for elem in root.iter('SVFROMDATE'):
                elem.text = from_date
        if to_date:
            for elem in root.iter('SVTODATE'):
                elem.text = to_date

        xml_str = ET.tostring(root, encoding='unicode')

        # Substitute text placeholders that can't be placed as XML elements
        alter_id_value = alter_id if alter_id is not None else 0
        xml_str = xml_str.replace('PLACEHOLDER_ALTER_ID',  str(alter_id_value))
        xml_str = xml_str.replace('PLACEHOLDER_FROM_DATE', from_date or '')
        xml_str = xml_str.replace('PLACEHOLDER_TO_DATE',   to_date   or '')

        return xml_str.encode('utf-8')

    # ── Response sanitisation ─────────────────────────────────────────────────

    @staticmethod
    def sanitize_xml(xml_content) -> bytes:
        """
        Decode, strip control characters, and fix unescaped ampersands so the
        response can be safely parsed by ElementTree.

        NOTE: Tally uses '?' as a placeholder for the home/base currency symbol.
              This is intentional — do NOT strip or replace it.  The FCY parsers
              in data_processor.py rely on it to detect exchange-rate patterns
              such as '? 84.5/$'.
        """
        if isinstance(xml_content, bytes):
            for encoding in ('utf-8', 'windows-1252', 'latin-1'):
                try:
                    xml_content = xml_content.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue

        xml_content = str(xml_content)

        # Log which foreign-currency symbols are present (debug only)
        _known_symbols = {
            '$': 'USD', '£': 'GBP', '€': 'EUR', '¥': 'JPY/CNY',
            '₹': 'INR', '₨': 'INR/PKR', '₩': 'KRW', '₱': 'PHP',
            '₽': 'RUB', '₺': 'TRY', '₪': 'ILS', '₦': 'NGN',
            '฿': 'THB', '₫': 'VND',
            # '?' is intentionally excluded — it is Tally's home-currency marker,
            # NOT a corrupt byte, and must be preserved for FCY parsing.
        }
        found = [f"{sym}({cur})" for sym, cur in _known_symbols.items() if sym in xml_content]
        if found:
            logger.debug(f'Currency symbols in response: {", ".join(found)}')

        # Strip truly invalid XML control characters (except \t \n \r which are fine)
        xml_content = re.sub(r'&#([0-8]|1[1-2]|1[4-9]|2[0-9]|3[0-1]);', '', xml_content)
        xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_content)

        # Fix bare & that are not part of a valid entity reference
        xml_content = re.sub(
            r'&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)',
            '&amp;',
            xml_content,
        )

        return xml_content.encode('utf-8')

    # ── Debug helpers ─────────────────────────────────────────────────────────

    def _save_debug_file(
        self,
        content:      bytes,
        prefix:       str,
        company_name: str,
        suffix:       str = 'xml',
    ) -> str:
        safe_company = company_name.replace(' ', '_')
        timestamp    = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename     = f"{prefix}_{safe_company}_{timestamp}.{suffix}"
        with open(filename, 'wb') as f:
            f.write(content)
        logger.info(f'Saved debug file: {filename}')
        return filename

    # ── Core fetch ────────────────────────────────────────────────────────────

    def _fetch(
        self,
        template_path: str,
        data_type:     str,
        company_name:  str,
        from_date:     Optional[str] = None,
        to_date:       Optional[str] = None,
        alter_id:      Optional[int] = None,
        debug:         bool          = False,
    ) -> Optional[bytes]:
        try:
            logger.info(f'[{company_name}] Fetching {data_type}')

            xml_payload = self._prepare_xml_request(
                _resource_path(template_path), company_name, from_date, to_date, alter_id
            )

            if from_date and to_date:
                logger.info(f'  Date range : {from_date} → {to_date}')
            if alter_id is not None:
                logger.info(f'  CDC mode   : AlterID > {alter_id}')

            if debug:
                self._save_debug_file(
                    xml_payload,
                    f'req_{data_type.lower().replace(" ", "_")}',
                    company_name,
                )

            t0 = datetime.now()
            try:
                response = self.session.post(
                    url     = self.url,
                    headers = self.header,
                    data    = xml_payload,
                    timeout = self.timeout,
                    stream  = False,
                )
            except requests.exceptions.Timeout:
                logger.error(f'[{company_name}] Timeout fetching {data_type}')
                return None

            if response.status_code != 200:
                logger.error(f'[{company_name}] HTTP {response.status_code} for {data_type}')
                return None

            elapsed = (datetime.now() - t0).total_seconds()
            logger.info(f'[{company_name}] Received {data_type} in {elapsed:.1f}s')

            if debug:
                self._save_debug_file(
                    response.content,
                    f'resp_raw_{data_type.lower().replace(" ", "_")}',
                    company_name,
                )
                sanitized_debug = self.sanitize_xml(response.content)
                self._save_debug_file(
                    sanitized_debug,
                    f'resp_{data_type.lower().replace(" ", "_")}',
                    company_name,
                )

            if alter_id is not None:
                self._verify_alter_id_filter(response.content, alter_id, data_type)

            return self.sanitize_xml(response.content)

        except Exception as e:
            logger.error(
                f'[{company_name}] Unexpected error fetching {data_type}: {e}', exc_info=True
            )
            return None

    def _verify_alter_id_filter(self, raw_content: bytes, alter_id: int, data_type: str):
        """Log a warning if any returned records are at or below the CDC threshold."""
        try:
            sanitized = self.sanitize_xml(raw_content)
            root      = ET.fromstring(sanitized)
            ids       = [
                int(e.text)
                for e in root.iter('ALTERID')
                if e.text and e.text.strip().lstrip('-').isdigit()
            ]
            if ids:
                min_id, max_id = min(ids), max(ids)
                ok = '✓' if min_id > alter_id else '✗ WARNING: some records at/below threshold!'
                logger.debug(
                    f'[ALTER_ID CHECK] {data_type} | count={len(ids)} | '
                    f'range={min_id}→{max_id} | threshold={alter_id} | {ok}'
                )
            else:
                logger.debug(
                    f'[ALTER_ID CHECK] {data_type} | no ALTERID tags in response (empty result)'
                )
        except Exception as e:
            logger.debug(f'[ALTER_ID CHECK] {data_type} | could not inspect response: {e}')

    # ── Company master ────────────────────────────────────────────────────────

    def fetch_all_companies(self, debug: bool = False) -> list:
        try:
            tree        = ET.parse(_resource_path('utils/company.xml'))
            xml_payload = ET.tostring(tree.getroot(), encoding='utf-8')

            if debug:
                self._save_debug_file(xml_payload, 'req_all_companies', 'ALL')

            response = self.session.post(
                url     = self.url,
                headers = self.header,
                data    = xml_payload,
                timeout = 120,
            )

            if response.status_code != 200:
                logger.error(f'fetch_all_companies: HTTP {response.status_code}')
                return []

            if debug:
                self._save_debug_file(response.content, 'resp_raw_all_companies', 'ALL')
                sanitized_debug = self.sanitize_xml(response.content)
                self._save_debug_file(sanitized_debug, 'resp_all_companies', 'ALL')

            sanitized = self.sanitize_xml(response.content)
            root      = ET.fromstring(sanitized)
            companies = [self._parse_company(c) for c in root.findall('.//COMPANY')]
            logger.info(f'Found {len(companies)} companies in Tally')
            return companies

        except Exception as e:
            logger.error(f'fetch_all_companies error: {e}', exc_info=True)
            return []

    @staticmethod
    def _parse_company(company: ET.Element) -> Dict[str, Any]:
        return {
            'guid'          : company.findtext('GUID',                   ''),
            'name'          : company.findtext('NAME',                   ''),
            'formal_name'   : company.findtext('BASICCOMPANYFORMALNAME', ''),
            'company_number': company.findtext('COMPANYNUMBER',          ''),
            'starting_from' : company.findtext('STARTINGFROM',           ''),
            'books_from'    : company.findtext('BOOKSFROM',              ''),
            'audited_upto'  : company.findtext('AUDITEDUPTO',            ''),
        }

    # ── Master fetches ────────────────────────────────────────────────────────

    def fetch_ledgers(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/ledger.xml', 'Ledgers', company_name, from_date, to_date, debug=debug)

    def fetch_ledger_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/ledger_cdc.xml', 'Ledgers CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_groups(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/groups.xml', 'Groups', company_name, from_date, to_date, debug=debug)

    def fetch_groups_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/groups_cdc.xml', 'Groups CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    # ── Inventory voucher fetches (snapshot) ──────────────────────────────────

    def fetch_sales(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/sales_vouchers.xml', 'Sales', company_name, from_date, to_date, debug=debug)

    def fetch_purchase(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/purchase_vouchers.xml', 'Purchase', company_name, from_date, to_date, debug=debug)

    def fetch_credit_note(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/credit_note.xml', 'Credit Note', company_name, from_date, to_date, debug=debug)

    def fetch_debit_note(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/debit_note.xml', 'Debit Note', company_name, from_date, to_date, debug=debug)

    # ── Ledger voucher fetches (snapshot) ─────────────────────────────────────

    def fetch_receipt(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/receipt_vouchers.xml', 'Receipt', company_name, from_date, to_date, debug=debug)

    def fetch_payment(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/payment_vouchers.xml', 'Payment', company_name, from_date, to_date, debug=debug)

    def fetch_journal(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/journal_vouchers.xml', 'Journal', company_name, from_date, to_date, debug=debug)

    def fetch_contra(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/contra_vouchers.xml', 'Contra', company_name, from_date, to_date, debug=debug)

    # ── CDC fetches ───────────────────────────────────────────────────────────

    def fetch_sales_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/sales_cdc.xml', 'Sales CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_purchase_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/purchase_cdc.xml', 'Purchase CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_receipt_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/receipt_cdc.xml', 'Receipt CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_payment_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/payment_cdc.xml', 'Payment CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_journal_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/journal_cdc.xml', 'Journal CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_contra_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/contra_cdc.xml', 'Contra CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_credit_note_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/credit_cdc.xml', 'Credit Note CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    def fetch_debit_note_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        return self._fetch(
            'utils/cdc/debit_cdc.xml', 'Debit Note CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    # ── Report fetches ────────────────────────────────────────────────────────

    def fetch_trial_balance(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/reports/tb.xml', 'Trial Balance', company_name, from_date, to_date, debug=debug)

    def fetch_balance_sheet(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/reports/balance_sheet.xml', 'Balance Sheet', company_name, from_date, to_date, debug=debug)

    def fetch_profit_loss(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        return self._fetch('utils/reports/profit_loss.xml', 'Profit & Loss', company_name, from_date, to_date, debug=debug)


# ── Item (StockItem) master fetches ───────────────────────────────────────

    def fetch_items(
        self,
        company_name: str,
        debug:        bool = False,
    ) -> Optional[bytes]:
        """Full snapshot of all StockItem masters."""
        return self._fetch(
            'utils/item.xml', 'Items',
            company_name, debug=debug,
        )

    def fetch_items_cdc(
        self,
        company_name:  str,
        last_alter_id: int,
        debug:         bool = False,
    ) -> Optional[bytes]:
        """CDC fetch — only StockItems changed since last_alter_id."""
        return self._fetch(
            'utils/cdc/item_cdc.xml', 'Items CDC',
            company_name, alter_id=last_alter_id, debug=debug,
        )

    # ── Outstanding fetches ───────────────────────────────────────────────────

    def fetch_outstanding_debtors(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """
        Fetch all vouchers under Sundry Debtors (Receivables) for the given
        date range.  Parsed by data_processor.parse_outstanding().
        """
        return self._fetch(
            'utils/outstanding_debtors.xml', 'Outstanding Debtors',
            company_name, from_date, to_date, debug=debug,
        )

    def fetch_outstanding_creditors(
        self,
        company_name: str,
        from_date:    Optional[str] = None,
        to_date:      Optional[str] = None,
        debug:        bool          = False,
    ) -> Optional[bytes]:
        """
        Fetch all vouchers under Sundry Creditors (Payables) for the given
        date range.  Parsed by data_processor.parse_outstanding().
        """
        return self._fetch(
            'utils/outstanding_creditors.xml', 'Outstanding Creditors',
            company_name, from_date, to_date, debug=debug,
        )

    # ── Date utilities ────────────────────────────────────────────────────────

    @staticmethod
    def parse_tally_date(date_str: str) -> Optional[datetime]:
        try:
            if date_str and date_str != 'N/A':
                return datetime.strptime(date_str, '%Y%m%d')
        except Exception:
            pass
        return None

    @staticmethod
    def format_tally_date(dt: datetime) -> Optional[str]:
        return dt.strftime('%Y%m%d') if dt else None

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self):
        if hasattr(self, 'session'):
            self.session.close()
            logger.info('TallyConnector session closed')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()