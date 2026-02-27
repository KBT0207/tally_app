import re
import logging

logger = logging.getLogger(__name__)

CURRENCY_MAP = {
    'USD': {
        'symbols': ['$', 'US$', 'USD'],
        'pattern': r'\$|US\$|USD',
        'names': ['dollar', 'dollars', 'usd']
    },
    'EUR': {
        'symbols': ['€', 'EUR', '?'],
        'pattern': r'€|EUR|\?',
        'names': ['euro', 'euros', 'eur']
    },
    'GBP': {
        'symbols': ['£', 'G£', '\xa3', 'GBP', 'Gï¿½', '\ufffd', '�'],
        'pattern': r'£|G£|GBP|\xa3|\ufffd',
        'names': ['pound', 'pounds', 'sterling', 'gbp']
    },
    'JPY': {
        'symbols': ['¥', '¥', 'JPY'],
        'pattern': r'¥|JPY',
        'names': ['yen', 'jpy']
    },
    'CNY': {
        'symbols': ['¥', '元', 'CNY', 'RMB'],
        'pattern': r'CNY|RMB|元',
        'names': ['yuan', 'renminbi', 'cny', 'rmb']
    },
    'INR': {
        'symbols': ['₹', '₨', 'Rs', 'Rs.', 'INR'],
        'pattern': r'₹|₨|Rs\.?|INR',
        'names': ['rupee', 'rupees', 'inr']
    },
    'CHF': {
        'symbols': ['CHF', 'Fr', 'SFr'],
        'pattern': r'CHF|SFr\.?',
        'names': ['franc', 'francs', 'chf', 'swiss franc']
    },
    'CAD': {
        'symbols': ['C$', 'CAD', 'CA$'],
        'pattern': r'C\$|CA\$|CAD',
        'names': ['canadian dollar', 'cad']
    },
    'AUD': {
        'symbols': ['A$', 'AUD', 'AU$'],
        'pattern': r'A\$|AU\$|AUD',
        'names': ['australian dollar', 'aud']
    },
    'NZD': {
        'symbols': ['NZ$', 'NZD'],
        'pattern': r'NZ\$|NZD',
        'names': ['new zealand dollar', 'nzd']
    },
    'KRW': {
        'symbols': ['₩', 'KRW'],
        'pattern': r'₩|KRW',
        'names': ['won', 'krw']
    },
    'SGD': {
        'symbols': ['S$', 'SGD'],
        'pattern': r'S\$|SGD',
        'names': ['singapore dollar', 'sgd']
    },
    'HKD': {
        'symbols': ['HK$', 'HKD'],
        'pattern': r'HK\$|HKD',
        'names': ['hong kong dollar', 'hkd']
    },
    'NOK': {
        'symbols': ['kr', 'NOK'],
        'pattern': r'NOK',
        'names': ['norwegian krone', 'krone', 'nok']
    },
    'SEK': {
        'symbols': ['kr', 'SEK'],
        'pattern': r'SEK',
        'names': ['swedish krona', 'krona', 'sek']
    },
    'DKK': {
        'symbols': ['kr', 'DKK'],
        'pattern': r'DKK',
        'names': ['danish krone', 'dkk']
    },
    'PLN': {
        'symbols': ['zł', 'PLN'],
        'pattern': r'zł|PLN',
        'names': ['zloty', 'pln']
    },
    'THB': {
        'symbols': ['฿', 'THB'],
        'pattern': r'฿|THB',
        'names': ['baht', 'thb']
    },
    'MYR': {
        'symbols': ['RM', 'MYR'],
        'pattern': r'RM|MYR',
        'names': ['ringgit', 'myr']
    },
    'IDR': {
        'symbols': ['Rp', 'IDR'],
        'pattern': r'Rp\.?|IDR',
        'names': ['rupiah', 'idr']
    },
    'PHP': {
        'symbols': ['₱', 'PHP'],
        'pattern': r'₱|PHP',
        'names': ['peso', 'php', 'philippine peso']
    },
    'MXN': {
        'symbols': ['$', 'MX$', 'MXN'],
        'pattern': r'MX\$|MXN',
        'names': ['mexican peso', 'mxn']
    },
    'BRL': {
        'symbols': ['R$', 'BRL'],
        'pattern': r'R\$|BRL',
        'names': ['real', 'reais', 'brl', 'brazilian real']
    },
    'ARS': {
        'symbols': ['$', 'ARS'],
        'pattern': r'ARS',
        'names': ['argentine peso', 'ars']
    },
    'CLP': {
        'symbols': ['$', 'CLP'],
        'pattern': r'CLP',
        'names': ['chilean peso', 'clp']
    },
    'COP': {
        'symbols': ['$', 'COL$', 'COP'],
        'pattern': r'COL\$|COP',
        'names': ['colombian peso', 'cop']
    },
    'ZAR': {
        'symbols': ['R', 'ZAR'],
        'pattern': r'ZAR',
        'names': ['rand', 'zar', 'south african rand']
    },
    'RUB': {
        'symbols': ['₽', '₱', 'RUB'],
        'pattern': r'₽|RUB',
        'names': ['ruble', 'rouble', 'rub']
    },
    'TRY': {
        'symbols': ['₺', 'TL', 'TRY'],
        'pattern': r'₺|TRY',
        'names': ['lira', 'try', 'turkish lira']
    },
    'AED': {
        'symbols': ['د.إ', 'DH', 'AED'],
        'pattern': r'AED|DH',
        'names': ['dirham', 'aed', 'emirati dirham']
    },
    'SAR': {
        'symbols': ['﷼', 'SR', 'SAR'],
        'pattern': r'SAR|SR',
        'names': ['riyal', 'sar', 'saudi riyal']
    },
    'QAR': {
        'symbols': ['﷼', 'QR', 'QAR'],
        'pattern': r'QAR|QR',
        'names': ['qatari riyal', 'qar']
    },
    'KWD': {
        'symbols': ['KD', 'KWD'],
        'pattern': r'KWD|KD',
        'names': ['kuwaiti dinar', 'kwd']
    },
    'ILS': {
        'symbols': ['₪', 'ILS'],
        'pattern': r'₪|ILS',
        'names': ['shekel', 'ils', 'israeli shekel']
    },
    'EGP': {
        'symbols': ['E£', 'EGP', 'LE'],
        'pattern': r'E£|EGP|LE',
        'names': ['egyptian pound', 'egp']
    },
    'PKR': {
        'symbols': ['₨', 'Rs', 'PKR'],
        'pattern': r'PKR',
        'names': ['pakistani rupee', 'pkr']
    },
    'BDT': {
        'symbols': ['৳', 'Tk', 'BDT'],
        'pattern': r'৳|BDT',
        'names': ['taka', 'bdt', 'bangladeshi taka']
    },
    'LKR': {
        'symbols': ['Rs', 'LKR'],
        'pattern': r'LKR',
        'names': ['sri lankan rupee', 'lkr']
    },
    'NPR': {
        'symbols': ['Rs', 'NPR'],
        'pattern': r'NPR',
        'names': ['nepalese rupee', 'npr']
    },
    'VND': {
        'symbols': ['₫', 'VND'],
        'pattern': r'₫|VND',
        'names': ['dong', 'vnd', 'vietnamese dong']
    },
    'KZT': {
        'symbols': ['₸', 'KZT'],
        'pattern': r'₸|KZT',
        'names': ['tenge', 'kzt', 'kazakhstani tenge']
    },
    'UAH': {
        'symbols': ['₴', 'UAH'],
        'pattern': r'₴|UAH',
        'names': ['hryvnia', 'uah', 'ukrainian hryvnia']
    },
    'NGN': {
        'symbols': ['₦', 'NGN'],
        'pattern': r'₦|NGN',
        'names': ['naira', 'ngn', 'nigerian naira']
    },
    'KES': {
        'symbols': ['KSh', 'KES'],
        'pattern': r'KSh|KES',
        'names': ['kenyan shilling', 'kes']
    },
    'GHS': {
        'symbols': ['₵', 'GH₵', 'GHS'],
        'pattern': r'₵|GH₵|GHS',
        'names': ['cedi', 'ghs', 'ghanaian cedi']
    },
    'MAD': {
        'symbols': ['DH', 'MAD'],
        'pattern': r'MAD',
        'names': ['moroccan dirham', 'mad']
    },
    'TWD': {
        'symbols': ['NT$', 'TWD'],
        'pattern': r'NT\$|TWD',
        'names': ['new taiwan dollar', 'twd']
    },
    'CZK': {
        'symbols': ['Kč', 'CZK'],
        'pattern': r'Kč|CZK',
        'names': ['koruna', 'czk', 'czech koruna']
    },
    'HUF': {
        'symbols': ['Ft', 'HUF'],
        'pattern': r'Ft|HUF',
        'names': ['forint', 'huf', 'hungarian forint']
    },
    'RON': {
        'symbols': ['lei', 'RON'],
        'pattern': r'RON',
        'names': ['leu', 'ron', 'romanian leu']
    },
    'BGN': {
        'symbols': ['лв', 'BGN'],
        'pattern': r'BGN',
        'names': ['lev', 'bgn', 'bulgarian lev']
    },
    'HRK': {
        'symbols': ['kn', 'HRK'],
        'pattern': r'HRK',
        'names': ['kuna', 'hrk', 'croatian kuna']
    },
}


class CurrencyExtractor:
    
    def __init__(self, default_currency='INR'):
        self.currency_map = CURRENCY_MAP
        self.default_currency = default_currency
        
    def extract_currency(self, text, default=None):
        """
        Extract currency code from text.
        Handles corrupted currency symbols and ? placeholder pattern.
        """
        if not text or str(text).strip() == "" or str(text).strip() == "0":
            return default or self.default_currency
            
        text = str(text)
        
        # CRITICAL: Check for "? = ?" pattern (Tally's corrupted currency symbol)
        # Example: "9.60? = ? 864.00/Box" or "9.60 ? @ ? 105.18/ ? = ? 656651.36"
        if re.search(r'\d+\.?\d*\s*\?\s*[=@]', text):
            # This is foreign currency with corrupted symbol
            # Try to determine which currency based on exchange rate
            exchange_rate_match = re.search(r'@\s*\?\s*(\d+\.?\d*)', text)
            if exchange_rate_match:
                rate = float(exchange_rate_match.group(1))
                # GBP to INR: ~95-115, EUR to INR: ~85-95, USD to INR: ~75-85
                if 95 <= rate <= 115:
                    logger.debug(f"Detected GBP from ? pattern (rate={rate})")
                    return 'GBP'
                elif 85 <= rate <= 95:
                    logger.debug(f"Detected EUR from ? pattern (rate={rate})")
                    return 'EUR'
                elif 75 <= rate <= 85:
                    logger.debug(f"Detected USD from ? pattern (rate={rate})")
                    return 'USD'
            # Default to EUR for ? pattern without clear rate
            logger.debug("Detected foreign currency from ? pattern, defaulting to EUR")
            return 'EUR'
        
        # Enhanced GBP detection - catches various corrupted encodings including Unicode replacement char
        if re.search(r'G[\sï¿½\ufffd\xa3£Â£Ã‚Â£Ã¯Â¿Â½�]', text):
            return 'GBP'
        
        if '\xa3' in text or '£' in text or 'Â£' in text or 'Ã‚Â£' in text or 'Gï¿½' in text or '\ufffd' in text or '�' in text:
            return 'GBP'
        
        # Enhanced EUR detection
        if re.search(r'[€Ã¢â€šÂ¬ï¿½?]\s*=', text):
            return 'EUR'
        
        if re.search(r'(^|[\s])[€Ã¢â€šÂ¬ï¿½?][\s]*\d', text):
            return 'EUR'
        
        if 'ï¿½' in text and re.search(r'ï¿½\s*=\s*[?\s]*\d', text):
            return 'EUR'
        
        # Special handling for ? symbol with @ pattern (Tally EUR placeholder)
        if '?' in text and re.search(r'\?\s*@\s*\?', text):
            return 'EUR'
        
        # Enhanced CAD detection
        if re.search(r'CA\$|CAD', text, re.IGNORECASE):
            return 'CAD'
        
        if re.search(r'C\$', text):
            return 'CAD'
            
        # Enhanced AUD detection
        if re.search(r'AU\$|AUD', text, re.IGNORECASE):
            return 'AUD'
        
        if re.search(r'A\$', text):
            return 'AUD'
        
        # Standard pattern matching for all currencies
        for currency_code, currency_info in self.currency_map.items():
            pattern = currency_info['pattern']
            if re.search(pattern, text, re.IGNORECASE):
                return currency_code
        
        # Name-based matching
        text_lower = text.lower()
        for currency_code, currency_info in self.currency_map.items():
            for name in currency_info['names']:
                if name in text_lower:
                    return currency_code
        
        # If text contains numbers but no currency detected, use default
        if re.search(r'\d', text):
            return default or self.default_currency
            
        return default or self.default_currency
    
    def extract_foreign_currency_details(self, text):
        """
        Extract foreign currency details from Tally formatted text.
        
        Examples:
            "33.93 £ = ? 3568.76/Box" -> {'foreign_amount': 33.93, 'foreign_currency': 'GBP', 'base_amount': 3568.76}
            "6243.12 £ @ ? 105.18/ £ = ? 656651.36" -> {'foreign_amount': 6243.12, 'foreign_currency': 'GBP', 'exchange_rate': 105.18, 'base_amount': 656651.36}
            "1000 USD" -> {'foreign_amount': 1000, 'foreign_currency': 'USD', 'base_amount': None}
        
        Returns:
            dict with keys: foreign_amount, foreign_currency, exchange_rate (optional), base_amount (optional)
        """
        if not text or str(text).strip() == "" or str(text).strip() == "0":
            return {
                'foreign_amount': None,
                'foreign_currency': self.default_currency,
                'exchange_rate': None,
                'base_amount': None
            }
        
        text = str(text)
        result = {
            'foreign_amount': None,
            'foreign_currency': self.default_currency,
            'exchange_rate': None,
            'base_amount': None
        }
        
        
        # Pattern 2: "AMOUNT SYMBOL @ RATE/SYMBOL = BASE"
        # Example: "6243.12 £ @ ? 105.18/ £ = ? 656651.36" or "4900.00? @ ? 89.23/? = ? 437227.00"
        # Check this FIRST because it's more specific
        pattern2 = r'([-]?\d+\.?\d*)\s*([^\d\s=@]+)\s*@\s*([^\d\s=@]+)\s*([-]?\d+\.?\d*)\s*/\s*[^\d\s=]+\s*=\s*[?]?\s*([-]?\d+\.?\d*)'
        match2 = re.search(pattern2, text)
        
        if match2:
            result['foreign_amount'] = float(match2.group(1))
            currency_symbol = match2.group(2).strip()
            result['exchange_rate'] = float(match2.group(4))
            result['base_amount'] = float(match2.group(5))
            result['foreign_currency'] = self.extract_currency(currency_symbol)
            return result
        
        # Pattern 1: "AMOUNT SYMBOL = BASE" or "AMOUNT SYMBOL = ? BASE"
        # Example: "33.93 £ = ? 3568.76/Box" or "14.00? = ? 1249.22/Box"
        # Only check this if Pattern 2 didn't match (no @ symbol)
        pattern1 = r'([-]?\d+\.?\d*)\s*([^\d\s=@]+)\s*=\s*([^\d\s]+)\s*([-]?\d+\.?\d*)'
        match1 = re.search(pattern1, text)
        
        if match1:
            result['foreign_amount'] = float(match1.group(1))
            currency_symbol = match1.group(2).strip()
            result['base_amount'] = float(match1.group(4))
            result['foreign_currency'] = self.extract_currency(currency_symbol)
            
            # Calculate exchange rate if we have both amounts
            if result['foreign_amount'] and result['base_amount'] and result['foreign_amount'] != 0:
                result['exchange_rate'] = result['base_amount'] / result['foreign_amount']
            
            # Also check for explicit exchange rate in the same text
            # Pattern: "@ ? RATE/ SYMBOL"
            # Example: "@ ? 105.18/ £"
            rate_pattern = r'@\s*[?]?\s*([-]?\d+\.?\d*)\s*/\s*([^\d\s=]+)'
            rate_match = re.search(rate_pattern, text)
            if rate_match:
                result['exchange_rate'] = float(rate_match.group(1))
            
            return result
        
        # Pattern 3: Just "AMOUNT SYMBOL" (no conversion)
        # Example: "1000 USD" or "£ 1000"
        pattern3 = r'([^\d\s]+)\s*([-]?\d+\.?\d*)|^([-]?\d+\.?\d*)\s*([^\d\s]+)'
        match3 = re.search(pattern3, text)
        
        if match3:
            if match3.group(1):  # Symbol before amount
                currency_symbol = match3.group(1).strip()
                result['foreign_amount'] = float(match3.group(2))
            else:  # Amount before symbol
                result['foreign_amount'] = float(match3.group(3))
                currency_symbol = match3.group(4).strip()
            
            result['foreign_currency'] = self.extract_currency(currency_symbol)
            return result
        
        # Fallback: Try to extract just the first number
        number_match = re.search(r'([-]?\d+\.?\d*)', text)
        if number_match:
            result['foreign_amount'] = float(number_match.group(1))
            result['foreign_currency'] = self.extract_currency(text)
        
        return result
    
    def extract_rate_and_currency(self, rate_text):
        """
        Extract rate, currency, and base rate from rate field.
        
        Example: "33.93 £ = ? 3568.76/Box"
        Returns: (33.93, 'GBP', 3568.76)
        """
        details = self.extract_foreign_currency_details(rate_text)
        return (
            details['foreign_amount'],
            details['foreign_currency'],
            details['base_amount']
        )
    
    def extract_amount_and_currency(self, amount_text):
        """
        Extract amount, currency, and exchange details from amount field.
        
        Example: "6243.12 £ @ ? 105.18/ £ = ? 656651.36"
        Returns: {
            'foreign_amount': 6243.12,
            'foreign_currency': 'GBP',
            'exchange_rate': 105.18,
            'base_amount': 656651.36
        }
        """
        return self.extract_foreign_currency_details(amount_text)
    
    def extract_currency_symbol(self, text):
        if not text:
            return ''
            
        text = str(text)
        
        # Check for corrupted GBP symbols including Unicode replacement character
        if re.search(r'G[\sï¿½\ufffd\xa3£�]', text) or '\ufffd' in text or '�' in text:
            return '£'
        
        symbols = {
            '$': '$',
            '€': '€',
            '£': '£',
            '\xa3': '£',
            'G£': '£',
            '?': '€',  # Tally often uses ? as placeholder for €
            '¥': '¥',
            '₹': '₹',
            '₨': '₨',
            '₩': '₩',
            '₱': '₱',
            '₽': '₽',
            '₺': '₺',
            '₪': '₪',
            '₦': '₦',
            '₵': '₵',
            '₴': '₴',
            '₸': '₸',
            '₫': '₫',
            '৳': '৳',
        }
        
        for symbol_code, symbol_display in symbols.items():
            if symbol_code in text:
                return symbol_display
                
        return ''
    
    def get_currency_info(self, currency_code):
        return self.currency_map.get(currency_code.upper())
    
    def is_valid_currency(self, currency_code):
        return currency_code.upper() in self.currency_map
    
    def extract_all_currencies(self, text):
        if not text:
            return []
            
        text = str(text)
        found_currencies = []
        
        # Check for corrupted GBP symbols including Unicode replacement character
        if re.search(r'G[\sï¿½\ufffd\xa3£�]', text) or '\ufffd' in text or '�' in text:
            found_currencies.append('GBP')
        
        for currency_code, currency_info in self.currency_map.items():
            if currency_code in found_currencies:
                continue
            pattern = currency_info['pattern']
            if re.search(pattern, text, re.IGNORECASE):
                found_currencies.append(currency_code)
                
        return found_currencies


# Convenience functions for backward compatibility
def extract_currency(text, default='INR'):
    extractor = CurrencyExtractor(default_currency=default)
    return extractor.extract_currency(text)


def extract_currency_symbol(text):
    extractor = CurrencyExtractor(default_currency='INR')
    return extractor.extract_currency_symbol(text)


def extract_foreign_currency_details(text, default='INR'):
    """
    New function to extract complete foreign currency information.
    
    Returns dict with: foreign_amount, foreign_currency, exchange_rate, base_amount
    """
    extractor = CurrencyExtractor(default_currency=default)
    return extractor.extract_foreign_currency_details(text)