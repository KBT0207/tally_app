import os
import pandas as pd
from openpyxl import load_workbook
from xlwings import view

from services.data_processor import parse_inventory_voucher
from services.tally_connector import TallyConnector

tally = TallyConnector()
comps = tally.fetch_all_companies()
for comp in comps:
    name = comp.get("name")
    if name in ['', ' ', 'N/A', 'NA']:
        continue
    print(name)
    # data = tally.fetch_credit_note(name, from_date='20241004',to_date='20251004',debug=True)
    data = tally.fetch_sales(name, from_date='20260201',to_date='20260228',debug=True)
    rec = parse_inventory_voucher(data,company_name=name, material_centre='FCY KBEIPL', voucher_type_name='sales vouchers')
    df = pd.DataFrame(rec)
    view(df)
  

