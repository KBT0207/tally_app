import os
import pandas as pd
from openpyxl import load_workbook

from services.data_processor import parse_inventory_voucher
from services.tally_connector import TallyConnector

tally = TallyConnector()
comps = tally.fetch_all_companies()
for comp in comps:
    name = comp.get("name")
    print(name)
    data = tally.fetch_sales(name, from_date='20220827',to_date='20220827',debug=True)
    rec = parse_inventory_voucher(data,company_name=name, material_centre='Vashi KBEIPL')
    df = pd.DataFrame(rec)
    df.to_excel('sales.xlsx',index=False)
  

