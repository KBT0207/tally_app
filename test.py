import os
import pandas as pd
from openpyxl import load_workbook
from xlwings import view

from services.data_processor import parse_inventory_voucher, parse_guids,parse_items
from services.tally_connector import TallyConnector

tally = TallyConnector()
comps = tally.fetch_all_companies()
for comp in comps:
    name = comp.get("name")
    if name in ['', ' ', 'N/A', 'NA']:
        continue
    data = tally.fetch_purchase_cdc(name, last_alter_id=340072,debug=True)
    # data = tally.fetch_items(name,debug=True)
    rec = parse_inventory_voucher(data,company_name=name, material_centre='FCY KBEIPL')
    df = pd.DataFrame(rec)
    view(df)
  

