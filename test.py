import os
import pandas as pd
import xlwings as xw

from services.data_processor import parse_inventory_voucher, parse_outstanding, parse_ledger_voucher
from services.tally_connector import TallyConnector





EXCEL_FILE = "sales_data.xlsx"
ALTER_FILE = "last_alter_id.txt"


tally = TallyConnector()




companies = tally.fetch_all_companies(debug=False)

for c in companies:

    name = c.get("name")

    if not name:
        continue

    print("Fetching:", name)

    xml_data = tally.fetch_debit_note(
        name,
        from_date='20250401',
        to_date='20260331',
        # debug=True
    )


    rec = parse_inventory_voucher(
        xml_content=xml_data,
        company_name=name,
        material_centre='FCY KBEIPL',
        # voucher_type_name='Debit Note'
        # voucher_type_name='Debit Note'
    )

 
    df = pd.DataFrame(rec)
    file_name = f'{name}.xlsx'
    # df.to_excel(file_name)
    xw.view(df)