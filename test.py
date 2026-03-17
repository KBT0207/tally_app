import os
import pandas as pd
import xlwings as xw

from services.data_processor import parse_inventory_voucher, parse_outstanding_debtors
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

    xml_data = tally.fetch_outstanding_debtors(
        name,
        debug=True
    )

    rec = parse_outstanding_debtors(
        xml_content=xml_data,
        company_name=name
    )

 
    df = pd.DataFrame(rec)
    xw.view(df)