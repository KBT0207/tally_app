import os
import pandas as pd
import xlwings as xw

from services.data_processor import parse_inventory_voucher
from services.tally_connector import TallyConnector


EXCEL_FILE = "sales_data.xlsx"
ALTER_FILE = "last_alter_id.txt"


tally = TallyConnector()




companies = tally.fetch_all_companies()


for c in companies:

    name = c.get("name")

    if not name:
        continue

    print("Fetching:", name)

    xml_data = tally.fetch_purchase_cdc(
        name,
        last_alter_id=467,
        debug=True
    )

    rec = parse_inventory_voucher(
        xml_content=xml_data,
        company_name=name
    )

 
    df = pd.DataFrame(rec)
    xw.view(df)