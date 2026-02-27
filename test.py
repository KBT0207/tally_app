# import requests
# import xml.etree.ElementTree as ET
# from datetime import datetime

# COMPANY = "Freshnova Pvt Ltd (FCY)"
# TALLY_URL = "http://localhost:9000"

# def fetch_sales_vouchers(last_alter_id):
#     print(f"Requesting incremental data (ALTERID > {last_alter_id})...")

#     from_date = "20000101"
#     to_date = datetime.now().strftime("%Y%m%d")

#     # Define the XML with a Dynamic Collection and your specific columns
#     xml_data = f"""<ENVELOPE>
#     <HEADER>
#         <VERSION>1</VERSION>
#         <TALLYREQUEST>Export</TALLYREQUEST>
#         <TYPE>Collection</TYPE>
#         <ID>IncrementalSalesVouchers</ID>
#     </HEADER>
#     <BODY>
#         <DESC>
#             <STATICVARIABLES>
#                 <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
#                 <SVCURRENTCOMPANY>{COMPANY}</SVCURRENTCOMPANY>
#                 <SVFROMDATE>{from_date}</SVFROMDATE>
#                 <SVTODATE>{to_date}</SVTODATE>
#             </STATICVARIABLES>
#             <TDL>
#                 <TDLMESSAGE>
#                     <COLLECTION NAME="IncrementalSalesVouchers">
#                         <TYPE>Voucher</TYPE>
#                         <CHILDREEOF>$$VchTypeSales</CHILDREEOF>
#                         <FILTER>AlterIdFilter</FILTER>
                        
#                         <FETCH>GUID</FETCH>
#                         <FETCH>ALTERID</FETCH>
#                         <FETCH>MASTERID</FETCH>
#                         <FETCH>VOUCHERNUMBER</FETCH>
#                         <FETCH>VOUCHERTYPENAME</FETCH>
#                         <FETCH>DATE</FETCH>
#                         <FETCH>PARTYNAME</FETCH>
#                         <FETCH>REFERENCE</FETCH>
#                         <FETCH>NARRATION</FETCH>
#                         <FETCH>PARTYGSTIN</FETCH>
#                         <FETCH>IRNACKNO</FETCH>
#                         <FETCH>TEMPGSTEWAYBILLNUMBER</FETCH>
#                         <FETCH>ISDELETED</FETCH>

#                         <!-- Ledger Entry Fields -->
#                         <FETCH>ALLLEDGERENTRIES.LEDGERNAME</FETCH>
#                         <FETCH>ALLLEDGERENTRIES.AMOUNT</FETCH>

#                         <!-- Inventory Entry Fields -->
#                         <FETCH>ALLINVENTORYENTRIES.STOCKITEMNAME</FETCH>
#                         <FETCH>ALLINVENTORYENTRIES.ACTUALQTY</FETCH>
#                         <FETCH>ALLINVENTORYENTRIES.BILLEDQTY</FETCH>
#                         <FETCH>ALLINVENTORYENTRIES.RATE</FETCH>
#                         <FETCH>ALLINVENTORYENTRIES.AMOUNT</FETCH>
#                         <FETCH>ALLINVENTORYENTRIES.DISCOUNT</FETCH>

#                         <!-- Batch Allocation Fields -->
#                         <FETCH>ALLINVENTORYENTRIES.BATCHALLOCATIONS.BATCHNAME</FETCH>
#                         <FETCH>ALLINVENTORYENTRIES.BATCHALLOCATIONS.MFDON</FETCH>
#                         <FETCH>ALLINVENTORYENTRIES.BATCHALLOCATIONS.EXPIRYPERIOD</FETCH>

#                         <!-- Accounting Allocation Fields -->
#                         <FETCH>ALLINVENTORYENTRIES.ACCOUNTINGALLOCATIONS.GSTHSNSACCODE</FETCH>
#                     </COLLECTION>
#                     <SYSTEM TYPE="Formulae" NAME="AlterIdFilter">
#                         $ALTERID > {last_alter_id}
#                     </SYSTEM>
#                 </TDLMESSAGE>
#             </TDL>
#         </DESC>
#     </BODY>
# </ENVELOPE>"""

#     try:
#         response = requests.post(
#             TALLY_URL,
#             data=xml_data,
#             headers={"Content-Type": "text/xml"},
#             timeout=600  # Increased timeout for large nested data
#         )

#         response.raise_for_status()

#         if "<LINEERROR>" in response.text:
#             print("Tally Error detected in response.")
#             return None

#         # Save the file for debugging
#         filename = f"sales_sync_{datetime.now().strftime('%H%M%S')}.xml"
#         with open(filename, "w", encoding="utf-8") as f:
#             f.write(response.text)

#         root = ET.fromstring(response.text)
        
#         # Look for ALTERID within the VOUCHER tags
#         alter_ids = [
#             int(node.text)
#             for node in root.findall(".//ALTERID")
#             if node.text and node.text.isdigit()
#         ]

#         if alter_ids:
#             new_max = max(alter_ids)
#             print(f"Success! Fetched {len(alter_ids)} vouchers. New Max ALTERID: {new_max}")
#             return new_max
#         else:
#             print("No new/modified vouchers found.")
#             return last_alter_id

#     except Exception as e:
#         print("Error during fetch:", e)
#         return None

# if __name__ == "__main__":
#     # Ensure this matches your last successfully imported ID
#     current_last_id = 0
#     fetch_sales_vouchers(current_last_id)



from services.tally_connector import TallyConnector
from services.data_processor import *
import pandas as pd
from xlwings import view


tally = TallyConnector()

comp = tally.fetch_all_companies(debug=False)
for i in comp:
    name = i.get('name', '')
    if not name or name == 'N/A':
        continue

    
    sales = tally.fetch_items(company_name=name, debug=True)
    data = parse_items(sales, company_name=name)
    df = pd.DataFrame(data)
    # df.to_excel('sample.xlsx',index=False)
    view(df)


