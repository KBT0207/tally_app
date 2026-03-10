import requests
import re
import os
from lxml import etree
from datetime import datetime

COMPANY = "Freshnova Pvt Ltd (FCY)"
TALLY_URL = "http://localhost:9000"
XML_SAVE_DIR = "tally_responses"


def save_xml(xml_bytes, prefix):
    os.makedirs(XML_SAVE_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = f"{XML_SAVE_DIR}/{prefix}_{ts}.xml"
    with open(path, "wb") as f:
        f.write(xml_bytes)
    print(f"✅ Saved: {path}")
    return path


def build_xml(comp_name, group_filter):
    """
    Fetch only the fields we need from Vouchers under Sundry Debtors.
    VOUCHERNUMBER = Invoice/Bill Number
    ALLLEDGERENTRIES = gives us the amount per ledger line
    """
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<ENVELOPE>
    <HEADER>
        <VERSION>1</VERSION>
        <TALLYREQUEST>Export</TALLYREQUEST>
        <TYPE>Collection</TYPE>
        <ID>BillOutstanding</ID>
    </HEADER>
    <BODY>
        <DESC>
            <STATICVARIABLES>
                <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
                <SVCURRENTCOMPANY>{comp_name}</SVCURRENTCOMPANY>
                <SVFROMDATE>20250101</SVFROMDATE>
                <SVTODATE>20251231</SVTODATE>
            </STATICVARIABLES>
            <TDL>
                <TDLMESSAGE>
                    <COLLECTION NAME="BillOutstanding" ISMODIFY="No">
                        <TYPE>Voucher</TYPE>
                        <CHILDOF>{group_filter}</CHILDOF>
                        <FETCH>DATE</FETCH>
                        <FETCH>VOUCHERNUMBER</FETCH>
                        <FETCH>VOUCHERTYPENAME</FETCH>
                        <FETCH>PARTYLEDGERNAME</FETCH>
                        <FETCH>REFERENCE</FETCH>
                        <FETCH>NARRATION</FETCH>
                        <FETCH>BASICDUEDATEOFPYMT</FETCH>
                        <FETCH>ALLLEDGERENTRIES.LEDGERNAME</FETCH>
                        <FETCH>ALLLEDGERENTRIES.AMOUNT</FETCH>
                        <FETCH>ALLLEDGERENTRIES.ISDEEMEDPOSITIVE</FETCH>
                    </COLLECTION>
                </TDLMESSAGE>
            </TDL>
        </DESC>
    </BODY>
</ENVELOPE>""".encode("utf-8")


def fetch_from_tally(xml_bytes, prefix):
    try:
        print(f"📡 Fetching {prefix}...")
        resp = requests.post(TALLY_URL, data=xml_bytes,
                             headers={"Content-Type": "application/xml"}, timeout=30)
        resp.raise_for_status()
        save_xml(resp.content, prefix)
        return resp.content
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to Tally.")
    except requests.exceptions.Timeout:
        print("❌ Timed out.")
    except Exception as e:
        print(f"❌ {e}")
    return None


def parse_vouchers(raw_bytes, party_type):
    if not raw_bytes:
        return []

    parser = etree.XMLParser(recover=True, encoding="utf-8")
    root = etree.fromstring(raw_bytes, parser=parser)

    err = root.findtext(".//LINEERROR")
    if err:
        print(f"❌ Tally error: {err}")
        return []

    records = []
    for v in root.findall(".//VOUCHER"):
        party   = (v.findtext("PARTYLEDGERNAME") or "").strip()
        vnum    = (v.findtext("VOUCHERNUMBER")   or "").strip()
        vtype   = (v.findtext("VOUCHERTYPENAME") or "").strip()
        ref     = (v.findtext("REFERENCE")       or "").strip()
        narr    = (v.findtext("NARRATION")        or "").strip()
        raw_dt  = (v.findtext("DATE")             or "").strip()
        due_raw = (v.findtext("BASICDUEDATEOFPYMT") or "").strip()

        if not party and not vnum:
            continue

        # Format dates
        def fmt(d):
            try: return datetime.strptime(d, "%Y%m%d").strftime("%d-%b-%Y")
            except: return d or "—"

        txn_date = fmt(raw_dt)
        due_date = fmt(due_raw)

        # Overdue days
        overdue = "—"
        if due_raw and len(due_raw) == 8:
            try:
                delta = (datetime.today() - datetime.strptime(due_raw, "%Y%m%d")).days
                overdue = f"{delta}d overdue" if delta > 0 else "Not due"
            except: pass

        # Get amount from ALLLEDGERENTRIES — party leg is ISDEEMEDPOSITIVE=No (credit to party)
        amount = 0.0
        for le in v.findall(".//ALLLEDGERENTRIES.LIST"):
            le_name    = (le.findtext("LEDGERNAME") or "").strip()
            is_deemed  = (le.findtext("ISDEEMEDPOSITIVE") or "").strip()
            le_amt_str = (le.findtext("AMOUNT") or "0").strip()
            if le_name == party:
                try:
                    amount = abs(float(le_amt_str))
                except: pass

        records.append({
            "Party":      party,
            "Invoice No": vnum,
            "Type":       vtype,
            "Date":       txn_date,
            "Due Date":   due_date,
            "Reference":  ref,
            "Narration":  narr,
            "Amount":     amount,
            "Overdue":    overdue,
            "Party Type": party_type,
        })

    return records


def print_report(records, title, party_type):
    section = [r for r in records if r["Party Type"] == party_type]
    if not section:
        print(f"\n⚠️  No {title} records found.")
        return 0.0

    print(f"\n{'='*110}")
    print(f"  {title}  ({len(section)} invoices)")
    print(f"{'='*110}")
    print(f"{'Party':<30} {'Invoice No':<18} {'Type':<10} {'Date':<13} {'Due Date':<13} {'Amount':>14}  Status")
    print(f"{'-'*110}")

    # Group by party for subtotals
    from collections import defaultdict
    by_party = defaultdict(list)
    for r in section:
        by_party[r["Party"]].append(r)

    grand = 0.0
    for party, rows in sorted(by_party.items()):
        party_total = 0.0
        for r in rows:
            print(f"  {r['Party']:<28} {r['Invoice No']:<18} {r['Type']:<10} "
                  f"{r['Date']:<13} {r['Due Date']:<13} {r['Amount']:>14,.2f}  {r['Overdue']}")
            party_total += r["Amount"]
        grand += party_total
        print(f"  {'':28} {'Subtotal →':>18} {'':>10} {'':>13} {'':>13} {party_total:>14,.2f}")
        print()

    print(f"{'='*110}")
    print(f"  {'TOTAL':>90} {grand:>14,.2f}")
    print(f"{'='*110}")
    return grand


if __name__ == "__main__":
    print(f"🔍 Fetching Receivables (Sundry Debtors) for: {COMPANY}\n")

    raw = fetch_from_tally(build_xml(COMPANY, "$$GroupSundryDebtors"), "debtors_outstanding")
    records = parse_vouchers(raw, "Dr")

    total = print_report(records, "📥  SUNDRY DEBTORS — Receivables", "Dr")
    print(f"\n  Total Receivable: {total:,.2f}")
    print(f"\n✅ Done. {len(records)} invoices fetched.")