import os
import pandas as pd
from openpyxl import load_workbook

from services.data_processor import parse_outstanding
from services.tally_connector import TallyConnector


OUTPUT_FILE = "outstanding_all_companies.xlsx"


def fetch_company_data(tally: TallyConnector, company_name: str) -> list:
    """
    Fetch and parse outstanding data for a single company.
    Returns a non-empty list of records, or raises on failure.
    """
    print(f"  → Fetching XML from Tally ...")
    xml_data = tally.fetch_outstanding(company_name)

    if not xml_data:
        raise ValueError(f"Empty/None XML response from Tally for '{company_name}'")

    print(f"  → Parsing XML ...")
    records = parse_outstanding(xml_content=xml_data, company_name=company_name)

    if records is None:
        raise ValueError(f"parse_outstanding returned None for '{company_name}'")

    # Empty list is valid (company has zero outstanding bills) — not an error.
    print(f"  → Parsed {len(records)} record(s).")
    return records


def append_sheet_to_excel(file_path: str, sheet_name: str, df: pd.DataFrame) -> None:
    """
    Append (or overwrite) a sheet named `sheet_name` inside `file_path`.
    Creates the file if it does not yet exist.
    Sheet name is truncated to 31 chars (Excel limit).
    """
    sheet_name = sheet_name[:31]          # Excel sheet-name limit

    if os.path.exists(file_path):
        # File already exists — open it and add/replace the sheet
        with pd.ExcelWriter(
            file_path,
            engine="openpyxl",
            mode="a",                     # append mode
            if_sheet_exists="replace",    # overwrite if re-running same company
        ) as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    else:
        # First company — create a brand-new file
        with pd.ExcelWriter(file_path, engine="openpyxl", mode="w") as writer:
            df.to_excel(writer, sheet_name=sheet_name, index=False)


def main():
    tally = TallyConnector()

    # ── 1. Get company list ───────────────────────────────────────────────────
    print("\n=== Fetching company list ===")
    companies = tally.fetch_all_companies(debug=False)

    if not companies:
        print("No companies returned from Tally. Exiting.")
        return

    print(f"Found {len(companies)} company/companies.\n")

    # ── 2. Process each company fully before moving to the next ───────────────
    success_count = 0
    failed_companies = []

    for idx, c in enumerate(companies, start=1):
        name = c.get("name", "").strip()

        if not name:
            print(f"[{idx}] Skipping entry with no name.")
            continue

        print(f"[{idx}/{len(companies)}] Processing: {name}")

        try:
            # --- FETCH COMPLETE DATA (no partial writes) ----------------------
            records = fetch_company_data(tally, name)

            # --- ONLY WRITE after full successful fetch/parse -----------------
            df = pd.DataFrame(records)
            append_sheet_to_excel(OUTPUT_FILE, sheet_name=name, df=df)

            print(f"  ✓ Written to '{OUTPUT_FILE}' → sheet '{name[:31]}'\n")
            success_count += 1

        except Exception as e:
            print(f"  ✗ FAILED for '{name}': {e}\n")
            failed_companies.append(name)
            # Continue with next company — do NOT abort the whole run
            continue

    # ── 3. Summary ────────────────────────────────────────────────────────────
    print("=" * 60)
    print(f"Done. {success_count}/{len(companies)} companies written to '{OUTPUT_FILE}'.")

    if failed_companies:
        print(f"\nFailed companies ({len(failed_companies)}):")
        for fc in failed_companies:
            print(f"  • {fc}")
    else:
        print("All companies processed successfully.")


if __name__ == "__main__":
    main()