from __future__ import annotations

import os
import pandas as pd

from definitions import fetch_sheet_data


def main() -> None:
    spreadsheet_id = os.environ.get("SPREADSHEET_ID", "").strip()
    sheet_name = os.environ.get("SHEET_NAME", "").strip()
    if not spreadsheet_id or not sheet_name:
        raise SystemExit(
            "Missing configuration. Set env vars and rerun:\n"
            "  export SPREADSHEET_ID='...'; export SHEET_NAME='...'; .venv/bin/python test.py"
        )

    # Sheet configuration
    ID_COLUMN = "ID"
    WANTED_COLUMNS = [
        "Ação",
        "Aparelho_padrão",
        "Deflatores Concat",
    ]
    LAST_N_ROWS = 500

    # Fetch
    df = fetch_sheet_data(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        wanted_columns=WANTED_COLUMNS,
        last_n_rows=LAST_N_ROWS,
        id_column=ID_COLUMN,
    )

    # Display / sanity checks
    pd.set_option("display.max_columns", 200)
    pd.set_option("display.width", 200)
    print("rows:", len(df))
    print("cols:", list(df.columns))
    print(df.head(20))


if __name__ == "__main__":
    main()
