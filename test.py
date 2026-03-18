from __future__ import annotations

import os
import pandas as pd

from definitions import fetch_sheet_data
from definitions import build_offer_row_list_from_sheet_df, extract_unique_offer_keys
from definitions import build_query, load_config
from definitions import fetch_BQ_engagement_data
from definitions import calculate_new_price_from_history
from definitions import send_prices_to_sheets


def main() -> None:
    cfg = load_config("config.yaml")
    spreadsheet_id = os.environ.get("SPREADSHEET_ID") or cfg["sheets"]["spreadsheet_id"]
    sheet_name = os.environ.get("SHEET_NAME") or cfg["sheets"]["sheet_name"]

    # Sheet configuration
    ID_COLUMN = cfg["sheets"]["id_column"]
    WANTED_COLUMNS = [
        *cfg["sheets"]["wanted_columns"],
    ]
    LAST_N_ROWS = int(cfg["sheets"]["last_n_rows"])

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
    print(df.head(5))

    allowed_actions = tuple(cfg["pipeline"]["allowed_actions"])
    offer_rows = build_offer_row_list_from_sheet_df(df, allowed_actions=allowed_actions)
    keys = extract_unique_offer_keys(offer_rows)
    print("rows to process:", len(offer_rows))
    print("first rows:", offer_rows[:5])
    print("unique keys:", len(keys))
    print("first keys:", keys[:5])

    # BigQuery SQL build test (does not execute)
    bq = cfg["bigquery"]
    sql, params = build_query(
        project=bq["project"],
        dataset=bq["dataset"],
        view_name=bq["view_name"],
        keys=keys,
        start_date=bq.get("start_date"),
    )
    print("sql:\n", sql)
    print("params keys:", list(params.keys()))

    # Optional: execute BigQuery query if requested.
    if os.environ.get("RUN_BQ", "").strip() == "1":
        bq_df = fetch_BQ_engagement_data(sql=sql, params=params, project=bq["project"])
        print("bq rows:", len(bq_df))
        print("bq cols:", list(bq_df.columns))
        print(bq_df.head(5))

        priced_rows = calculate_new_price_from_history(
            offer_rows=offer_rows,
            bq_df=bq_df,
        )
        print("priced rows:", len(priced_rows))
        print("priced sample:", priced_rows[:5])

        # Optional: append to output sheet if requested.
        if os.environ.get("RUN_SHEETS", "").strip() == "1":
            out_cfg = cfg.get("output_sheets", {})
            resp = send_prices_to_sheets(
                spreadsheet_id=out_cfg.get(
                    "spreadsheet_id",
                    "146U6ZWg2Fxmp2VRkzMidm48J6m68trfW9-3hOVGVFkg",
                ),
                sheet_name=out_cfg.get("sheet_name", "price_tweak"),
                rows=priced_rows,
            )
            print("sheets append:", resp)


if __name__ == "__main__":
    main()
