from __future__ import annotations

import os

from definitions import (
    build_offer_row_list_from_sheet_df,
    build_query,
    calculate_new_price_from_history,
    extract_unique_offer_keys,
    fetch_BQ_engagement_data,
    fetch_sheet_data,
    load_config,
    send_prices_to_sheets,
)


def run() -> None:
    cfg = load_config("config.yaml")

    # Sheets input
    spreadsheet_id = os.environ.get("SPREADSHEET_ID") or cfg["sheets"]["spreadsheet_id"]
    sheet_name = os.environ.get("SHEET_NAME") or cfg["sheets"]["sheet_name"]

    df = fetch_sheet_data(
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        wanted_columns=list(cfg["sheets"]["wanted_columns"]),
        last_n_rows=int(cfg["sheets"]["last_n_rows"]),
        id_column=cfg["sheets"]["id_column"],
    )

    allowed_actions = tuple(cfg["pipeline"]["allowed_actions"])
    offer_rows = build_offer_row_list_from_sheet_df(df, allowed_actions=allowed_actions)
    if not offer_rows:
        print("No rows to process.")
        return

    keys = extract_unique_offer_keys(offer_rows)
    if not keys:
        print("No valid keys to query.")
        return

    # BigQuery
    bq = cfg["bigquery"]
    sql, params = build_query(
        project=bq["project"],
        dataset=bq["dataset"],
        view_name=bq["view_name"],
        keys=keys,
        start_date=bq.get("start_date"),
    )
    bq_df = fetch_BQ_engagement_data(sql=sql, params=params, project=bq["project"])

    # Pricing
    priced_rows = calculate_new_price_from_history(
        offer_rows=offer_rows,
        bq_df=bq_df,
    )

    # Sheets output
    out_cfg = cfg.get("output_sheets", {})
    resp = send_prices_to_sheets(
        spreadsheet_id=out_cfg.get(
            "spreadsheet_id",
            "146U6ZWg2Fxmp2VRkzMidm48J6m68trfW9-3hOVGVFkg",
        ),
        sheet_name=out_cfg.get("sheet_name", "price_tweak"),
        rows=priced_rows,
    )
    print("Output sheet append:", resp)


if __name__ == "__main__":
    run()
