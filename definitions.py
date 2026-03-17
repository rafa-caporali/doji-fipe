import pandas as pd
import re
from datetime import datetime


def _col_index_to_a1(col_index_1_based: int) -> str:
    """Convert 1-based column index to A1 column letters (1->A, 27->AA)."""
    if col_index_1_based <= 0:
        raise ValueError(f"col_index_1_based must be >= 1, got {col_index_1_based}")

    letters: list[str] = []
    n = col_index_1_based
    while n:
        n, rem = divmod(n - 1, 26)
        letters.append(chr(ord("A") + rem))
    return "".join(reversed(letters))


def _normalize_header(value: object) -> str:
    return str(value).strip()


def _normalize_product_name(value: object) -> str:
    """Normalize product_name string to improve matching."""
    s = str(value).strip()
    # Collapse repeated whitespace.
    s = re.sub(r"\s+", " ", s)
    return s


def _normalize_deflators_concat(value: object) -> str:
    """Normalize deflators string to match BigQuery canonical format.

    Expected BigQuery format: comma-separated tokens, TRIMed, DISTINCT, sorted.
    """
    if value is None:
        return "NO_DEFLATOR"

    s = str(value).strip()
    if not s:
        return "NO_DEFLATOR"

    lowered = s.casefold()
    if lowered in {"without deflators", "sem deflator", "sem deflators", "no deflator", "no deflators"}:
        return "NO_DEFLATOR"
    if lowered == "no_deflator":
        return "NO_DEFLATOR"

    parts = [p.strip() for p in s.split(",")]
    parts = [p for p in parts if p]
    if not parts:
        return "NO_DEFLATOR"

    # DISTINCT + stable ordering.
    parts = sorted(set(parts))
    return ",".join(parts)


def build_offer_row_list_from_sheet_df(
    df: pd.DataFrame,
    *,
    sheet_row_col: str = "sheet_row",
    id_col: str = "ID",
    action_col: str = "Ação",
    allowed_actions: tuple[str, ...] = ("Primeiro Anúncio", "Segundo Anúncio", "Terceiro Anúncio"),
    product_col: str = "Aparelho_padrão",
    deflators_col: str = "Deflatores Concat",
) -> list[dict[str, object]]:
    """Build a row-level list for pricing.

    Returns 1 dict per sheet row to be processed, including sheet_row and ID so
    we can write results back unambiguously.
    """
    required = [sheet_row_col, id_col, action_col, product_col, deflators_col]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"Missing required columns in sheet df: {missing}")

    filtered = df[df[action_col].isin(allowed_actions)].copy()
    if filtered.empty:
        return []

    filtered[product_col] = filtered[product_col].map(_normalize_product_name)
    filtered[deflators_col] = filtered[deflators_col].map(_normalize_deflators_concat)

    rows_df = filtered[[sheet_row_col, id_col, action_col, product_col, deflators_col]].copy()
    rows_df = rows_df.rename(
        columns={
            sheet_row_col: "sheet_row",
            id_col: "id",
            action_col: "acao",
            product_col: "product_name",
            deflators_col: "deflators",
        }
    )

    # Avoid empty product_name rows.
    rows_df = rows_df.dropna(subset=["product_name"])  # type: ignore[arg-type]
    rows_df = rows_df[rows_df["product_name"].astype(str).str.strip() != ""]

    # Cast sheet_row to int when possible.
    if "sheet_row" in rows_df.columns:
        rows_df["sheet_row"] = pd.to_numeric(rows_df["sheet_row"], errors="coerce").astype("Int64")

    return rows_df.to_dict(orient="records")


def extract_unique_offer_keys(
    offer_rows: list[dict[str, object]],
) -> list[dict[str, str]]:
    """Extract unique (product_name, deflators) keys from row-level list."""
    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    for r in offer_rows:
        pn = str(r.get("product_name", "")).strip()
        df = str(r.get("deflators", "")).strip()
        if not pn:
            continue
        key = (pn, df)
        if key in seen:
            continue
        seen.add(key)
        out.append({"product_name": pn, "deflators": df})
    return out


def _build_sheets_service(*, credentials=None):
    """Create a Google Sheets API client.

    Uses Application Default Credentials when credentials is None.

    NOTE: We currently use end-user OAuth/ADC for development. For production
    automation, switch to a service account (or SA impersonation). Google may
    block certain scopes for the default gcloud client ID over time.
    """
    try:
        from googleapiclient.discovery import build  # type: ignore
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "Missing Google Sheets client dependencies. "
            "Install with: pip install google-api-python-client google-auth"
        ) from e

    if credentials is None:
        try:
            import google.auth  # type: ignore
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(
                "Missing Google auth dependency. Install with: pip install google-auth"
            ) from e

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"]
        )

    return build("sheets", "v4", credentials=credentials, cache_discovery=False)


def load_config(path: str = "config.yaml") -> dict:
    """Load YAML config from disk."""
    try:
        import yaml  # type: ignore
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "Missing dependency for YAML config. Install with: pip install pyyaml"
        ) from e

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    if not isinstance(data, dict):
        raise ValueError(f"Invalid config format in {path}: expected a YAML mapping")
    return data


def fetch_sheet_data(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    wanted_columns: list[str],
    last_n_rows: int,
    id_column: str,
    service=None,
) -> pd.DataFrame:
    """Fetch only selected columns from an append-only Google Sheet.

    Strategy:
    - Read header row to map header name -> column letter.
    - Read the ID column to find the last non-empty row.
    - Fetch only the last N rows for the requested columns (plus ID).

    Returns a DataFrame with:
    - sheet_row: absolute row number in the sheet
    - id_column: the ID column value
    - requested columns (in the order provided)
    """

    if last_n_rows <= 0:
        raise ValueError(f"last_n_rows must be > 0, got {last_n_rows}")
    if not wanted_columns:
        raise ValueError("wanted_columns must not be empty")

    wanted_norm = [_normalize_header(h) for h in wanted_columns]
    id_norm = _normalize_header(id_column)

    # Build Sheets service lazily.
    if service is None:
        service = _build_sheets_service()

    sheets = service.spreadsheets().values()

    # 1) Fetch headers. Read wide enough to cover typical 52 columns.
    header_range = f"{sheet_name}!A1:AZ1"
    header_resp = sheets.get(spreadsheetId=spreadsheet_id, range=header_range).execute()
    header_row = (header_resp.get("values") or [[]])[0]
    headers = [_normalize_header(h) for h in header_row]

    header_to_index: dict[str, int] = {}
    for idx, h in enumerate(headers, start=1):
        if not h:
            continue
        # Keep the first occurrence if duplicates exist.
        header_to_index.setdefault(h, idx)

    missing = [h for h in ([id_norm] + wanted_norm) if h not in header_to_index]
    if missing:
        raise KeyError(
            "Sheet is missing required headers: "
            + ", ".join(missing)
            + f". Looked in range {header_range}."
        )

    id_col_letter = _col_index_to_a1(header_to_index[id_norm])

    # 2) Fetch ID column to find last non-empty row.
    # Note: values().get returns only up to the last non-empty cell, but can include
    # empty strings in between. We'll compute the last non-empty by scanning.
    id_range = f"{sheet_name}!{id_col_letter}2:{id_col_letter}"
    id_resp = sheets.get(spreadsheetId=spreadsheet_id, range=id_range).execute()
    id_values = id_resp.get("values") or []  # list[list[str]] as rows
    ids = [(row[0] if row else "") for row in id_values]

    last_non_empty_offset = None
    for i in range(len(ids) - 1, -1, -1):
        if str(ids[i]).strip() != "":
            last_non_empty_offset = i
            break

    if last_non_empty_offset is None:
        # No data rows.
        cols_out = ["sheet_row", id_norm] + wanted_norm
        return pd.DataFrame(columns=cols_out)

    last_row = 2 + last_non_empty_offset  # absolute row number
    start_row = max(2, last_row - last_n_rows + 1)

    # 3) Batch fetch requested columns for the narrowed row window.
    # Always include ID column even if not explicitly requested.
    fetch_headers = [id_norm] + [h for h in wanted_norm if h != id_norm]
    ranges: list[str] = []
    for h in fetch_headers:
        col_letter = _col_index_to_a1(header_to_index[h])
        ranges.append(f"{sheet_name}!{col_letter}{start_row}:{col_letter}{last_row}")

    batch = sheets.batchGet(
        spreadsheetId=spreadsheet_id,
        ranges=ranges,
        majorDimension="COLUMNS",
    ).execute()

    value_ranges = batch.get("valueRanges") or []
    if len(value_ranges) != len(ranges):
        raise RuntimeError(
            f"Unexpected Sheets batchGet response: expected {len(ranges)} ranges, got {len(value_ranges)}"
        )

    columns_data: dict[str, list[object]] = {}
    max_len = 0
    for h, vr in zip(fetch_headers, value_ranges):
        # With majorDimension=COLUMNS and a single-column range, values is either:
        # - [] (all blank)
        # - [[...]]
        values = vr.get("values") or []
        col = values[0] if values else []
        # Normalize to Python objects (strings from API) and preserve empties.
        col_list: list[object] = [v if v != "" else "" for v in col]
        columns_data[h] = col_list
        max_len = max(max_len, len(col_list))

    # Right-pad shorter columns because Sheets truncates trailing blanks.
    for h, col_list in columns_data.items():
        if len(col_list) < max_len:
            columns_data[h] = col_list + [None] * (max_len - len(col_list))

    df = pd.DataFrame(columns_data)
    df.insert(0, "sheet_row", list(range(start_row, start_row + len(df))))

    # Keep output columns stable: sheet_row, id, wanted columns in requested order.
    out_cols = ["sheet_row", id_norm] + [h for h in wanted_norm if h != id_norm]
    return df.reindex(columns=out_cols)


def build_query(
    *,
    project: str,
    dataset: str,
    view_name: str,
    keys: list[dict[str, str]],
    start_date: str | datetime | None = None,
) -> tuple[str, dict[str, object]]:
    """Build a parameterized SQL query against the combined view.

    The view is expected to include at least: product_name, deflators, created_at.

    Returns:
    - sql: query text with BigQuery named parameters (@keys, @start_date)
    - params: dict with parameter values (caller binds via BigQuery client)
    """
    if not project or not dataset or not view_name:
        raise ValueError("project, dataset, and view_name must be provided")
    if not keys:
        raise ValueError("keys must not be empty")

    view_fqn = f"{project}.{dataset}.{view_name}"

    where_parts: list[str] = []
    params: dict[str, object] = {"keys": keys}

    if start_date is not None:
        if isinstance(start_date, datetime):
            start_dt = start_date.strftime("%Y-%m-%d %H:%M:%S")
        else:
            start_dt = str(start_date).strip()
        if start_dt:
            where_parts.append("v.created_at >= @start_date")
            params["start_date"] = start_dt

    where_sql = "\nWHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
WITH keys AS (
  SELECT * FROM UNNEST(@keys)
)
SELECT
  v.product_name,
  v.seller_price,
  v.created_at,
  v.converted,
  v.deflators,
  v.unique_engagement_pre_release,
  v.buy_clicks_pre_release,
  v.photos_views_pre_release,
  v.offer_views_pre_release,
  v.unique_engagement_5m,
  v.unique_buyers_intent_5m,
  v.buy_clicks_after_release,
  v.photos_views_after_release,
  v.offer_views_after_release
FROM `{view_fqn}` v
JOIN keys k
  ON v.product_name = k.product_name
 AND v.deflators = k.deflators
{where_sql}
""".strip()

    return sql, params


def fetch_BQ_engagement_data(
    *,
    sql: str,
    params: dict[str, object],
    project: str | None = None,
    location: str | None = None,
    client=None,
) -> pd.DataFrame:
    """Execute a parameterized BigQuery query and return a DataFrame.

    Expects params to include:
    - keys: list[dict[str, str]] with keys: product_name, deflators
    Optional:
    - start_date: DATETIME string ("YYYY-MM-DD HH:MM:SS")
    """
    try:
        from google.cloud import bigquery  # type: ignore
    except ModuleNotFoundError as e:
        raise ModuleNotFoundError(
            "Missing BigQuery dependency. Install with: pip install google-cloud-bigquery"
        ) from e

    if not sql or not sql.strip():
        raise ValueError("sql must not be empty")

    if "keys" not in params:
        raise KeyError("params must include 'keys'")

    keys = params["keys"]
    if not isinstance(keys, list) or not keys:
        raise ValueError("params['keys'] must be a non-empty list")

    struct_values: list[object] = []
    for item in keys:
        if not isinstance(item, dict):
            raise TypeError("Each element in params['keys'] must be a dict")
        pn = str(item.get("product_name", "")).strip()
        df = str(item.get("deflators", "")).strip()
        struct_values.append(
            bigquery.StructQueryParameter(
                None,
                bigquery.ScalarQueryParameter("product_name", "STRING", pn),
                bigquery.ScalarQueryParameter("deflators", "STRING", df),
            )
        )

    query_parameters: list[object] = [
        bigquery.ArrayQueryParameter("keys", "STRUCT", struct_values)
    ]

    start_date = params.get("start_date")
    if start_date is not None and str(start_date).strip() != "":
        query_parameters.append(
            bigquery.ScalarQueryParameter("start_date", "DATETIME", str(start_date).strip())
        )

    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

    if client is None:
        client = bigquery.Client(project=project)

    job = client.query(sql, job_config=job_config, location=location)
    return job.to_dataframe()


def send_prices_to_sheets(
    *,
    spreadsheet_id: str,
    sheet_name: str,
    rows: list[dict[str, object]],
    header: list[str] | None = None,
    service=None,
) -> dict[str, object]:
    """Append pricing rows to a destination sheet, skipping duplicates by sheet_row.

    The destination sheet is treated as append-only, with a header row.
    The function checks existing sheet_row values to avoid duplicates.
    """
    if not spreadsheet_id or not sheet_name:
        raise ValueError("spreadsheet_id and sheet_name are required")
    if not rows:
        return {"appended": 0, "skipped": 0, "reason": "no rows"}

    if header is None:
        header = ["sheet_row", "id", "acao", "product_name", "deflators", "new_price"]

    if service is None:
        service = _build_sheets_service()

    sheets = service.spreadsheets()
    values = sheets.values()

    # Check if sheet exists and get header row; create sheet if needed.
    try:
        header_resp = values.get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1:ZZ1",
        ).execute()
    except Exception:
        sheets.batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
        ).execute()
        header_resp = {"values": []}

    header_row = (header_resp.get("values") or [[]])[0]
    header_row_norm = [_normalize_header(h) for h in header_row]

    # Write header if sheet is empty.
    if not header_row_norm or all(h == "" for h in header_row_norm):
        values.update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption="RAW",
            body={"values": [header]},
        ).execute()
        header_row_norm = header

    # Find sheet_row column index in destination.
    header_to_index = {h: i + 1 for i, h in enumerate(header_row_norm) if h}
    if "sheet_row" not in header_to_index:
        raise KeyError(
            "Destination sheet is missing 'sheet_row' header; cannot dedupe safely."
        )

    sheet_row_col_letter = _col_index_to_a1(header_to_index["sheet_row"])
    existing_resp = values.get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!{sheet_row_col_letter}2:{sheet_row_col_letter}",
    ).execute()
    existing_vals = existing_resp.get("values") or []
    existing_rows = {
        str(row[0]).strip() for row in existing_vals if row and str(row[0]).strip() != ""
    }

    # Filter rows by sheet_row not already present.
    filtered_rows: list[dict[str, object]] = []
    skipped = 0
    for r in rows:
        sr = str(r.get("sheet_row", "")).strip()
        if not sr:
            skipped += 1
            continue
        if sr in existing_rows:
            skipped += 1
            continue
        filtered_rows.append(r)

    if not filtered_rows:
        return {"appended": 0, "skipped": skipped, "reason": "all rows already present"}

    # Build row values in header order.
    out_values: list[list[object]] = []
    for r in filtered_rows:
        row_vals: list[object] = []
        for col in header:
            row_vals.append(r.get(col, ""))
        out_values.append(row_vals)

    append_resp = values.append(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": out_values},
    ).execute()

    return {
        "appended": len(out_values),
        "skipped": skipped,
        "updates": append_resp.get("updates"),
    }


def calculate_new_price():
    return
