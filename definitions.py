import pandas as pd


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


def _build_sheets_service(*, credentials=None):
    """Create a Google Sheets API client.

    Uses Application Default Credentials when credentials is None.
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


def build_query():
    return
def fetch_BQ_engagement_data():
    return
def calculate_new_price():
    return
def send_prices_to_sheets():
    return
