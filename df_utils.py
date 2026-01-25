# df_utils.py
"""
All DataFrame formatting and printable utilities live here.
This file contains NO Tkinter imports and NO ChatTab imports.

tk_chat_tabs.py -> only prints text returned by these functions.
job_status.py, interactive_bot.py -> call print(insert_table_black(df)).
"""

import pandas as pd


def insert_table_black(df, max_cell_width=30, max_rows=5000):
    """
    Returns a clean, text-based table representation of a DataFrame.

    - No GUI code here
    - No colors or tags (Tkinter handles styling)
    - Always print using print(insert_table_black(df)) inside threads
    """

    if df is None:
        return "No DataFrame provided."

    if df.empty:
        return "No data to display."

    # Limit rows if dataset is large
    original_rows = len(df)
    df = df.head(max_rows)

    # Extract columns + rows
    columns = df.columns.tolist()
    rows = df.to_dict("records")

    # Compute column widths
    col_widths = {}
    for col in columns:
        max_len = max(
            len(str(col)),
            max(len(str(row.get(col, ""))) for row in rows)
        )
        col_widths[col] = min(max_len, max_cell_width)

    def make_border(left, mid, right):
        return left + mid.join("-" * (col_widths[col] + 2) for col in columns) + right

    lines = []

    # Top border
    lines.append(make_border("+", "+", "+"))

    # Header
    header = "| " + " | ".join(col.center(col_widths[col]) for col in columns) + " |"
    lines.append(header)

    # Header bottom border
    lines.append(make_border("+", "+", "+"))

    # Data rows
    for row in rows:
        cells = []
        for col in columns:
            v = str(row.get(col, ""))
            if len(v) > col_widths[col]:
                v = v[:col_widths[col] - 3] + "..."
            cells.append(v.ljust(col_widths[col]))

        lines.append("| " + " | ".join(cells) + " |")

    # Bottom border
    lines.append(make_border("+", "+", "+"))

    # Show trimming note if applicable
    if original_rows > max_rows:
        lines.append(f"(Showing first {max_rows} rows out of {original_rows})")

    # Join lines into final text block
    return "\n".join(lines)


def pretty_series(series):
    """
    Utility: Convert a pandas Series into a list of "key: value" lines.
    """
    return "\n".join(f"{idx}: {val}" for idx, val in series.items())
