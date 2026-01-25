# display_utils.py
# Small helpers that print structured text markers to stdout.
# interactive_bot.py will import and use these instead of GUI-only functions.

import sys
import pandas as pd
import textwrap

_MARK_TABLE_START = "<<TABLE_START>>"
_MARK_TABLE_END = "<<TABLE_END>>"
_MARK_BLOCK_START = "<<BLOCK_START>>"
_MARK_BLOCK_END = "<<BLOCK_END>>"
_MARK_BOT = "<<BOT>>"
_MARK_ERROR = "<<ERROR>>"
_MARK_USER = "<<USER>>"

def _print_raw(s: str):
    # Use stdout.write to avoid extra spaces/newlines; flush for real-time UI.
    sys.stdout.write(s)
    sys.stdout.flush()

def print_bot(line: str):
    # Use to print a single bot line (non-table)
    _print_raw(f"{_MARK_BOT}{line}\n")

def print_error(line: str):
    _print_raw(f"{_MARK_ERROR}{line}\n")

def print_user_echo(line: str):
    _print_raw(f"{_MARK_USER}{line}\n")

def print_block(obj):
    """
    Prints a copy-friendly block wrapped in markers.
    The GUI will render a copy button when seeing these markers.
    obj may be str or other object.
    """
    if isinstance(obj, str):
        text = obj
    elif hasattr(obj, "to_string"):
        try:
            text = obj.to_string()
        except Exception:
            text = str(obj)
    else:
        import pprint
        text = pprint.pformat(obj, indent=2, width=100)

    _print_raw(_MARK_BLOCK_START + "\n")
    _print_raw(text + "\n")
    _print_raw(_MARK_BLOCK_END + "\n")

def print_table(df: pd.DataFrame, title: str = None, max_cell_width: int = 30):
    """
    Convert dataframe to a nice ASCII table and print with markers.
    GUI will parse and render it as a styled clickable table.
    """
    if df is None:
        _print_raw(f"{_MARK_TABLE_START}\n<empty>\n{_MARK_TABLE_END}\n")
        return

    nrows = len(df)
    if nrows == 0:
        _print_raw(f"{_MARK_TABLE_START}\n<empty>\n{_MARK_TABLE_END}\n")
        return

    # Generate ASCII table similar to your old insert_table_black
    cols = list(df.columns)
    rows = df.to_dict("records")

    # compute widths
    col_widths = {}
    for col in cols:
        max_cell = max([len(str(r.get(col)).replace("\n", " ").replace("\r", " ")[:max_cell_width]) if r.get(col) is not None else 0 for r in rows] + [len(str(col))])
        col_widths[col] = min(max_cell_width, max_cell)

    def border_line(left, mid, right):
        return left + mid.join("-" * (col_widths[c] + 2) for c in cols) + right

    lines = []
    if title:
        lines.append(title)
    lines.append(border_line("+", "+", "+"))
    header = "| " + " | ".join(str(c).center(col_widths[c]) for c in cols) + " |"
    lines.append(header)
    lines.append(border_line("+", "+", "+"))

    for i, row in enumerate(rows):
        cells = []
        for c in cols:
            v = row.get(c)
            if v is None:
                v = ""
            s = str(v).replace("\n", " ").replace("\r", " ")
            if len(s) > max_cell_width:
                s = s[:max_cell_width-3] + "..."
            cells.append(s.ljust(col_widths[c]))
        lines.append("| " + " | ".join(cells) + " |")
        if i < len(rows) - 1:
            lines.append(border_line("|", "+", "|"))

    lines.append(border_line("+", "+", "+"))

    # Now print with markers
    _print_raw(_MARK_TABLE_START + "\n")
    for L in lines:
        _print_raw(L + "\n")
    if nrows > 5000:
        _print_raw(f"... (showing only first 5000 rows out of {nrows})\n")
    _print_raw(_MARK_TABLE_END + "\n")
