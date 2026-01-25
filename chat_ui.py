import tkinter as tk
from tkinter import messagebox
from tkinter import scrolledtext
import builtins
import sys
import pandas as pd
from datetime import datetime
import threading
import traceback
import pprint
from tkinter import ttk

chat_display = None # Global variable to hold the chat display widget
last_displayed_df = None # Global variable to store the last DataFrame displayed

from tkinter import ttk

def insert_table_black_clickable(df):
    """
    Display DataFrame as ASCII-art (like insert_table_black),
    but with each cell clickable to copy its value.
    Highlight the clicked cell for feedback.
    """
    global chat_display, last_displayed_df
    last_displayed_df = df

    MAX_CELL_DISPLAY_WIDTH = 30

    if chat_display is None:
        return

    timestamp = get_timestamp()
    chat_display.insert(tk.END, f"{timestamp} 🤖 Bot: ", "bot")

    if df.empty:
        chat_display.insert(tk.END, "No data to display.\n", "bot")
        chat_display.see(tk.END)
        return

    max_rows = 5000
    original_row_count = len(df)
    limited_df = df.head(max_rows) if original_row_count > max_rows else df

    columns = limited_df.columns.tolist()
    rows = limited_df.to_dict('records')

    # Calculate col widths
    col_widths = {}
    for col in columns:
        max_cell = max(
            [
                len(
                    str(row.get(col)).replace('\n', ' ').replace('\r', ' ')[:MAX_CELL_DISPLAY_WIDTH]
                ) if row.get(col) is not None else 0
                for row in rows
            ] + [len(str(col))]
        )
        col_widths[col] = min(MAX_CELL_DISPLAY_WIDTH, max_cell)

    def make_border_line(sep_left, sep_mid, sep_right):
        return sep_left + sep_mid.join('-' * (col_widths[col] + 2) for col in columns) + sep_right

    # Draw header
    chat_display.insert(tk.END, "Here's the data (click cells to copy):\n", "bot")
    chat_display.insert(tk.END, make_border_line('+', '+', '+') + "\n", 'table_border')
    header_line = "| " + " | ".join(str(col).center(col_widths[col]) for col in columns) + " |"
    chat_display.insert(tk.END, header_line + "\n", 'table_header')
    chat_display.insert(tk.END, make_border_line('+', '+', '+') + "\n", 'table_border')

    # Draw rows
    for i, row in enumerate(rows):
        chat_display.insert(tk.END, "| ", 'table_mono')
        for j, col in enumerate(columns):
            val = row.get(col)
            if val is None:
                val = ""
            val = str(val).replace('\n', ' ').replace('\r', ' ')
            if len(val) > MAX_CELL_DISPLAY_WIDTH:
                val = val[:MAX_CELL_DISPLAY_WIDTH-3] + "..."

            cell_text = val.ljust(col_widths[col])
            tag_name = f"cell_{i}_{j}"

            # Insert each cell with monospace font
            chat_display.insert(tk.END, cell_text, (tag_name, 'table_mono'))

            # Bind click → copy + highlight
            def copy_cell(event, value=val, tag=tag_name):
                # Remove old highlights
                chat_display.tag_remove("cell_highlight", "1.0", tk.END)
                # Highlight current cell
                chat_display.tag_add("cell_highlight", f"{chat_display.index(tk.CURRENT)} wordstart", f"{chat_display.index(tk.CURRENT)} wordend")
                chat_display.clipboard_clear()
                chat_display.clipboard_append(value)
                custom_print(f"✅ Copied cell: {value}", tag="bot")

            chat_display.tag_bind(tag_name, "<Button-1>", copy_cell)

            chat_display.insert(tk.END, " | ", 'table_mono')

        chat_display.insert(tk.END, "\n", 'table_row_even' if i % 2 == 0 else 'table_row_odd')
        if i < len(rows) - 1:
            chat_display.insert(tk.END, make_border_line('|', '+', '|') + "\n", 'table_separator')

    # Footer
    chat_display.insert(tk.END, make_border_line('+', '+', '+') + "\n", 'table_border')

    if original_row_count > max_rows:
        chat_display.insert(
            tk.END,
            f"... (showing only first {max_rows} rows out of {original_row_count})\n",
            'table_note'
        )

    chat_display.see(tk.END)



def get_timestamp():
    """Returns the current timestamp in HH:MM:SS format."""
    return datetime.now().strftime("[%H:%M:%S]")

# Store the original built-in print function
original_print = builtins.print

def custom_print(*args, **kwargs):
    """
    A custom print function that directs output to the Tkinter chat display.
    Supports multi-line messages and optional tags for styling.
    """
    global chat_display
    # Extract 'tag' from kwargs, defaulting to 'bot' if not provided
    tag = kwargs.pop("tag", "bot")

    # If printing to stderr or chat_display is not yet initialized, use original print
    if kwargs.get("file") == sys.stderr or chat_display is None:
        original_print(*args, **kwargs)
        return

    text = ' '.join(str(arg) for arg in args)
    timestamp = get_timestamp()
    lines = text.splitlines()

    if not lines: # If the text is empty after splitting, do nothing
        return

    # Insert each line with the appropriate tag
    first_line_prefix_len = len(timestamp) + len(" 🤖 Bot: ") # Calculate length for alignment
    for i, line in enumerate(lines):
        if i == 0:
            chat_display.insert(tk.END, f"{timestamp} 🤖 Bot: {line}\n", tag)
        else:
            # Indent subsequent lines to align with the first line's text content
            chat_display.insert(tk.END, f"{' ' * first_line_prefix_len}{line}\n", tag)
    chat_display.see(tk.END) # Scroll to the end to show the latest message

# ================= NEW FUNCTION (independent from chat) =================
def copy_friendly_print(obj):
    """
    Displays any Python object inside the chatbot window
    in a polished code-like block with a Copy button.
    """
    global chat_display
    if chat_display is None:
        return

    # Format object into text
    if isinstance(obj, str):
        text_data = obj
    elif hasattr(obj, "to_string"):  # e.g. pandas DataFrame, Series
        try:
            text_data = obj.to_string()
        except Exception:
            text_data = str(obj)
    else:
        text_data = pprint.pformat(obj, indent=2, width=100)

    timestamp = get_timestamp()
    chat_display.insert(tk.END, f"{timestamp} 🤖 Bot:\n", "bot")

    # --- Force block to match chat_display width ---
    chat_display.update_idletasks()  # make sure width is up-to-date
    chat_width = chat_display.winfo_width() or 600

    # Block frame styled like a code block
    block_frame = tk.Frame(chat_display,
                           bg="#fdfdfd",
                           bd=1,
                           relief="solid")
    block_frame.pack(fill="x", expand=True, padx=5, pady=5)

    # Insert into chat_display
    chat_display.window_create(tk.END, window=block_frame, stretch=True)

    # Copy button
    def copy_to_clipboard():
        chat_display.clipboard_clear()
        chat_display.clipboard_append(text_data)
        custom_print("✅ Block copied to clipboard!", tag="bot")

    copy_btn = tk.Button(block_frame,
                         text="📋 Copy",
                         command=copy_to_clipboard,
                         bg="#0d6efd",
                         fg="white",
                         font=("Segoe UI", 9, "bold"),
                         relief="flat",
                         cursor="hand2",
                         padx=8, pady=2)
    copy_btn.place(relx=1.0, x=-8, y=8, anchor="ne")

    # Inner frame for text + scrollbar
    inner_frame = tk.Frame(block_frame, bg="#fdfdfd")
    inner_frame.pack(fill="both", expand=True, padx=2, pady=(30, 2))

    # Read-only text widget
    text_widget = tk.Text(inner_frame,
                          wrap="none",
                          height=min(15, text_data.count("\n") + 2),
                          font=("Consolas", 10),
                          bg="#f8f9fa",
                          fg="#212529",
                          relief="flat",
                          borderwidth=0,
                          padx=8, pady=6)
    text_widget.insert("1.0", text_data)
    text_widget.config(state="disabled")
    text_widget.pack(side="left", fill="both", expand=True)

    # Scrollbar
    yscroll = tk.Scrollbar(inner_frame, command=text_widget.yview)
    text_widget.configure(yscrollcommand=yscroll.set)
    yscroll.pack(side="right", fill="y")

    # Insert into chat_display and stretch
    chat_display.window_create(tk.END, window=block_frame, stretch=True)
    chat_display.insert(tk.END, "\n")
    chat_display.see(tk.END)




# =======================================================================

# Copy DataFrame to clipboard
def copy_df_to_clipboard():
    """
    Copies the last displayed DataFrame to the system clipboard as tab-separated values (TSV).
    This format is ideal for pasting directly into spreadsheet applications.
    """
    global last_displayed_df
    if last_displayed_df is not None and not last_displayed_df.empty:
        try:
            last_displayed_df.to_clipboard(index=False, sep='\t')
            custom_print("✅ Table data copied to clipboard!", tag='bot')
        except Exception as e:
            custom_print(f"❌ Failed to copy table to clipboard: {e}", tag='error')
    elif last_displayed_df is not None and last_displayed_df.empty:
        custom_print("ℹ️ The last displayed table was empty. Nothing to copy.", tag='bot')
    else:
        custom_print("ℹ️ No table has been displayed yet to copy.", tag='bot')

def insert_table_black(df):
    global chat_display, last_displayed_df
    last_displayed_df = df

    MAX_CELL_DISPLAY_WIDTH = 30

    if chat_display is None:
        return

    timestamp = get_timestamp()
    chat_display.insert(tk.END, f"{timestamp} 🤖 Bot: ", "bot")

    if df.empty:
        chat_display.insert(tk.END, "No data to display.\n", "bot")
        chat_display.see(tk.END)
        return

    max_rows = 5000
    original_row_count = len(df)
    limited_df = df.head(max_rows) if original_row_count > max_rows else df

    columns = limited_df.columns.tolist()
    rows = limited_df.to_dict('records')

    # Calculate col widths (up to 100, as before)
    col_widths = {}
    for col in columns:
        max_cell = max(
            [
                len(
                    str(row.get(col)).replace('\n', ' ').replace('\r', ' ')[:MAX_CELL_DISPLAY_WIDTH]
                ) if row.get(col) is not None else 0
                for row in rows
            ] + [len(str(col))]
        )
        col_widths[col] = min(MAX_CELL_DISPLAY_WIDTH, max_cell)

    def make_border_line(sep_left, sep_mid, sep_right):
        return sep_left + sep_mid.join('-' * (col_widths[col] + 2) for col in columns) + sep_right

    chat_display.insert(tk.END, "Here's the data:\n", "bot")
    chat_display.insert(tk.END, make_border_line('+', '+', '+') + "\n", 'table_border')
    header_line = "| " + " | ".join(str(col).center(col_widths[col]) for col in columns) + " |"
    chat_display.insert(tk.END, header_line + "\n", 'table_header')
    chat_display.insert(tk.END, make_border_line('+', '+', '+') + "\n", 'table_border')

    # Print rows
    for i, row in enumerate(rows):
        formatted_cells = []
        for col in columns:
            val = row.get(col)
            if val is None:
                val = ""
            val = str(val).replace('\n', ' ').replace('\r', ' ')
            if len(val) > MAX_CELL_DISPLAY_WIDTH:
                val = val[:MAX_CELL_DISPLAY_WIDTH-3] + "..."
            formatted_cells.append(val.ljust(col_widths[col]))
        row_text = "| " + " | ".join(formatted_cells) + " |"
        tag = 'table_row_even' if i % 2 == 0 else 'table_row_odd'
        chat_display.insert(tk.END, row_text + "\n", tag)
        if i < len(rows) - 1:
            chat_display.insert(tk.END, make_border_line('|', '+', '|') + "\n", 'table_separator')

    chat_display.insert(tk.END, make_border_line('+', '+', '+') + "\n", 'table_border')

    if original_row_count > max_rows:
        chat_display.insert(tk.END, f"... (showing only first {max_rows} rows out of {original_row_count})\n", 'table_note')

    chat_display.see(tk.END)



import threading

def show_dataframe(df, title="Data Viewer", geometry="900x450",
                   heading_bg="#3C8DBC", row_sep_color="#E0E6ED",
                   show_row_separators=True, wheel_smooth=True):
    """
    Non-blocking Tkinter viewer for a pandas DataFrame (faster, smoother scrolling).
    - Colored header (heading_bg).
    - Single-click: highlight the clicked cell (blue border, no dimming).
    - Ctrl/Cmd+C: copy the highlighted cell's text to clipboard.
    - Double-click: select the entire row.
    - Optional thin horizontal borders between visible rows (optimized).
    - Debounced overlay refresh (~60 fps) for smoother scrolling.

    Returns:
        threading.Thread: UI thread handle.
    """
    def _run_ui():
        import tkinter as tk
        from tkinter import ttk

        root = tk.Tk()
        root.title(title)
        root.geometry(geometry)

        # Theme and styles (ensure header color applies).
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass

        style.configure("DF.Treeview", rowheight=24, borderwidth=0)
        style.configure("DF.Treeview.Heading",
                        background=heading_bg, foreground="white",
                        relief="flat", padding=6)
        style.map("DF.Treeview.Heading",
                  background=[("active", heading_bg)],
                  foreground=[("active", "white")])
        # Fallback for some platforms/themes:
        style.configure("Treeview.Heading",
                        background=heading_bg, foreground="white", relief="flat")

        frame = ttk.Frame(root)
        frame.pack(fill="both", expand=True, padx=10, pady=10)

        # Columns from DataFrame.
        columns = [str(c) for c in df.columns] if len(df.columns) else ["Value"]
        tree = ttk.Treeview(frame, columns=columns, show="headings",
                            selectmode="none", style="DF.Treeview")

        # Set headings and column widths.
        for col in columns:
            tree.heading(col, text=col)
            tree.column(col, width=max(80, min(280, len(col) * 12)), anchor="w")

        # Zebra striping via tags (lightweight).
        tree.tag_configure("odd", background="#F7FBFF")
        tree.tag_configure("even", background="#FFFFFF")

        # Insert data rows with alternating tags.
        if df.empty:
            tree.insert("", "end", values=["<Empty DataFrame>"] + [""] * (len(columns) - 1), tags=("odd",))
        else:
            for idx, (_, row) in enumerate(df.iterrows()):
                vals = ["" if (v is None) else str(v) for v in row.tolist()]
                tag = "odd" if (idx % 2 == 0) else "even"
                tree.insert("", "end", values=vals, tags=(tag,))

        # Scrollbars.
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)

        # Layout.
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        frame.grid_rowconfigure(0, weight=1)
        frame.grid_columnconfigure(0, weight=1)

        # Cell highlight (4 thin borders, no fill) — fast and non-obstructive.
        sel_color = "#1E90FF"
        thickness = 2
        line_top = tk.Frame(tree, bg=sel_color, height=thickness)
        line_bottom = tk.Frame(tree, bg=sel_color, height=thickness)
        line_left = tk.Frame(tree, bg=sel_color, width=thickness)
        line_right = tk.Frame(tree, bg=sel_color, width=thickness)

        selected_cell = {"iid": None, "col": None}

        def clear_cell_outline():
            selected_cell["iid"] = None
            selected_cell["col"] = None
            for w in (line_top, line_bottom, line_left, line_right):
                w.place_forget()

        def draw_cell_outline(iid, col):
            bbox = tree.bbox(iid, col)
            if not bbox:
                clear_cell_outline()
                return
            x, y, w, h = bbox
            line_top.place(x=x, y=y, width=w, height=thickness)
            line_bottom.place(x=x, y=y + h - thickness, width=w, height=thickness)
            line_left.place(x=x, y=y, width=thickness, height=h)
            line_right.place(x=x + w - thickness, y=y, width=thickness, height=h)

        def reoutline():
            if selected_cell["iid"] and selected_cell["col"]:
                draw_cell_outline(selected_cell["iid"], selected_cell["col"])

        # Optimized row separators (only visible slice; debounced).
        grid_lines = []

        def clear_row_separators():
            for ln in grid_lines:
                ln.place_forget()
            grid_lines.clear()

        def draw_row_separators():
            if not show_row_separators:
                clear_row_separators()
                return
            clear_row_separators()
            tw = tree.winfo_width()
            children = tree.get_children("")
            if not children:
                return
            # Compute visible range using yview fraction; add small buffer.
            first, last = tree.yview()
            n = len(children)
            start = max(0, int(first * n) - 1)
            end = min(n, int(last * n) + 2)
            for iid in children[start:end]:
                bbox = tree.bbox(iid)
                if not bbox:
                    continue
                x, y, w, h = bbox
                if y >= 0 and h > 0:
                    ln = tk.Frame(tree, bg=row_sep_color, height=1)
                    ln.place(x=0, y=y + h - 1, width=tw, height=1)
                    grid_lines.append(ln)

        def refresh_overlays():
            reoutline()
            draw_row_separators()

        # Debounce overlay refresh to ~60 fps.
        scroll_job = {"id": None}

        def schedule_refresh():
            if scroll_job["id"] is not None:
                root.after_cancel(scroll_job["id"])
            scroll_job["id"] = root.after(16, refresh_overlays)

        # Keep overlays aligned during scroll and resize.
        tree.configure(
            yscrollcommand=lambda f, l: (vsb.set(f, l), schedule_refresh()),
            xscrollcommand=lambda f, l: (hsb.set(f, l), schedule_refresh()),
        )
        vsb.configure(command=lambda *args: (tree.yview(*args), schedule_refresh()))
        hsb.configure(command=lambda *args: (tree.xview(*args), schedule_refresh()))
        tree.bind("<Configure>", lambda e: schedule_refresh())

        # Copy highlighted cell via keyboard.
        def copy_selected_cell(event=None):
            iid = selected_cell["iid"]
            col_id = selected_cell["col"]
            if not iid or not col_id or col_id == "#0":
                root.bell()
                return
            values = tree.item(iid, "values")
            try:
                idx = int(col_id[1:]) - 1  # '#1' -> 0
            except Exception:
                root.bell()
                return
            if 0 <= idx < len(values):
                text = str(values[idx])
                root.clipboard_clear()
                root.clipboard_append(text)
                root.update()

        # Single click: highlight only the clicked cell.
        def on_single_click(event):
            iid = tree.identify_row(event.y)
            col_id = tree.identify_column(event.x)
            if not iid or not col_id or col_id == "#0":
                clear_cell_outline()
                schedule_refresh()
                return
            selected_cell["iid"] = iid
            selected_cell["col"] = col_id
            draw_cell_outline(iid, col_id)
            tree.focus_set()
            schedule_refresh()

        # Double click: select entire row and clear cell outline.
        def on_double_click(event):
            iid = tree.identify_row(event.y)
            if iid:
                tree.selection_remove(tree.selection())
                tree.selection_add(iid)
                tree.focus(iid)
                clear_cell_outline()
                schedule_refresh()

        # Optional smoother wheel handling (unit scrolling with debounce).
        def on_wheel_windows(event):
            steps = -1 if event.delta > 0 else 1
            tree.yview_scroll(steps, "units")
            schedule_refresh()

        def on_wheel_linux_up(event):
            tree.yview_scroll(-1, "units")
            schedule_refresh()

        def on_wheel_linux_down(event):
            tree.yview_scroll(1, "units")
            schedule_refresh()

        tree.bind("<Button-1>", on_single_click)
        tree.bind("<Double-1>", on_double_click)
        tree.bind("<Control-c>", copy_selected_cell)
        tree.bind("<Control-C>", copy_selected_cell)
        tree.bind("<Command-c>", copy_selected_cell)  # macOS
        tree.bind("<Command-C>", copy_selected_cell)

        if wheel_smooth:
            tree.bind("<MouseWheel>", on_wheel_windows)      # Windows
            tree.bind("<Button-4>", on_wheel_linux_up)       # Linux up
            tree.bind("<Button-5>", on_wheel_linux_down)     # Linux down

        # Initial draw.
        root.after(50, schedule_refresh)

        root.mainloop()

    t = threading.Thread(target=_run_ui, name="DataFrameViewer")
    t.daemon = False
    t.start()
    return t



def ask_confirmation(message, title="Confirmation", parent=None):
    """
    Generic popup that asks user to confirm an action.
    Returns 'Y' if user clicks Yes, 'N' otherwise.
    """
    root = None
    if parent is None:
        root = tk.Tk()
        root.withdraw()  # Hide the main window if not inside Tk app

    response = messagebox.askyesno(title, message, parent=parent)

    if root:
        root.destroy()

    return 'Y' if response else 'N'


# Example usage:

def run_chat_bot(main_logic_function):
    """
    Initializes and runs the Tkinter chatbot GUI.
    """
    global chat_display, user_input_ready, user_input_value

    user_input_ready = False
    user_input_value = ""

    def on_send(event=None):
        nonlocal input_field
        global user_input_ready, user_input_value

        msg = input_field.get().strip()
        if not msg:
            return

        timestamp = get_timestamp()
        chat_display.insert(tk.END, f"{timestamp} 🧑 You: {msg}\n", "user")
        input_field.delete(0, tk.END)

        user_input_value = msg
        user_input_ready = True

    def custom_input(prompt=""):
        global user_input_ready, user_input_value
        custom_print(prompt)
        user_input_ready = False
        while not user_input_ready:
            root.update()
        user_input_ready = False
        return user_input_value

    builtins.print = custom_print
    builtins.input = custom_input

    root = tk.Tk()
    root.title("Interactive Bot")
    root.state('zoomed')
    root.geometry("800x600")

    text_frame = tk.Frame(root)
    text_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

    chat_display = tk.Text(
        text_frame,
        wrap="none",
        font=("Segoe UI", 11),
        undo=True,
        bg="#F0F0F0",
        bd=0,
        padx=5, pady=5
    )
    chat_display.grid(row=0, column=0, sticky="nsew")

    chat_display.tag_configure('user', foreground='#0056B3', font=("Segoe UI", 11, "bold"))
    chat_display.tag_configure('bot', foreground='#28A745', font=("Segoe UI", 11))
    chat_display.tag_configure('error', foreground='#DC3545', font=("Segoe UI", 11, "bold"))

    table_font = ("Consolas", 10)
    chat_display.tag_configure('table_border', foreground='#6C757D', font=table_font)
    chat_display.tag_configure('table_header', foreground='white', background='#007BFF',font=(table_font[0], table_font[1], "bold"))
    chat_display.tag_configure('table_row_even', foreground='#343A40', background='#F8F9FA', font=table_font)
    chat_display.tag_configure('table_row_odd', foreground='#343A40', background='#E9ECEF', font=table_font)
    chat_display.tag_configure('table_separator', foreground='#ADB5BD', font=table_font)
    chat_display.tag_configure('table_note', foreground='#6C757D', font=(table_font[0], table_font[1], "italic"))

    scroll_x = tk.Scrollbar(text_frame, orient=tk.HORIZONTAL, command=chat_display.xview)
    chat_display.configure(xscrollcommand=scroll_x.set)
    scroll_x.grid(row=1, column=0, sticky="ew")

    scroll_y = tk.Scrollbar(text_frame, orient=tk.VERTICAL, command=chat_display.yview)
    chat_display.configure(yscrollcommand=scroll_y.set)
    scroll_y.grid(row=0, column=1, sticky="ns")

    text_frame.grid_rowconfigure(0, weight=1)
    text_frame.grid_columnconfigure(0, weight=1)

    input_frame = tk.Frame(root, bg="#E0E0E0", padx=10, pady=5)
    input_frame.pack(fill=tk.X, padx=10, pady=5)

    input_field = tk.Entry(input_frame, font=("Segoe UI", 12), bd=2, relief="groove", bg="white")
    input_field.pack(side=tk.LEFT, fill=tk.X, expand=True)
    input_field.bind("<Return>", on_send)

    send_button = tk.Button(input_frame, text="Send", command=on_send,
        bg="#007BFF", fg="white", font=("Segoe UI", 10, "bold"), relief="raised", bd=2)
    send_button.pack(side=tk.RIGHT, padx=(5,0))

    copy_table_button = tk.Button(input_frame, text="Copy Table", command=copy_df_to_clipboard,
        bg="#6C757D", fg="white", font=("Segoe UI", 10, "bold"), relief="raised", bd=2)
    copy_table_button.pack(side=tk.RIGHT, padx=(5,0))

    close_button = tk.Button(input_frame, text="Close", command=root.destroy,
        bg="#DC3545", fg="white", font=("Segoe UI", 10, "bold"), relief="raised", bd=2)
    close_button.pack(side=tk.RIGHT, padx=(5,0))

    input_field.focus()

    def thread_target():
        try:
            main_logic_function()
        except Exception:
            error_message = traceback.format_exc()
            root.after(0, lambda: custom_print(f"⚠️ Error in bot logic:\n{error_message}", tag='error'))

    threading.Thread(target=thread_target, daemon=True).start()

    def on_window_close():
        custom_print("⚠️ Chatbox closed abruptly.", tag='error')
        root.after(100, root.quit)
    root.protocol("WM_DELETE_WINDOW", on_window_close)

    root.mainloop()
    sys.exit(0)

# ==== Example main logic function ====
if __name__ == "__main__":
    def main_logic():
        import time
        print("Welcome to CircuIT, your universal AI assistant!")
        time.sleep(0.5)

        df_small = pd.DataFrame({
            'ID': [101, 102, 103],
            'Product': ['Laptop', 'Mouse', 'Keyboard'],
            'Price (USD)': [1200.50, 25.99, 75.00],
            'In Stock': [True, False, True]
        })
        insert_table_black(df_small)
        time.sleep(1)

        # Demonstrate copy_friendly_print (independent)
        copy_friendly_print("Hello, this is a copy-friendly string!")
        copy_friendly_print([1, 2, 3, {"a": 10, "b": 20}])
        copy_friendly_print(df_small)

        print("What can I assist you with today?")
        user_query = input("Please type your question:")
        print(f"You asked: '{user_query}'. I'll process that for you!")








    run_chat_bot(main_logic)
