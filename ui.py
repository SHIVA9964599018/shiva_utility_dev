import tkinter as tk
from tkinter import ttk

def create_data_display():
    root = tk.Tk()
    root.title("Treeview: Cell Highlight on Click, Row Select on Double-Click")
    root.geometry("900x450")

    frame = ttk.Frame(root)
    frame.pack(fill="both", expand=True, padx=10, pady=10)

    columns = ("id", "first_name", "last_name", "email", "country")
    tree = ttk.Treeview(frame, columns=columns, show="headings", selectmode="none")

    # Headings and columns
    tree.heading("id", text="ID")
    tree.heading("first_name", text="First Name")
    tree.heading("last_name", text="Last Name")
    tree.heading("email", text="Email")
    tree.heading("country", text="Country")

    tree.column("id", width=60, anchor="center")
    tree.column("first_name", width=140, anchor="w")
    tree.column("last_name", width=140, anchor="w")
    tree.column("email", width=260, anchor="w")
    tree.column("country", width=120, anchor="w")

    # Sample data
    data = [
        (1, "John", "Doe", "john.doe@example.com", "USA"),
        (2, "Jane", "Smith", "jane.smith@example.com", "Canada"),
        (3, "Peter", "Jones", "peter.jones@example.com", "UK"),
        (4, "Alice", "Williams", "alice.w@example.com", "Australia"),
        (5, "Mohammed", "Khan", "mo.khan@example.com", "India"),
        (6, "Maria", "Garcia", "maria.g@example.com", "Spain"),
        (7, "Hiroshi", "Tanaka", "hiroshi.t@example.com", "Japan"),
        (8, "Sophie", "Dubois", "sophie.d@example.com", "France"),
        (9, "Carlos", "Silva", "carlos.s@example.com", "Brazil"),
        (10, "Anna", "Müller", "anna.m@example.com", "Germany"),
        (11, "David", "Lee", "david.lee@example.com", "South Korea"),
        (12, "Fatima", "Al-Farsi", "fatima.a@example.com", "UAE"),
        (13, "Wei", "Chen", "wei.chen@example.com", "China"),
        (14, "Olga", "Petrova", "olga.p@example.com", "Russia"),
        (15, "Luca", "Rossi", "luca.r@example.com", "Italy"),
        (16, "Emily", "Brown", "emily.b@example.com", "New Zealand"),
        (17, "Omar", "Hassan", "omar.h@example.com", "Egypt"),
        (18, "Chloe", "Martin", "chloe.m@example.com", "Belgium"),
        (19, "Daniel", "Kim", "daniel.k@example.com", "Canada"),
        (20, "Isabella", "Santos", "isabella.s@example.com", "Portugal"),
        (21, "Michael", "Johnson", "michael.j@example.com", "USA"),
        (22, "Sarah", "Miller", "sarah.m@example.com", "UK"),
        (23, "Chris", "Davis", "chris.d@example.com", "Australia"),
        (24, "Jessica", "Wilson", "jessica.w@example.com", "Canada"),
        (25, "Matthew", "Moore", "matthew.m@example.com", "USA"),
        (26, "Laura", "Taylor", "laura.t@example.com", "UK"),
        (27, "James", "Anderson", "james.a@example.com", "New Zealand"),
        (28, "Olivia", "Thomas", "olivia.t@example.com", "Australia"),
        (29, "William", "Jackson", "william.j@example.com", "USA"),
        (30, "Sophia", "White", "sophia.w@example.com", "Canada"),
    ]
    for row in data:
        tree.insert("", "end", values=row)

    # Scrollbars
    vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
    hsb = ttk.Scrollbar(frame, orient="horizontal", command=tree.xview)
    tree.configure(
        yscrollcommand=lambda f, l: (vsb.set(f, l), reoutline()),
        xscrollcommand=lambda f, l: (hsb.set(f, l), reoutline())
    )

    # Layout
    tree.grid(row=0, column=0, sticky="nsew")
    vsb.grid(row=0, column=1, sticky="ns")
    hsb.grid(row=1, column=0, sticky="ew")
    frame.grid_rowconfigure(0, weight=1)
    frame.grid_columnconfigure(0, weight=1)

    # Non-obstructive cell outline using 4 thin frames (no background fill)
    border_color = "#1E90FF"
    thickness = 2
    line_top = tk.Frame(tree, bg=border_color, height=thickness)
    line_bottom = tk.Frame(tree, bg=border_color, height=thickness)
    line_left = tk.Frame(tree, bg=border_color, width=thickness)
    line_right = tk.Frame(tree, bg=border_color, width=thickness)

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
        # Place thin borders around the cell; center area remains untouched so text is visible
        line_top.place(x=x, y=y, width=w, height=thickness)
        line_bottom.place(x=x, y=y + h - thickness, width=w, height=thickness)
        line_left.place(x=x, y=y, width=thickness, height=h)
        line_right.place(x=x + w - thickness, y=y, width=thickness, height=h)

    def reoutline():
        if selected_cell["iid"] and selected_cell["col"]:
            draw_cell_outline(selected_cell["iid"], selected_cell["col"])

    # Copy highlighted cell via keyboard
    def copy_selected_cell(event=None):
        iid = selected_cell["iid"]
        col_id = selected_cell["col"]
        if not iid or not col_id or col_id == "#0":
            root.bell()
            return
        values = tree.item(iid, "values")
        try:
            idx = int(col_id[1:]) - 1
        except Exception:
            root.bell()
            return
        if 0 <= idx < len(values):
            text = str(values[idx])
            root.clipboard_clear()
            root.clipboard_append(text)
            root.update()

    # Single click: highlight only the clicked cell
    def on_single_click(event):
        iid = tree.identify_row(event.y)
        col_id = tree.identify_column(event.x)
        if not iid or not col_id or col_id == "#0":
            clear_cell_outline()
            return
        selected_cell["iid"] = iid
        selected_cell["col"] = col_id
        draw_cell_outline(iid, col_id)
        tree.focus_set()  # Ensure Ctrl/Cmd+C works

    # Double click: select entire row and clear cell outline
    def on_double_click(event):
        iid = tree.identify_row(event.y)
        if iid:
            tree.selection_remove(tree.selection())
            tree.selection_add(iid)
            tree.focus(iid)
            clear_cell_outline()

    # Bindings
    tree.bind("<Button-1>", on_single_click)
    tree.bind("<Double-1>", on_double_click)
    tree.bind("<Configure>", lambda e: reoutline())
    tree.bind("<Control-c>", copy_selected_cell)
    tree.bind("<Control-C>", copy_selected_cell)
    tree.bind("<Command-c>", copy_selected_cell)  # macOS
    tree.bind("<Command-C>", copy_selected_cell)

    root.mainloop()

if __name__ == "__main__":
    create_data_display()