import tkinter as tk
import threading
import sys
import time


class ChatTab:
    """
    A minimal clean chat tab with:
    - isolated thread
    - isolated print() and input()
    - clean chat UI
    """

    def __init__(self, parent, name, logic_function, on_close=None):
        self.parent = parent
        self.name = name
        self.logic_function = logic_function
        self.on_close = on_close

        # create tab frame
        self.frame = tk.Frame(parent)
        self.frame.grid_rowconfigure(0, weight=1)
        self.frame.grid_columnconfigure(0, weight=1)

        # chat display
        self.chat_display = tk.Text(
            self.frame,
            wrap="word",
            font=("Segoe UI", 11),
            bg="white"
        )
        self.chat_display.grid(row=0, column=0, sticky="nsew")

        # scroll bar
        scroll_y = tk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.chat_display.yview)
        self.chat_display.configure(yscrollcommand=scroll_y.set)
        scroll_y.grid(row=0, column=1, sticky="ns")

        # input frame
        bottom = tk.Frame(self.frame)
        bottom.grid(row=1, column=0, columnspan=2, sticky="ew", padx=4, pady=4)

        self.input_field = tk.Entry(bottom, font=("Segoe UI", 12))
        self.input_field.pack(side=tk.LEFT, fill=tk.X, expand=True)
        self.input_field.bind("<Return>", self.on_user_send)

        tk.Button(bottom, text="Send", command=self.on_user_send).pack(side=tk.RIGHT)

        # input-handling state
        self.waiting_for_input = threading.Event()
        self.last_input_value = None

        # thread
        self.thread = None

    # ---------------------------------------------------------
    # BASIC UTILITIES
    # ---------------------------------------------------------
    def print_to_chat(self, text):
        """Print inside this tab only."""
        formatted = f"{time.strftime('%H:%M:%S')}  {text}\n"
        self.chat_display.insert(tk.END, formatted)
        self.chat_display.see(tk.END)

    def local_print(self, *args, **kwargs):
        text = " ".join(str(a) for a in args)
        self.print_to_chat(text)

    def local_input(self, prompt=""):
        if prompt:
            self.print_to_chat(prompt)
        self.waiting_for_input.clear()
        self.waiting_for_input.wait()
        value = self.last_input_value
        self.last_input_value = None
        return value

    # ---------------------------------------------------------
    # SEND MESSAGE TO THREAD
    # ---------------------------------------------------------
    def on_user_send(self, event=None):
        msg = self.input_field.get().strip()
        if not msg:
            return

        self.print_to_chat(f"You: {msg}")
        self.input_field.delete(0, tk.END)

        self.last_input_value = msg
        self.waiting_for_input.set()

    # ---------------------------------------------------------
    # START LOGIC THREAD
    # ---------------------------------------------------------
    def start_logic(self):
        if self.thread and self.thread.is_alive():
            self.print_to_chat("Logic already running.")
            return

        def run_logic():
            try:
                original_print = sys.modules["builtins"].print
                original_input = sys.modules["builtins"].input

                # override print/input in this thread only
                sys.modules["builtins"].print = self.local_print
                sys.modules["builtins"].input = self.local_input

                self.logic_function()

            except Exception as e:
                self.print_to_chat(f"ERROR: {e}")

            finally:
                sys.modules["builtins"].print = original_print
                sys.modules["builtins"].input = original_input

        self.thread = threading.Thread(target=run_logic, daemon=True)
        self.thread.start()
        self.print_to_chat("Chat started.")

    # ---------------------------------------------------------
    # CLOSE TAB
    # ---------------------------------------------------------
    def close_tab(self):
        try:
            self.frame.destroy()
        except:
            pass

        if self.on_close:
            self.on_close(self)


# ============================================================
#                MAIN SIDEBAR APP (MINIMAL)
# ============================================================

class ChatApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Chat Tabs - Minimal")

        # layout config
        self.root.grid_rowconfigure(0, weight=1)
        self.root.grid_columnconfigure(1, weight=1)

        # sidebar
        self.sidebar = tk.Frame(root, width=200, bg="#f0f0f0")
        self.sidebar.grid(row=0, column=0, sticky="ns")
        self.sidebar.grid_propagate(False)

        tk.Label(self.sidebar, text="Chats", bg="#f0f0f0", font=("Segoe UI", 12, "bold")).pack(pady=6)

        self.chat_list_frame = tk.Frame(self.sidebar, bg="#f0f0f0")
        self.chat_list_frame.pack(fill="both", expand=True)

        # main area
        self.main_area = tk.Frame(root, bg="white")
        self.main_area.grid(row=0, column=1, sticky="nsew")
        self.main_area.grid_rowconfigure(0, weight=1)
        self.main_area.grid_columnconfigure(0, weight=1)

        self.tabs = []
        self.chat_buttons = []
        self.active_tab = None
        self.counter = 0

        # new chat button
        tk.Button(
            self.sidebar,
            text="+ New Chat",
            bg="#007BFF",
            fg="white",
            font=("Segoe UI", 10, "bold"),
            command=self.create_new_chat
        ).pack(pady=10, padx=8, fill="x")

        # start with one tab
        self.create_new_chat()

    # ---------------------------------------------------------
    def create_new_chat(self, logic_fn=None):
        self.counter += 1
        name = f"Chat {self.counter}"

        if logic_fn is None:
            # import here to avoid circular import
            from interactive_bot import main_menu
            logic_fn = main_menu

        tab = ChatTab(
            parent=self.main_area,
            name=name,
            logic_function=logic_fn,
            on_close=self._on_tab_closed
        )
        self.tabs.append(tab)

        btn = tk.Button(
            self.chat_list_frame,
            text=name,
            anchor="w",
            command=lambda t=tab: self.switch_to_tab(t)
        )
        btn.pack(fill="x", padx=5, pady=2)
        self.chat_buttons.append((btn, tab))

        self.switch_to_tab(tab)
        tab.start_logic()

    # ---------------------------------------------------------
    def switch_to_tab(self, tab):
        if self.active_tab is not None:
            try:
                self.active_tab.frame.grid_forget()
            except:
                pass
        tab.frame.grid(row=0, column=0, sticky="nsew")
        self.active_tab = tab

    # ---------------------------------------------------------
    def _on_tab_closed(self, tab):
        # remove button
        for i, (btn, t) in enumerate(self.chat_buttons):
            if t is tab:
                btn.destroy()
                self.chat_buttons.pop(i)
                break
        # remove tab
        try:
            self.tabs.remove(tab)
        except:
            pass
        # switch to last tab
        if self.tabs:
            self.switch_to_tab(self.tabs[-1])
        else:
            self.active_tab = None


# ============================================================
#                ENTRY POINT
# ============================================================

if __name__ == "__main__":
    root = tk.Tk()
    root.state("zoomed")
    ChatApp(root)
    root.mainloop()
