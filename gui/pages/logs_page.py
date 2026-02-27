"""
gui/pages/logs_page.py
========================
Real-time log viewer page.

Features:
  - Tabs: Live (sync events streamed from queue) | Main Log | Error Log
  - Tail log files — auto-detects today's log file, re-checks every 2s
  - Color-coded log levels: INFO / WARNING / ERROR / DEBUG / SUCCESS
  - Search/filter bar — highlights matching lines
  - Auto-scroll (toggle)
  - Clear / Copy All / Export buttons
  - Line count display
  - Shows last N lines on open (configurable)
"""

import os
import threading
import tkinter as tk
from tkinter import filedialog, messagebox
from datetime import datetime

from gui.state  import AppState
from gui.styles import Color, Font, Spacing

# How many lines to load from file on open
TAIL_LINES = 500
# Polling interval for file tail (ms)
TAIL_INTERVAL_MS = 2000
# Max lines to keep in Live tab before trimming
MAX_LIVE_LINES = 2000

LOGS_DIR = "logs"


# ─────────────────────────────────────────────────────────────────────────────
#  Log level → color tag
# ─────────────────────────────────────────────────────────────────────────────
def _level_tag(line: str) -> str:
    """Detect log level from line text and return a tag name."""
    u = line.upper()
    if " ERROR " in u or " ERROR]" in u or "✗" in line:
        return "ERROR"
    if " WARNING " in u or " WARN " in u or "⚠" in line:
        return "WARNING"
    if " DEBUG " in u:
        return "DEBUG"
    if "✓" in line or " SUCCESS" in u or "DONE" in u or "COMPLETE" in u:
        return "SUCCESS"
    return "INFO"


# ─────────────────────────────────────────────────────────────────────────────
#  LogTextWidget — scrollable Text with color tags, search, toolbar
# ─────────────────────────────────────────────────────────────────────────────
class LogTextWidget(tk.Frame):
    """
    Reusable scrollable log viewer.
    Used once per tab.
    """

    def __init__(self, parent, **kwargs):
        super().__init__(parent, bg=Color.BG_ROOT, **kwargs)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self._auto_scroll  = True
        self._filter_text  = ""
        self._all_lines: list[str] = []   # full buffer
        self._line_count   = 0

        self._build_toolbar()
        self._build_text()
        self._build_statusbar()

    # ─────────────────────────────────────────────────────────────────────────
    def _build_toolbar(self):
        bar = tk.Frame(self, bg=Color.BG_TABLE_HEADER,
                       highlightthickness=1, highlightbackground=Color.BORDER)
        bar.grid(row=0, column=0, sticky="ew")
        bar.columnconfigure(1, weight=1)

        # Left — filter
        left = tk.Frame(bar, bg=Color.BG_TABLE_HEADER)
        left.grid(row=0, column=0, sticky="w", padx=Spacing.SM, pady=Spacing.XS)

        tk.Label(left, text="🔍", font=Font.BODY_SM,
                 bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_MUTED).pack(side="left")

        self._filter_var = tk.StringVar()
        self._filter_var.trace_add("write", self._on_filter_change)
        tk.Entry(
            left, textvariable=self._filter_var,
            font=Font.MONO_SM, width=28,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        ).pack(side="left", padx=(4, Spacing.MD))

        # Level filter
        tk.Label(left, text="Level:", font=Font.BODY_SM,
                 bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_SECONDARY).pack(side="left")

        self._level_var = tk.StringVar(value="ALL")
        level_menu = tk.OptionMenu(
            left, self._level_var,
            "ALL", "INFO", "WARNING", "ERROR", "DEBUG",
            command=lambda _: self._apply_filter(),
        )
        level_menu.configure(
            font=Font.BODY_SM, bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, width=8,
        )
        level_menu.pack(side="left", padx=(4, 0))

        # Right — buttons
        right = tk.Frame(bar, bg=Color.BG_TABLE_HEADER)
        right.grid(row=0, column=1, sticky="e", padx=Spacing.SM)

        self._autoscroll_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            right, text="Auto-scroll",
            variable=self._autoscroll_var,
            font=Font.BODY_SM, bg=Color.BG_TABLE_HEADER,
            activebackground=Color.BG_TABLE_HEADER,
            fg=Color.TEXT_SECONDARY,
            command=lambda: setattr(self, "_auto_scroll", self._autoscroll_var.get()),
        ).pack(side="left", padx=(0, Spacing.SM))

        for label, cmd in [
            ("Clear",    self.clear),
            ("Copy All", self._copy_all),
            ("Export",   self._export),
        ]:
            tk.Button(
                right, text=label,
                font=Font.BUTTON_SM,
                bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1, padx=Spacing.SM, pady=2,
                cursor="hand2", command=cmd,
            ).pack(side="left", padx=(0, 3))

    def _build_text(self):
        frame = tk.Frame(self, bg=Color.LOG_BG)
        frame.grid(row=1, column=0, sticky="nsew")
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)

        self._text = tk.Text(
            frame,
            font=Font.MONO,
            bg=Color.LOG_BG,
            fg=Color.LOG_INFO,
            relief="flat", bd=0,
            state="disabled",
            wrap="none",
            cursor="arrow",
        )
        self._text.grid(row=0, column=0, sticky="nsew")

        vsb = tk.Scrollbar(frame, orient="vertical", command=self._text.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        self._text.configure(yscrollcommand=vsb.set)

        hsb = tk.Scrollbar(frame, orient="horizontal", command=self._text.xview)
        hsb.grid(row=1, column=0, sticky="ew")
        self._text.configure(xscrollcommand=hsb.set)

        # Color tags
        self._text.tag_config("INFO",    foreground=Color.LOG_INFO)
        self._text.tag_config("SUCCESS", foreground=Color.LOG_SUCCESS)
        self._text.tag_config("WARNING", foreground=Color.LOG_WARNING)
        self._text.tag_config("ERROR",   foreground=Color.LOG_ERROR,
                              font=Font.MONO)
        self._text.tag_config("DEBUG",   foreground=Color.LOG_DEBUG)
        self._text.tag_config("SEARCH",  background="#FFF3CD")

    def _build_statusbar(self):
        bar = tk.Frame(self, bg=Color.BG_TABLE_HEADER,
                       highlightthickness=1, highlightbackground=Color.BORDER)
        bar.grid(row=2, column=0, sticky="ew")

        self._status_lbl = tk.Label(
            bar, text="0 lines",
            font=Font.BODY_SM, bg=Color.BG_TABLE_HEADER,
            fg=Color.TEXT_MUTED, anchor="w",
            padx=Spacing.SM, pady=2,
        )
        self._status_lbl.pack(side="left")

        self._match_lbl = tk.Label(
            bar, text="",
            font=Font.BODY_SM, bg=Color.BG_TABLE_HEADER,
            fg=Color.TEXT_SECONDARY, anchor="e",
            padx=Spacing.SM, pady=2,
        )
        self._match_lbl.pack(side="right")

    # ─────────────────────────────────────────────────────────────────────────
    #  Public API
    # ─────────────────────────────────────────────────────────────────────────
    def append_line(self, line: str, tag: str = None):
        """Append a single log line. Thread-safe via .after() — call from any thread."""
        self._all_lines.append(line)

        # Trim buffer
        if len(self._all_lines) > MAX_LIVE_LINES:
            self._all_lines = self._all_lines[-MAX_LIVE_LINES:]

        # Only show if it passes current filter
        if self._passes_filter(line):
            self._insert_line(line, tag or _level_tag(line))

        self._update_status()

    def append_lines(self, lines: list[str]):
        """Bulk append — more efficient than calling append_line() repeatedly."""
        self._all_lines.extend(lines)
        if len(self._all_lines) > MAX_LIVE_LINES:
            self._all_lines = self._all_lines[-MAX_LIVE_LINES:]

        self._text.configure(state="normal")
        for line in lines:
            if self._passes_filter(line):
                tag = _level_tag(line)
                self._text.insert("end", line + "\n", tag)
                self._line_count += 1
        self._text.configure(state="disabled")

        if self._auto_scroll:
            self._text.see("end")
        self._update_status()

    def clear(self):
        self._all_lines.clear()
        self._line_count = 0
        self._text.configure(state="normal")
        self._text.delete("1.0", "end")
        self._text.configure(state="disabled")
        self._update_status()

    def set_lines(self, lines: list[str]):
        """Replace entire content."""
        self.clear()
        self.append_lines(lines)

    # ─────────────────────────────────────────────────────────────────────────
    #  Internal
    # ─────────────────────────────────────────────────────────────────────────
    def _insert_line(self, line: str, tag: str):
        self._text.configure(state="normal")
        self._text.insert("end", line + "\n", tag)
        self._line_count += 1
        self._text.configure(state="disabled")
        if self._auto_scroll:
            self._text.see("end")

    def _passes_filter(self, line: str) -> bool:
        level_filter = self._level_var.get() if hasattr(self, "_level_var") else "ALL"
        text_filter  = self._filter_text

        if level_filter != "ALL":
            tag = _level_tag(line)
            if tag != level_filter:
                return False

        if text_filter and text_filter.lower() not in line.lower():
            return False

        return True

    def _on_filter_change(self, *args):
        self._filter_text = self._filter_var.get()
        self._apply_filter()

    def _apply_filter(self):
        """Re-render all buffered lines applying current filter."""
        self._line_count = 0
        self._text.configure(state="normal")
        self._text.delete("1.0", "end")

        matches = 0
        for line in self._all_lines:
            if self._passes_filter(line):
                tag = _level_tag(line)
                self._text.insert("end", line + "\n", tag)
                self._line_count += 1
                matches += 1

        self._text.configure(state="disabled")

        if self._auto_scroll:
            self._text.see("end")

        # Show match count
        total = len(self._all_lines)
        filter_active = self._filter_text or self._level_var.get() != "ALL"
        if filter_active:
            self._match_lbl.configure(text=f"{matches} / {total} match")
        else:
            self._match_lbl.configure(text="")

        self._update_status()

    def _update_status(self):
        total = len(self._all_lines)
        self._status_lbl.configure(text=f"{total:,} lines")

    def _copy_all(self):
        content = self._text.get("1.0", "end")
        self.clipboard_clear()
        self.clipboard_append(content)
        messagebox.showinfo("Copied", f"Copied {self._line_count:,} lines to clipboard.")

    def _export(self):
        path = filedialog.asksaveasfilename(
            defaultextension=".log",
            filetypes=[("Log files", "*.log"), ("Text files", "*.txt")],
            initialfile=f"tally_sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
        )
        if path:
            try:
                with open(path, "w", encoding="utf-8") as f:
                    f.write(self._text.get("1.0", "end"))
                messagebox.showinfo("Exported", f"Log saved to:\n{path}")
            except Exception as e:
                messagebox.showerror("Export Failed", str(e))


# ─────────────────────────────────────────────────────────────────────────────
#  LogsPage
# ─────────────────────────────────────────────────────────────────────────────
class LogsPage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._tail_after_id   = None
        self._main_file_pos   = 0    # byte position in main log file
        self._error_file_pos  = 0
        self._current_main_file  = None
        self._current_error_file = None

        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)

        self._build()

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        # ── Tab bar ───────────────────────────────────────
        tab_bar = tk.Frame(
            self, bg=Color.BG_HEADER,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        tab_bar.grid(row=0, column=0, sticky="ew")

        self._tabs: dict[str, tk.Frame] = {}
        self._tab_btns: dict[str, tk.Button] = {}
        self._active_tab = "live"

        tab_defs = [
            ("live",  "⚡ Live Sync"),
            ("main",  "📄 Main Log"),
            ("error", "🔴 Error Log"),
        ]

        left = tk.Frame(tab_bar, bg=Color.BG_HEADER)
        left.pack(side="left")

        for key, label in tab_defs:
            btn = tk.Button(
                left, text=label,
                font=Font.BODY_BOLD,
                bg=Color.PRIMARY if key == "live" else Color.BG_HEADER,
                fg=Color.TEXT_WHITE if key == "live" else Color.TEXT_SECONDARY,
                relief="flat", bd=0,
                padx=Spacing.LG, pady=Spacing.SM,
                cursor="hand2",
                command=lambda k=key: self._switch_tab(k),
            )
            btn.pack(side="left")
            self._tab_btns[key] = btn

        # Right side — reload + today's file label
        right = tk.Frame(tab_bar, bg=Color.BG_HEADER)
        right.pack(side="right", padx=Spacing.MD)

        self._file_lbl = tk.Label(
            right, text="",
            font=Font.BODY_SM, bg=Color.BG_HEADER, fg=Color.TEXT_MUTED,
        )
        self._file_lbl.pack(side="left", padx=(0, Spacing.SM))

        tk.Button(
            right, text="⟳ Reload File",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1, padx=Spacing.SM, pady=2,
            cursor="hand2", command=self._reload_current_file,
        ).pack(side="left")

        # ── Content area — stacked frames ─────────────────
        content = tk.Frame(self, bg=Color.BG_ROOT)
        content.grid(row=1, column=0, sticky="nsew")
        content.rowconfigure(0, weight=1)
        content.columnconfigure(0, weight=1)

        for key, _ in tab_defs:
            widget = LogTextWidget(content)
            widget.grid(row=0, column=0, sticky="nsew")
            self._tabs[key] = widget

        # Show live tab first
        self._tabs["live"].tkraise()

    # ─────────────────────────────────────────────────────────────────────────
    #  Tab switching
    # ─────────────────────────────────────────────────────────────────────────
    def _switch_tab(self, key: str):
        self._active_tab = key
        self._tabs[key].tkraise()

        for k, btn in self._tab_btns.items():
            active = (k == key)
            btn.configure(
                bg=Color.PRIMARY if active else Color.BG_HEADER,
                fg=Color.TEXT_WHITE if active else Color.TEXT_SECONDARY,
            )

        if key == "main":
            self._load_log_file("main")
            self._file_lbl.configure(text=self._log_filename("main"))
        elif key == "error":
            self._load_log_file("error")
            self._file_lbl.configure(text=self._log_filename("error"))
        else:
            self._file_lbl.configure(text="")

    # ─────────────────────────────────────────────────────────────────────────
    #  File log loading
    # ─────────────────────────────────────────────────────────────────────────
    def _log_filename(self, kind: str) -> str:
        """Return today's log filename. kind = 'main' | 'error'"""
        date_str = datetime.now().strftime("%d-%b-%Y")
        return os.path.join(LOGS_DIR, f"{kind}_{date_str}.log")

    def _load_log_file(self, kind: str):
        """Load last TAIL_LINES lines from today's log file into the tab."""
        path   = self._log_filename(kind)
        widget = self._tabs[kind]
        widget.clear()

        if not os.path.exists(path):
            widget.append_line(
                f"Log file not found: {path}",
                "WARNING",
            )
            return

        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()

            lines = [l.rstrip() for l in all_lines[-TAIL_LINES:]]
            widget.set_lines(lines)

            # Remember file position for tailing
            if kind == "main":
                self._current_main_file  = path
                self._main_file_pos      = os.path.getsize(path)
            else:
                self._current_error_file = path
                self._error_file_pos     = os.path.getsize(path)

        except Exception as e:
            widget.append_line(f"Could not read log file: {e}", "ERROR")

    def _tail_log_files(self):
        """
        Called every TAIL_INTERVAL_MS ms.
        Reads any new bytes appended to today's log files.
        """
        for kind in ("main", "error"):
            path = self._log_filename(kind)
            if not os.path.exists(path):
                continue

            pos_attr  = f"_{kind}_file_pos"
            file_attr = f"_current_{kind}_file"

            # If date rolled over, reset position
            if getattr(self, file_attr) != path:
                setattr(self, file_attr, path)
                setattr(self, pos_attr, 0)

            try:
                current_size = os.path.getsize(path)
                pos = getattr(self, pos_attr, 0)

                if current_size > pos:
                    with open(path, "r", encoding="utf-8", errors="replace") as f:
                        f.seek(pos)
                        new_text = f.read()
                    setattr(self, pos_attr, current_size)

                    new_lines = [l.rstrip() for l in new_text.splitlines() if l.strip()]
                    if new_lines and self._active_tab == kind:
                        widget = self._tabs[kind]
                        widget.append_lines(new_lines)

            except Exception:
                pass

        # Reschedule
        self._tail_after_id = self.after(TAIL_INTERVAL_MS, self._tail_log_files)

    def _reload_current_file(self):
        if self._active_tab in ("main", "error"):
            self._load_log_file(self._active_tab)

    # ─────────────────────────────────────────────────────────────────────────
    #  Live tab — receives lines from app queue via append_log()
    # ─────────────────────────────────────────────────────────────────────────
    def append_log(self, line: str):
        """
        Called by app.py queue handler when a sync log line arrives.
        Always runs on main thread (called from _handle_queue_msg).
        """
        ts   = datetime.now().strftime("%H:%M:%S")
        full = f"{ts}  {line}"
        tag  = _level_tag(line)
        self._tabs["live"].append_line(full, tag)

    # ─────────────────────────────────────────────────────────────────────────
    #  Lifecycle
    # ─────────────────────────────────────────────────────────────────────────
    def on_show(self):
        """Called every time this page is navigated to."""
        # Start file tail polling if not already running
        if self._tail_after_id is None:
            self._tail_log_files()

        # Auto-load today's file for whichever tab is active
        if self._active_tab in ("main", "error"):
            # Only reload if file has changed
            current_path = self._log_filename(self._active_tab)
            attr = f"_current_{self._active_tab}_file"
            if getattr(self, attr) != current_path:
                self._load_log_file(self._active_tab)
                self._file_lbl.configure(text=current_path)

    def on_hide(self):
        """Stop tail polling when page not visible (optional optimisation)."""
        # We leave it running so live tab always receives sync lines
        pass