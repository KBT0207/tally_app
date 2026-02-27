"""
gui/pages/sync_page.py
========================
Beginner-friendly sync page — step-by-step wizard.

Key features:
  • Per-company custom date range (click the pill to toggle Global ↔ Custom)
  • Collapsible voucher selector per company (starts collapsed)
  • Global "Apply to ALL" voucher strip
  • Live validation before sync starts
"""

import copy
import queue
import tkinter as tk
from tkinter import messagebox
from datetime import datetime, date

from gui.state      import AppState, CompanyStatus, SyncMode, VoucherSelection
from gui.styles     import Color, Font, Spacing
from gui.components.sync_progress_panel import SyncProgressPanel
from gui.controllers.sync_controller    import SyncController


# ─────────────────────────────────────────────────────────────────────────────
#  Voucher definitions
# ─────────────────────────────────────────────────────────────────────────────
VOUCHER_COLS = [
    ("Ld", "ledgers",       "Ledgers",       "📒"),
    ("It", "items",         "Items / Stock", "📦"),
    ("Sl", "sales",         "Sales",         "🧾"),
    ("Pu", "purchase",      "Purchase",      "🛒"),
    ("CN", "credit_note",   "Credit Note",   "📄"),
    ("DN", "debit_note",    "Debit Note",    "📄"),
    ("Rc", "receipt",       "Receipt",       "💰"),
    ("Pm", "payment",       "Payment",       "💸"),
    ("Jn", "journal",       "Journal",       "📓"),
    ("Co", "contra",        "Contra",        "🔄"),
    ("TB", "trial_balance", "Trial Balance", "⚖️"),
]

# Colour tokens
_IND_BG   = "#EEF2FF"   # indigo-50
_IND_BDR  = "#C7D2FE"   # indigo-200
_IND_FG   = "#4338CA"   # indigo-700
_AMB_BG   = "#FFFBEB"   # amber-50
_AMB_BDR  = "#FDE68A"   # amber-200
_AMB_FG   = "#92400E"   # amber-800
_GRN_BG   = "#ECFDF5"
_GRN_BDR  = "#6EE7B7"
_GRN_FG   = "#065F46"


# ─────────────────────────────────────────────────────────────────────────────
#  Date helpers
# ─────────────────────────────────────────────────────────────────────────────
def _parse8(s):
    try:
        return datetime.strptime(str(s).strip()[:8], "%Y%m%d").date()
    except Exception:
        return None

def _disp(d):
    return d.strftime("%d-%b-%Y") if d else ""

def _yyyymmdd(d):
    return d.strftime("%Y%m%d") if d else ""

def _parse_disp(s):
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d", "%Y%m%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None

def _fy_start(ref=None):
    d = ref or date.today()
    return date(d.year if d.month >= 4 else d.year - 1, 4, 1)


# ─────────────────────────────────────────────────────────────────────────────
#  Tooltip
# ─────────────────────────────────────────────────────────────────────────────
def _tip(widget, text):
    tip_win = [None]
    def show(e):
        tw = tk.Toplevel(widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{widget.winfo_rootx()+10}+{widget.winfo_rooty()-36}")
        tk.Label(tw, text=text, font=Font.BODY_SM, bg="#FFFBE6", fg="#333",
                 relief="solid", bd=1, padx=8, pady=4,
                 wraplength=280, justify="left").pack()
        tip_win[0] = tw
    def hide(e):
        if tip_win[0]:
            tip_win[0].destroy()
            tip_win[0] = None
    widget.bind("<Enter>", show)
    widget.bind("<Leave>", hide)


# ─────────────────────────────────────────────────────────────────────────────
#  Step header widget
# ─────────────────────────────────────────────────────────────────────────────
class _StepHeader(tk.Frame):
    def __init__(self, parent, number, title, subtitle="", **kw):
        super().__init__(parent, bg=Color.BG_CARD, **kw)
        tk.Frame(self, bg=_IND_FG, width=4).pack(side="left", fill="y")
        inner = tk.Frame(self, bg=Color.BG_CARD, padx=Spacing.LG, pady=10)
        inner.pack(side="left", fill="x", expand=True)
        top = tk.Frame(inner, bg=Color.BG_CARD)
        top.pack(anchor="w")
        c = tk.Canvas(top, width=26, height=26, bg=Color.BG_CARD, highlightthickness=0)
        c.pack(side="left", padx=(0, 8))
        c.create_oval(1, 1, 25, 25, fill=_IND_FG, outline="")
        c.create_text(13, 13, text=str(number), font=(Font.FAMILY, 10, "bold"), fill="white")
        tk.Label(top, text=title, font=Font.LABEL_BOLD,
                 bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY).pack(side="left")
        if subtitle:
            tk.Label(inner, text=subtitle, font=Font.BODY_SM,
                     bg=Color.BG_CARD, fg=Color.TEXT_MUTED, anchor="w").pack(anchor="w")


# ─────────────────────────────────────────────────────────────────────────────
#  VoucherPanel  —  collapsible per-company voucher selector
# ─────────────────────────────────────────────────────────────────────────────
class VoucherPanel(tk.Frame):
    """
    Summary bar: [☑ All types] · [Customize ▸]  (partial count shown in amber)
    Expanded grid: 3-column list of full-name checkboxes.

    KEY DESIGN:
      _grid_wrap is built completely but NOT packed during __init__.
      _do_expand() / _do_collapse() pack / pack_forget it.
      This guarantees pack_forget() always works (can only forget what was packed).
    """

    def __init__(self, parent, sel: VoucherSelection, bg, on_change=None, **kw):
        super().__init__(parent, bg=bg, **kw)
        self._sel      = sel
        self._bg       = bg
        self._cb       = on_change
        self._expanded = False
        self._vars     = {}
        self._all_var  = tk.BooleanVar()

        self._build_bar()
        self._build_grid()   # builds but does NOT pack _grid_wrap

        self._sync_all_var()
        self._update_count()

        # Auto-expand only if already partial — user can see their selection
        if not sel.all_selected():
            self._do_expand()

    # ── summary bar (always visible) ───────────────────────────────────────────
    def _build_bar(self):
        bar = tk.Frame(self, bg=self._bg)
        bar.pack(side="top", anchor="w", fill="x")

        self._all_cb = tk.Checkbutton(
            bar, text="All types",
            variable=self._all_var,
            font=Font.BODY_SM_BOLD,
            bg=self._bg, activebackground=self._bg,
            fg=_IND_FG, selectcolor=self._bg,
            relief="flat", bd=0,
            command=self._on_all,
        )
        self._all_cb.pack(side="left")

        tk.Label(bar, text=" · ", font=Font.BODY_SM,
                 bg=self._bg, fg=Color.TEXT_MUTED).pack(side="left")

        self._tog_btn = tk.Button(
            bar, text="Customize ▸",
            font=(Font.FAMILY, 8, "normal"),
            bg=self._bg, fg=Color.PRIMARY,
            relief="flat", bd=0, cursor="hand2",
            command=self._toggle,
        )
        self._tog_btn.pack(side="left")

        self._count_lbl = tk.Label(
            bar, text="",
            font=Font.BODY_SM, bg=self._bg, fg=Color.WARNING_FG,
        )
        self._count_lbl.pack(side="left", padx=(8, 0))

    # ── expandable grid (built here, NOT packed yet) ────────────────────────────
    def _build_grid(self):
        self._grid_wrap = tk.Frame(self, bg=self._bg)
        # separator
        tk.Frame(self._grid_wrap, bg=Color.BORDER_LIGHT, height=1).pack(
            fill="x", pady=(6, 4))
        inner = tk.Frame(self._grid_wrap, bg=self._bg)
        inner.pack(anchor="w", padx=(4, 0), pady=(0, 4))

        for idx, (short, attr, full, icon) in enumerate(VOUCHER_COLS):
            var = tk.BooleanVar(value=getattr(self._sel, attr, True))
            self._vars[attr] = var
            r, c = divmod(idx, 3)
            tk.Checkbutton(
                inner,
                text=f"{icon}  {full}",
                variable=var,
                font=Font.BODY_SM,
                bg=self._bg, activebackground=self._bg,
                fg=Color.TEXT_PRIMARY, selectcolor=self._bg,
                relief="flat", bd=0, anchor="w",
                command=lambda a=attr, v=var: self._on_item(a, v),
            ).grid(row=r, column=c, sticky="w", padx=(0, 28), pady=2)
        # _grid_wrap intentionally NOT packed here

    # ── toggle ─────────────────────────────────────────────────────────────────
    def _toggle(self):
        if self._expanded:
            self._do_collapse()
        else:
            self._do_expand()

    def _do_expand(self):
        self._expanded = True
        self._grid_wrap.pack(side="top", anchor="w", fill="x")
        self._tog_btn.configure(text="Collapse ▴")

    def _do_collapse(self):
        self._expanded = False
        self._grid_wrap.pack_forget()
        self._tog_btn.configure(text="Customize ▸")

    # ── item-level callback ────────────────────────────────────────────────────
    def _on_item(self, attr, var):
        setattr(self._sel, attr, var.get())
        self._sync_all_var()
        self._update_count()
        if self._cb:
            self._cb()

    def _on_all(self):
        val = self._all_var.get()
        for attr, var in self._vars.items():
            var.set(val)
            setattr(self._sel, attr, val)
        self._update_count()
        if self._cb:
            self._cb()

    def _sync_all_var(self):
        self._all_var.set(self._sel.all_selected())

    def _update_count(self):
        n     = sum(v.get() for v in self._vars.values())
        total = len(VOUCHER_COLS)
        self._all_var.set(n == total)
        if n == total:
            self._count_lbl.configure(text="")
        elif n == 0:
            self._count_lbl.configure(text="⚠ none selected!", fg=Color.DANGER)
        else:
            self._count_lbl.configure(text=f"({n}/{total})", fg=Color.WARNING_FG)

    # ── external API ──────────────────────────────────────────────────────────
    def set_attr(self, attr, val):
        if attr in self._vars:
            self._vars[attr].set(val)
            setattr(self._sel, attr, val)
        self._sync_all_var()
        self._update_count()

    def refresh(self):
        for attr, var in self._vars.items():
            var.set(getattr(self._sel, attr, True))
        self._sync_all_var()
        self._update_count()


# ─────────────────────────────────────────────────────────────────────────────
#  CompanySyncRow
# ─────────────────────────────────────────────────────────────────────────────
class CompanySyncRow(tk.Frame):
    """
    One row per company in the sync table.
    Layout:  [Company Info]  [Date Pill + entries]  [VoucherPanel]

    Date pill:
      • "🌐 Global ▼"  → shows global date range (read-only)
      • "✏ Custom  ▼"  → shows From / To entry fields for this company
    """

    def __init__(self, parent, company_name, co_state,
                 selection: VoucherSelection,
                 global_from_var: tk.StringVar,
                 global_to_var:   tk.StringVar,
                 row_bg,
                 show_dates=True,
                 on_voucher_change=None,
                 **kw):
        super().__init__(parent, bg=row_bg, **kw)
        self.company_name = company_name
        self._co          = co_state
        self.selection    = selection
        self._gfrom       = global_from_var
        self._gto         = global_to_var
        self._bg          = row_bg
        self._show_dates  = show_dates
        self._on_vchange  = on_voucher_change
        self._date_mode   = "global"   # "global" | "custom"
        self._dates_saved = False      # True once user clicks Save Dates

        # Custom date StringVars — prefill from company state
        self._cust_from = tk.StringVar()
        self._cust_to   = tk.StringVar()
        self._prefill_custom_dates()

        self._build()

    # ── prefill ────────────────────────────────────────────────────────────────
    def _prefill_custom_dates(self):
        co  = self._co
        raw = (co.starting_from or co.books_from) if co else None
        fd  = _parse8(raw) if raw else _fy_start()
        self._cust_from.set(_disp(fd))
        self._cust_to.set(_disp(date.today()))

    # ── layout ─────────────────────────────────────────────────────────────────
    def _build(self):
        bg = self._bg

        # ── LEFT: company name + subtitle ──────────────────────────────────
        left = tk.Frame(self, bg=bg, padx=Spacing.LG, pady=Spacing.MD)
        left.pack(side="left", fill="y", anchor="nw")
        left.configure(width=260)
        left.pack_propagate(False)

        co = self._co
        tk.Label(left, text=self.company_name,
                 font=Font.BODY_BOLD, bg=bg, fg=Color.TEXT_PRIMARY,
                 anchor="w", wraplength=240, justify="left").pack(anchor="w")

        if co and co.last_sync_time:
            sub    = f"⟳ Last sync: {co.last_sync_time.strftime('%d-%b-%Y %H:%M')}"
            sub_fg = Color.SUCCESS
        elif co and (co.starting_from or co.books_from):
            raw    = co.starting_from or co.books_from
            sub    = f"📅 Books from: {_disp(_parse8(raw))}"
            sub_fg = Color.TEXT_MUTED
        else:
            sub    = "⚠ No prior sync on record"
            sub_fg = Color.WARNING_FG
        tk.Label(left, text=sub, font=Font.BODY_SM,
                 bg=bg, fg=sub_fg, anchor="w").pack(anchor="w")

        # ── MIDDLE: date range picker ───────────────────────────────────────
        # Outer wrapper so we can show/hide the whole block
        self._date_wrap = tk.Frame(self, bg=bg)
        if self._show_dates:
            self._date_wrap.pack(side="left", fill="y", anchor="nw",
                                 padx=(0, Spacing.MD), pady=Spacing.SM)
        self._build_date_picker(self._date_wrap, bg)

        # ── RIGHT: voucher panel ────────────────────────────────────────────
        right = tk.Frame(self, bg=bg, padx=Spacing.MD, pady=Spacing.SM)
        right.pack(side="right", fill="both", expand=True, anchor="nw")

        self._voucher_panel = VoucherPanel(
            right, self.selection, bg=bg,
            on_change=self._on_vchange,
        )
        self._voucher_panel.pack(anchor="w")

    # ── date picker ────────────────────────────────────────────────────────────
    def _build_date_picker(self, parent, bg):
        """
        Layout (top to bottom inside parent):
          1. pill_row  — always visible toggle button
          2. _view_box — fixed-height container; holds BOTH global_lbl AND
                         custom_frame as children. We show/hide them using
                         pack()/pack_forget() INSIDE this stable container.
                         Because the container itself is always packed, tkinter
                         never loses track of widget order and pack_forget works
                         reliably every time.
        """
        # ── Row 1: pill toggle ─────────────────────────────────────────────
        pill_row = tk.Frame(parent, bg=bg)
        pill_row.pack(side="top", anchor="w", pady=(Spacing.SM, 2))

        self._pill_btn = tk.Button(
            pill_row,
            text="🌐 Global  ▼",
            font=(Font.FAMILY, 8, "bold"),
            bg=_IND_BG, fg=_IND_FG,
            relief="flat", bd=0, padx=10, pady=4,
            cursor="hand2",
            command=self._toggle_date_mode,
        )
        self._pill_btn.pack(side="left")

        self._pill_hint = tk.Label(
            pill_row, text="same as global",
            font=Font.BODY_SM, bg=bg, fg=Color.TEXT_MUTED)
        self._pill_hint.pack(side="left", padx=(8, 0))

        # ── Row 2: stable view-box container ──────────────────────────────
        # Always packed — its children are swapped inside it.
        self._view_box = tk.Frame(parent, bg=bg)
        self._view_box.pack(side="top", anchor="w", fill="x")

        # ── Child A: global range label (visible in global mode) ───────────
        self._global_lbl = tk.Label(
            self._view_box, text="",
            font=Font.BODY_SM, bg=bg, fg=Color.TEXT_SECONDARY, anchor="w")
        # Packed first → will be at top of view_box
        self._global_lbl.pack(side="top", anchor="w", padx=(2, 0))
        self._refresh_global_label()
        self._gfrom.trace_add("write", lambda *_: self._refresh_global_label())
        self._gto.trace_add("write",   lambda *_: self._refresh_global_label())

        # ── Child B: custom date entries (hidden initially) ────────────────
        self._custom_frame = tk.Frame(self._view_box, bg=bg)
        # NOT packed yet — _switch_to_custom() will pack it

        self._cust_entries = []   # keep refs to Entry widgets for focus
        self._dates_saved  = False  # tracks whether user has saved custom dates

        for label_text, var, tip_text in [
            ("From:", self._cust_from,
             "Custom start date for THIS company only.\nFormat: DD-Mon-YYYY  e.g. 01-Apr-2024"),
            ("To:",   self._cust_to,
             "Custom end date for THIS company only.\nFormat: DD-Mon-YYYY  e.g. 27-Feb-2026"),
        ]:
            row_f = tk.Frame(self._custom_frame, bg=bg)
            row_f.pack(side="top", anchor="w", pady=1)
            tk.Label(row_f, text=label_text, font=Font.BODY_SM,
                     bg=bg, fg=Color.TEXT_SECONDARY,
                     width=5, anchor="w").pack(side="left")
            ent = tk.Entry(
                row_f, textvariable=var,
                font=Font.BODY, width=13,
                bg="white", fg=Color.TEXT_PRIMARY,
                relief="solid", bd=1,
                insertbackground=_IND_FG,
            )
            ent.pack(side="left")
            _tip(ent, tip_text)
            self._cust_entries.append(ent)

        tk.Label(self._custom_frame,
                 text="Format: DD-Mon-YYYY",
                 font=(Font.FAMILY, 7, "italic"),
                 bg=bg, fg=Color.TEXT_MUTED).pack(side="top", anchor="w", pady=(1, 0))

        # ── Save / Reset row ─────────────────────────────────────────────────
        btn_row = tk.Frame(self._custom_frame, bg=bg)
        btn_row.pack(side="top", anchor="w", pady=(4, 0))

        self._save_date_btn = tk.Button(
            btn_row, text="💾 Save Dates",
            font=(Font.FAMILY, 8, "bold"),
            bg=_AMB_FG, fg="white",
            relief="flat", bd=0, padx=8, pady=3,
            cursor="hand2",
            command=self._save_custom_dates,
        )
        self._save_date_btn.pack(side="left", padx=(0, 6))

        self._reset_date_btn = tk.Button(
            btn_row, text="✕ Use Global",
            font=(Font.FAMILY, 8, "normal"),
            bg=bg, fg=Color.TEXT_MUTED,
            relief="flat", bd=0, padx=6, pady=3,
            cursor="hand2",
            command=self._switch_to_global,
        )
        self._reset_date_btn.pack(side="left")

        self._saved_label = tk.Label(
            self._custom_frame, text="",
            font=(Font.FAMILY, 7, "italic"),
            bg=bg, fg=Color.SUCCESS,
        )
        self._saved_label.pack(side="top", anchor="w")

    def _refresh_global_label(self):
        try:
            self._global_lbl.configure(
                text=f"{self._gfrom.get()}  →  {self._gto.get()}")
        except Exception:
            pass

    # ── date mode toggle ───────────────────────────────────────────────────────
    def _toggle_date_mode(self):
        if self._date_mode == "global":
            self._switch_to_custom()
        else:
            self._switch_to_global()

    def _switch_to_global(self):
        self._date_mode    = "global"
        self._dates_saved  = False
        self._custom_frame.pack_forget()
        self._global_lbl.pack(side="top", anchor="w", padx=(2, 0))
        self._pill_btn.configure(text="🌐 Global  ▼", bg=_IND_BG, fg=_IND_FG)
        self._pill_hint.configure(text="same as global", fg=Color.TEXT_MUTED)

    def _switch_to_custom(self):
        self._date_mode = "custom"
        self._dates_saved = False
        self._global_lbl.pack_forget()
        self._custom_frame.pack(side="top", anchor="w")
        self._pill_btn.configure(text="✏ Custom  ▼", bg=_AMB_BG, fg=_AMB_FG)
        self._pill_hint.configure(text="⚠ unsaved", fg=Color.DANGER_FG)
        self._saved_label.configure(text="")
        # Focus first entry
        try:
            self._cust_entries[0].focus_set()
            self._cust_entries[0].select_range(0, "end")
        except Exception:
            pass

    def _save_custom_dates(self):
        """Validate and lock in custom dates for this company."""
        fd = _parse_disp(self._cust_from.get().strip())
        td = _parse_disp(self._cust_to.get().strip())
        if not fd:
            self._saved_label.configure(
                text="⚠ Invalid From date", fg=Color.DANGER_FG)
            self._cust_entries[0].focus_set()
            return
        if not td:
            self._saved_label.configure(
                text="⚠ Invalid To date", fg=Color.DANGER_FG)
            self._cust_entries[1].focus_set()
            return
        if fd > td:
            self._saved_label.configure(
                text="⚠ From > To", fg=Color.DANGER_FG)
            return
        # Normalise display
        self._cust_from.set(_disp(fd))
        self._cust_to.set(_disp(td))
        self._dates_saved = True
        self._pill_hint.configure(
            text=f"✓ {_disp(fd)} → {_disp(td)}", fg=Color.SUCCESS)
        self._saved_label.configure(
            text=f"✓ Saved: {_disp(fd)} → {_disp(td)}", fg=Color.SUCCESS)

    # ── show / hide whole date block ───────────────────────────────────────────
    def set_show_dates(self, show):
        self._show_dates = show
        if show:
            self._date_wrap.pack(side="left", fill="y", anchor="nw",
                                 padx=(0, Spacing.MD), pady=Spacing.SM,
                                 before=self._voucher_panel.master)
        else:
            self._date_wrap.pack_forget()

    # ── voucher API ────────────────────────────────────────────────────────────
    def set_voucher_attr(self, attr, val):
        self._voucher_panel.set_attr(attr, val)

    def refresh_vouchers(self):
        self._voucher_panel.refresh()

    # ── date API used by SyncPage ───────────────────────────────────────────────
    def get_from_date(self, fallback):
        if self._date_mode == "global":
            return fallback
        d = _parse_disp(self._cust_from.get())
        return _yyyymmdd(d) if d else fallback

    def get_to_date(self, fallback):
        if self._date_mode == "global":
            return fallback
        d = _parse_disp(self._cust_to.get())
        return _yyyymmdd(d) if d else fallback

    def validate_dates(self):
        if self._date_mode == "global":
            return True, ""
        if not self._dates_saved:
            return False, (f"{self.company_name}:\n"
                           "Custom dates are not saved yet.\n"
                           "Please click '💾 Save Dates' or switch back to Global.")
        fd = _parse_disp(self._cust_from.get().strip())
        td = _parse_disp(self._cust_to.get().strip())
        nm = self.company_name
        if not fd:
            return False, (f"{nm}:\nInvalid 'From' date — \"{self._cust_from.get()}\"\n"
                           "Format: DD-Mon-YYYY  e.g. 01-Apr-2024")
        if not td:
            return False, (f"{nm}:\nInvalid 'To' date — \"{self._cust_to.get()}\"\n"
                           "Format: DD-Mon-YYYY  e.g. 27-Feb-2026")
        if fd > td:
            return False, f"{nm}:\nFrom date ({_disp(fd)}) cannot be after To date ({_disp(td)})."
        return True, ""

    @property
    def is_custom(self):
        return self._date_mode == "custom"


# ─────────────────────────────────────────────────────────────────────────────
#  SyncPage
# ─────────────────────────────────────────────────────────────────────────────
class SyncPage(tk.Frame):

    def __init__(self, parent, state: AppState, navigate, app):
        super().__init__(parent, bg=Color.BG_ROOT)
        self.state    = state
        self.navigate = navigate
        self.app      = app

        self._sync_q    = queue.Queue()
        self._ctrl      = None
        self._panels    = {}

        self._per_co_vouchers: dict[str, VoucherSelection] = {}
        self._company_rows:    dict[str, CompanySyncRow]   = {}

        self._global_col_vars: dict[str, tk.BooleanVar] = {}
        self._global_all_var  = tk.BooleanVar(value=True)

        today = date.today()
        self._gfrom_var = tk.StringVar(value=_disp(_fy_start()))
        self._gto_var   = tk.StringVar(value=_disp(today))
        self._mode_var  = tk.StringVar(value=SyncMode.INCREMENTAL)
        self._batch_var = tk.BooleanVar(value=True)

        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self._build_options_frame()
        self._build_progress_frame()
        self._show_options()

    # ══════════════════════════════════════════════════════════════════════════
    #  PHASE A  —  Options
    # ══════════════════════════════════════════════════════════════════════════
    def _build_options_frame(self):
        self._opt_frame = tk.Frame(self, bg=Color.BG_ROOT)
        self._opt_frame.grid(row=0, column=0, sticky="nsew")
        self._opt_frame.columnconfigure(0, weight=1)
        self._opt_frame.rowconfigure(0, weight=1)

        canvas = tk.Canvas(self._opt_frame, bg=Color.BG_ROOT,
                           highlightthickness=0, bd=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        vsb = tk.Scrollbar(self._opt_frame, orient="vertical", command=canvas.yview)
        vsb.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        self._inner = tk.Frame(canvas, bg=Color.BG_ROOT)
        self._inner.columnconfigure(0, weight=1)
        cw = canvas.create_window((0, 0), window=self._inner, anchor="nw")

        self._inner.bind("<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
            lambda e: canvas.itemconfig(cw, width=e.width))
        canvas.bind_all("<MouseWheel>",
            lambda e: canvas.yview_scroll(int(-1*(e.delta/120)), "units"))

        self._build_page_header(self._inner)
        self._build_step1(self._inner)
        self._build_step2(self._inner)
        self._build_step3(self._inner)
        self._build_action_bar(self._inner)

    # ── Page header ────────────────────────────────────────────────────────────
    def _build_page_header(self, parent):
        hdr = tk.Frame(parent, bg=Color.BG_ROOT, pady=Spacing.LG)
        hdr.grid(row=0, column=0, sticky="ew", padx=Spacing.XL)

        tk.Label(hdr, text="🔄  Sync Data from Tally",
                 font=Font.HEADING_3, bg=Color.BG_ROOT,
                 fg=Color.TEXT_PRIMARY, anchor="w").pack(side="left")

        self._hdr_badge = tk.Label(hdr, text="",
                                   font=Font.BODY_SM_BOLD,
                                   bg=_IND_BG, fg=_IND_FG,
                                   padx=10, pady=3)
        self._hdr_badge.pack(side="left", padx=(Spacing.MD, 0))

    def _update_header_badge(self):
        n = len(self.state.selected_companies)
        if n == 0:
            self._hdr_badge.configure(
                text="No companies selected",
                bg=Color.WARNING_BG, fg=Color.WARNING_FG)
        else:
            self._hdr_badge.configure(
                text=f"{n} {'company' if n==1 else 'companies'} selected",
                bg=_IND_BG, fg=_IND_FG)

    # ── Step 1: Sync Type ──────────────────────────────────────────────────────
    def _build_step1(self, parent):
        card = self._make_card(parent, row=1)
        _StepHeader(card, 1, "Choose Sync Type",
                    "What kind of data pull do you want to do?").pack(fill="x")
        self._hdiv(card)

        body = tk.Frame(card, bg=Color.BG_CARD, padx=Spacing.LG, pady=Spacing.MD)
        body.pack(fill="x")
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)

        self._mode_cards = {}
        MODES = [
            (SyncMode.INCREMENTAL, "⚡", "Quick Update",
             "Only syncs new & changed records since last run.",
             "Fast · Low Tally load · Best for daily use",
             ["✓  Fetches only added/changed records",
              "✓  Usually finishes in seconds",
              "✓  Safe to run multiple times a day",
              "✓  Recommended for scheduled sync"],
             _GRN_BG, _GRN_BDR, _GRN_FG),
            (SyncMode.SNAPSHOT, "📷", "Full Snapshot",
             "Fetches ALL records within a date range you choose.",
             "Complete pull · Slower · First-time or backfill",
             ["✓  Pulls everything in the date range",
              "✓  Good for first-time company setup",
              "✓  Use to backfill missing historical data",
              "⚠  May take several minutes"],
             _AMB_BG, _AMB_BDR, _AMB_FG),
        ]

        for col, (val, icon, title, tagline, tags,
                  bullets, cbg, cbdr, cfg) in enumerate(MODES):
            c = tk.Frame(body, cursor="hand2", bg=cbg,
                         highlightthickness=2, highlightbackground=cbdr,
                         padx=Spacing.LG, pady=Spacing.LG)
            c.grid(row=0, column=col, sticky="nsew",
                   padx=(0, Spacing.MD) if col == 0 else 0, pady=2)
            c.bind("<Button-1>", lambda e, v=val: self._set_mode(v))

            top = tk.Frame(c, bg=cbg)
            top.pack(fill="x")
            tk.Label(top, text=icon, font=(Font.FAMILY, 22), bg=cbg).pack(
                side="left", padx=(0, Spacing.SM))

            txt = tk.Frame(top, bg=cbg)
            txt.pack(side="left", fill="x", expand=True)
            rb = tk.Radiobutton(txt, text=title,
                                variable=self._mode_var, value=val,
                                font=Font.HEADING_4, bg=cbg,
                                activebackground=cbg, fg=cfg,
                                selectcolor=cbg,
                                command=lambda v=val: self._set_mode(v))
            rb.pack(anchor="w")
            tk.Label(txt, text=tagline, font=Font.BODY_SM,
                     bg=cbg, fg=Color.TEXT_SECONDARY, anchor="w").pack(anchor="w")

            tk.Label(c, text=tags, font=(Font.FAMILY, 8, "italic"),
                     bg=cbg, fg=cfg).pack(anchor="w", pady=(Spacing.SM, Spacing.SM))
            tk.Frame(c, bg=cbdr, height=1).pack(fill="x", pady=(0, Spacing.SM))
            for b in bullets:
                tk.Label(c, text=b, font=Font.BODY_SM,
                         bg=cbg, fg=Color.TEXT_SECONDARY,
                         anchor="w").pack(anchor="w", pady=1)

            self._mode_cards[val] = c

        # Set initial selection highlight
        self._highlight_mode(SyncMode.INCREMENTAL)

    # ── Step 2: Companies ──────────────────────────────────────────────────────
    def _build_step2(self, parent):
        card = self._make_card(parent, row=2)
        _StepHeader(card, 2, "Configure Companies",
                    "Set date range and choose what data to pull for each company."
                    ).pack(fill="x")
        self._hdiv(card)

        s2 = tk.Frame(card, bg=Color.BG_CARD, padx=Spacing.LG, pady=Spacing.SM)
        s2.pack(fill="x")

        # ── Global date strip (snapshot only) ─────────────────────────────────
        self._gdate_strip = tk.Frame(
            s2, bg=_IND_BG,
            highlightthickness=1, highlightbackground=_IND_BDR)
        # packed by _set_mode when snapshot is active

        gd = tk.Frame(self._gdate_strip, bg=_IND_BG, padx=Spacing.LG, pady=Spacing.SM)
        gd.pack(fill="x")

        left = tk.Frame(gd, bg=_IND_BG)
        left.pack(side="left", padx=(0, Spacing.XL))
        tk.Label(left, text="🌐  Global Date Range",
                 font=Font.LABEL_BOLD, bg=_IND_BG, fg=_IND_FG).pack(anchor="w")
        tk.Label(left, text="Applied to all companies unless you set Custom ↓",
                 font=Font.BODY_SM, bg=_IND_BG, fg="#6366F1").pack(anchor="w")

        mid = tk.Frame(gd, bg=_IND_BG)
        mid.pack(side="left")
        for lbl, var, tt in [
            ("From:", self._gfrom_var,
             "Earliest date to pull.\nFormat: DD-Mon-YYYY  e.g. 01-Apr-2024"),
            ("To:",   self._gto_var,
             "Latest date to pull.\nFormat: DD-Mon-YYYY  defaults to today"),
        ]:
            rf = tk.Frame(mid, bg=_IND_BG)
            rf.pack(side="left", padx=(0, Spacing.LG))
            tk.Label(rf, text=lbl, font=Font.BODY_BOLD,
                     bg=_IND_BG, fg=_IND_FG).pack(side="left")
            e = tk.Entry(rf, textvariable=var, font=Font.BODY, width=13,
                         bg="white", fg="#1E1B4B", relief="solid", bd=1,
                         insertbackground=_IND_FG)
            e.pack(side="left", padx=(Spacing.SM, 0))
            _tip(e, tt)
        tk.Label(gd, text="Format: DD-Mon-YYYY",
                 font=Font.BODY_SM, bg=_IND_BG, fg="#818CF8").pack(side="left")

        # ── Apply-to-ALL strip ─────────────────────────────────────────────────
        self._all_strip = tk.Frame(
            s2, bg="#F5F3FF",
            highlightthickness=1, highlightbackground=_IND_BDR)
        self._all_strip.pack(fill="x", pady=(0, 2))

        aa = tk.Frame(self._all_strip, bg="#F5F3FF", padx=Spacing.LG, pady=6)
        aa.pack(fill="x")
        tk.Label(aa, text="▸  Apply to ALL companies at once:",
                 font=Font.BODY_SM_BOLD, bg="#F5F3FF", fg=_IND_FG).pack(side="left")

        for short, attr, full, icon in VOUCHER_COLS:
            var = tk.BooleanVar(value=True)
            self._global_col_vars[attr] = var
            cb = tk.Checkbutton(
                aa, text=short, variable=var,
                font=(Font.FAMILY, 8, "bold"),
                bg="#F5F3FF", activebackground="#F5F3FF",
                fg=_IND_FG, selectcolor="#F5F3FF",
                relief="flat", bd=0,
                command=lambda a=attr, v=var: self._gcol(a, v))
            cb.pack(side="left", padx=2)
            _tip(cb, f"Toggle '{full}' for ALL companies")

        tk.Label(aa, text=" | ", font=Font.BODY_SM,
                 bg="#F5F3FF", fg=Color.BORDER).pack(side="left")
        all_cb = tk.Checkbutton(
            aa, text="All", variable=self._global_all_var,
            font=(Font.FAMILY, 8, "bold"),
            bg="#F5F3FF", activebackground="#F5F3FF",
            fg=_IND_FG, selectcolor="#F5F3FF",
            relief="flat", bd=0, command=self._gall)
        all_cb.pack(side="left", padx=(2, 0))
        _tip(all_cb, "Toggle ALL voucher types for ALL companies")

        # ── Company table ──────────────────────────────────────────────────────
        self._tbl = tk.Frame(s2, bg=Color.BG_CARD)
        self._tbl.pack(fill="x")

    # ── Step 3: Batch Mode ─────────────────────────────────────────────────────
    def _build_step3(self, parent):
        card = self._make_card(parent, row=3)
        _StepHeader(card, 3, "Batch Mode",
                    "How should multiple companies be processed?").pack(fill="x")
        self._hdiv(card)

        body = tk.Frame(card, bg=Color.BG_CARD, padx=Spacing.LG, pady=Spacing.MD)
        body.pack(fill="x")

        self._batch_cards = {}
        for val, icon, title, hint, rec in [
            (True,  "🔁", "Sequential",
             "Process one company at a time — waits for each to finish.",
             "Recommended for 1–5 companies"),
            (False, "⚡", "Parallel",
             "Run all companies simultaneously — faster but higher Tally load.",
             "Use with caution on large datasets"),
        ]:
            bf = tk.Frame(body, cursor="hand2", bg=Color.BG_CARD,
                          highlightthickness=1, highlightbackground=Color.BORDER,
                          padx=Spacing.LG, pady=Spacing.SM)
            bf.pack(side="left", padx=(0, Spacing.MD), pady=2)
            bf.bind("<Button-1>", lambda e, v=val: self._set_batch(v))
            self._batch_cards[val] = bf

            top = tk.Frame(bf, bg=Color.BG_CARD)
            top.pack(anchor="w")
            tk.Label(top, text=icon, font=(Font.FAMILY, 16), bg=Color.BG_CARD).pack(side="left")
            tk.Radiobutton(top, text=f"  {title}",
                           variable=self._batch_var, value=val,
                           font=Font.BODY_BOLD, bg=Color.BG_CARD,
                           activebackground=Color.BG_CARD, selectcolor=Color.BG_CARD,
                           command=lambda v=val: self._set_batch(v)).pack(side="left")
            tk.Label(bf, text=hint, font=Font.BODY_SM,
                     bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY).pack(anchor="w")
            tk.Label(bf, text=rec, font=(Font.FAMILY, 8, "italic"),
                     bg=Color.BG_CARD, fg=Color.TEXT_MUTED).pack(anchor="w")

        self._set_batch(True, init=True)

    # ── Action bar ─────────────────────────────────────────────────────────────
    def _build_action_bar(self, parent):
        outer = tk.Frame(parent, bg=Color.BG_ROOT, pady=Spacing.LG)
        outer.grid(row=4, column=0, sticky="ew", padx=Spacing.XL)

        self._action_info = tk.Label(outer, text="",
                                     font=Font.BODY_SM, bg=Color.BG_ROOT,
                                     fg=Color.TEXT_MUTED, anchor="w")
        self._action_info.pack(anchor="w", pady=(0, Spacing.SM))

        btns = tk.Frame(outer, bg=Color.BG_ROOT)
        btns.pack(fill="x")

        tk.Button(btns, text="← Back to Companies",
                  font=Font.BUTTON, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
                  relief="solid", bd=1, padx=Spacing.LG, pady=Spacing.MD,
                  cursor="hand2",
                  command=lambda: self.navigate("home")).pack(side="left")

        self._start_btn = tk.Button(
            btns, text="▶  Start Sync",
            font=Font.BUTTON, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XXL, pady=Spacing.MD,
            cursor="hand2", command=self._on_start_sync)
        self._start_btn.pack(side="right")
        self._start_btn.bind("<Enter>",
            lambda e: self._start_btn.configure(bg=Color.PRIMARY_HOVER))
        self._start_btn.bind("<Leave>",
            lambda e: self._start_btn.configure(bg=Color.PRIMARY))

    # ══════════════════════════════════════════════════════════════════════════
    #  Mode / batch switching
    # ══════════════════════════════════════════════════════════════════════════
    def _set_mode(self, mode, init=False):
        self._mode_var.set(mode)
        self._highlight_mode(mode)
        is_snap = (mode == SyncMode.SNAPSHOT)

        # Show/hide global date strip
        if hasattr(self, '_gdate_strip'):
            if is_snap:
                self._gdate_strip.pack(fill="x", pady=(0, Spacing.SM),
                                       before=self._all_strip)
            else:
                self._gdate_strip.pack_forget()

        # Action info text
        if hasattr(self, '_action_info'):
            if is_snap:
                self._action_info.configure(
                    text="ℹ  Full Snapshot fetches ALL records in the date range. "
                         "May take several minutes for large datasets.")
            else:
                self._action_info.configure(
                    text="ℹ  Quick Update fetches only new & changed records. "
                         "Usually completes in seconds.")

        # Update date visibility on existing rows
        for row in self._company_rows.values():
            row.set_show_dates(is_snap)

        if not init:
            self._rebuild_table()

    def _highlight_mode(self, mode):
        for val, card in self._mode_cards.items():
            active = (val == mode)
            card.configure(highlightthickness=3 if active else 2)

    def _set_batch(self, val, init=False):
        self._batch_var.set(val)
        for v, card in self._batch_cards.items():
            active = (v == val)
            bg = "#F0F4FF" if active else Color.BG_CARD
            hl = Color.PRIMARY if active else Color.BORDER
            card.configure(bg=bg, highlightbackground=hl,
                           highlightthickness=2 if active else 1)
            for child in card.winfo_children():
                try:
                    child.configure(bg=bg)
                    for cc in child.winfo_children():
                        try: cc.configure(bg=bg)
                        except: pass
                except: pass

    # ══════════════════════════════════════════════════════════════════════════
    #  Rebuild company table
    # ══════════════════════════════════════════════════════════════════════════
    def _rebuild_table(self):
        for w in self._tbl.winfo_children():
            w.destroy()
        self._company_rows.clear()

        selected = self.state.selected_companies
        is_snap  = (self._mode_var.get() == SyncMode.SNAPSHOT)

        if not selected:
            ef = tk.Frame(self._tbl, bg=Color.BG_CARD, pady=Spacing.XL)
            ef.pack(fill="x")
            tk.Label(ef, text="⚠  No companies selected",
                     font=Font.HEADING_4, bg=Color.BG_CARD,
                     fg=Color.WARNING_FG).pack()
            tk.Label(ef,
                     text="Go back to the Companies page and select at least one company.",
                     font=Font.BODY_SM, bg=Color.BG_CARD,
                     fg=Color.TEXT_MUTED).pack(pady=(4, 0))
            tk.Button(ef, text="← Go to Companies",
                      font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
                      relief="flat", bd=0, padx=Spacing.LG, pady=Spacing.SM,
                      cursor="hand2",
                      command=lambda: self.navigate("home")).pack(pady=(Spacing.MD, 0))
            if hasattr(self, '_start_btn'):
                self._start_btn.configure(state="disabled", bg=Color.TEXT_MUTED)
            self._update_header_badge()
            return

        if hasattr(self, '_start_btn'):
            self._start_btn.configure(state="normal", bg=Color.PRIMARY)

        # Auto-fill global From = earliest company books_from
        earliest = None
        for name in selected:
            co = self.state.get_company(name)
            if co:
                d = _parse8(co.starting_from or co.books_from)
                if d and (earliest is None or d < earliest):
                    earliest = d
        if earliest:
            self._gfrom_var.set(_disp(earliest))
        self._gto_var.set(_disp(date.today()))

        # Column header
        hdr = tk.Frame(self._tbl, bg=Color.BG_TABLE_HEADER)
        hdr.pack(fill="x")
        tk.Label(hdr, text="Company", font=Font.BODY_SM_BOLD,
                 bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_SECONDARY,
                 padx=Spacing.LG, pady=7, anchor="w").pack(side="left")
        if is_snap:
            tk.Label(hdr, text="Date Range", font=Font.BODY_SM_BOLD,
                     bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_SECONDARY,
                     padx=Spacing.SM, pady=7).pack(side="left")
        tk.Label(hdr, text="Voucher Types to Sync", font=Font.BODY_SM_BOLD,
                 bg=Color.BG_TABLE_HEADER, fg=Color.TEXT_SECONDARY,
                 padx=Spacing.LG, pady=7).pack(side="right")
        tk.Frame(self._tbl, bg=Color.BORDER, height=1).pack(fill="x")

        # Company rows
        for i, name in enumerate(selected):
            if name not in self._per_co_vouchers:
                self._per_co_vouchers[name] = copy.deepcopy(
                    self.state.voucher_selection)

            sel = self._per_co_vouchers[name]
            co  = self.state.get_company(name)
            bg  = Color.BG_TABLE_ODD if i % 2 == 0 else Color.BG_TABLE_EVEN

            row = CompanySyncRow(
                self._tbl,
                company_name=name,
                co_state=co,
                selection=sel,
                global_from_var=self._gfrom_var,
                global_to_var=self._gto_var,
                row_bg=bg,
                show_dates=is_snap,
                on_voucher_change=self._sync_global_checks,
            )
            row.pack(fill="x")
            self._company_rows[name] = row
            tk.Frame(self._tbl, bg=Color.BORDER_LIGHT, height=1).pack(fill="x")

        # Footer hint
        n = len(selected)
        hint = ("Each company uses the global date range unless you click the pill to set Custom."
                if is_snap else
                "Quick Update: only records changed since last sync will be fetched.")
        foot = tk.Frame(self._tbl, bg="#FAFBFF", padx=Spacing.LG, pady=6)
        foot.pack(fill="x")
        tk.Label(foot,
                 text=f"ℹ  {n} {'company' if n==1 else 'companies'} selected  —  {hint}",
                 font=Font.BODY_SM, bg="#FAFBFF", fg=Color.TEXT_MUTED,
                 anchor="w", wraplength=900, justify="left").pack(anchor="w")

        self._sync_global_checks()
        self._update_header_badge()

    # ══════════════════════════════════════════════════════════════════════════
    #  Global voucher toggles
    # ══════════════════════════════════════════════════════════════════════════
    def _gcol(self, attr, var):
        val = var.get()
        for row in self._company_rows.values():
            row.set_voucher_attr(attr, val)
        self._global_all_var.set(
            all(v.get() for v in self._global_col_vars.values()))

    def _gall(self):
        val = self._global_all_var.get()
        for col_var in self._global_col_vars.values():
            col_var.set(val)
        for attr in self._global_col_vars:
            for row in self._company_rows.values():
                row.set_voucher_attr(attr, val)

    def _sync_global_checks(self):
        for attr, col_var in self._global_col_vars.items():
            all_on = all(
                getattr(self._per_co_vouchers.get(n, VoucherSelection()), attr, True)
                for n in self.state.selected_companies)
            col_var.set(all_on)
        self._global_all_var.set(
            all(v.get() for v in self._global_col_vars.values()))

    # ══════════════════════════════════════════════════════════════════════════
    #  Helpers
    # ══════════════════════════════════════════════════════════════════════════
    def _make_card(self, parent, row):
        outer = tk.Frame(parent, bg=Color.BG_CARD,
                         highlightthickness=1, highlightbackground=Color.BORDER)
        outer.grid(row=row, column=0, sticky="ew",
                   padx=Spacing.XL, pady=(0, Spacing.MD))
        outer.columnconfigure(0, weight=1)
        return outer

    def _hdiv(self, parent):
        tk.Frame(parent, bg=Color.BORDER, height=1).pack(fill="x")

    # ══════════════════════════════════════════════════════════════════════════
    #  PHASE B  —  Progress
    # ══════════════════════════════════════════════════════════════════════════
    def _build_progress_frame(self):
        self._prog_frame = tk.Frame(self, bg=Color.BG_ROOT)
        self._prog_frame.grid(row=0, column=0, sticky="nsew")
        self._prog_frame.columnconfigure(0, weight=1)
        self._prog_frame.rowconfigure(1, weight=1)

        top = tk.Frame(self._prog_frame, bg=Color.BG_CARD,
                       highlightthickness=1, highlightbackground=Color.BORDER,
                       pady=Spacing.MD)
        top.grid(row=0, column=0, sticky="ew",
                 padx=Spacing.XL, pady=(Spacing.LG, Spacing.SM))
        top.columnconfigure(1, weight=1)

        self._prog_icon = tk.Label(top, text="⟳",
                                   font=(Font.FAMILY, 20), bg=Color.BG_CARD,
                                   fg=Color.PRIMARY)
        self._prog_icon.grid(row=0, column=0, padx=Spacing.LG, rowspan=2)

        self._prog_title = tk.Label(top, text="Sync in Progress...",
                                    font=Font.HEADING_4, bg=Color.BG_CARD,
                                    fg=Color.TEXT_PRIMARY, anchor="w")
        self._prog_title.grid(row=0, column=1, sticky="w")

        self._prog_sub = tk.Label(top,
                                  text="Please wait — do not close the application",
                                  font=Font.BODY_SM, bg=Color.BG_CARD,
                                  fg=Color.TEXT_MUTED, anchor="w")
        self._prog_sub.grid(row=1, column=1, sticky="w")

        self._cancel_btn = tk.Button(
            top, text="✖  Cancel All",
            font=Font.BUTTON, bg=Color.DANGER_BG, fg=Color.DANGER_FG,
            relief="flat", bd=0, padx=Spacing.LG, pady=Spacing.SM,
            cursor="hand2", command=self._on_cancel)
        self._cancel_btn.grid(row=0, column=2, padx=Spacing.LG, rowspan=2)

        canvas = tk.Canvas(self._prog_frame, bg=Color.BG_ROOT,
                           highlightthickness=0, bd=0)
        canvas.grid(row=1, column=0, sticky="nsew",
                    padx=Spacing.XL, pady=Spacing.SM)
        vsb = tk.Scrollbar(self._prog_frame, orient="vertical", command=canvas.yview)
        vsb.grid(row=1, column=1, sticky="ns")
        canvas.configure(yscrollcommand=vsb.set)

        self._panels_frame = tk.Frame(canvas, bg=Color.BG_ROOT)
        pw = canvas.create_window((0, 0), window=self._panels_frame, anchor="nw")
        self._panels_frame.columnconfigure(0, weight=1)
        self._panels_frame.bind("<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.bind("<Configure>",
            lambda e: canvas.itemconfig(pw, width=e.width))

        # _done_btn lives OUTSIDE _panels_frame so it is never destroyed
        # during panel rebuild — place it in _prog_frame directly (row 2).
        self._done_btn = tk.Button(
            self._prog_frame, text="✓  Done — Back to Companies",
            font=Font.BUTTON, bg=Color.SUCCESS, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=Spacing.XL, pady=Spacing.MD,
            cursor="hand2", command=lambda: self.navigate("home"))

    def _show_options(self):
        self._prog_frame.lower()
        self._opt_frame.tkraise()

    def _show_progress(self):
        self._opt_frame.lower()
        self._prog_frame.tkraise()

    # ══════════════════════════════════════════════════════════════════════════
    #  Start sync
    # ══════════════════════════════════════════════════════════════════════════
    def _on_start_sync(self):
        companies = list(self.state.selected_companies)
        if not companies:
            messagebox.showwarning("No Companies Selected",
                "Please go back and select at least one company to sync.")
            self.navigate("home")
            return

        if self.state.sync_active:
            messagebox.showwarning("Already Running",
                "A sync is already running. Please wait for it to finish.")
            return

        # Validate vouchers
        no_v = [n for n in companies
                if not self._per_co_vouchers.get(n, VoucherSelection()).selected_types()]
        if no_v:
            messagebox.showwarning("No Vouchers Selected",
                "Please select at least one voucher type for:\n\n  • " +
                "\n  • ".join(no_v[:5]))
            return

        mode    = self._mode_var.get()
        is_snap = (mode == SyncMode.SNAPSHOT)
        global_from = ""
        global_to   = datetime.now().strftime("%Y%m%d")

        if is_snap:
            gf = _parse_disp(self._gfrom_var.get())
            gt = _parse_disp(self._gto_var.get())
            if not gf:
                messagebox.showerror("Invalid Date",
                    "Global 'From' date is missing or invalid.\nFormat: DD-Mon-YYYY")
                return
            if not gt:
                messagebox.showerror("Invalid Date",
                    "Global 'To' date is missing or invalid.\nFormat: DD-Mon-YYYY")
                return
            if gf > gt:
                messagebox.showerror("Invalid Date Range",
                    "From date cannot be after To date.")
                return
            global_from = _yyyymmdd(gf)
            global_to   = _yyyymmdd(gt)

            for name, row in self._company_rows.items():
                ok, err = row.validate_dates()
                if not ok:
                    messagebox.showerror("Invalid Date", err)
                    return

        vouchers_map   = {}
        from_dates_map = {}
        to_dates_map   = {}

        for name in companies:
            vouchers_map[name] = copy.deepcopy(
                self._per_co_vouchers.get(name, VoucherSelection()))
            row = self._company_rows.get(name)
            if row and is_snap:
                from_dates_map[name] = row.get_from_date(global_from)
                to_dates_map[name]   = row.get_to_date(global_to)
            else:
                from_dates_map[name] = None
                to_dates_map[name]   = global_to

        # Build panels
        for w in self._panels_frame.winfo_children():
            w.destroy()
        self._panels.clear()

        for i, name in enumerate(companies):
            panel = SyncProgressPanel(self._panels_frame, company_name=name,
                                      on_cancel=self._on_cancel_one)
            panel.grid(row=i, column=0, sticky="ew", pady=(0, Spacing.SM))
            self._panels[name] = panel
            if i > 0 and self.state.batch_sequential:
                panel.mark_waiting()

        self._done_btn.grid_remove()   # safe: _done_btn is in stable _prog_frame
        self._prog_title.configure(text="Sync in Progress...")
        self._prog_sub.configure(
            text=f"Syncing {len(companies)} "
                 f"{'company' if len(companies)==1 else 'companies'} — please wait")
        self._prog_icon.configure(text="⟳", fg=Color.PRIMARY)
        self._cancel_btn.configure(state="normal", text="✖  Cancel All")
        self._show_progress()

        while not self._sync_q.empty():
            try: self._sync_q.get_nowait()
            except queue.Empty: break

        self.state.sync_mode        = mode
        self.state.sync_from_date   = global_from or None
        self.state.sync_to_date     = global_to
        self.state.batch_sequential = self._batch_var.get()

        self._ctrl = SyncController(
            state          = self.state,
            out_queue      = self._sync_q,
            companies      = companies,
            sync_mode      = mode,
            from_date      = global_from or None,
            to_date        = global_to,
            vouchers       = vouchers_map,
            from_dates_map = from_dates_map,
            to_dates_map   = to_dates_map,
            sequential     = self.state.batch_sequential,
        )
        self._ctrl.start()
        self._poll()

    # ── queue polling ──────────────────────────────────────────────────────────
    def _poll(self):
        try:
            while True:
                self._handle(self._sync_q.get_nowait())
        except queue.Empty:
            pass
        if self.state.sync_active:
            self.after(100, self._poll)

    def _handle(self, msg):
        ev = msg[0]
        if ev == "log":
            _, co, line, level = msg
            p = self._panels.get(co)
            if p:
                p.append_log(f"{datetime.now().strftime('%H:%M:%S')}  {line}", level)
            self.app.post("sync_log", f"[{co}] {line}")
        elif ev == "progress":
            _, co, pct, lbl = msg
            p = self._panels.get(co)
            if p: p.set_progress(pct, lbl)
            self.state.set_company_progress(co, pct, lbl)
        elif ev == "status":
            _, co, status = msg
            p = self._panels.get(co)
            if p: p.set_status(status)
            self.state.set_company_status(co, status)
        elif ev == "done":
            _, co, ok = msg
            p = self._panels.get(co)
            if p: p.mark_done(ok)
        elif ev == "all_done":
            self._all_done()

    def _on_cancel(self):
        if self._ctrl:
            self._ctrl.cancel()
        self._cancel_btn.configure(state="disabled", text="Cancelling...")

    def _on_cancel_one(self, _):
        self._on_cancel()

    def _all_done(self):
        self.state.sync_active = False
        self.state.emit("sync_finished")
        self._prog_title.configure(text="Sync Complete ✓")
        self._prog_sub.configure(text="All companies processed.")
        self._prog_icon.configure(text="✓", fg=Color.SUCCESS)
        self._cancel_btn.configure(state="disabled")
        # Show done button (row 2 of _prog_frame, which is stable)
        self._done_btn.grid(row=2, column=0,
                            padx=Spacing.XL, pady=Spacing.LG, sticky="e")

    # ── lifecycle ──────────────────────────────────────────────────────────────
    def on_show(self):
        if self.state.sync_active:
            self._show_progress()
            return
        if hasattr(self.state, "sync_mode") and self.state.sync_mode:
            self._mode_var.set(self.state.sync_mode)
        self._rebuild_table()
        self._set_mode(self._mode_var.get(), init=True)
        self._show_options()