"""
gui/components/global_config_dialog.py
=======================================
Bulk-configure multiple Tally companies at once.

Lets the user:
  1. Pick which companies to apply settings to (checkbox list — all/some)
  2. Choose what fields to overwrite  (credentials / host+port / company type / all)
  3. Enter shared Username + Password
  4. Set shared Tally Host + Port  (default: localhost / 9000)
  5. Select company type  (Local / Remote / TDS)
  6. Click Apply — writes to DB + updates in-memory CompanyState for each
     selected company, then emits  company_updated  so cards refresh.

Usage (from home_page.py):
    from gui.components.global_config_dialog import GlobalConfigDialog
    dlg = GlobalConfigDialog(self.winfo_toplevel(), self.state, self.app)
    self.wait_window(dlg)
    if dlg.applied:
        self._last_rendered_keys = []
        self._render_cards(...)
        self._update_summary()
"""

import tkinter as tk
from tkinter import messagebox

from gui.styles import Color, Font, Spacing
from gui.state  import CompanyStatus


class GlobalConfigDialog(tk.Toplevel):
    """Modal dialog to bulk-apply settings to multiple companies."""

    def __init__(self, parent, state, app):
        super().__init__(parent)
        self.title("Global Company Configuration")
        self.resizable(True, True)
        self.grab_set()

        self.applied  = False   # True after at least one company is saved
        self._state   = state
        self._app     = app

        self._build()
        self._center(parent)

        self.bind("<Escape>", lambda e: self.destroy())

    # ─────────────────────────────────────────────────────────────────────────
    def _center(self, parent):
        self.update_idletasks()
        sw, sh = self.winfo_screenwidth(), self.winfo_screenheight()
        w = min(600, int(sw * 0.95))
        h = min(720, int(sh * 0.92))
        px = parent.winfo_rootx() + parent.winfo_width()  // 2
        py = parent.winfo_rooty() + parent.winfo_height() // 2
        x  = max(8, min(px - w // 2, sw - w - 8))
        y  = max(8, min(py - h // 2, sh - h - 8))
        self.geometry(f"{w}x{h}+{x}+{y}")
        self.minsize(520, 560)

    # ─────────────────────────────────────────────────────────────────────────
    def _build(self):
        root = tk.Frame(self, bg=Color.BG_CARD)
        root.pack(fill="both", expand=True, padx=28, pady=22)

        # ── Header ────────────────────────────────────────────────────────────
        tk.Label(
            root, text="Global Company Configuration",
            font=Font.HEADING_4, bg=Color.BG_CARD, fg=Color.TEXT_PRIMARY,
        ).pack(anchor="w")

        tk.Label(
            root,
            text="Apply shared settings to multiple companies at once.",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).pack(anchor="w", pady=(2, 10))

        tk.Frame(root, bg=Color.BORDER, height=1).pack(fill="x", pady=(0, 14))

        # ── What to apply ─────────────────────────────────────────────────────
        self._add_section(root, "What to Apply")

        scope_row = tk.Frame(root, bg=Color.BG_CARD)
        scope_row.pack(anchor="w", pady=(6, 10))

        self._apply_creds_var     = tk.BooleanVar(value=True)
        self._apply_host_port_var = tk.BooleanVar(value=False)
        self._apply_type_var      = tk.BooleanVar(value=True)

        tk.Checkbutton(
            scope_row, text="Credentials  (username / password)",
            variable=self._apply_creds_var,
            font=Font.BODY, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
            fg=Color.TEXT_PRIMARY,
            command=self._on_scope_change,
        ).pack(side="left")

        tk.Checkbutton(
            scope_row, text="Host / Port",
            variable=self._apply_host_port_var,
            font=Font.BODY, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
            fg=Color.TEXT_PRIMARY,
            command=self._on_scope_change,
        ).pack(side="left", padx=(20, 0))

        tk.Checkbutton(
            scope_row, text="Company Type  (Local / Remote / TDS)",
            variable=self._apply_type_var,
            font=Font.BODY, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
            fg=Color.TEXT_PRIMARY,
            command=self._on_scope_change,
        ).pack(side="left", padx=(20, 0))

        # ── Credentials ───────────────────────────────────────────────────────
        self._creds_frame = tk.Frame(root, bg=Color.BG_CARD)
        self._creds_frame.pack(fill="x")

        self._add_section(self._creds_frame, "Tally Credentials  (leave blank if not required)")

        cred_row = tk.Frame(self._creds_frame, bg=Color.BG_CARD)
        cred_row.pack(anchor="w", pady=(6, 0))

        tk.Label(cred_row, text="Username:", font=Font.BODY,
                 bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=10, anchor="w").pack(side="left")
        self._username_var = tk.StringVar()
        tk.Entry(
            cred_row, textvariable=self._username_var,
            font=Font.BODY, width=20,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        ).pack(side="left", padx=(4, 20))

        tk.Label(cred_row, text="Password:", font=Font.BODY,
                 bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=9, anchor="w").pack(side="left")
        self._password_var = tk.StringVar()
        tk.Entry(
            cred_row, textvariable=self._password_var,
            font=Font.BODY, width=20, show="●",
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        ).pack(side="left", padx=(4, 0))

        # show/hide password toggle
        self._show_pw_var = tk.BooleanVar(value=False)
        self._pw_entry = cred_row.winfo_children()[-1]   # the password Entry
        tk.Checkbutton(
            cred_row, text="Show",
            variable=self._show_pw_var,
            font=Font.BODY_SM, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
            fg=Color.TEXT_MUTED,
            command=self._toggle_pw,
        ).pack(side="left", padx=(6, 0))

        # ── Host / Port ───────────────────────────────────────────────────────
        self._host_port_frame = tk.Frame(root, bg=Color.BG_CARD)
        self._host_port_frame.pack(fill="x", pady=(10, 0))

        self._add_section(self._host_port_frame, "Tally Connection  (Host / Port)")

        hp_row = tk.Frame(self._host_port_frame, bg=Color.BG_CARD)
        hp_row.pack(anchor="w", pady=(6, 0))

        tk.Label(hp_row, text="Host:", font=Font.BODY,
                 bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=6, anchor="w").pack(side="left")
        self._host_var = tk.StringVar(value="localhost")
        self._host_entry = tk.Entry(
            hp_row, textvariable=self._host_var,
            font=Font.BODY, width=20,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        )
        self._host_entry.pack(side="left", padx=(4, 20))

        tk.Label(hp_row, text="Port:", font=Font.BODY,
                 bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
                 width=5, anchor="w").pack(side="left")
        self._port_var = tk.StringVar(value="9000")
        self._port_entry = tk.Entry(
            hp_row, textvariable=self._port_var,
            font=Font.BODY, width=7,
            bg=Color.BG_INPUT, fg=Color.TEXT_PRIMARY,
            relief="solid", bd=1,
        )
        self._port_entry.pack(side="left", padx=(4, 0))

        tk.Label(
            self._host_port_frame,
            text="Note: applies the same host and port to every selected company.",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).pack(anchor="w")

        # ── Company Type ──────────────────────────────────────────────────────
        self._type_frame = tk.Frame(root, bg=Color.BG_CARD)
        self._type_frame.pack(fill="x", pady=(10, 0))

        self._add_section(self._type_frame, "Company Type")

        type_row = tk.Frame(self._type_frame, bg=Color.BG_CARD)
        type_row.pack(anchor="w", pady=(6, 4))

        self._type_var = tk.StringVar(value="local")
        for val, label in [("local", "Local"), ("remote", "Remote (Drive)"), ("tds", "TDS Server")]:
            tk.Radiobutton(
                type_row, text=label,
                variable=self._type_var, value=val,
                font=Font.BODY, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
                fg=Color.TEXT_PRIMARY,
            ).pack(side="left", padx=(0, 18))

        tk.Label(
            self._type_frame,
            text="Note: this sets the type field only. Data paths are kept per-company.",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        ).pack(anchor="w")

        # ── Company picker ────────────────────────────────────────────────────
        tk.Frame(root, bg=Color.BORDER, height=1).pack(fill="x", pady=(14, 0))
        self._add_section(root, "Apply To")

        # Select-all row
        sel_row = tk.Frame(root, bg=Color.BG_CARD)
        sel_row.pack(fill="x", pady=(6, 4))

        self._all_var = tk.BooleanVar(value=True)
        tk.Checkbutton(
            sel_row, text="Select All",
            variable=self._all_var,
            font=Font.BODY_SM_BOLD, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
            fg=Color.TEXT_PRIMARY,
            command=self._toggle_all,
        ).pack(side="left")

        # Status filter: configured only vs all
        self._configured_only_var = tk.BooleanVar(value=False)
        tk.Checkbutton(
            sel_row, text="Configured companies only",
            variable=self._configured_only_var,
            font=Font.BODY_SM, bg=Color.BG_CARD, activebackground=Color.BG_CARD,
            fg=Color.TEXT_MUTED,
            command=self._rebuild_company_list,
        ).pack(side="left", padx=(16, 0))

        self._count_lbl = tk.Label(
            sel_row, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.TEXT_MUTED,
        )
        self._count_lbl.pack(side="right")

        # Scrollable company list
        list_border = tk.Frame(
            root, bg=Color.BG_CARD,
            highlightthickness=1, highlightbackground=Color.BORDER,
        )
        list_border.pack(fill="both", expand=True)

        canvas = tk.Canvas(list_border, bg=Color.BG_CARD, highlightthickness=0, bd=0)
        canvas.pack(side="left", fill="both", expand=True)

        vsb = tk.Scrollbar(list_border, orient="vertical", command=canvas.yview)
        vsb.pack(side="right", fill="y")
        canvas.configure(yscrollcommand=vsb.set)

        self._co_frame = tk.Frame(canvas, bg=Color.BG_CARD)
        cw = canvas.create_window((0, 0), window=self._co_frame, anchor="nw")

        self._co_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        canvas.bind(
            "<Configure>",
            lambda e: canvas.itemconfig(cw, width=e.width)
        )
        canvas.bind("<Enter>",
            lambda e: canvas.bind_all("<MouseWheel>",
                lambda ev: canvas.yview_scroll(int(-1*(ev.delta/120)), "units")))
        canvas.bind("<Leave>",
            lambda e: canvas.unbind_all("<MouseWheel>"))

        self._company_vars: dict[str, tk.BooleanVar] = {}
        self._rebuild_company_list()

        # ── Feedback ──────────────────────────────────────────────────────────
        self._feedback = tk.Label(
            root, text="",
            font=Font.BODY_SM, bg=Color.BG_CARD, fg=Color.DANGER,
            wraplength=520, justify="left",
        )
        self._feedback.pack(anchor="w", pady=(8, 0))

        # ── Buttons ───────────────────────────────────────────────────────────
        tk.Frame(root, bg=Color.BORDER, height=1).pack(fill="x", pady=(10, 0))

        btn_row = tk.Frame(root, bg=Color.BG_CARD)
        btn_row.pack(fill="x", pady=(12, 0))

        tk.Button(
            btn_row, text="Cancel",
            font=Font.BUTTON_SM, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
            relief="solid", bd=1, padx=14, pady=5, cursor="hand2",
            command=self.destroy,
        ).pack(side="right", padx=(8, 0))

        tk.Button(
            btn_row, text="✓  Apply to Selected",
            font=Font.BUTTON_SM, bg=Color.PRIMARY, fg=Color.TEXT_WHITE,
            relief="flat", bd=0, padx=16, pady=5, cursor="hand2",
            command=self._on_apply,
        ).pack(side="right")

    # ─────────────────────────────────────────────────────────────────────────
    def _add_section(self, parent, text: str):
        f = tk.Frame(parent, bg=Color.BG_CARD)
        f.pack(fill="x", pady=(6, 0))
        tk.Label(
            f, text=text,
            font=Font.BODY_SM_BOLD, bg=Color.BG_CARD, fg=Color.TEXT_SECONDARY,
        ).pack(side="left")
        tk.Frame(f, bg=Color.BORDER_LIGHT, height=1).pack(
            side="left", fill="x", expand=True, padx=(8, 0),
        )

    # ─────────────────────────────────────────────────────────────────────────
    def _rebuild_company_list(self):
        """Clear and repopulate the scrollable company checkbox list."""
        for w in self._co_frame.winfo_children():
            w.destroy()
        self._company_vars.clear()

        configured_only = self._configured_only_var.get()
        companies = sorted(self._state.companies.values(), key=lambda c: c.name.lower())
        if configured_only:
            companies = [c for c in companies if c.status != CompanyStatus.NOT_CONFIGURED]

        for i, co in enumerate(companies):
            is_cfg = (co.status != CompanyStatus.NOT_CONFIGURED)
            row_bg = Color.BG_TABLE_ODD if i % 2 == 0 else Color.BG_TABLE_EVEN

            row = tk.Frame(self._co_frame, bg=row_bg)
            row.pack(fill="x")

            var = tk.BooleanVar(value=self._all_var.get())
            self._company_vars[co.name] = var

            var.trace_add("write", lambda *_, v=var: self._on_company_toggle())

            chk = tk.Checkbutton(
                row, variable=var,
                bg=row_bg, activebackground=row_bg,
                relief="flat", bd=0,
            )
            chk.pack(side="left", padx=(8, 4), pady=4)

            tk.Label(
                row, text=co.name,
                font=Font.BODY, bg=row_bg,
                fg=Color.TEXT_PRIMARY if is_cfg else Color.TEXT_MUTED,
                anchor="w",
            ).pack(side="left", fill="x", expand=True)

            # Status pill
            if is_cfg:
                pill_bg, pill_fg, pill_txt = Color.SUCCESS_BG, Color.SUCCESS_FG, "✓ Configured"
            else:
                pill_bg, pill_fg, pill_txt = Color.WARNING_BG, Color.WARNING_FG, "○ Not configured"

            tk.Label(
                row, text=pill_txt,
                font=Font.BADGE, bg=pill_bg, fg=pill_fg,
                padx=5, pady=1,
            ).pack(side="right", padx=(0, 10))

            tk.Frame(self._co_frame, bg=Color.BORDER_LIGHT, height=1).pack(fill="x")

        self._update_count()

    # ─────────────────────────────────────────────────────────────────────────
    def _toggle_all(self):
        val = self._all_var.get()
        for v in self._company_vars.values():
            v.set(val)
        self._update_count()

    def _on_company_toggle(self):
        selected = sum(1 for v in self._company_vars.values() if v.get())
        total    = len(self._company_vars)
        self._all_var.set(selected == total)
        self._update_count()

    def _update_count(self):
        selected = sum(1 for v in self._company_vars.values() if v.get())
        total    = len(self._company_vars)
        self._count_lbl.configure(
            text=f"{selected} / {total} selected",
            fg=Color.PRIMARY if selected > 0 else Color.TEXT_MUTED,
        )

    def _on_scope_change(self):
        """Grey out credential / host-port / type sections based on checkboxes."""
        def _set_frame_state(frame, enabled: bool):
            bg_in = Color.BG_INPUT if enabled else Color.BG_CARD
            for w in frame.winfo_children():
                try:
                    if isinstance(w, tk.Entry):
                        w.configure(state="normal" if enabled else "disabled",
                                    bg=bg_in)
                    elif isinstance(w, tk.Frame):
                        # Recurse into nested frames (e.g. hp_row inside host_port_frame)
                        for child in w.winfo_children():
                            if isinstance(child, tk.Entry):
                                child.configure(state="normal" if enabled else "disabled",
                                                bg=bg_in)
                except Exception:
                    pass

        _set_frame_state(self._creds_frame,     self._apply_creds_var.get())
        _set_frame_state(self._host_port_frame,  self._apply_host_port_var.get())
        _set_frame_state(self._type_frame,       self._apply_type_var.get())

    def _toggle_pw(self):
        show = self._show_pw_var.get()
        self._pw_entry.configure(show="" if show else "●")

    # ─────────────────────────────────────────────────────────────────────────
    def _on_apply(self):
        apply_creds = self._apply_creds_var.get()
        apply_type  = self._apply_type_var.get()

        if not apply_creds and not apply_host_port and not apply_type:
            self._feedback.configure(
                text="Please select at least one thing to apply  (Credentials, Host/Port, or Company Type).",
                fg=Color.DANGER,
            )
            return

        targets = [name for name, v in self._company_vars.items() if v.get()]
        if not targets:
            self._feedback.configure(
                text="Please select at least one company to apply settings to.",
                fg=Color.DANGER,
            )
            return

        username = self._username_var.get().strip()
        password = self._password_var.get().strip()
        host     = self._host_var.get().strip() or "localhost"
        try:
            port = int(self._port_var.get().strip() or "9000")
            if not (1 <= port <= 65535):
                raise ValueError
        except ValueError:
            self._feedback.configure(
                text="Port must be a number between 1 and 65535.",
                fg=Color.DANGER,
            )
            return
        ctype    = self._type_var.get()

        ok_count  = 0
        fail_msgs = []

        for name in targets:
            co = self._state.get_company(name)
            if not co:
                continue

            # Build kwargs — only override the fields we're applying
            kwargs = dict(
                company_name  = co.name,
                guid          = co.guid or "",
                starting_from = co.starting_from or "",
                books_from    = co.books_from or co.starting_from or "",
                tally_username= username  if apply_creds     else (getattr(co, "tally_username", "") or ""),
                tally_password= password  if apply_creds     else (getattr(co, "tally_password", "") or ""),
                tally_host    = host      if apply_host_port else (getattr(co, "tally_host", "localhost") or "localhost"),
                tally_port    = port      if apply_host_port else int(getattr(co, "tally_port", 9000) or 9000),
                company_type  = ctype     if apply_type      else (getattr(co, "company_type", "local") or "local"),
                data_path     = getattr(co, "data_path",   "") or "",
                tds_path      = getattr(co, "tds_path",    "") or "",
                drive_letter  = getattr(co, "drive_letter","") or "",
            )

            # Companies that were never configured yet have no starting_from —
            # we can still write credentials/type without a date.
            # save_company_to_db requires starting_from → skip unconfigured
            # companies and just update in-memory state instead.
            if not kwargs["starting_from"]:
                # Only update in-memory for unconfigured companies
                if apply_creds:
                    co.tally_username = username
                    co.tally_password = password
                if apply_host_port:
                    co.tally_host = host
                    co.tally_port = port
                if apply_type:
                    co.company_type = ctype
                ok_count += 1
                self._state.emit("company_updated", name=co.name, company=co)
                continue

            ok, detail = self._app.save_company_to_db(**kwargs)
            if ok:
                # Mirror into in-memory state
                if apply_creds:
                    co.tally_username = username
                    co.tally_password = password
                if apply_host_port:
                    co.tally_host = host
                    co.tally_port = port
                if apply_type:
                    co.company_type = ctype
                ok_count += 1
                self._state.emit("company_updated", name=co.name, company=co)
            else:
                fail_msgs.append(f"{name}: {detail}")

        if fail_msgs:
            self._feedback.configure(
                text=f"Applied to {ok_count} companies. Errors:\n" + "\n".join(fail_msgs),
                fg=Color.DANGER,
            )
        else:
            self._feedback.configure(
                text=f"✓  Applied to {ok_count} {'company' if ok_count == 1 else 'companies'} successfully.",
                fg=Color.SUCCESS_FG,
            )

        if ok_count > 0:
            self.applied = True
            # Auto-close after short delay so user sees the success message
            self.after(1200, self.destroy)
