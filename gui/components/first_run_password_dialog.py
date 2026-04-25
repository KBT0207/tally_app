"""
gui/components/first_run_password_dialog.py
============================================
Mandatory password dialog shown ONLY on first launch of the app
on a new PC. The user must enter the correct admin password to
proceed to DB setup. There is no Cancel button — the only way
out is the correct password or closing the window (which exits the app).

Uses the same hardcoded password as ProtectedAccessDialog.
No OTP required on first run — just password verification.
"""

import hashlib
import tkinter as tk
from tkinter import messagebox

# ── Imported from admin_config.py — edit that file only ──────────────────────
from admin_config import ADMIN_PASSWORD as _ADMIN_PASSWORD
_ADMIN_PASS_HASH = hashlib.sha256(_ADMIN_PASSWORD.encode()).hexdigest()


class FirstRunPasswordDialog(tk.Toplevel):
    """
    Shown once on first launch. Blocks until correct password is entered.

    Properties:
        verified (bool) — True if correct password was entered, False if closed

    Usage (in app.py):
        dlg = FirstRunPasswordDialog(self.root)
        self.root.wait_window(dlg)
        if not dlg.verified:
            self.root.destroy()   # user closed without correct password → exit
            return False
        # proceed to DB setup wizard
    """

    def __init__(self, parent):
        super().__init__(parent)
        self.verified = False

        self.title("TallySyncManager — First Time Setup")
        self.resizable(True, True)
        self.minsize(420, 200)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self.configure(bg="#f7f8fa")

        self._centre(parent, 480, 320)
        self._build()
        self._pass_entry.focus_set()

    # ── Layout ────────────────────────────────────────────────────────────────
    def _centre(self, parent, w, h):
        self.update_idletasks()
        x = parent.winfo_rootx() + (parent.winfo_width()  - w) // 2
        y = parent.winfo_rooty() + (parent.winfo_height() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _build(self):
        outer = tk.Frame(self, bg="#f7f8fa", padx=32, pady=26)
        outer.pack(fill="both", expand=True)

        # ── Header ────────────────────────────────────────────────────────────
        tk.Label(
            outer,
            text="🔐  Welcome to TallySyncManager",
            font=("Segoe UI", 13, "bold"),
            bg="#f7f8fa", fg="#1a202c", anchor="w",
        ).pack(fill="x")

        tk.Label(
            outer,
            text=(
                "This is the first time the app is running on this computer.\n"
                "Enter the admin password to begin setup."
            ),
            font=("Segoe UI", 9),
            bg="#f7f8fa", fg="#718096",
            anchor="w", justify="left", wraplength=420,
        ).pack(fill="x", pady=(4, 18))

        # ── Card ──────────────────────────────────────────────────────────────
        card = tk.Frame(outer, bg="white", relief="solid", bd=1,
                        padx=22, pady=20)
        card.pack(fill="both", expand=True)

        tk.Label(
            card, text="Admin Password",
            font=("Segoe UI", 9), bg="white",
            fg="#4a5568", anchor="w",
        ).pack(fill="x", pady=(0, 4))

        # Password field with eye toggle
        pw_row = tk.Frame(card, bg="white")
        pw_row.pack(fill="x")

        self._pass_entry = tk.Entry(
            pw_row,
            font=("Segoe UI", 11),
            bg="#f7fafc", fg="#1a202c",
            relief="solid", bd=1,
            show="●",
            insertbackground="#1a202c",
        )
        self._pass_entry.pack(side="left", fill="x", expand=True, ipady=8)
        self._pass_entry.bind("<Return>", lambda e: self._verify())

        _vis = [False]
        def _toggle():
            _vis[0] = not _vis[0]
            self._pass_entry.configure(show="" if _vis[0] else "●")
            eye.configure(text="🙈" if _vis[0] else "👁")
        eye = tk.Button(
            pw_row, text="👁",
            font=("Segoe UI", 10),
            bg="#edf2f7", fg="#555",
            relief="flat", bd=0, padx=9, pady=6,
            cursor="hand2", command=_toggle,
        )
        eye.pack(side="left", padx=(4, 0))

        # Error label (hidden initially)
        self._err_lbl = tk.Label(
            card, text="",
            font=("Segoe UI", 9), bg="white",
            fg="#e53e3e", anchor="w",
        )
        self._err_lbl.pack(fill="x", pady=(6, 0))

        # ── Submit button ─────────────────────────────────────────────────────
        btn_row = tk.Frame(card, bg="white")
        btn_row.pack(fill="x", pady=(16, 0))

        tk.Button(
            btn_row,
            text="Confirm & Continue →",
            font=("Segoe UI", 10, "bold"),
            bg="#4299e1", fg="white",
            relief="flat", bd=0,
            padx=18, pady=9,
            cursor="hand2",
            activebackground="#3182ce",
            command=self._verify,
        ).pack(side="right")

        # ── Footer note ───────────────────────────────────────────────────────
        tk.Label(
            outer,
            text="Contact your system administrator if you don't have the password.",
            font=("Segoe UI", 8),
            bg="#f7f8fa", fg="#a0aec0",
            anchor="w",
        ).pack(fill="x", pady=(10, 0))

    # ── Verification ──────────────────────────────────────────────────────────
    def _verify(self):
        entered = self._pass_entry.get()
        if not entered:
            self._err_lbl.configure(text="⚠  Please enter the admin password.")
            return

        if hashlib.sha256(entered.encode()).hexdigest() != _ADMIN_PASS_HASH:
            self._pass_entry.delete(0, "end")
            self._pass_entry.focus_set()
            self._err_lbl.configure(text="⚠  Incorrect password. Please try again.")
            return

        # ✅ Correct
        self.verified = True
        self.grab_release()
        self.destroy()

    def _on_close(self):
        """User closed the window without correct password → app should exit."""
        if messagebox.askyesno(
            "Exit Setup",
            "Setup is required to use TallySyncManager.\n\nAre you sure you want to exit?",
            parent=self,
        ):
            self.verified = False
            self.grab_release()
            self.destroy()
