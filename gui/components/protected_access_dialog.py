"""
gui/components/protected_access_dialog.py
==========================================
Hardcoded 2-step admin verification.

Step 1 — Enter admin password
Step 2 — Enter 6-digit OTP sent to hardcoded email
Step 3 — DB settings open

NO setup screen. NO config needed. Change the 4 constants below only.
"""

import hashlib
import logging
import random
import smtplib
import string
import threading
import tkinter as tk
from email.mime.multipart import MIMEMultipart
from email.mime.text      import MIMEText
from tkinter              import messagebox

from logging_config import logger

# ═════════════════════════════════════════════════════════════════════════════
#  All settings are in admin_config.py — edit that file only
# ═════════════════════════════════════════════════════════════════════════════
from admin_config import (
    ADMIN_PASSWORD    as _ADMIN_PASSWORD,
    OTP_TO_EMAILS     as _OTP_TO_EMAILS,
    SMTP_USER         as _SMTP_USER,
    SMTP_APP_PASSWORD as _SMTP_APP_PASSWORD,
    SMTP_HOST         as _SMTP_HOST,
    SMTP_PORT         as _SMTP_PORT,
)

_ADMIN_PASS_HASH = hashlib.sha256(_ADMIN_PASSWORD.encode()).hexdigest()


def _make_otp(length: int = 6) -> str:
    return "".join(random.choices(string.digits, k=length))


class ProtectedAccessDialog(tk.Toplevel):
    """
    Modal 2-step admin verification dialog.
    On success calls `callback()`.
    On cancel / wrong credentials — does nothing.

    Usage:
        ProtectedAccessDialog(parent, config_manager, callback=self.open_db_settings)
        (config_manager parameter accepted but not used — kept for API compatibility)
    """

    def __init__(self, parent, config_manager=None, callback=None):
        super().__init__(parent)
        self._callback = callback
        self._otp      = None

        self.title("Admin Verification")
        self.resizable(True, True)
        self.grab_set()
        self.protocol("WM_DELETE_WINDOW", self._cancel)
        self.configure(bg="#f7f8fa")
        self.minsize(400, 180)

        self._centre(parent, 460, 250)
        self._show_password()
        self.wait_window(self)

    # ── Positioning ───────────────────────────────────────────────────────────
    def _centre(self, parent, w, h):
        self.update_idletasks()
        x = parent.winfo_rootx() + (parent.winfo_width()  - w) // 2
        y = parent.winfo_rooty() + (parent.winfo_height() - h) // 2
        self.geometry(f"{w}x{h}+{x}+{y}")

    def _resize(self, w, h):
        cw = self.winfo_width() or w
        self.geometry(f"{max(cw, w)}x{h}")

    def _clear(self):
        for w in self.winfo_children():
            w.destroy()

    # ── Shared widgets ────────────────────────────────────────────────────────
    def _outer(self) -> tk.Frame:
        f = tk.Frame(self, bg="#f7f8fa", padx=30, pady=22)
        f.pack(fill="both", expand=True)
        return f

    def _header(self, outer, title: str, subtitle: str):
        tk.Label(outer, text=title,
                 font=("Segoe UI", 13, "bold"),
                 bg="#f7f8fa", fg="#1a202c", anchor="w").pack(fill="x")
        tk.Label(outer, text=subtitle,
                 font=("Segoe UI", 9),
                 bg="#f7f8fa", fg="#718096", anchor="w",
                 wraplength=400, justify="left").pack(fill="x", pady=(3, 14))

    def _card(self, outer) -> tk.Frame:
        card = tk.Frame(outer, bg="white", relief="solid", bd=1,
                        padx=20, pady=18)
        card.pack(fill="both", expand=True)
        return card

    def _pw_field(self, parent, label: str) -> tk.Entry:
        """Password entry with 👁 show/hide toggle."""
        tk.Label(parent, text=label,
                 font=("Segoe UI", 9), bg="white",
                 fg="#4a5568", anchor="w").pack(fill="x", pady=(4, 2))

        row = tk.Frame(parent, bg="white")
        row.pack(fill="x")

        entry = tk.Entry(row, font=("Segoe UI", 11),
                         bg="#f7fafc", fg="#1a202c",
                         relief="solid", bd=1, show="●",
                         insertbackground="#1a202c")
        entry.pack(side="left", fill="x", expand=True, ipady=7)

        visible = [False]

        def _toggle():
            visible[0] = not visible[0]
            entry.configure(show="" if visible[0] else "●")
            eye_btn.configure(text="🙈" if visible[0] else "👁")

        eye_btn = tk.Button(row, text="👁",
                            font=("Segoe UI", 10),
                            bg="#edf2f7", fg="#555",
                            relief="flat", bd=0,
                            padx=9, pady=5,
                            cursor="hand2",
                            command=_toggle)
        eye_btn.pack(side="left", padx=(4, 0))
        return entry

    def _plain_field(self, parent, label: str) -> tk.Entry:
        """Normal (non-secret) entry."""
        tk.Label(parent, text=label,
                 font=("Segoe UI", 9), bg="white",
                 fg="#4a5568", anchor="w").pack(fill="x", pady=(4, 2))
        entry = tk.Entry(parent, font=("Segoe UI", 11),
                         bg="#f7fafc", fg="#1a202c",
                         relief="solid", bd=1,
                         insertbackground="#1a202c")
        entry.pack(fill="x", ipady=7)
        return entry

    def _btn_row(self, parent, ok_text, ok_cmd,
                 cancel=True, ok_color="#4299e1"):
        row = tk.Frame(parent, bg="white")
        row.pack(fill="x", pady=(18, 0))
        if cancel:
            tk.Button(row, text="Cancel",
                      font=("Segoe UI", 9),
                      bg="#edf2f7", fg="#4a5568",
                      relief="flat", bd=0, padx=14, pady=8,
                      cursor="hand2",
                      command=self._cancel).pack(side="right", padx=(6, 0))
        tk.Button(row, text=ok_text,
                  font=("Segoe UI", 9, "bold"),
                  bg=ok_color, fg="white",
                  relief="flat", bd=0, padx=14, pady=8,
                  cursor="hand2",
                  activebackground="#3182ce",
                  command=ok_cmd).pack(side="right")

    # ── STEP 1 — Password ─────────────────────────────────────────────────────
    def _show_password(self):
        self._clear()
        self._resize(460, 250)
        outer = self._outer()
        self._header(outer,
                     "🔐  Admin Verification",
                     "Enter the admin password to continue.")
        card = self._card(outer)

        self._pass_entry = self._pw_field(card, "Admin Password")
        self._pass_entry.bind("<Return>", lambda e: self._verify_password())
        self._pass_entry.focus_set()

        self._btn_row(card, "Continue →", self._verify_password)

    def _verify_password(self):
        entered = self._pass_entry.get()
        if not entered:
            return
        if hashlib.sha256(entered.encode()).hexdigest() != _ADMIN_PASS_HASH:
            self._pass_entry.delete(0, "end")
            self._pass_entry.focus_set()
            messagebox.showerror(
                "Wrong Password",
                "Incorrect password. Please try again.",
                parent=self)
            return

        # ✅ Password correct — generate OTP and send email
        self._otp = _make_otp()
        self._show_sending()
        threading.Thread(target=self._send_otp, daemon=True).start()

    # ── Sending screen ────────────────────────────────────────────────────────
    def _show_sending(self):
        self._clear()
        self._resize(460, 200)
        outer = self._outer()
        self._header(outer,
                     "🔐  Admin Verification",
                     f"Sending OTP to {len(_OTP_TO_EMAILS)} email(s)…")
        card = self._card(outer)
        tk.Label(card, text="⏳  Please wait…",
                 font=("Segoe UI", 10), bg="white",
                 fg="#718096").pack(pady=16)

    def _send_otp(self):
        try:
            msg            = MIMEMultipart("alternative")
            msg["Subject"] = "TallySyncManager — Admin OTP"
            msg["From"]    = _SMTP_USER
            msg["To"]      = ", ".join(_OTP_TO_EMAILS)

            body = (
                f"Your one-time password (OTP):\n\n"
                f"        {self._otp}\n\n"
                f"Valid for 5 minutes. Do not share.\n"
                f"— TallySyncManager"
            )
            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(_SMTP_HOST, _SMTP_PORT, timeout=15) as srv:
                srv.ehlo()
                srv.starttls()
                srv.login(_SMTP_USER, _SMTP_APP_PASSWORD)
                srv.sendmail(_SMTP_USER, _OTP_TO_EMAILS, msg.as_string())

            logger.info("[ProtectedAccess] OTP sent OK")
            self.after(0, self._show_otp_screen)

        except Exception as exc:
            logger.error(f"[ProtectedAccess] Email failed: {exc}")
            self.after(0, lambda: self._show_error(str(exc)))

    # ── STEP 2 — OTP entry ────────────────────────────────────────────────────
    def _show_otp_screen(self):
        self._clear()
        self._resize(460, 280)
        outer = self._outer()

        # Mask email for display
        count = len(_OTP_TO_EMAILS)
        shown = ", ".join(
            f"{e.split('@')[0][:3]}***@{e.split('@')[1]}" if "@" in e else e
            for e in _OTP_TO_EMAILS
        )
        self._header(outer,
                     "🔐  Admin Verification",
                     f"A 6-digit OTP has been sent to {count} email(s):\n{shown}\n\nEnter it below to open DB settings.")
        card = self._card(outer)

        self._otp_entry = self._plain_field(card, "One-Time Password (OTP)")
        self._otp_entry.bind("<Return>", lambda e: self._verify_otp())
        self._otp_entry.focus_set()

        # Resend link
        link_row = tk.Frame(card, bg="white")
        link_row.pack(fill="x", pady=(4, 0))
        tk.Label(link_row, text="Didn't get it?",
                 font=("Segoe UI", 8), bg="white",
                 fg="#a0aec0").pack(side="left")
        tk.Button(link_row, text=" Resend",
                  font=("Segoe UI", 8, "underline"),
                  bg="white", fg="#4299e1",
                  relief="flat", bd=0, cursor="hand2",
                  command=self._resend).pack(side="left")

        self._btn_row(card, "✓  Verify & Open DB",
                      self._verify_otp, ok_color="#38a169")

    def _resend(self):
        self._otp = _make_otp()
        self._show_sending()
        threading.Thread(target=self._send_otp, daemon=True).start()

    def _verify_otp(self):
        entered = self._otp_entry.get().strip()
        if entered != self._otp:
            self._otp_entry.delete(0, "end")
            self._otp_entry.focus_set()
            messagebox.showerror(
                "Wrong OTP",
                "The OTP is incorrect. Please try again or click Resend.",
                parent=self)
            return

        # ✅ Authenticated
        logger.info("[ProtectedAccess] Admin authenticated OK")
        self.grab_release()
        self.destroy()
        if self._callback:
            self._callback()

    # ── Error screen ──────────────────────────────────────────────────────────
    def _show_error(self, error: str):
        self._clear()
        self._resize(460, 280)
        outer = self._outer()
        self._header(outer,
                     "🔐  Admin Verification",
                     "Failed to send OTP email.")
        card = self._card(outer)
        tk.Label(card, text=f"Error: {error}",
                 font=("Segoe UI", 9), bg="white", fg="#e53e3e",
                 wraplength=400, justify="left", anchor="w").pack(fill="x")
        tk.Label(card,
                 text="Check your internet connection and try again.",
                 font=("Segoe UI", 9), bg="white", fg="#718096",
                 anchor="w").pack(fill="x", pady=(8, 0))
        self._btn_row(card, "← Try Again", self._show_password, cancel=True)

    # ── Cancel ────────────────────────────────────────────────────────────────
    def _cancel(self):
        self.grab_release()
        self.destroy()
