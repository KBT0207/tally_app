"""
gui/styles.py
=============
Single source of truth for all visual constants.
Import this everywhere — never hardcode colors or fonts in pages/components.

Theme: Clean Professional Light
"""


# ─────────────────────────────────────────────────────────────────────────────
#  ttkbootstrap theme name
#  Options: "flatly" | "litera" | "cosmo" | "journal" | "lumen" | "minty"
#  "flatly" is the most professional clean light theme
# ─────────────────────────────────────────────────────────────────────────────
BOOTSTRAP_THEME = "flatly"


# ─────────────────────────────────────────────────────────────────────────────
#  Color Palette
# ─────────────────────────────────────────────────────────────────────────────
class Color:
    # Brand / Primary
    PRIMARY         = "#2C3E7A"    # Deep professional blue
    PRIMARY_HOVER   = "#1A2B5E"
    PRIMARY_LIGHT   = "#EEF1FB"    # Very light blue tint for backgrounds

    # Accent
    ACCENT          = "#3498DB"    # Lighter blue for links/highlights

    # Status colors
    SUCCESS         = "#27AE60"    # Green — synced / configured
    WARNING         = "#F39C12"    # Amber — not configured / partial
    DANGER          = "#E74C3C"    # Red — error / offline
    INFO            = "#2980B9"    # Blue — informational / syncing
    MUTED           = "#95A5A6"    # Grey — disabled / inactive

    # Status badge backgrounds (light tint + matching text)
    SUCCESS_BG      = "#EAFAF1"
    SUCCESS_FG      = "#1E8449"
    WARNING_BG      = "#FEF9E7"
    WARNING_FG      = "#B7770D"
    DANGER_BG       = "#FDEDEC"
    DANGER_FG       = "#C0392B"
    INFO_BG         = "#EBF5FB"
    INFO_FG         = "#1A6FA8"
    MUTED_BG        = "#F2F3F4"
    MUTED_FG        = "#6C757D"

    # Backgrounds
    BG_ROOT         = "#F0F2F5"    # App window background
    BG_SIDEBAR      = "#2C3E7A"    # Left sidebar
    BG_CARD         = "#FFFFFF"    # Card / panel background
    BG_CARD_HOVER   = "#F8F9FF"    # Card hover
    BG_HEADER       = "#FFFFFF"    # Top header bar
    BG_INPUT        = "#FFFFFF"
    BG_TABLE_ODD    = "#FFFFFF"
    BG_TABLE_EVEN   = "#F8F9FA"
    BG_TABLE_HEADER = "#F1F3F9"

    # Text
    TEXT_PRIMARY    = "#2C3E50"    # Main text — near black
    TEXT_SECONDARY  = "#5D6D7E"    # Labels, hints
    TEXT_MUTED      = "#95A5A6"    # Disabled / placeholder
    TEXT_WHITE      = "#FFFFFF"
    TEXT_LINK       = "#2980B9"

    # Borders
    BORDER          = "#DEE2E6"
    BORDER_FOCUS    = "#2C3E7A"
    BORDER_LIGHT    = "#F0F0F0"

    # Sidebar text
    SIDEBAR_TEXT    = "#FFFFFF"
    SIDEBAR_TEXT_MUTED = "#A8B5D1"
    SIDEBAR_ACTIVE_BG  = "#1A2B5E"
    SIDEBAR_HOVER_BG   = "#3A4E8C"

    # Progress bar
    PROGRESS_BG     = "#E8ECEF"
    PROGRESS_FILL   = "#2C3E7A"
    PROGRESS_SUCCESS= "#27AE60"
    PROGRESS_ERROR  = "#E74C3C"

    # Log colors (for Text widget tags)
    LOG_INFO        = "#2C3E50"
    LOG_SUCCESS     = "#1E8449"
    LOG_WARNING     = "#B7770D"
    LOG_ERROR       = "#C0392B"
    LOG_DEBUG       = "#7F8C8D"
    LOG_BG          = "#FAFAFA"


# ─────────────────────────────────────────────────────────────────────────────
#  Typography
# ─────────────────────────────────────────────────────────────────────────────
class Font:
    FAMILY          = "Segoe UI"          # Windows — clean & professional
    FAMILY_MONO     = "Consolas"          # Monospace for logs

    # Sizes
    SIZE_XS         = 8
    SIZE_SM         = 9
    SIZE_BASE       = 10
    SIZE_MD         = 11
    SIZE_LG         = 13
    SIZE_XL         = 16
    SIZE_2XL        = 20
    SIZE_3XL        = 26

    # Pre-built tuples (family, size, weight)
    HEADING_1       = (FAMILY, SIZE_3XL, "bold")
    HEADING_2       = (FAMILY, SIZE_2XL, "bold")
    HEADING_3       = (FAMILY, SIZE_XL,  "bold")
    HEADING_4       = (FAMILY, SIZE_LG,  "bold")

    BODY            = (FAMILY, SIZE_BASE, "normal")
    BODY_BOLD       = (FAMILY, SIZE_BASE, "bold")
    BODY_SM         = (FAMILY, SIZE_SM,   "normal")
    BODY_SM_BOLD    = (FAMILY, SIZE_SM,   "bold")

    LABEL           = (FAMILY, SIZE_MD,   "normal")
    LABEL_BOLD      = (FAMILY, SIZE_MD,   "bold")

    BUTTON          = (FAMILY, SIZE_BASE, "bold")
    BUTTON_SM       = (FAMILY, SIZE_SM,   "bold")

    SIDEBAR_ITEM    = (FAMILY, SIZE_MD,   "normal")
    SIDEBAR_TITLE   = (FAMILY, SIZE_LG,   "bold")

    BADGE           = (FAMILY, SIZE_XS,   "bold")

    MONO            = (FAMILY_MONO, SIZE_SM, "normal")
    MONO_SM         = (FAMILY_MONO, SIZE_XS, "normal")


# ─────────────────────────────────────────────────────────────────────────────
#  Spacing & Layout
# ─────────────────────────────────────────────────────────────────────────────
class Spacing:
    XS              = 4
    SM              = 8
    MD              = 12
    LG              = 16
    XL              = 24
    XXL             = 32

    # Padding presets (x, y) for common widgets
    PAD_CARD        = (XL, XL)
    PAD_BUTTON      = (LG, SM)
    PAD_INPUT       = (SM, SM)
    PAD_SECTION     = (XL, LG)


class Layout:
    SIDEBAR_WIDTH   = 200
    HEADER_HEIGHT   = 60
    CARD_RADIUS     = 8       # Not native in Tkinter but used in canvas drawings
    COMPANY_ROW_H   = 72      # Height of each company row in list
    MIN_WIDTH       = 1100
    MIN_HEIGHT      = 680


# ─────────────────────────────────────────────────────────────────────────────
#  Status → Visual mapping
#  Use: STATUS_STYLE[company.status] → dict with bg, fg, icon
# ─────────────────────────────────────────────────────────────────────────────
STATUS_STYLE = {
    "Configured":     {"bg": Color.SUCCESS_BG, "fg": Color.SUCCESS_FG, "dot": Color.SUCCESS,  "icon": "●"},
    "Not Configured": {"bg": Color.WARNING_BG, "fg": Color.WARNING_FG, "dot": Color.WARNING,  "icon": "○"},
    "Syncing":        {"bg": Color.INFO_BG,    "fg": Color.INFO_FG,    "dot": Color.INFO,     "icon": "⟳"},
    "Sync Done":      {"bg": Color.SUCCESS_BG, "fg": Color.SUCCESS_FG, "dot": Color.SUCCESS,  "icon": "✓"},
    "Sync Error":     {"bg": Color.DANGER_BG,  "fg": Color.DANGER_FG,  "dot": Color.DANGER,   "icon": "✗"},
    "Tally Offline":  {"bg": Color.DANGER_BG,  "fg": Color.DANGER_FG,  "dot": Color.DANGER,   "icon": "✗"},
    "Scheduled":      {"bg": Color.INFO_BG,    "fg": Color.INFO_FG,    "dot": Color.ACCENT,   "icon": "⏰"},
}


# ─────────────────────────────────────────────────────────────────────────────
#  Sidebar navigation items
#  Each dict: label, icon (unicode), page_key
# ─────────────────────────────────────────────────────────────────────────────
NAV_ITEMS = [
    {"label": "Companies",  "icon": "🏢", "page": "home"},
    {"label": "Sync",       "icon": "🔄", "page": "sync"},
    {"label": "Scheduler",  "icon": "⏰", "page": "scheduler"},
    {"label": "Logs",       "icon": "📋", "page": "logs"},
    {"label": "Settings",   "icon": "⚙️",  "page": "settings"},
]


# ─────────────────────────────────────────────────────────────────────────────
#  Window config
# ─────────────────────────────────────────────────────────────────────────────
APP_TITLE   = "Tally Sync Manager"
APP_VERSION = "v1.0.0"
ICON_PATH   = None   # Set to path of .ico file if available