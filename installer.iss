; ============================================================
; installer.iss
; Inno Setup 6 script for TallySyncManager
;
; HOW TO BUILD:
;   Option A - Automatic: run build.bat (it calls this after PyInstaller)
;   Option B - Manual:    open this file in Inno Setup Compiler and press F9
;
; PRE-REQUISITES:
;   1. PyInstaller build already done:  dist\TallySyncManager\ folder must exist
;   2. Inno Setup 6 installed from:    https://jrsoftware.org/isinfo.php
;      (Free, ~5 MB, takes 30 seconds to install)
;
; CUSTOMISE BEFORE SHIPPING:
;   - AppPublisher     : your company / your name
;   - AppVersion       : match your release version
;   - AppURL           : your website (shows in Add/Remove Programs)
;   - AppSupportURL    : support email or link
;   - SetupIconFile    : add a .ico file path if you have one
;   - WizardImageFile  : optional 164x314 left-panel BMP for a branded installer
; ============================================================

#define AppName      "TallySyncManager"
#define AppVersion   "1.0.0"
#define AppPublisher "Kay Bee Exports"
#define AppURL       "https://kaybeeexports.com/"
#define AppExeName   "TallySyncManager.exe"
#define AppDataDir   "TallySyncManager"

; ── Identity ────────────────────────────────────────────────────────────────
[Setup]
AppId={{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}
AppName={#AppName}
AppVersion={#AppVersion}
AppVerName={#AppName} {#AppVersion}
AppPublisher={#AppPublisher}
AppPublisherURL={#AppURL}
AppSupportURL={#AppURL}
AppUpdatesURL={#AppURL}

; Install into "Program Files\TallySyncManager" on both 32/64-bit Windows
DefaultDirName={autopf}\{#AppDataDir}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes

; Installer output location (relative to this .iss file)
OutputDir=installer_output
OutputBaseFilename=TallySyncManager_Setup_v{#AppVersion}

; ── Compression ─────────────────────────────────────────────────────────────
; LZMA2 gives best compression ratio (~30-40% smaller than zip)
Compression=lzma2/ultra64
SolidCompression=yes
LZMAUseSeparateProcess=yes

; ── Display ─────────────────────────────────────────────────────────────────
WizardStyle=modern
WizardResizable=no

; Uncomment and set path if you have a .ico file:
; SetupIconFile=assets\tally_icon.ico

; Uncomment for a branded left-panel image (164x314 pixels, BMP format):
; WizardImageFile=assets\installer_banner.bmp
; WizardSmallImageFile=assets\installer_small.bmp

; ── Requirements ────────────────────────────────────────────────────────────
; Require 64-bit Windows (Python 3.13 is 64-bit only)
ArchitecturesAllowed=x64compatible
ArchitecturesInstallIn64BitMode=x64compatible
MinVersion=10.0

; ── Privileges ──────────────────────────────────────────────────────────────
; "lowest" means no UAC prompt if user has write access to their own AppData.
; Change to "admin" if you need to write to HKLM or Program Files with UAC.
PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog

; ── Upgrade behaviour ───────────────────────────────────────────────────────
; Automatically closes a running copy of the app before upgrading
CloseApplications=yes
RestartApplications=no
UninstallDisplayName={#AppName}
UninstallDisplayIcon={app}\{#AppExeName}
CreateUninstallRegKey=yes

; ── Misc ────────────────────────────────────────────────────────────────────
DisableDirPage=no
DisableReadyPage=no
ShowLanguageDialog=no
LanguageDetectionMethod=none

; ── Source files ─────────────────────────────────────────────────────────────
[Files]
; Copy everything from the PyInstaller output folder into the install directory
; The {#AppDataDir} folder is what PyInstaller creates under dist\
Source: "dist\{#AppDataDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

; ── Shortcuts ────────────────────────────────────────────────────────────────
[Icons]
; Start Menu shortcut
Name: "{group}\{#AppName}";       Filename: "{app}\{#AppExeName}"; WorkingDir: "{app}"; Comment: "Launch {#AppName}"

; Desktop shortcut (user can untick this in the installer wizard)
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}"; WorkingDir: "{app}"; Tasks: desktopicon

; Uninstall entry in Start Menu
Name: "{group}\Uninstall {#AppName}"; Filename: "{uninstallexe}"

; ── Optional tasks shown on the installer's "Select Additional Tasks" page ──
[Tasks]
Name: "desktopicon"; Description: "Create a &desktop shortcut"; GroupDescription: "Additional shortcuts:"; Flags: unchecked

; ── Registry ─────────────────────────────────────────────────────────────────
[Registry]
; Register in Windows "Apps & features" / Add/Remove Programs
Root: HKCU; Subkey: "Software\{#AppPublisher}\{#AppName}"; ValueType: string; ValueName: "InstallPath"; ValueData: "{app}"; Flags: uninsdeletekey
Root: HKCU; Subkey: "Software\{#AppPublisher}\{#AppName}"; ValueType: string; ValueName: "Version";     ValueData: "{#AppVersion}"

; ── Run after install ────────────────────────────────────────────────────────
[Run]
; Offer to launch the app when installation finishes
Filename: "{app}\{#AppExeName}"; Description: "Launch {#AppName} now"; Flags: nowait postinstall skipifsilent

; ── Uninstall cleanup ────────────────────────────────────────────────────────
[UninstallDelete]
; Remove any files the app creates at runtime (logs, db, config cache)
; Add or remove lines here to match what your app writes to its install folder
Type: filesandordirs; Name: "{app}\logs"
Type: filesandordirs; Name: "{app}\*.db"
Type: filesandordirs; Name: "{app}\.env"

; ── Installer look & feel (messages) ─────────────────────────────────────────
[Messages]
BeveledLabel={#AppName} {#AppVersion}
WelcomeLabel1=Welcome to the {#AppName} Setup Wizard
WelcomeLabel2=This will install {#AppName} {#AppVersion} on your computer.%n%nClick Next to continue.
FinishedHeadingLabel=Completing {#AppName} Setup
FinishedLabel={#AppName} has been installed successfully.%n%nClick Finish to close this wizard.
