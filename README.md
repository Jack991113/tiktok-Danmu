# TikTok Danmu Public

This is an independent public build of the TikTok comment printing tool.

It is prepared for GitHub publishing and intentionally excludes:

- admin client
- licensing server
- customer authorization system
- remote permanent ID sync
- cloud admin center
- server relay mode

This public build is local-only and does not require any license or admin permission.
Users can enter their own `Sign API Base` and `Sign API Key` directly in the app.

## Included Files

- `app.py`
- `db.py`
- `printer_utils.py`
- `tiktok_live_listener.py`
- `license_client.py`
- `security_utils.py`
- `requirements.txt`
- `requirements-win.txt`
- `build_windows.ps1`
- `build_public_bundle.ps1`
- `CONFIG_GUIDE.txt`

## Public Build Behavior

- no license activation
- no admin button
- no customer portal
- no cloud center entry
- no forced backend server
- local listen mode only
- users configure their own API and optional proxy locally

## Quick Start

1. Install Python 3.10.
2. Install dependencies:

```powershell
py -3.10 -m pip install -r requirements.txt -r requirements-win.txt
```

3. Run locally:

```powershell
py -3.10 app.py
```

4. Or build the Windows executable:

```powershell
powershell -ExecutionPolicy Bypass -File .\build_public_bundle.ps1
```

## Runtime Configuration

This public build stores settings in:

- `%LOCALAPPDATA%\SenNails\app_settings_public.json`

Main user inputs:

- TikTok live URL
- Sign API Base
- Sign API Key
- proxy and proxy route mode if needed
- printer and paper size settings

See `CONFIG_GUIDE.txt` for the exact fields.

## Default Sign API Base

If you are using Euler Stream, the default base is:

```text
https://tiktok.eulerstream.com
```

The user must provide their own valid `Sign API Key`.

## Build Output

The public bundle script creates:

- `dist\tiktok_danmu_public.exe`
- `release\tiktok_danmu_public_bundle_YYYYMMDD\`
- `release\tiktok_danmu_public_bundle_YYYYMMDD.zip`

## Notes

- This repository is intentionally separated from the production/admin project.
- Changes here do not affect the current managed version unless you deploy them manually somewhere else.
