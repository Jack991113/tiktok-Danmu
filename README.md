# tiktok-Danmu

TikTok 弹幕打印公开版  
Public TikTok comment-printing build for GitHub

---

## 中文

### 项目简介

这是一个独立的 TikTok 弹幕打印公开版仓库，用于公开发布源码和本地使用。

这个版本已经主动移除所有后台管理和授权体系，适合：

- 本地单机使用
- 自己填写 Sign API
- 公开发布到 GitHub
- 与你当前正式商用版本完全隔离

### 这个版本保留了什么

- TikTok 直播间监听
- 弹幕打印
- 模板编辑
- 画布设计
- 打印机与纸张尺寸配置
- 本地数据库与本地设置
- 本地代理配置

### 这个版本移除了什么

- 管理员客户端
- 授权服务器
- 客户授权系统
- 云数据中心
- 服务器中转模式
- 远程永久编号同步
- 后台管理入口

### 适用场景

- 自己本地使用，不接后端
- 开源展示
- 让用户自行填写 `Sign API Base` 和 `Sign API Key`

### 快速开始

1. 安装 Python 3.10
2. 安装依赖

```powershell
py -3.10 -m pip install -r requirements.txt -r requirements-win.txt
```

3. 运行程序

```powershell
py -3.10 app.py
```

4. 构建公开版 exe

```powershell
powershell -ExecutionPolicy Bypass -File .\build_public_bundle.ps1
```

### 默认配置

- 默认 `Sign API Base`

```text
https://tiktok.eulerstream.com
```

- 设置文件位置

```text
%LOCALAPPDATA%\SenNails\app_settings_public.json
```

### 目录说明

- `app.py`：主程序
- `db.py`：本地数据库逻辑
- `printer_utils.py`：打印相关功能
- `tiktok_live_listener.py`：直播监听辅助逻辑
- `security_utils.py`：本地安全工具
- `build_public_bundle.ps1`：公开版一键打包脚本
- `CONFIG_GUIDE.txt`：配置说明，中英双语
- `docs/GITHUB_RELEASE_TEMPLATE.md`：GitHub Release 模板
- `docs/SCREENSHOT_GUIDE.md`：仓库截图整理说明

### 输出内容

打包后会生成：

- `dist\tiktok_danmu_public.exe`
- `release\tiktok_danmu_public_bundle_YYYYMMDD\`
- `release\tiktok_danmu_public_bundle_YYYYMMDD.zip`

### 说明

- 这是公开版，不会影响你当前正式项目
- 这里的改动不会回写到你现在的商用管理版
- 这个仓库适合公开展示和源码分发

---

## English

### Overview

This is an independent public repository for the TikTok comment-printing tool.

This version is intended for source-code publishing and local-only usage.
All backend management and licensing components have been removed.

It is suitable for:

- local standalone usage
- user-managed Sign API configuration
- GitHub publishing
- full separation from the current production/commercial build

### What this build keeps

- TikTok live-room listening
- comment printing
- template editor
- canvas designer
- printer and paper-size configuration
- local database and local settings
- local proxy configuration

### What this build removes

- admin client
- licensing server
- customer authorization system
- cloud admin center
- server relay mode
- remote permanent-ID sync
- backend management entry points

### Typical use case

- local usage without backend services
- open-source publishing
- users enter their own `Sign API Base` and `Sign API Key`

### Quick Start

1. Install Python 3.10
2. Install dependencies

```powershell
py -3.10 -m pip install -r requirements.txt -r requirements-win.txt
```

3. Run the app

```powershell
py -3.10 app.py
```

4. Build the public executable

```powershell
powershell -ExecutionPolicy Bypass -File .\build_public_bundle.ps1
```

### Default Configuration

- Default `Sign API Base`

```text
https://tiktok.eulerstream.com
```

- Settings file path

```text
%LOCALAPPDATA%\SenNails\app_settings_public.json
```

### Project Files

- `app.py`: main application
- `db.py`: local database logic
- `printer_utils.py`: printer-related helpers
- `tiktok_live_listener.py`: live listener helpers
- `security_utils.py`: local security utilities
- `build_public_bundle.ps1`: one-click public build script
- `CONFIG_GUIDE.txt`: bilingual configuration guide
- `docs/GITHUB_RELEASE_TEMPLATE.md`: GitHub release template
- `docs/SCREENSHOT_GUIDE.md`: screenshot guidance for the repo page

### Build Output

The build process creates:

- `dist\tiktok_danmu_public.exe`
- `release\tiktok_danmu_public_bundle_YYYYMMDD\`
- `release\tiktok_danmu_public_bundle_YYYYMMDD.zip`

### Notes

- This repository is the public build only
- It does not affect the current managed production version
- It is intended for source distribution and public presentation
