# tiktok-Danmu

TikTok 弹幕打印公开版  
Public TikTok comment-printing build for GitHub

---

## 中文

### 项目简介

这是一个独立的 TikTok 弹幕打印公开版仓库，用于公开发布源码和本地使用。

### Windows EXE 下载

请在 [GitHub Releases](https://github.com/Jack991113/tiktok-Danmu/releases/latest) 下载：

- `TikTokDanmuPrinter.exe`：Windows 64 位单文件程序
- `TikTokDanmuPrinter-v1.0.1-Windows-x64.zip`：程序与使用说明完整包
- `SHA256SUMS.txt`：文件完整性校验值

当前 EXE 未使用商业代码签名证书，Windows SmartScreen 可能在首次运行时显示提示。

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
- 可见 Chrome 浏览器会话与持久登录状态

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

监听模式：

- `API 接口`：使用填写的 `Sign API Base/Key`。
- `本机直连`：忽略已保存的自定义 Key，直接连接 TikTok；TikTokLive 底层仍需要默认公共签名服务。
- `浏览器会话`：打开真实 Chrome 直播间，复用 User-Agent 和非敏感 Cookie，不会将 `sessionid` 等登录凭据交给签名服务。

浏览器会话模式需要电脑已安装 Google Chrome。浏览器资料保存在本机 `%LOCALAPPDATA%\SenNails\tiktok_browser_profile`，不要将该目录上传或分享。

### 热敏纸实寸打印

- `W×H` 表示最终热敏纸物理宽高，例如 `40×30mm` 不会自动交换方向。
- 画布位置和尺寸按毫米一比一映射，字号按 `pt` 输出；打印机驱动未接受指定纸张时任务会失败，不会静默缩放到其他纸张。
- 打印机不可打印边缘仍受硬件限制。请把元素放在设计器的边距参考线内，并先用“测试打印”和“打印校准向导”实测。

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
- visible persistent Chrome sessions for live-room listening

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

Listening modes:

- `API 接口`: uses the configured `Sign API Base/Key`.
- `本机直连`: ignores a saved custom key and connects directly to TikTok; TikTokLive still needs its default public signing service.
- `浏览器会话`: opens the real room in Chrome and reuses its user agent and non-sensitive cookies without forwarding login credentials such as `sessionid` to the signing service.

Browser-session mode requires Google Chrome. Its profile stays in `%LOCALAPPDATA%\SenNails\tiktok_browser_profile` and must not be uploaded or shared.

### Physical-size thermal printing

- `W x H` is the final physical label size; `40 x 30mm` is not silently rotated.
- Canvas geometry maps to millimeters at 1:1 scale and font sizes are printed in points. A driver that rejects the requested media size causes a failed job instead of hidden scaling.
- Hardware non-printable edges still apply. Keep elements inside the designer margin guide and verify each printer with test printing and the calibration wizard.

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
