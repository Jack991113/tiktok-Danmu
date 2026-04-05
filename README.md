# TikTok Danmu Public

## 中文说明

这是一个独立的公开版 TikTok 弹幕打印项目。

这个版本专门用于公开到 GitHub，已经主动移除了以下内容：

- 管理员客户端
- 授权服务器
- 客户授权系统
- 远程永久编号同步
- 云数据中心
- 服务器中转模式

这个公开版是本地直连版本：

- 不需要授权码
- 不需要管理员权限
- 不依赖你的现有后端
- 用户可自行填写 `Sign API Base` 和 `Sign API Key`

### 包含文件

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

### 公开版行为

- 无授权激活
- 无管理员按钮
- 无客户前端
- 无云数据中心入口
- 无固定后端地址
- 只允许本地监听
- 用户本地自行配置 API 和代理

### 快速开始

1. 安装 Python 3.10
2. 安装依赖：

```powershell
py -3.10 -m pip install -r requirements.txt -r requirements-win.txt
```

3. 本地运行：

```powershell
py -3.10 app.py
```

4. 或直接构建 Windows 可执行文件：

```powershell
powershell -ExecutionPolicy Bypass -File .\build_public_bundle.ps1
```

### 运行配置

公开版配置文件位置：

- `%LOCALAPPDATA%\SenNails\app_settings_public.json`

用户主要需要填写：

- TikTok 直播间地址
- `Sign API Base`
- `Sign API Key`
- 代理及代理路由模式
- 打印机和纸张尺寸

详细字段说明见 `CONFIG_GUIDE.txt`。

### 默认 Sign API Base

如果你使用 Euler Stream，默认值是：

```text
https://tiktok.eulerstream.com
```

用户仍然需要自行提供有效的 `Sign API Key`。

### 打包输出

打包脚本会生成：

- `dist\tiktok_danmu_public.exe`
- `release\tiktok_danmu_public_bundle_YYYYMMDD\`
- `release\tiktok_danmu_public_bundle_YYYYMMDD.zip`

### 说明

- 这个仓库与当前生产/管理版本完全独立
- 这里的修改不会影响你现在在用的版本

---

## English

This is an independent public build of the TikTok comment printing tool.

It is prepared for GitHub publishing and intentionally excludes:

- admin client
- licensing server
- customer authorization system
- remote permanent ID sync
- cloud admin center
- server relay mode

This public build is local-only:

- no license code required
- no admin permission required
- no dependency on your current backend
- users can enter their own `Sign API Base` and `Sign API Key`

### Included Files

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

### Public Build Behavior

- no license activation
- no admin button
- no customer portal
- no cloud center entry
- no fixed backend server
- local listen mode only
- users configure API and proxy locally

### Quick Start

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

### Runtime Configuration

The public build stores settings in:

- `%LOCALAPPDATA%\SenNails\app_settings_public.json`

Main user inputs:

- TikTok live URL
- `Sign API Base`
- `Sign API Key`
- proxy and proxy route mode
- printer and paper size settings

See `CONFIG_GUIDE.txt` for detailed field descriptions.

### Default Sign API Base

If you are using Euler Stream, the default base is:

```text
https://tiktok.eulerstream.com
```

The user must provide their own valid `Sign API Key`.

### Build Output

The build script creates:

- `dist\tiktok_danmu_public.exe`
- `release\tiktok_danmu_public_bundle_YYYYMMDD\`
- `release\tiktok_danmu_public_bundle_YYYYMMDD.zip`

### Notes

- This repository is fully separated from the production/admin version
- Changes here do not affect your current managed project
