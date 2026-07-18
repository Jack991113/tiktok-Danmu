# TikTok 弹幕打印 v1.0.0

首个 Windows 可执行版本，面向本地单机直播弹幕打印。

## 主要功能

- 支持 TikTok 直播间直连和可见 Chrome 浏览器会话抓取。
- 纯数字弹幕创建永久编号，数字或数字英文内容可自动入库并按时间顺序打印。
- 支持打印失败重试、不确定任务人工确认补打与本地数据持久化。
- 热敏纸宽高按毫米实寸输出，打印驱动不接受目标纸张时会明确报错。
- 单打印机严格串行发送，高并发弹幕依据入库时间 FIFO 处理。

## 下载说明

- `TikTokDanmuPrinter.exe`：可直接运行的 Windows 64 位程序。
- `TikTokDanmuPrinter-v1.0.0-Windows-x64.zip`：包含 EXE、README 和配置说明。
- 浏览器会话模式需要电脑已安装 Google Chrome。
- 热敏打印需要先在 Windows 安装对应打印机驱动。
- 本版本未使用商业代码签名证书，首次运行可能触发 SmartScreen 提示。
