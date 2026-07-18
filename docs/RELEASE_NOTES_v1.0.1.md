# TikTok 弹幕打印 v1.0.1

本次为 Windows 补丁版本，重点修复监听模式区分和登录凭据安全。

## 更新内容

- 新增明确的 `API 接口`、`本机直连`和`浏览器会话`三种监听模式。
- 仅 `API 接口`模式使用自定义 Sign API Base/Key。
- 本机直连和浏览器模式忽略已保存的自定义 Key，避免配置串用。
- 修复关闭 Sign API Key 后，连接失败时又自动启用 Key 的问题。
- 浏览器会话不再向签名服务传递 `sessionid` 等登录 Cookie。
- Windows 上使用 DPAPI 保护本地设置中的 Sign API Key。
- 监听运行期间锁定模式与 API 配置，避免重连时切换路由。

## 使用说明

- `本机直连`无需填写自定义 Key，但 TikTokLive 底层仍使用默认公共签名服务。
- `浏览器会话`需要电脑已安装 Google Chrome。
- 热敏打印需要先安装对应的 Windows 打印机驱动。
- EXE 未使用商业代码签名证书，首次运行可能触发 SmartScreen 提示。
