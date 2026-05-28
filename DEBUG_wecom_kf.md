# 微信客服消息无响应 — 调试记录

**日期：** 2026-05-29 (更新)
**状态：** 已修复 6 个 bug，测试通过，待实际验证

## 问题描述

企业微信的微信客服发来消息后，AstrBot 可以收到并处理（LLM 生成了回复），但回复没有发送给用户。

## 已修复的 Bug

### Bug 1: 闭包捕获空字符串（`_make_kf_sender`）

**文件：** `astrbot/core/platform/sources/wecom_ai_bot/wecomai_adapter.py`

**问题：** Python 字符串不可变，闭包捕获的是 `_last_open_kfid` 当时的**值**（空字符串），后续即使 `self._last_open_kfid` 被更新，闭包里读到的仍是旧值。

**修复：** 改为捕获 `adapter = self`，在闭包内动态读取。

### Bug 2: `comp.type` 类型比较失败（根本原因 ✅）

**文件：** `astrbot/core/platform/sources/wecom_ai_bot/wecomai_adapter.py`
**方法：** `_send_kf_message`

**问题：** `ComponentType` 是 `str` Enum，但 `str(ComponentType.Plain)` 返回 `"ComponentType.Plain"`（包含类名），而非 `"Plain"`。代码用小写 `"plain"` 比较，永远不匹配。**所有消息组件都被 `continue` 跳过，消息没发出，也没有任何日志。**

**修复：** 使用 `isinstance(comp, Plain)` 和 `isinstance(comp, Image)` 替代字符串比较。

### Bug 3: sync_msg 缺少 cursor / 消息重复处理

**文件：** `astrbot/core/platform/sources/wecom_ai_bot/wecomai_adapter.py`

**问题：** `kf/sync_msg` 没传 `cursor` 参数，每次从头拉取重复消息。`_sync_kf_messages` 被 `kf_msg_or_event` 事件触发时，没有并发保护，可能多次同时拉取。

**修复：**
- 添加 `self._kf_msg_cursor` 字段，从 `next_cursor` 响应更新
- 添加 `self._kf_sync_lock` 防止并发拉取
- 支持 `has_more` 翻页，递归拉取剩余消息

### Bug 4: 消息去重缺失

**问题：** 同一条消息被重复拉取后会重复处理，导致 LLM 多次回复。

**修复：** 添加 `self._kf_processed_msgids` 集合，按 `msgid` 去重，限制集合大小防止内存泄漏。

### Bug 5: `_send_kf_message` 无 API 响应日志

**问题：** KF API 调用后不记录响应。成功时无日志，失败时也无日志，无法判断消息是否真正发送成功。

**修复：** 添加详细的 API 响应日志，包含 `errcode`、`errmsg`、`touser`、`msgtype`。跳过空 Plain 组件和缺少 media_id 的 Image 组件。

### Bug 6: KF 消息走 stream 队列（无人消费）

**问题：** `send()` 和 `send_streaming()` 将消息推入 `back_queue`（stream 队列），但 KF 消息不使用 stream 机制（没有 WeChat 服务器来 poll），导致队列堆积无人消费。

**修复：** 在 `send()` 和 `send_streaming()` 中，当 `_kf_sender` 存在时，直接通过 KF API 发送，跳过 stream 队列。`send_streaming()` 累积所有文本后一次性发送。

### 附带修复: miniprogram 消息支持

**问题：** `_process_kf_message` 允许 `"miniprogram"` 类型通过检查，但没有处理逻辑，消息被静默丢弃。

**修复：** 将 miniprogram 消息转为 `[title]` 文本形式。

## 修改的文件

| 文件 | 改动内容 |
|------|---------|
| `WXBizJsonMsgCrypt.py` | receiveid 不匹配时允许通过（兼容微信客服） |
| `wecomai_api.py` | 解密后支持 XML 格式（微信客服回调解密后是 XML 非 JSON） |
| `wecomai_event.py` | KF 消息直接走 KF API，跳过 stream 队列；send_streaming 累积后一次性发送 |
| `wecomai_adapter.py` | 主要修改：isinstance 替代字符串比较、API 响应日志、消息去重、并发锁、cursor 翻页 |

## 测试结果

```
[PASS] _send_kf_message: API response logging works
[PASS] _send_kf_message: error responses are logged
[PASS] _send_kf_message: empty Plain components are skipped
[PASS] _process_kf_message: duplicate messages are skipped
[PASS] _process_kf_message: miniprogram messages are handled
[PASS] send(): KF messages use kf_sender directly, skip stream queue
Results: 6 passed, 0 failed
```

## 当前状态

- AstrBot 正在运行（PID 1858346，端口 6185）
- 配置：`wecom_ai_bot`，webhook 模式，corpid/corpsecret 已配置
- Webhook 地址：`https://oc4k.aixifs.com/api/platform/webhook/c10c9467452b4bec`
- 日志：`tail -f unattended.log`

## 待验证

1. 用户从微信客服发一条消息
2. 检查日志中是否出现：
   - `拉取到 N 条微信客服消息`
   - `微信客服消息发送成功: touser=xxx msgtype=text`
3. 微信客服端是否能收到回复
