"""企业微信智能机器人平台适配器
基于企业微信智能机器人 API 的消息平台适配器，支持 HTTP 回调与长连接
参考webchat_adapter.py的队列机制，实现异步消息处理和流式响应
"""

import asyncio
import base64
import hashlib
import os
import tempfile
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp

from astrbot.api import logger
from astrbot.api.event import MessageChain
from astrbot.api.message_components import At, Image, Plain, Record
from astrbot.api.platform import (
    AstrBotMessage,
    MessageMember,
    MessageType,
    Platform,
    PlatformMetadata,
)
from astrbot.core.platform.astr_message_event import MessageSesion
from astrbot.core.utils.webhook_utils import log_webhook_info

from ...register import register_platform_adapter
from .wecomai_api import (
    WecomAIBotAPIClient,
    WecomAIBotMessageParser,
    WecomAIBotStreamMessageBuilder,
)
from .wecomai_event import WecomAIBotMessageEvent
from .wecomai_long_connection import WecomAIBotLongConnectionClient
from .wecomai_queue_mgr import WecomAIQueueMgr
from .wecomai_server import WecomAIBotServer
from .wecomai_utils import (
    WecomAIBotConstants,
    format_session_id,
    generate_random_string,
    process_encrypted_image,
)
from .wecomai_webhook import WecomAIBotWebhookClient, WecomAIBotWebhookError


class WecomAIQueueListener:
    """企业微信智能机器人队列监听器，参考webchat的QueueListener设计"""

    def __init__(
        self,
        queue_mgr: WecomAIQueueMgr,
        callback: Callable[[dict], Awaitable[None]],
    ) -> None:
        self.queue_mgr = queue_mgr
        self.callback = callback

    async def run(self) -> None:
        """注册监听回调并定期清理过期响应。"""
        self.queue_mgr.set_listener(self.callback)
        while True:
            self.queue_mgr.cleanup_expired_responses()
            await asyncio.sleep(1)


@register_platform_adapter(
    "wecom_ai_bot",
    "企业微信智能机器人适配器，支持 HTTP 回调接收消息",
)
class WecomAIBotAdapter(Platform):
    """企业微信智能机器人适配器"""

    def __init__(
        self,
        platform_config: dict,
        platform_settings: dict,
        event_queue: asyncio.Queue,
    ) -> None:
        super().__init__(platform_config, event_queue)
        self.settings = platform_settings

        # 初始化配置参数
        self.connection_mode = self.config.get(
            "wecom_ai_bot_connection_mode", "webhook"
        )
        self.token = self.config.get("token", self.config.get("wecomaibot_token", ""))
        self.encoding_aes_key = self.config.get(
            "encoding_aes_key", self.config.get("wecomaibot_encoding_aes_key", "")
        )
        self.port = int(self.config["port"])
        self.host = self.config.get("callback_server_host", "0.0.0.0")
        self.bot_name = self.config.get("wecom_ai_bot_name", "")
        self.initial_respond_text = self.config.get(
            "wecomaibot_init_respond_text",
            "",
        )
        self.friend_message_welcome_text = self.config.get(
            "wecomaibot_friend_message_welcome_text",
            "",
        )
        self.unified_webhook_mode = self.config.get("unified_webhook_mode", False)
        self.msg_push_webhook_url = self.config.get("msg_push_webhook_url", "").strip()
        self.only_use_webhook_url_to_send = bool(
            self.config.get("only_use_webhook_url_to_send", False),
        )
        self.long_connection_bot_id = self.config.get(
            "wecomaibot_ws_bot_id", self.config.get("long_connection_bot_id", "")
        )
        self.long_connection_secret = self.config.get(
            "wecomaibot_ws_secret", self.config.get("long_connection_secret", "")
        )
        self.long_connection_ws_url = self.config.get(
            "wecomaibot_ws_url",
            "wss://openws.work.weixin.qq.com",
        )
        self.long_connection_heartbeat_interval = int(
            self.config.get("wecomaibot_heartbeat_interval", 30),
        )

        # 平台元数据
        self.metadata = PlatformMetadata(
            name="wecom_ai_bot",
            description="企业微信智能机器人适配器，支持 HTTP 回调和长连接模式",
            id=self.config.get("id", "wecom_ai_bot"),
            support_proactive_message=bool(self.msg_push_webhook_url),
        )

        self.api_client: WecomAIBotAPIClient | None = None
        self.server: WecomAIBotServer | None = None
        self.long_connection_client: WecomAIBotLongConnectionClient | None = None

        if self.connection_mode == "long_connection":
            if not self.long_connection_bot_id or not self.long_connection_secret:
                logger.warning(
                    "企业微信智能机器人长连接模式缺少 BotID 或 Secret，连接可能失败"
                )
            self.long_connection_client = WecomAIBotLongConnectionClient(
                bot_id=self.long_connection_bot_id,
                secret=self.long_connection_secret,
                ws_url=self.long_connection_ws_url,
                heartbeat_interval=self.long_connection_heartbeat_interval,
                message_handler=self._process_long_connection_payload,
            )
        else:
            self.api_client = WecomAIBotAPIClient(self.token, self.encoding_aes_key)
            self.server = WecomAIBotServer(
                host=self.host,
                port=self.port,
                api_client=self.api_client,
                message_handler=self._process_message,
            )

        # 事件循环和关闭信号
        self.shutdown_event = asyncio.Event()

        # 队列管理器
        self.queue_mgr = WecomAIQueueMgr()

        # 队列监听器
        self.queue_listener = WecomAIQueueListener(
            self.queue_mgr,
            self._handle_queued_message,
        )
        self._stream_plain_cache: dict[str, str] = {}

        self.webhook_client: WecomAIBotWebhookClient | None = None
        if self.msg_push_webhook_url:
            try:
                self.webhook_client = WecomAIBotWebhookClient(
                    self.msg_push_webhook_url,
                )
            except WecomAIBotWebhookError as e:
                logger.error("企业微信消息推送 webhook 配置无效: %s", e)

        # 微信客服最新会话信息
        self._last_open_kfid: str = ""
        self._last_external_userid: str = ""
        self._kf_msg_cursor: str = ""  # kf/sync_msg 翻页游标
        self._kf_processed_msgids: set[str] = set()  # 已处理消息 ID，用于去重
        self._kf_sync_lock = asyncio.Lock()  # 防止并发拉取消息
        self._kf_last_sync_time: int = int(time.time())  # 上次同步时间戳，初始化为当前时间

    async def _handle_queued_message(self, data: dict) -> None:
        """处理队列中的消息，类似webchat的callback"""
        try:
            abm = await self.convert_message(data)
            await self.handle_msg(abm)
        except Exception as e:
            logger.error(f"处理队列消息时发生异常: {e}")

    async def _process_message(
        self,
        message_data: dict[str, Any],
        callback_params: dict[str, str],
    ) -> str | None:
        """处理接收到的消息

        Args:
            message_data: 解密后的消息数据
            callback_params: 回调参数 (nonce, timestamp)

        Returns:
            加密后的响应消息，无需响应时返回 None

        """
        if not self.api_client:
            logger.error("Webhook 消息处理失败: API 客户端未初始化")
            return None
        msgtype = message_data.get("msgtype")
        if not msgtype:
            logger.warning(f"消息类型未知，忽略: {message_data}")
            return None
        session_id = self._extract_session_id(message_data)
        if msgtype in ("text", "image", "mixed"):
            # user sent a text / image / mixed message
            try:
                # create a brand-new unique stream_id for this message session
                stream_id = f"{session_id}_{generate_random_string(10)}"
                await self._enqueue_message(
                    message_data,
                    callback_params,
                    stream_id,
                    session_id,
                )
                self.queue_mgr.set_pending_response(stream_id, callback_params)

                if self.only_use_webhook_url_to_send and self.webhook_client:
                    return None
                if self.initial_respond_text:
                    resp = WecomAIBotStreamMessageBuilder.make_text_stream(
                        stream_id,
                        self.initial_respond_text,
                        False,
                    )
                    return await self.api_client.encrypt_message(
                        resp,
                        callback_params["nonce"],
                        callback_params["timestamp"],
                    )
            except Exception as e:
                logger.error("处理消息时发生异常: %s", e)
                return None
        elif msgtype == "stream":
            # wechat server is requesting for updates of a stream
            stream_id = message_data["stream"]["id"]
            if not self.queue_mgr.has_back_queue(stream_id):
                self._stream_plain_cache.pop(stream_id, None)
                if self.queue_mgr.is_stream_finished(stream_id):
                    logger.debug(
                        f"Stream already finished, returning end message: {stream_id}"
                    )
                else:
                    logger.warning(f"Cannot find back queue for stream_id: {stream_id}")

                # 返回结束标志，告诉微信服务器流已结束
                end_message = WecomAIBotStreamMessageBuilder.make_text_stream(
                    stream_id,
                    "",
                    True,
                )
                resp = await self.api_client.encrypt_message(
                    end_message,
                    callback_params["nonce"],
                    callback_params["timestamp"],
                )
                return resp
            queue = self.queue_mgr.get_or_create_back_queue(stream_id)
            if queue.empty():
                logger.debug(
                    f"No new messages in back queue for stream_id: {stream_id}",
                )
                return None

            # aggregate all delta chains in the back queue
            cached_plain_content = self._stream_plain_cache.get(stream_id, "")
            latest_plain_content = cached_plain_content
            image_base64 = []
            finish = False
            while not queue.empty():
                msg = await queue.get()
                if msg["type"] == "plain":
                    plain_data = msg.get("data") or ""
                    if msg.get("streaming", False):
                        # streaming plain payload is already cumulative
                        cached_plain_content = plain_data
                    else:
                        # segmented non-stream send() pushes plain chunks, needs append
                        cached_plain_content += plain_data
                    latest_plain_content = cached_plain_content
                elif msg["type"] == "image":
                    image_base64.append(msg["image_data"])
                elif msg["type"] == "break":
                    continue
                elif msg["type"] in {"end", "complete"}:
                    # stream end
                    finish = True
                    self.queue_mgr.remove_queues(stream_id, mark_finished=True)
                    self._stream_plain_cache.pop(stream_id, None)
                    break

            logger.debug(
                f"Aggregated content: {latest_plain_content}, image: {len(image_base64)}, finish: {finish}",
            )
            if not finish:
                self._stream_plain_cache[stream_id] = cached_plain_content
            if finish and not latest_plain_content and not image_base64:
                end_message = WecomAIBotStreamMessageBuilder.make_text_stream(
                    stream_id,
                    "",
                    True,
                )
                return await self.api_client.encrypt_message(
                    end_message,
                    callback_params["nonce"],
                    callback_params["timestamp"],
                )
            if latest_plain_content or image_base64:
                msg_items = []
                if finish and image_base64:
                    for img_b64 in image_base64:
                        # get md5 of image
                        img_data = base64.b64decode(img_b64)
                        img_md5 = hashlib.md5(img_data).hexdigest()
                        msg_items.append(
                            {
                                "msgtype": WecomAIBotConstants.MSG_TYPE_IMAGE,
                                "image": {"base64": img_b64, "md5": img_md5},
                            },
                        )
                    image_base64 = []

                plain_message = WecomAIBotStreamMessageBuilder.make_mixed_stream(
                    stream_id,
                    latest_plain_content,
                    msg_items,
                    finish,
                )
                encrypted_message = await self.api_client.encrypt_message(
                    plain_message,
                    callback_params["nonce"],
                    callback_params["timestamp"],
                )
                if encrypted_message:
                    logger.debug(
                        f"Stream message sent successfully, stream_id: {stream_id}",
                    )
                else:
                    logger.error("消息加密失败")
                return encrypted_message
            return None
        elif msgtype == "event":
            event = message_data.get("event")
            if event == "kf_msg_or_event":
                # 微信客服事件通知：有新的客服消息需要拉取
                logger.info("收到微信客服消息事件通知，开始拉取消息...")
                asyncio.create_task(self._sync_kf_messages())
                return None
            if event == "enter_chat" and self.friend_message_welcome_text:
                # 用户进入会话，发送欢迎消息
                try:
                    resp = WecomAIBotStreamMessageBuilder.make_text(
                        self.friend_message_welcome_text,
                    )
                    return await self.api_client.encrypt_message(
                        resp,
                        callback_params["nonce"],
                        callback_params["timestamp"],
                    )
                except Exception as e:
                    logger.error("处理欢迎消息时发生异常: %s", e)
                    return None

    async def _process_long_connection_payload(
        self,
        payload: dict[str, Any],
    ) -> None:
        """处理长连接回调消息。"""
        cmd = payload.get("cmd")
        headers = payload.get("headers") or {}
        body = payload.get("body") or {}
        req_id = headers.get("req_id")
        if not isinstance(body, dict):
            return

        if cmd == "aibot_msg_callback":
            session_id = self._extract_session_id(body)
            stream_id = f"{session_id}_{generate_random_string(10)}"
            await self._enqueue_message(
                body, {"req_id": req_id or ""}, stream_id, session_id
            )
            self.queue_mgr.set_pending_response(
                stream_id,
                {
                    "req_id": req_id or "",
                    "connection_mode": "long_connection",
                },
            )

            if self.initial_respond_text and req_id:
                await self._send_long_connection_respond_msg(
                    req_id=req_id,
                    body={
                        "msgtype": "stream",
                        "stream": {
                            "id": stream_id,
                            "finish": False,
                            "content": self.initial_respond_text,
                        },
                    },
                )
            return

        if cmd == "aibot_event_callback":
            event = body.get("event") or {}
            event_type = event.get("eventtype")
            if (
                event_type == "enter_chat"
                and self.friend_message_welcome_text
                and req_id
            ):
                await self._send_long_connection_respond_welcome(req_id)
            elif event_type == "disconnected_event":
                logger.warning(
                    "[WecomAI][LongConn] 收到 disconnected_event，旧连接将被关闭"
                )

    async def _send_long_connection_respond_welcome(self, req_id: str) -> bool:
        client = self.long_connection_client
        if not client:
            return False
        return await client.send_command(
            cmd="aibot_respond_welcome_msg",
            req_id=req_id,
            body={
                "msgtype": "text",
                "text": {
                    "content": self.friend_message_welcome_text,
                },
            },
        )

    async def _send_long_connection_respond_msg(
        self,
        req_id: str,
        body: dict[str, Any],
    ) -> bool:
        client = self.long_connection_client
        if not client:
            return False
        return await client.send_command(
            cmd="aibot_respond_msg",
            req_id=req_id,
            body=body,
        )

    def _extract_session_id(self, message_data: dict[str, Any]) -> str:
        """从消息数据中提取会话ID
        群聊使用 chatid，单聊使用 userid
        """
        chattype = message_data.get("chattype", "single")
        if chattype == "group":
            chat_id = message_data.get("chatid", "default_group")
            return format_session_id("wecomai", chat_id)
        else:
            user_id = message_data.get("from", {}).get("userid", "default_user")
            return format_session_id("wecomai", user_id)

    async def _enqueue_message(
        self,
        message_data: dict[str, Any],
        callback_params: dict[str, str],
        stream_id: str,
        session_id: str,
    ) -> None:
        """将消息放入队列进行异步处理"""
        input_queue = self.queue_mgr.get_or_create_queue(stream_id)
        _ = self.queue_mgr.get_or_create_back_queue(stream_id)
        message_payload = {
            "message_data": message_data,
            "callback_params": callback_params,
            "session_id": session_id,
            "stream_id": stream_id,
        }
        await input_queue.put(message_payload)
        logger.debug(f"[WecomAI] 消息已入队: {stream_id}")

    async def convert_message(self, payload: dict) -> AstrBotMessage:
        """转换队列中的消息数据为AstrBotMessage，类似webchat的convert_message"""
        message_data = payload["message_data"]
        session_id = payload["session_id"]
        # callback_params = payload["callback_params"]  # 保留但暂时不使用

        # 解析消息内容
        msgtype = message_data.get("msgtype")
        content = ""
        image_base64 = []

        _img_url_to_process: list[tuple[str, str | None]] = []
        msg_items = []

        if msgtype == WecomAIBotConstants.MSG_TYPE_TEXT:
            content = WecomAIBotMessageParser.parse_text_message(message_data)
        elif msgtype == WecomAIBotConstants.MSG_TYPE_IMAGE:
            image_payload = message_data.get("image", {})
            image_url = image_payload.get("url", "")
            if image_url:
                _img_url_to_process.append((image_url, image_payload.get("aeskey")))
        elif msgtype == WecomAIBotConstants.MSG_TYPE_MIXED:
            # 提取混合消息中的文本内容
            msg_items = WecomAIBotMessageParser.parse_mixed_message(message_data)
            text_parts = []
            for item in msg_items or []:
                if item.get("msgtype") == WecomAIBotConstants.MSG_TYPE_TEXT:
                    text_content = item.get("text", {}).get("content", "")
                    if text_content:
                        text_parts.append(text_content)
                elif item.get("msgtype") == WecomAIBotConstants.MSG_TYPE_IMAGE:
                    image_payload = item.get("image", {})
                    image_url = image_payload.get("url", "")
                    if image_url:
                        _img_url_to_process.append(
                            (image_url, image_payload.get("aeskey"))
                        )
            content = " ".join(text_parts) if text_parts else ""
        else:
            content = f"[{msgtype}消息]"

        # 并行处理图片下载和解密
        if _img_url_to_process:
            tasks = [
                process_encrypted_image(url, aes_key or self.encoding_aes_key)
                for url, aes_key in _img_url_to_process
            ]
            results = await asyncio.gather(*tasks)
            for success, result in results:
                if success:
                    image_base64.append(result)
                else:
                    logger.error(f"处理加密图片失败: {result}")

        # 构建 AstrBotMessage
        abm = AstrBotMessage()
        abm.self_id = self.bot_name
        abm.message_str = content or "[未知消息]"
        abm.message_id = str(uuid.uuid4())
        abm.timestamp = int(time.time())
        abm.raw_message = payload

        # 发送者信息
        abm.sender = MessageMember(
            user_id=message_data.get("from", {}).get("userid", "unknown"),
            nickname=message_data.get("from", {}).get("userid", "unknown"),
        )

        # 消息类型
        abm.type = (
            MessageType.GROUP_MESSAGE
            if message_data.get("chattype") == "group"
            else MessageType.FRIEND_MESSAGE
        )
        abm.session_id = session_id

        # 消息内容
        abm.message = []

        # 处理 At
        if self.bot_name and f"@{self.bot_name}" in abm.message_str:
            abm.message_str = abm.message_str.replace(f"@{self.bot_name}", "").strip()
            abm.message.append(At(qq=self.bot_name, name=self.bot_name))
        abm.message.append(Plain(abm.message_str))
        if image_base64:
            for img_b64 in image_base64:
                abm.message.append(Image.fromBase64(img_b64))

        logger.debug(f"WecomAIAdapter: {abm.message}")
        return abm

    async def send_by_session(
        self,
        session: MessageSesion,
        message_chain: MessageChain,
    ) -> None:
        """通过微信客服 API 或 webhook 发送消息。"""
        # 尝试通过微信客服 API 发送
        logger.debug(f"[KF] send_by_session called, open_kfid={self._last_open_kfid!r}, ext_userid={self._last_external_userid!r}")
        if self._last_open_kfid and self._last_external_userid:
            try:
                await self._send_kf_message(
                    self._last_open_kfid,
                    self._last_external_userid,
                    message_chain,
                )
                await super().send_by_session(session, message_chain)
                return
            except Exception as e:
                logger.error(
                    "微信客服 API 发送失败(session=%s): %s",
                    session.session_id,
                    e,
                )

        # fallback: webhook
        if not self.webhook_client:
            logger.warning(
                "主动消息发送失败: 未配置企业微信消息推送 Webhook URL。session_id=%s",
                session.session_id,
            )
            await super().send_by_session(session, message_chain)
            return

        try:
            await self.webhook_client.send_message_chain(message_chain)
        except Exception as e:
            logger.error(
                "企业微信消息推送失败(session=%s): %s",
                session.session_id,
                e,
            )
        await super().send_by_session(session, message_chain)

    async def _send_kf_message(
        self, open_kfid: str, external_userid: str, message_chain: MessageChain
    ) -> None:
        """通过微信客服 API 发送消息"""
        corpid = self.config.get("corpid", "")
        corpsecret = self.config.get("corpsecret", "")
        if not corpid or not corpsecret:
            raise RuntimeError("corpid/corpsecret 未配置")

        async with aiohttp.ClientSession() as session:
            # 获取 access_token
            async with session.get(
                "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                params={"corpid": corpid, "corpsecret": corpsecret},
            ) as resp:
                token_data = await resp.json()
                if token_data.get("errcode", -1) != 0:
                    raise RuntimeError(
                        f"获取 access_token 失败: {token_data.get('errmsg')}"
                    )
                access_token = token_data["access_token"]

            # 遍历 message_chain 发送
            sent_any = False
            for comp in message_chain.chain:
                if isinstance(comp, Plain):
                    if not comp.text or not comp.text.strip():
                        continue
                    payload = {
                        "touser": external_userid,
                        "open_kfid": open_kfid,
                        "msgtype": "text",
                        "text": {"content": comp.text},
                    }
                elif isinstance(comp, Image):
                    media_id = getattr(comp, "media_id", None) or ""
                    if not media_id:
                        logger.warning("微信客服图片发送跳过: 缺少 media_id")
                        continue
                    payload = {
                        "touser": external_userid,
                        "open_kfid": open_kfid,
                        "msgtype": "image",
                        "image": {"media_id": media_id},
                    }
                else:
                    logger.debug("微信客服发送跳过不支持的组件类型: %s", type(comp).__name__)
                    continue

                sent_any = True
                async with session.post(
                    "https://qyapi.weixin.qq.com/cgi-bin/kf/send_msg",
                    params={"access_token": access_token},
                    json=payload,
                ) as resp:
                    result = await resp.json()
                    errcode = result.get("errcode", -1)
                    if errcode != 0:
                        logger.error(
                            "微信客服发送消息失败: errcode=%s errmsg=%s",
                            errcode,
                            result.get("errmsg"),
                        )
                    else:
                        logger.info(
                            "微信客服消息发送成功: touser=%s msgtype=%s",
                            external_userid,
                            payload.get("msgtype"),
                        )

            if not sent_any:
                logger.warning(
                    "微信客服发送消息跳过: message_chain 中无可发送的组件"
                )

    async def _download_voice_media(self, media_id: str) -> str | None:
        """下载微信客服语音媒体文件，返回本地文件路径"""
        corpid = self.config.get("corpid", "")
        corpsecret = self.config.get("corpsecret", "")
        if not corpid or not corpsecret:
            logger.warning("无法下载语音: corpid/corpsecret 未配置")
            return None

        async with aiohttp.ClientSession() as session:
            # 获取 access_token
            async with session.get(
                "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                params={"corpid": corpid, "corpsecret": corpsecret},
            ) as resp:
                token_data = await resp.json()
                if token_data.get("errcode", -1) != 0:
                    logger.warning(f"获取 access_token 失败: {token_data.get('errmsg')}")
                    return None
                access_token = token_data["access_token"]

            # 下载语音文件
            url = f"https://qyapi.weixin.qq.com/cgi-bin/media/get?access_token={access_token}&media_id={media_id}"
            async with session.get(url) as resp:
                if resp.status != 200:
                    logger.warning(f"语音下载失败: HTTP {resp.status}")
                    return None

                # 检查是否是 JSON 错误响应
                content_type = resp.content_type or ""
                if "json" in content_type:
                    err_data = await resp.json()
                    logger.warning(f"语音下载返回错误: {err_data}")
                    return None

                # 保存到临时文件
                audio_data = await resp.read()
                if not audio_data:
                    logger.warning("语音下载: 空数据")
                    return None

                tmp_dir = tempfile.gettempdir()
                tmp_path = os.path.join(tmp_dir, f"wecom_voice_{media_id}.amr")
                with open(tmp_path, "wb") as f:
                    f.write(audio_data)
                logger.info(f"语音已下载: {tmp_path} ({len(audio_data)} bytes)")
                return f"file:///{tmp_path}"

    async def _sync_kf_messages(self) -> None:
        """拉取微信客服消息并处理。"""
        if self._kf_sync_lock.locked():
            logger.debug("微信客服消息拉取正在进行中，跳过")
            return
        async with self._kf_sync_lock:
            await self._sync_kf_messages_inner()

    async def _sync_kf_messages_inner(self) -> None:
        """实际拉取微信客服消息。"""
        corpid = self.config.get("corpid", "")
        corpsecret = self.config.get("corpsecret", "")
        if not corpid or not corpsecret:
            logger.error("微信客服消息拉取失败: 未配置 corpid/corpsecret")
            return

        try:
            async with aiohttp.ClientSession() as session:
                # 1. 获取 access_token
                token_url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
                async with session.get(
                    token_url,
                    params={"corpid": corpid, "corpsecret": corpsecret},
                ) as resp:
                    token_data = await resp.json()
                    if token_data.get("errcode", -1) != 0:
                        logger.error(
                            "获取 access_token 失败: %s", token_data.get("errmsg")
                        )
                        return
                    access_token = token_data["access_token"]

                # 2. 拉取消息（使用 cursor 翻页，避免重复拉取）
                sync_url = "https://qyapi.weixin.qq.com/cgi-bin/kf/sync_msg"
                payload: dict = {"limit": 100}
                if self._kf_msg_cursor:
                    payload["cursor"] = self._kf_msg_cursor

                logger.debug(
                    "拉取微信客服消息: cursor=%r",
                    self._kf_msg_cursor or "(empty)",
                )

                async with session.post(
                    sync_url,
                    params={"access_token": access_token},
                    json=payload,
                ) as resp:
                    sync_data = await resp.json()
                    if sync_data.get("errcode", -1) != 0:
                        logger.error(
                            "同步微信客服消息失败: errcode=%s errmsg=%s",
                            sync_data.get("errcode"),
                            sync_data.get("errmsg"),
                        )
                        return

                    # 更新 cursor 以便下次从断点拉取
                    next_cursor = sync_data.get("next_cursor", "")
                    has_more = sync_data.get("has_more", False)
                    if next_cursor:
                        self._kf_msg_cursor = next_cursor
                        logger.debug("更新 cursor: %s (has_more=%s)", next_cursor, has_more)

                    msg_list = sync_data.get("msg_list", [])
                    if not msg_list:
                        logger.debug("没有新的微信客服消息")
                        return

                    # 过滤：只处理用户发的消息（排除机器人自己的回复）
                    # 且只处理上次同步之后的新消息
                    new_user_msgs = []
                    for msg in msg_list:
                        msgtype = msg.get("msgtype", "")
                        if msgtype not in ("text", "image", "miniprogram", "voice"):
                            continue
                        send_time = msg.get("send_time", 0)
                        if send_time <= self._kf_last_sync_time:
                            continue
                        new_user_msgs.append(msg)

                    logger.info(
                        "拉取到 %d 条微信客服消息，其中 %d 条新用户消息",
                        len(msg_list),
                        len(new_user_msgs),
                    )

                    if not new_user_msgs:
                        return

                    # 更新同步时间戳（使用最新消息的时间）
                    latest_time = max(m.get("send_time", 0) for m in new_user_msgs)
                    if latest_time > self._kf_last_sync_time:
                        self._kf_last_sync_time = latest_time

                    for msg in new_user_msgs:
                        await self._process_kf_message(msg)

        except Exception as e:
            logger.error(f"微信客服消息拉取异常: {e}")

    async def _process_kf_message(
        self, msg: dict
    ) -> None:
        """处理单条微信客服消息"""
        msgtype = msg.get("msgtype", "")
        msgid = msg.get("msgid", "")

        # 消息去重
        if msgid and msgid in self._kf_processed_msgids:
            logger.debug("跳过已处理的微信客服消息: %s", msgid)
            return
        if msgid:
            self._kf_processed_msgids.add(msgid)
            # 限制集合大小，防止内存泄漏
            if len(self._kf_processed_msgids) > 1000:
                # 保留最近 500 条
                self._kf_processed_msgids = set(list(self._kf_processed_msgids)[-500:])

        if msgtype not in ("text", "image", "miniprogram", "voice"):
            logger.debug("忽略微信客服消息类型: %s", msgtype)
            return

        # 构建 AstrBotMessage
        abm = AstrBotMessage()
        abm.self_id = self.bot_name or "astrbot"
        abm.message_id = msgid or str(uuid.uuid4())
        abm.timestamp = msg.get("send_time", int(time.time()))
        abm.raw_message = msg
        abm.sender = MessageMember(
            user_id=msg.get("external_userid", "unknown"),
            nickname=msg.get("external_userid", "unknown"),
        )
        abm.type = MessageType.FRIEND_MESSAGE
        session_id = format_session_id(
            "wecomai", msg.get("external_userid", "unknown")
        )
        abm.session_id = session_id
        abm.message = []

        # 记录最新会话信息
        self._last_open_kfid = msg.get("open_kfid", "")
        self._last_external_userid = msg.get("external_userid", "")

        if msgtype == "text":
            content = msg.get("text", {}).get("content", "")
            abm.message_str = content
            abm.message.append(Plain(content))
        elif msgtype == "image":
            abm.message_str = "[图片]"
            abm.message.append(Plain("[图片]"))
        elif msgtype == "miniprogram":
            # 小程序消息转为文本提示
            title = msg.get("miniprogram", {}).get("title", "小程序消息")
            abm.message_str = f"[{title}]"
            abm.message.append(Plain(f"[{title}]"))
        elif msgtype == "voice":
            voice_data = msg.get("voice", {})
            media_id = voice_data.get("media_id", "")
            if media_id:
                try:
                    audio_url = await self._download_voice_media(media_id)
                    if audio_url:
                        abm.message_str = "[语音消息]"
                        abm.message.append(Record(file=audio_url))
                    else:
                        abm.message_str = "[语音消息]"
                        abm.message.append(Plain("[语音消息]"))
                except Exception as e:
                    logger.warning(f"语音下载失败: {e}")
                    abm.message_str = "[语音消息]"
                    abm.message.append(Plain("[语音消息]"))
            else:
                abm.message_str = "[语音消息]"
                abm.message.append(Plain("[语音消息]"))

        logger.debug(f"微信客服消息: {abm.message_str}")

        # 创建消息事件，传入 KF 发送回调
        message_event = WecomAIBotMessageEvent(
            message_str=abm.message_str,
            message_obj=abm,
            platform_meta=self.meta(),
            session_id=abm.session_id,
            api_client=self.api_client,
            queue_mgr=self.queue_mgr,
            webhook_client=self.webhook_client,
            only_use_webhook_url_to_send=self.only_use_webhook_url_to_send,
            kf_sender=self._make_kf_sender(),
        )
        message_event.is_at_or_wake_command = True
        message_event.is_wake = True
        self.commit_event(message_event)

        # 立即发送确认消息并记录开始时间
        open_kfid = self._last_open_kfid
        external_userid = self._last_external_userid
        asyncio.create_task(self._send_kf_ack(open_kfid, external_userid))
        self._kf_task_start_time = time.time()

    async def _send_kf_ack(self, open_kfid: str, external_userid: str) -> None:
        """立即发送任务确认消息"""
        ack_chain = MessageChain([Plain("任务收到，正在安排处理中。。。")])
        try:
            await self._send_kf_message(open_kfid, external_userid, ack_chain)
        except Exception as e:
            logger.warning("发送确认消息失败: %s", e)

    def _make_kf_sender(self):
        """创建一个闭包，用于在事件 send() 时通过微信客服 API 发送消息，并附带耗时"""
        adapter = self

        async def kf_send(message_chain: MessageChain) -> None:
            open_kfid = adapter._last_open_kfid
            external_userid = adapter._last_external_userid
            if not open_kfid or not external_userid:
                logger.warning("微信客服发送跳过: 缺少 open_kfid 或 external_userid")
                return

            # 计算耗时并附带到回复中
            start_time = getattr(adapter, "_kf_task_start_time", 0)
            if start_time:
                elapsed = time.time() - start_time
                adapter._kf_task_start_time = 0
                elapsed_text = f"\n\n⏱ 耗时: {elapsed:.1f}秒"
                # 在第一个 Plain 组件前插入耗时信息
                for comp in message_chain.chain:
                    if isinstance(comp, Plain):
                        comp.text = elapsed_text + "\n" + comp.text
                        break

            logger.info(f"通过微信客服 API 发送回复给 {external_userid}")
            await adapter._send_kf_message(open_kfid, external_userid, message_chain)

        return kf_send

    def run(self) -> Awaitable[Any]:
        """运行适配器，同时启动HTTP服务器和队列监听器"""

        async def run_both() -> None:
            if self.connection_mode == "long_connection":
                if not self.long_connection_client:
                    raise RuntimeError("长连接客户端未初始化")
                logger.info(
                    "启动企业微信智能机器人长连接模式: %s", self.long_connection_ws_url
                )
                await asyncio.gather(
                    self.long_connection_client.start(),
                    self.queue_listener.run(),
                )
            else:
                # 如果启用统一 webhook 模式，则不启动独立服务器
                webhook_uuid = self.config.get("webhook_uuid")
                if self.unified_webhook_mode and webhook_uuid:
                    log_webhook_info(
                        f"{self.meta().id}(企业微信智能机器人)", webhook_uuid
                    )
                    # 只运行队列监听器
                    await self.queue_listener.run()
                else:
                    if not self.server:
                        raise RuntimeError("Webhook 服务器未初始化")
                    logger.info(
                        "启动企业微信智能机器人适配器，监听 %s:%d", self.host, self.port
                    )
                    # 同时运行HTTP服务器和队列监听器
                    await asyncio.gather(
                        self.server.start_server(),
                        self.queue_listener.run(),
                    )

        return run_both()

    async def webhook_callback(self, request: Any) -> Any:
        """统一 Webhook 回调入口"""
        if self.connection_mode == "long_connection" or not self.server:
            return "long_connection mode does not accept webhook callbacks", 400
        # 根据请求方法分发到不同的处理函数
        if request.method == "GET":
            return await self.server.handle_verify(request)
        else:
            return await self.server.handle_callback(request)

    async def terminate(self) -> None:
        """终止适配器"""
        logger.info("企业微信智能机器人适配器正在关闭...")
        self.shutdown_event.set()
        if self.long_connection_client:
            await self.long_connection_client.shutdown()
        if self.server:
            await self.server.shutdown()

    def meta(self) -> PlatformMetadata:
        """获取平台元数据"""
        return self.metadata

    async def handle_msg(self, message: AstrBotMessage) -> None:
        """处理消息，创建消息事件并提交到事件队列"""
        try:
            message_event = WecomAIBotMessageEvent(
                message_str=message.message_str,
                message_obj=message,
                platform_meta=self.meta(),
                session_id=message.session_id,
                api_client=self.api_client,
                queue_mgr=self.queue_mgr,
                webhook_client=self.webhook_client,
                only_use_webhook_url_to_send=self.only_use_webhook_url_to_send,
                long_connection_sender=self._send_long_connection_respond_msg,
            )
            message_event.is_at_or_wake_command = (
                True  # 企业微信智能机器人默认消息都是 at 或唤醒命令
            )
            message_event.is_wake = True  # 企业微信智能机器人消息默认当做唤醒命令处理

            self.commit_event(message_event)

        except Exception as e:
            logger.error("处理消息时发生异常: %s", e)

    def get_client(self) -> WecomAIBotAPIClient | None:
        """获取 API 客户端"""
        return self.api_client

    def get_server(self) -> WecomAIBotServer | None:
        """获取 HTTP 服务器实例"""
        return self.server
