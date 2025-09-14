import os
import json
import asyncio
import threading
import contextlib
import logging
from collections import defaultdict, deque
from typing import Dict, Any
from flask import Flask, request, Response
from dotenv import load_dotenv

from botbuilder.core import TurnContext
from botbuilder.schema import Activity, ActivityTypes, ConversationReference
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)

import aiohttp

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.DEBUG,  # switch to INFO in prod
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
log = logging.getLogger("teams_lambda_bot")

# -------------------- Configuration --------------------
load_dotenv()


class BotConfig:
    """Configuration class matching Bot Framework SDK expectations."""

    def __init__(self):
        # Lambda configuration
        self.LAMBDA_URL = self._get_required_env("LAMBDA_URL")
        self.LAMBDA_TIMEOUT = float(os.getenv("LAMBDA_TIMEOUT", "120"))
        self.TYPING_INTERVAL = float(os.getenv("TYPING_INTERVAL", "3"))

        # Bot Framework config (SingleTenant)
        self.PORT = int(os.getenv("PORT", "3978"))
        self.APP_ID = os.getenv("MICROSOFT_APP_ID", "")
        self.APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD", "")
        self.APP_TYPE = os.getenv("MICROSOFT_APP_TYPE", "SingleTenant")
        tenant_id = os.getenv("MICROSOFT_APP_TENANT_ID", "")
        self.APP_TENANTID = tenant_id

        # For possible future compatibility
        self.MicrosoftAppId = self.APP_ID
        self.MicrosoftAppPassword = self.APP_PASSWORD
        self.MicrosoftAppType = self.APP_TYPE
        self.MicrosoftAppTenantId = tenant_id

        # Optional
        self.MAX_MESSAGE_HISTORY = int(os.getenv("MAX_MESSAGE_HISTORY", "10"))
        self.MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
        self.RETRY_DELAY = float(os.getenv("RETRY_DELAY", "1.0"))

        self._validate_config()

    def _get_required_env(self, key: str) -> str:
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable {key} is not set")
        return value

    def _validate_config(self):
        if self.LAMBDA_TIMEOUT <= 0:
            raise ValueError("LAMBDA_TIMEOUT must be positive")
        if self.TYPING_INTERVAL <= 0:
            raise ValueError("TYPING_INTERVAL must be positive")
        if not self.APP_ID:
            log.warning("MICROSOFT_APP_ID is empty. Proactive messages may fail.")
        if self.APP_TYPE == "SingleTenant" and not self.APP_TENANTID:
            raise ValueError(
                "MICROSOFT_APP_TENANT_ID is required for SingleTenant applications"
            )


CONFIG = BotConfig()

# -------------------- App & Adapter --------------------
app = Flask(__name__)

# For botbuilder==4.17.0, pass a dict config (SingleTenant)
auth = ConfigurationBotFrameworkAuthentication(
    {
        "MicrosoftAppId": CONFIG.APP_ID,
        "MicrosoftAppPassword": CONFIG.APP_PASSWORD,
        "MicrosoftAppType": CONFIG.APP_TYPE,           # "SingleTenant"
        "MicrosoftAppTenantId": CONFIG.APP_TENANTID,   # your tenant GUID
        # Leave ChannelService unset for public cloud
    }
)
adapter = CloudAdapter(auth)

# In-memory storage
conv_refs: Dict[str, ConversationReference] = {}
message_history = defaultdict(lambda: deque(maxlen=CONFIG.MAX_MESSAGE_HISTORY))
_storage_lock = threading.RLock()


def get_conversation_key(activity: Activity) -> str:
    """Generate unique conversation key (conversation + user)."""
    return f"{activity.conversation.id}_{activity.from_property.id if activity.from_property else 'unknown'}"


# -------------------- Lambda Integration --------------------
class LambdaClient:
    """Handles Lambda communication with retry logic and error handling."""

    def __init__(self, url: str, timeout: float, max_retries: int = 3):
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries

    async def call_async(self, payload: Dict[str, Any]) -> str:
        timeout = aiohttp.ClientTimeout(total=self.timeout)

        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    log.info(f"Calling Lambda (attempt {attempt + 1}/{self.max_retries})")
                    async with session.post(self.url, json=payload) as response:
                        return await self._process_response(response)

            except asyncio.TimeoutError:
                log.warning(f"Lambda timeout on attempt {attempt + 1}")
                if attempt == self.max_retries - 1:
                    return "Вибачте, обробка запиту зайняла занадто багато часу. Спробуйте ще раз."
                await asyncio.sleep(CONFIG.RETRY_DELAY * (attempt + 1))

            except aiohttp.ClientError as e:
                log.error(f"Lambda client error on attempt {attempt + 1}: {e}")
                if attempt == self.max_retries - 1:
                    return f"Помилка з'єднання з сервісом: {str(e)}"
                await asyncio.sleep(CONFIG.RETRY_DELAY * (attempt + 1))

            except Exception:
                log.exception(f"Unexpected error on attempt {attempt + 1}")
                if attempt == self.max_retries - 1:
                    return "Сталася неочікувана помилка обробки запиту."
                await asyncio.sleep(CONFIG.RETRY_DELAY * (attempt + 1))

        return "Не вдалося обробити запит після кількох спроб."

    async def _process_response(self, response: aiohttp.ClientResponse) -> str:
        body = await response.text()

        if response.status >= 400:
            log.error(f"Lambda returned error {response.status}: {body[:400]}")
            return f"Сервіс повернув помилку {response.status}. Спробуйте пізніше."

        # Try JSON; else return text
        try:
            data = json.loads(body)
        except json.JSONDecodeError:
            return body or "Отримано порожню відповідь"

        # API Gateway/ALB style
        if isinstance(data, dict):
            if "statusCode" in data and "body" in data:
                try:
                    inner = json.loads(data["body"])
                    if isinstance(inner, dict) and "text" in inner:
                        return inner["text"]
                    return str(data["body"])
                except json.JSONDecodeError:
                    return str(data["body"])
            if "text" in data:
                return data["text"]
            if "error" in data:
                log.error(f"Lambda returned error: {data['error']}")
                return "Сталася помилка під час обробки запиту."

        return json.dumps(data, ensure_ascii=False, indent=2)


lambda_client = LambdaClient(
    CONFIG.LAMBDA_URL, CONFIG.LAMBDA_TIMEOUT, CONFIG.MAX_RETRY_ATTEMPTS
)

# -------------------- Proactive Messaging --------------------
class ProactiveMessenger:
    """Handles proactive messaging with proper error handling."""

    @staticmethod
    def _ensure_conversation_reference(obj_or_dict) -> ConversationReference:
        log.debug(f"_ensure_conversation_reference called with type: {type(obj_or_dict)}")
        if isinstance(obj_or_dict, ConversationReference):
            log.debug(f"Valid ConversationReference object: channel_id={obj_or_dict.channel_id}")
            return obj_or_dict
        log.error(
            f"Expected ConversationReference object, got {type(obj_or_dict)}: {str(obj_or_dict)[:200]}"
        )
        raise TypeError(f"Expected ConversationReference object, got {type(obj_or_dict)}")

    @staticmethod
    async def _continue(ref_obj: ConversationReference, logic):
        """
        Version-safe continue_conversation:
        - Newer SDKs: continue_conversation(reference, logic, bot_app_id)
        - 4.17.0 (fallback): continue_conversation(bot_app_id, reference, logic)
        """
        try:
            await adapter.continue_conversation(ref_obj, logic, CONFIG.APP_ID)
        except TypeError:
            await adapter.continue_conversation(CONFIG.APP_ID, ref_obj, logic)

    @staticmethod
    async def send_typing(reference_obj: ConversationReference) -> bool:
        try:
            ref_obj = ProactiveMessenger._ensure_conversation_reference(reference_obj)

            async def _typing_logic(turn_context: TurnContext):
                await turn_context.send_activity(Activity(type=ActivityTypes.typing))

            log.debug(f"Using app_id: '{CONFIG.APP_ID}', ref channel_id: '{ref_obj.channel_id}'")
            await ProactiveMessenger._continue(ref_obj, _typing_logic)
            return True
        except Exception as e:
            log.error(f"Failed to send typing indicator: {e}")
            log.exception("Full typing indicator error trace:")
            return False

    @staticmethod
    async def send_message(reference_obj: ConversationReference, text: str) -> bool:
        try:
            ref_obj = ProactiveMessenger._ensure_conversation_reference(reference_obj)

            async def _message_logic(turn_context: TurnContext):
                await turn_context.send_activity(text)

            log.debug(f"Using app_id: '{CONFIG.APP_ID}', ref channel_id: '{ref_obj.channel_id}'")
            await ProactiveMessenger._continue(ref_obj, _message_logic)
            return True
        except Exception as e:
            log.error(f"Failed to send proactive message: {e}")
            log.exception("Full proactive message error trace:")
            return False


# -------------------- Background Worker --------------------
class BackgroundWorker:
    """Manages background processing with typing indicators."""

    def __init__(self, messenger: ProactiveMessenger, lambda_client: LambdaClient):
        self.messenger = messenger
        self.lambda_client = lambda_client

    def start_processing(self, reference_obj: ConversationReference, payload: Dict[str, Any]):
        """Start background processing in a separate thread."""

        def _thread_main():
            try:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(self._process_request(reference_obj, payload))
                finally:
                    loop.close()
            except Exception:
                log.exception("Background worker thread failed")
                try:
                    emergency_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(emergency_loop)
                    emergency_loop.run_until_complete(
                        self.messenger.send_message(
                            reference_obj, "Сталася критична помилка обробки запиту."
                        )
                    )
                    emergency_loop.close()
                except Exception:
                    log.error("Failed to send emergency message")

        thread = threading.Thread(target=_thread_main, daemon=True)
        thread.start()
        log.info("Background worker started")

    async def _process_request(self, reference_obj: ConversationReference, payload: Dict[str, Any]):
        stop_event = asyncio.Event()
        typing_task = None

        try:
            typing_task = asyncio.create_task(self._typing_loop(reference_obj, stop_event))

            log.info(f"Processing Lambda request for conversation: {payload.get('conversation_id', 'unknown')}")
            answer = await self.lambda_client.call_async(payload)

            stop_event.set()
            if typing_task and not typing_task.done():
                typing_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await typing_task

            if answer:
                success = await self.messenger.send_message(reference_obj, answer)
                if success:
                    log.info("Successfully sent response to Teams")
                else:
                    log.error("Failed to send response to Teams")
            else:
                await self.messenger.send_message(
                    reference_obj, "Не вдалося отримати відповідь від сервісу."
                )

        except Exception as e:
            log.exception("Error in background processing")
            stop_event.set()
            if typing_task and not typing_task.done():
                typing_task.cancel()
            await self.messenger.send_message(reference_obj, f"Сталася помилка обробки: {str(e)}")

    async def _typing_loop(self, reference_obj: ConversationReference, stop_event: asyncio.Event):
        await self.messenger.send_typing(reference_obj)
        while not stop_event.is_set():
            try:
                await asyncio.sleep(CONFIG.TYPING_INTERVAL)
                if stop_event.is_set():
                    break
                await self.messenger.send_typing(reference_obj)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"Error in typing loop: {e}")


# -------------------- Bot Implementation --------------------
class TeamsLambdaBot:
    """Main bot class handling Teams messages."""

    def __init__(self):
        self.messenger = ProactiveMessenger()
        self.worker = BackgroundWorker(self.messenger, lambda_client)

    async def on_turn(self, turn_context: TurnContext):
        try:
            if turn_context.activity.type != ActivityTypes.message:
                log.info(f"Ignoring non-message activity: {turn_context.activity.type}")
                return

            user_input = (turn_context.activity.text or "").strip()
            if not user_input:
                await turn_context.send_activity("Будь ласка, надішліть текстове повідомлення.")
                return

            conversation_key = get_conversation_key(turn_context.activity)

            # Store conversation reference and history
            with _storage_lock:
                conversation_ref = TurnContext.get_conversation_reference(turn_context.activity)
                conv_refs[conversation_key] = conversation_ref
                log.debug(f"Stored conversation reference: {type(conversation_ref)}")
                message_history[conversation_key].append(
                    {
                        "timestamp": turn_context.activity.timestamp,
                        "text": user_input,
                        "user": turn_context.activity.from_property.name
                        if turn_context.activity.from_property
                        else "Unknown",
                    }
                )

            # Immediate ack + typing (within turn)
            await turn_context.send_activity("Обробляю ваш запит...")
            await turn_context.send_activity(Activity(type=ActivityTypes.typing))

            # Payload to Lambda
            payload = {
                "text": user_input,
                "conversation_id": conversation_key,
                "user_info": {
                    "name": turn_context.activity.from_property.name
                    if turn_context.activity.from_property
                    else "Unknown",
                    "id": turn_context.activity.from_property.id
                    if turn_context.activity.from_property
                    else "unknown",
                },
                "metadata": {
                    "timestamp": str(turn_context.activity.timestamp),
                    "channel": "teams",
                },
            }

            # Offload long work + proactive updates
            self.worker.start_processing(conv_refs[conversation_key], payload)
            log.info(f"Started processing for conversation: {conversation_key}")

        except Exception as e:
            log.exception("Error in on_turn")
            try:
                await turn_context.send_activity(f"Сталася помилка обробки повідомлення: {str(e)}")
            except Exception:
                log.error("Failed to send error message to user")


# -------------------- Flask Endpoints --------------------
bot = TeamsLambdaBot()


@app.route("/api/messages", methods=["POST"])
def messages():
    """Handle incoming messages from Teams."""
    try:
        if "application/json" not in request.headers.get("Content-Type", ""):
            log.warning("Invalid content type")
            return Response("Invalid content type", status=415)

        try:
            activity = Activity().deserialize(request.json)
        except Exception as e:
            log.error(f"Failed to deserialize activity: {e}")
            return Response("Invalid activity format", status=400)

        auth_header = request.headers.get("Authorization", "")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            task = loop.create_task(adapter.process_activity(auth_header, activity, bot.on_turn))
            loop.run_until_complete(task)
            return Response(status=200)
        finally:
            loop.close()

    except Exception as e:
        log.exception("Error in messages endpoint")
        return Response(f"Internal server error: {str(e)}", status=500)


@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "healthy", "service": "teams-lambda-bot"}


@app.route("/stats", methods=["GET"])
def stats():
    with _storage_lock:
        return {
            "active_conversations": len(conv_refs),
            "total_messages": sum(len(history) for history in message_history.values()),
            "config": {
                "lambda_timeout": CONFIG.LAMBDA_TIMEOUT,
                "typing_interval": CONFIG.TYPING_INTERVAL,
                "max_retries": CONFIG.MAX_RETRY_ATTEMPTS,
            },
        }


# -------------------- Error Handlers --------------------
@app.errorhandler(404)
def not_found(error):
    return {"error": "Endpoint not found"}, 404


@app.errorhandler(500)
def internal_error(error):
    log.exception("Internal server error")
    return {"error": "Internal server error"}, 500


# -------------------- Application Entry Point --------------------
if __name__ == "__main__":
    try:
        log.info(f"Starting Teams Lambda Bot on port {CONFIG.PORT}")
        log.info(f"Lambda URL: {CONFIG.LAMBDA_URL}")
        log.info(f"App ID configured: {'Yes' if CONFIG.APP_ID else 'No'}")
        log.info(f"App Type: {CONFIG.APP_TYPE}, Tenant: {CONFIG.APP_TENANTID or 'N/A'}")

        app.run(
            host="0.0.0.0",
            port=CONFIG.PORT,
            debug=False,  # False in prod
            threaded=True,
        )
    except Exception:
        log.exception("Failed to start application")
        raise
