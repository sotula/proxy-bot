import os
import json
import asyncio
import threading
import contextlib
import logging
from collections import defaultdict, deque
from typing import Dict, Optional, Any
from flask import Flask, request, Response
from dotenv import load_dotenv

from botbuilder.core import TurnContext
from botbuilder.schema import Activity, ActivityTypes, ConversationReference
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)
import base64
# from botocore.session import get_session as boto_get_session
# from botocore.auth import SigV4Auth
# from botocore.awsrequest import AWSRequest

import aiohttp

# -------------------- Logging --------------------
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for troubleshooting
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
        
        # Bot Framework SDK expects these exact property names
        self.PORT = int(os.getenv("PORT", "3978"))
        self.APP_ID = os.getenv("MICROSOFT_APP_ID", "")
        self.APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD", "")
        self.APP_TYPE = os.getenv("MICROSOFT_APP_TYPE", "SingleTenant")
        
        # The SDK specifically looks for APP_TENANTID (no underscore)
        tenant_id = os.getenv("MICROSOFT_APP_TENANT_ID", "")
        self.APP_TENANTID = tenant_id
        
        # Also provide newer naming convention for future compatibility  
        self.MicrosoftAppId = self.APP_ID
        self.MicrosoftAppPassword = self.APP_PASSWORD
        self.MicrosoftAppType = self.APP_TYPE
        self.MicrosoftAppTenantId = tenant_id
        
        # Optional configurations
        self.MAX_MESSAGE_HISTORY = int(os.getenv("MAX_MESSAGE_HISTORY", "10"))
        self.MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
        self.RETRY_DELAY = float(os.getenv("RETRY_DELAY", "1.0"))
        
        self._validate_config()
    
    def _get_required_env(self, key: str) -> str:
        """Get required environment variable or raise error."""
        value = os.getenv(key)
        if not value:
            raise ValueError(f"Required environment variable {key} is not set")
        return value
    
    def _validate_config(self):
        """Validate configuration values."""
        if self.LAMBDA_TIMEOUT <= 0:
            raise ValueError("LAMBDA_TIMEOUT must be positive")
        if self.TYPING_INTERVAL <= 0:
            raise ValueError("TYPING_INTERVAL must be positive")
        if not self.APP_ID:
            log.warning("MICROSOFT_APP_ID is empty. Proactive messages may fail in production.")
        
        # For SingleTenant apps, tenant ID is required
        if self.APP_TYPE == "SingleTenant" and not self.APP_TENANTID:
            raise ValueError("MICROSOFT_APP_TENANT_ID is required for SingleTenant applications")

CONFIG = BotConfig()

# -------------------- App & Adapter --------------------
app = Flask(__name__)
adapter = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))

# Thread-safe in-memory storage
conv_refs: Dict[str, ConversationReference] = {}
message_history = defaultdict(lambda: deque(maxlen=CONFIG.MAX_MESSAGE_HISTORY))
_storage_lock = threading.RLock()

def get_conversation_key(activity: Activity) -> str:
    """Generate unique conversation key."""
    return f"{activity.conversation.id}_{activity.from_property.id if activity.from_property else 'unknown'}"

# -------------------- Lambda Integration --------------------
class LambdaClient:
    """Handles Lambda communication with retry logic and error handling."""
    
    def __init__(self, url: str, timeout: float, max_retries: int = 3):
        self.url = url
        self.timeout = timeout
        self.max_retries = max_retries
        # Optional auth for Function URL
        self.auth_mode = os.getenv("LAMBDA_URL_AUTH", "NONE").upper()  # "NONE" or "AWS_IAM"
        self.region = os.getenv("AWS_REGION", "eu-central-1")

    def _sign_if_needed(self, body_bytes: bytes) -> dict:
        """
        Returns headers (including SigV4) if auth_mode == AWS_IAM, else minimal headers.
        """
        headers = {"Content-Type": "application/json", "Connection": "keep-alive"}
        if self.auth_mode != "AWS_IAM":
            return headers

        # # Build a botocore AWSRequest and sign it
        # session = boto_get_session()
        # creds = session.get_credentials()
        # if creds is None:
        #     raise RuntimeError("No AWS credentials available for SigV4 signing (AWS_IAM).")

        # aws_req = AWSRequest(method="POST", url=self.url, data=body_bytes, headers=headers.copy())
        # SigV4Auth(creds, "lambda", self.region).add_auth(aws_req)
        # # Convert botocore headers to a plain dict for aiohttp
        # signed_headers = dict(aws_req.headers.items())
        # # Ensure keep-alive is present (optional)
        # signed_headers.setdefault("Connection", "keep-alive")
        # return signed_headers
    
    async def call_async(self, payload: Dict[str, Any]) -> str:
        """Call Lambda Function URL with retry logic and proper error handling."""
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers = self._sign_if_needed(body_bytes)

        for attempt in range(self.max_retries):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    log.info(f"Calling Lambda URL (attempt {attempt + 1}/{self.max_retries}, auth={self.auth_mode})")
                    async with session.post(self.url, data=body_bytes, headers=headers) as response:
                        return await self._process_response(response)

            except asyncio.TimeoutError:
                log.warning(f"Lambda URL timeout on attempt {attempt + 1}")
                if attempt == self.max_retries - 1:
                    return "Вибачте, обробка запиту зайняла занадто багато часу. Спробуйте ще раз."
                await asyncio.sleep(CONFIG.RETRY_DELAY * (attempt + 1))

            except aiohttp.ClientError as e:
                log.error(f"Lambda URL client error on attempt {attempt + 1}: {e}")
                if attempt == self.max_retries - 1:
                    return f"Помилка з'єднання з сервісом: {str(e)}"
                await asyncio.sleep(CONFIG.RETRY_DELAY * (attempt + 1))

            except Exception as e:
                log.exception(f"Unexpected error on attempt {attempt + 1}")
                if attempt == self.max_retries - 1:
                    return "Сталася неочікувана помилка обробки запиту."
                await asyncio.sleep(CONFIG.RETRY_DELAY * (attempt + 1))

        return "Не вдалося обробити запит після кількох спроб."
    
    async def _process_response(self, response: aiohttp.ClientResponse) -> str:
        """
        Process Lambda Function URL response.
        Function URLs use Lambda’s HTTP payload format (v2). Your handler usually returns:
          {"statusCode": 200, "headers": {...}, "body": "..."}  (optionally isBase64Encoded)
        """
        body = await response.text()

        # 502 can happen if Lambda function itself errored; still parse body.
        if response.status >= 400:
            log.error(f"Lambda URL returned error {response.status}: {body[:400]}")
            # Try to extract Lambda-style error message
            try:
                data = json.loads(body)
                msg = data.get("errorMessage") or data.get("message")
                if msg:
                    return f"Сервіс повернув помилку {response.status}: {msg}"
            except Exception:
                pass
            return f"Сервіс повернув помилку {response.status}. Спробуйте пізніше."

        # Parse success path
        try:
            data = json.loads(body) if body else {}
        except json.JSONDecodeError:
            # If you return raw text from Lambda, just relay it
            return body or "Отримано порожню відповідь"

        # Handle standard Lambda URL proxy shape
        if isinstance(data, dict) and "statusCode" in data:
            # Function might return base64-encoded body
            raw_body = data.get("body", "")
            if data.get("isBase64Encoded"):
                try:
                    raw_body = base64.b64decode(raw_body or "").decode("utf-8", "ignore")
                except Exception:
                    pass
            # Your Lambda often wraps {"text": "..."} in body
            try:
                inner = json.loads(raw_body)
                if isinstance(inner, dict) and "text" in inner:
                    return inner["text"]
                return raw_body if raw_body else "Отримано порожню відповідь"
            except Exception:
                return raw_body if raw_body else "Отримано порожню відповідь"

        # Direct JSON shape: {"text": "..."} (if your handler returns dict directly)
        if isinstance(data, dict) and "text" in data:
            return data["text"]

        # Fallback
        return json.dumps(data, ensure_ascii=False, indent=2)

lambda_client = LambdaClient(CONFIG.LAMBDA_URL, CONFIG.LAMBDA_TIMEOUT, CONFIG.MAX_RETRY_ATTEMPTS)

# -------------------- Proactive Messaging --------------------
class ProactiveMessenger:
    """Handles proactive messaging with proper error handling."""
    
    @staticmethod
    def _ensure_conversation_reference(obj_or_dict) -> ConversationReference:
        """Convert dict to ConversationReference object."""
        log.debug(f"_ensure_conversation_reference called with type: {type(obj_or_dict)}")
        
        if isinstance(obj_or_dict, ConversationReference):
            log.debug(f"Valid ConversationReference object: channel_id={obj_or_dict.channel_id}")
            log.debug(obj_or_dict)
            return obj_or_dict
        
        log.error(f"Expected ConversationReference object, got {type(obj_or_dict)}: {str(obj_or_dict)[:200]}")
        raise TypeError(f"Expected ConversationReference object, got {type(obj_or_dict)}")
    
    @staticmethod
    async def send_typing(reference_obj: ConversationReference) -> bool:
        """Send typing indicator proactively."""
        try:
            ref_obj = ProactiveMessenger._ensure_conversation_reference(reference_obj)
            
            async def _typing_logic(turn_context: TurnContext):
                await turn_context.send_activity(Activity(type=ActivityTypes.typing))
            
            # Use string version of APP_ID to avoid any object reference issues
            app_id = str(CONFIG.APP_ID) if CONFIG.APP_ID else ""
            log.debug(f"Using app_id: '{app_id}', ref channel_id: '{ref_obj.channel_id}'")
            
            await adapter.continue_conversation(
                bot_app_id=app_id, 
                reference=ref_obj, 
                callback=_typing_logic
                )
            return True
        except Exception as e:
            log.error(f"Failed to send typing indicator: {e}")
            log.exception("Full typing indicator error trace:")
            return False
    
    @staticmethod
    async def send_message(reference_obj: ConversationReference, text: str) -> bool:
        """Send message proactively."""
        try:
            ref_obj = ProactiveMessenger._ensure_conversation_reference(reference_obj)
            
            async def _message_logic(turn_context: TurnContext):
                await turn_context.send_activity(text)
            
            # Use string version of APP_ID to avoid any object reference issues
            app_id = str(CONFIG.APP_ID) if CONFIG.APP_ID else ""
            log.debug(f"Using app_id: '{app_id}', ref channel_id: '{ref_obj.channel_id}'")
            
            await adapter.continue_conversation(
                bot_app_id=app_id, 
                reference=ref_obj, 
                callback=_message_logic
            )
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
            """Main thread function with proper async event loop handling."""
            try:
                # Create new event loop for this thread
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    loop.run_until_complete(self._process_request(reference_obj, payload))
                finally:
                    loop.close()
            except Exception as e:
                log.exception("Background worker thread failed")
                # Attempt emergency message send
                try:
                    emergency_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(emergency_loop)
                    emergency_loop.run_until_complete(
                        self.messenger.send_message(
                            reference_obj, 
                            "Сталася критична помилка обробки запиту."
                        )
                    )
                    emergency_loop.close()
                except:
                    log.error("Failed to send emergency message")
        
        thread = threading.Thread(target=_thread_main, daemon=True)
        thread.start()
        log.info("Background worker started")
    
    async def _process_request(self, reference_obj: ConversationReference, payload: Dict[str, Any]):
        """Process the request with typing indicators."""
        stop_typing = False
        typing_task = None
        
        try:
            # Start typing indicator loop
            typing_task = asyncio.create_task(self._typing_loop(reference_obj, lambda: stop_typing))
            
            # Process the request
            log.info(f"Processing Lambda request for conversation: {payload.get('conversation_id', 'unknown')}")
            answer = await self.lambda_client.call_async(payload)
            
            # Stop typing
            stop_typing = True
            if typing_task and not typing_task.done():
                typing_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await typing_task
            
            # Send final answer
            if answer:
                success = await self.messenger.send_message(reference_obj, answer)
                if success:
                    log.info("Successfully sent response to Teams")
                else:
                    log.error("Failed to send response to Teams")
            else:
                await self.messenger.send_message(
                    reference_obj, 
                    "Не вдалося отримати відповідь від сервісу."
                )
        
        except Exception as e:
            log.exception("Error in background processing")
            stop_typing = True
            if typing_task and not typing_task.done():
                typing_task.cancel()
            
            # Send error message
            await self.messenger.send_message(
                reference_obj,
                f"Сталася помилка обробки: {str(e)}"
            )
    
    async def _typing_loop(self, reference_obj: ConversationReference, stop_check):
        """Send typing indicators periodically."""
        # Send initial typing indicator
        await self.messenger.send_typing(reference_obj)
        
        while not stop_check():
            try:
                await asyncio.sleep(CONFIG.TYPING_INTERVAL)
                if stop_check():
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
        """Handle incoming message from Teams."""
        try:
            # Only process message activities
            if turn_context.activity.type != ActivityTypes.message:
                log.info(f"Ignoring non-message activity: {turn_context.activity.type}")
                return
            
            user_input = (turn_context.activity.text or "").strip()
            if not user_input:
                await turn_context.send_activity("Будь ласка, надішліть текстове повідомлення.")
                return
            
            conversation_key = get_conversation_key(turn_context.activity)
            
            # Store conversation reference for proactive messaging
            with _storage_lock:
                conversation_ref = TurnContext.get_conversation_reference(turn_context.activity)
                # Store the actual ConversationReference object instead of serializing
                conv_refs[conversation_key] = conversation_ref
                log.debug(f"Stored conversation reference: {type(conversation_ref)}")
                
                # Store message in history
                message_history[conversation_key].append({
                    "timestamp": turn_context.activity.timestamp,
                    "text": user_input,
                    "user": turn_context.activity.from_property.name if turn_context.activity.from_property else "Unknown"
                })
            
            # Send immediate acknowledgment
            await turn_context.send_activity("Обробляю ваш запит...")
            
            # Send initial typing indicator (within turn context)
            await turn_context.send_activity(Activity(type=ActivityTypes.typing))
            
            # Prepare payload for Lambda
            payload = {
                "text": user_input,
                "conversation_id": conversation_key,
                "user_info": {
                    "name": turn_context.activity.from_property.name if turn_context.activity.from_property else "Unknown",
                    "id": turn_context.activity.from_property.id if turn_context.activity.from_property else "unknown"
                },
                "metadata": {
                    "timestamp": str(turn_context.activity.timestamp),
                    "channel": "teams"
                }
            }
            
            # Start background processing
            self.worker.start_processing(conv_refs[conversation_key], payload)
            
            log.info(f"Started processing for conversation: {conversation_key}")
            
        except Exception as e:
            log.exception("Error in on_turn")
            try:
                await turn_context.send_activity(
                    f"Сталася помилка обробки повідомлення: {str(e)}"
                )
            except:
                log.error("Failed to send error message to user")

# -------------------- Flask Endpoints --------------------
bot = TeamsLambdaBot()

@app.route("/api/messages", methods=["POST"])
def messages():
    """Handle incoming messages from Teams."""
    try:
        # Validate content type
        if "application/json" not in request.headers.get("Content-Type", ""):
            log.warning("Invalid content type")
            return Response("Invalid content type", status=415)
        
        # Parse activity
        try:
            activity = Activity().deserialize(request.json)
        except Exception as e:
            log.error(f"Failed to deserialize activity: {e}")
            return Response("Invalid activity format", status=400)
        
        # Get auth header
        auth_header = request.headers.get("Authorization", "")
        
        # Process activity
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            task = loop.create_task(
                adapter.process_activity(auth_header, activity, bot.on_turn)
            )
            loop.run_until_complete(task)
            return Response(status=200)
        finally:
            loop.close()
    
    except Exception as e:
        log.exception("Error in messages endpoint")
        return Response(f"Internal server error: {str(e)}", status=500)

@app.route("/health", methods=["GET"])
def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "service": "teams-lambda-bot"}

@app.route("/stats", methods=["GET"])
def stats():
    """Basic stats endpoint."""
    with _storage_lock:
        return {
            "active_conversations": len(conv_refs),
            "total_messages": sum(len(history) for history in message_history.values()),
            "config": {
                "lambda_timeout": CONFIG.LAMBDA_TIMEOUT,
                "typing_interval": CONFIG.TYPING_INTERVAL,
                "max_retries": CONFIG.MAX_RETRY_ATTEMPTS
            }
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
        
        app.run(
            host="0.0.0.0", 
            port=CONFIG.PORT,
            debug=False,  # Set to False in production
            threaded=True
        )
    except Exception as e:
        log.exception("Failed to start application")
        raise