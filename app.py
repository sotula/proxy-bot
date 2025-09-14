import os, json, asyncio, threading, contextlib, logging
from collections import defaultdict, deque
from flask import Flask, request, Response
from dotenv import load_dotenv

from botbuilder.core import TurnContext
from botbuilder.schema import Activity, ActivityTypes, ConversationReference
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)

import aiohttp  # async HTTP for Lambda calls

# -------------------- Logging --------------------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("bot")

# -------------------- Config --------------------
load_dotenv()
LAMBDA_URL = os.environ["LAMBDA_URL"]
LAMBDA_TIMEOUT = float(os.getenv("LAMBDA_TIMEOUT", "120"))  # for Function URL/ALB
TYPING_INTERVAL = float(os.getenv("TYPING_INTERVAL", "3"))  # seconds

class DefaultConfig:
    PORT = int(os.environ.get("PORT", 3978))
    APP_ID = os.environ.get("MICROSOFT_APP_ID", "")  # MUST be set for CloudAdapter proactive
    APP_PASSWORD = os.environ.get("MICROSOFT_APP_PASSWORD", "")
    APP_TYPE = os.environ.get("MICROSOFT_APP_TYPE", "MultiTenant")
    APP_TENANTID = os.environ.get("MICROSOFT_APP_TENANT_ID", "")

CONFIG = DefaultConfig()
if not CONFIG.APP_ID:
    log.warning("MICROSOFT_APP_ID is empty. Proactive messages may fail.")

# -------------------- App & Adapter --------------------
app = Flask(__name__)
adapter = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))

# In-memory storage (OK for a single-process bot)
# Store serialized refs (dict), not objects — safer across threads.
conv_refs: dict[str, dict] = {}
message_history = defaultdict(lambda: deque(maxlen=5))

def conv_key(activity) -> str:
    # one key per conversation; add user id if you want per-user streams
    return activity.conversation.id

# -------------------- Async helpers --------------------
async def lambda_call_async(payload: dict) -> str:
    """Call Lambda (Function URL / ALB) asynchronously and return text."""
    timeout = aiohttp.ClientTimeout(total=LAMBDA_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(LAMBDA_URL, json=payload) as resp:
            body = await resp.text()
            if resp.status >= 400:
                return f"Upstream error {resp.status}: {body[:400]}"
            # Accept both direct {"text": "..."} and proxy {"statusCode":..,"body": "..."}
            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return body
            if isinstance(data, dict) and "statusCode" in data and "body" in data:
                try:
                    data = json.loads(data["body"])
                except Exception:
                    return str(data["body"])
            if isinstance(data, dict) and "text" in data:
                return data["text"]
            return json.dumps(data, ensure_ascii=False)

def _ensure_convref(obj_or_dict) -> ConversationReference:
    """
    Make sure we have a real ConversationReference object.
    Accepts a dict (serialized) or an object; returns object.
    """
    if isinstance(obj_or_dict, ConversationReference):
        return obj_or_dict
    if isinstance(obj_or_dict, dict):
        # The SDK's models provide .deserialize(dict) to rebuild
        return ConversationReference().deserialize(obj_or_dict)
    raise TypeError(f"Expected ConversationReference or dict, got {type(obj_or_dict)}")

async def send_typing_proactive(reference_dict: dict):
    ref_obj = _ensure_convref(reference_dict)
    async def _logic(tc: TurnContext):
        await tc.send_activity(Activity(type=ActivityTypes.typing))
    await adapter.continue_conversation(CONFIG.APP_ID, ref_obj, _logic)

async def send_message_proactive(reference_dict: dict, text: str):
    ref_obj = _ensure_convref(reference_dict)
    async def _logic(tc: TurnContext):
        await tc.send_activity(text)
    await adapter.continue_conversation(CONFIG.APP_ID, ref_obj, _logic)

# -------------------- Background Worker --------------------
def start_background_worker(reference_dict: dict, payload: dict):
    """
    Runs in a dedicated thread:
      - sends typing every TYPING_INTERVAL seconds
      - calls Lambda
      - stops typing and posts the final answer
    """
    def _thread_main():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        stop = False

        async def typing_loop():
            try:
                # immediate typing pulse
                await send_typing_proactive(reference_dict)
            except Exception as e:
                log.warning(f"typing initial send failed: {e}")
            while not stop:
                await asyncio.sleep(TYPING_INTERVAL)
                if stop:
                    break
                with contextlib.suppress(Exception):
                    await send_typing_proactive(reference_dict)

        async def run():
            nonlocal stop
            t_task = asyncio.create_task(typing_loop())
            try:
                answer = await lambda_call_async(payload)
            except Exception as e:
                log.exception("lambda_call_async failed")
                answer = f"Сталася помилка обробки: {e}"
            finally:
                stop = True
                with contextlib.suppress(Exception):
                    await t_task
            # final message
            try:
                await send_message_proactive(reference_dict, answer or "Не вдалося отримати відповідь.")
            except Exception as e:
                log.exception(f"proactive send failed: {e}")

        try:
            loop.run_until_complete(run())
        finally:
            loop.close()

    th = threading.Thread(target=_thread_main, daemon=True)
    th.start()

# -------------------- Bot --------------------
class LambdaBot:
    async def on_turn(self, turn_context: TurnContext):
        if turn_context.activity.type != "message":
            return

        user_input = (turn_context.activity.text or "").strip()
        key = conv_key(turn_context.activity)

        # save conversation reference for proactive messages (serialize!)
        cref_obj = TurnContext.get_conversation_reference(turn_context.activity)
        cref_dict = cref_obj.serialize() if hasattr(cref_obj, "serialize") else json.loads(json.dumps(cref_obj.__dict__, default=str))
        conv_refs[key] = cref_dict

        # quick ack (finish the turn fast)
        await turn_context.send_activity("Працюю над вашим запитом…")

        # optional: small local typing once (no proactive path, still within turn)
        await turn_context.send_activity(Activity(type=ActivityTypes.typing))

        # fire background worker (proactive typing + final answer)
        payload = {"text": user_input, "conversation_id": key}
        start_background_worker(cref_dict, payload)

# -------------------- Bot Framework endpoint --------------------
bot = LambdaBot()

@app.route("/api/messages", methods=["POST"])
def messages():
    if "application/json" not in request.headers.get("Content-Type", ""):
        return Response(status=415)
    activity = Activity().deserialize(request.json)
    auth_header = request.headers.get("Authorization", "")

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(
        # Positional args to avoid 4.17.x signature quirks
        adapter.process_activity(auth_header, activity, bot.on_turn)
    )
    loop.run_until_complete(task)
    return Response(status=200)

# -------------------- Entrypoint --------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=CONFIG.PORT)
