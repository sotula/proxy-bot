import os
import json
import asyncio
from collections import defaultdict, deque

from flask import Flask, request, Response
from dotenv import load_dotenv

from botbuilder.core import TurnContext
from botbuilder.schema import Activity, ActivityTypes

# Bot Framework (AIOHTTP adapter)
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)

import aiohttp

load_dotenv()

# ---------------- Config ----------------
LAMBDA_URL = os.getenv("LAMBDA_URL")
LAMBDA_TIMEOUT = float(os.getenv("LAMBDA_TIMEOUT", "60"))  # seconds (Function URL/ALB can allow >30s)
TYPING_INTERVAL = float(os.getenv("TYPING_INTERVAL", "3"))  # seconds between typing pings

class DefaultConfig:
    PORT = int(os.environ.get("PORT", 3978))
    APP_ID = os.environ.get("MICROSOFT_APP_ID", "")
    APP_PASSWORD = os.environ.get("MICROSOFT_APP_PASSWORD", "")
    APP_TYPE = os.environ.get("MICROSOFT_APP_TYPE", "MultiTenant")
    APP_TENANTID = os.environ.get("MICROSOFT_APP_TENANT_ID", "")

CONFIG = DefaultConfig()

# Flask app
app = Flask(__name__)

# Bot adapter
adapter = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))

# (Optional) small history buffer if you need it later
message_history = defaultdict(lambda: deque(maxlen=5))

# -------------- Helpers --------------

async def fetch_lambda_answer(payload: dict) -> str:
    """
    Async call to Lambda URL.
    Accepts both:
      - {"text": "..."} directly
      - {"statusCode": 200, "body": "{\"text\":\"...\"}"} (Lambda proxy form)
    Returns the "text" to send back to Teams.
    """
    timeout = aiohttp.ClientTimeout(total=LAMBDA_TIMEOUT)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.post(LAMBDA_URL, json=payload) as resp:
            raw = await resp.text()
            if resp.status >= 400:
                return f"Upstream error {resp.status}: {raw[:400]}"

            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                # Lambda returned plain text — pass through
                return raw

            # Unwrap Lambda proxy shape if present
            if isinstance(data, dict) and "statusCode" in data and "body" in data:
                try:
                    data = json.loads(data["body"])
                except Exception:
                    # If body isn't JSON, return as text
                    return str(data["body"])

            if isinstance(data, dict) and "text" in data:
                return data["text"]

            # Fallback: stringify whatever we got
            return json.dumps(data, ensure_ascii=False)

async def typing_loop(turn_context: TurnContext, stop_event: asyncio.Event):
    """
    Sends typing activities every TYPING_INTERVAL seconds until stop_event is set.
    """
    try:
        # first immediate typing to show responsiveness
        await turn_context.send_activity(Activity(type=ActivityTypes.typing))
        while not stop_event.is_set():
            await asyncio.sleep(TYPING_INTERVAL)
            if stop_event.is_set():
                break
            await turn_context.send_activity(Activity(type=ActivityTypes.typing))
    except Exception:
        # swallow typing errors; don't break the main flow
        pass

# -------------- Bot --------------

class LambdaBot:
    async def on_turn(self, turn_context: TurnContext):
        if turn_context.activity.type != "message":
            return

        user_input = (turn_context.activity.text or "").strip()
        conversation_id = turn_context.activity.conversation.id

        # Prepare payload for Lambda
        payload = {
            "text": user_input,
            "conversation_id": conversation_id,
        }

        # 1) Send quick ack to the user
        await turn_context.send_activity("Працюю над вашим запитом…")

        # 2) Start typing loop and Lambda call concurrently
        stop_event = asyncio.Event()
        typing_task = asyncio.create_task(typing_loop(turn_context, stop_event))
        lambda_task = asyncio.create_task(fetch_lambda_answer(payload))

        reply_text = None
        try:
            reply_text = await lambda_task
        except asyncio.TimeoutError:
            reply_text = ("Перевищено ліміт часу обробки. Уточніть, будь ласка, період/фільтри "
                          "або спробуйте коротший запит.")
        except Exception as e:
            reply_text = f"Сталася тимчасова помилка: {e}"
        finally:
            # stop typing loop
            stop_event.set()
            # ensure typing task is finished
            with contextlib.suppress(Exception):
                await typing_task

        # 3) Send the final Lambda answer
        await turn_context.send_activity(reply_text or "Не вдалося отримати відповідь.")

# -------------- Flask endpoint --------------

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
        adapter.process_activity(
            auth_header_or_authenticate_request_result=auth_header,
            activity=activity,
            logic=bot.on_turn,
        )
    )
    loop.run_until_complete(task)
    return Response(status=200)

# -------------- Entrypoint --------------
if __name__ == "__main__":
    import contextlib
    app.run(host="0.0.0.0", port=CONFIG.PORT)
