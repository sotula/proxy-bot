import os, json, asyncio, requests
from collections import defaultdict, deque
from flask import Flask, request, Response
from dotenv import load_dotenv

from botbuilder.core import TurnContext
from botbuilder.schema import Activity

# ✅ CloudAdapter + config-based auth (Python)
from botbuilder.integration.aiohttp import (
    CloudAdapter,
    ConfigurationBotFrameworkAuthentication,
)

load_dotenv()

app = Flask(__name__)

LAMBDA_URL = os.getenv("LAMBDA_URL")
# APP_ID = os.getenv("MICROSOFT_APP_ID")
# APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD")
# APP_TENANT_ID = os.getenv("MICROSOFT_APP_TENANT_ID")
# APP_TYPE = os.getenv("MICROSOFT_APP_TYPE", "SingleTenant")  # or MultiTenant

class DefaultConfig:
    """ Bot Configuration """

    PORT = 3978
    APP_ID = os.environ.get("MICROSOFT_APP_ID", "")
    APP_PASSWORD = os.environ.get("MICROSOFT_APP_PASSWORD", "")
    APP_TYPE = os.environ.get("MICROSOFT_APP_TYPE", "MultiTenant")
    APP_TENANTID = os.environ.get("MICROSOFT_APP_TENANT_ID", "")
    # CONNECTION_NAME = os.environ.get("ConnectionName", "")

CONFIG = DefaultConfig()

adapter = CloudAdapter(ConfigurationBotFrameworkAuthentication(CONFIG))

# adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
# adapter = BotFrameworkAdapter(adapter_settings)

# Holds up to 6 recent (user, bot) message pairs per conversation
message_history = defaultdict(lambda: deque(maxlen=5))

class LambdaBot:
    async def on_turn(self, turn_context: TurnContext):
        if turn_context.activity.type != "message":
            return

        user_input = turn_context.activity.text
        conversation_id = turn_context.activity.conversation.id

        # Get previous history
        history = message_history[conversation_id]

        # Build history list of dicts for sending
        history_payload = [{"user": u, "bot": b} for u, b in history]

        # Compose request payload
        payload = {
            "text": user_input,
            "history": history_payload
        }

        try:
            response = requests.post(LAMBDA_URL, json=payload)
            reply_text = response.text
            reply_text = json.loads(reply_text)['text']
        except Exception as e:
            reply_text = f"Lambda error: {str(e)}"

        # Save new message to history
        history.append((user_input, reply_text))

        await turn_context.send_activity(reply_text)

bot = LambdaBot()

@app.route("/api/messages", methods=["POST"])
def messages():
    print("✅ Received a POST request from Teams!")
    if "application/json" not in request.headers.get("Content-Type", ""):
        return Response(status=415)
    activity = Activity().deserialize(request.json)
    auth_header = request.headers.get("Authorization", "")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(adapter.process_activity(activity, auth_header, bot.on_turn))
    loop.run_until_complete(task)
    return Response(status=200)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3978)
