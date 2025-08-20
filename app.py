import os
import requests
import json
from flask import Flask, request, Response
from dotenv import load_dotenv
from botbuilder.core import TurnContext
from botbuilder.schema import Activity

# ✅ CloudAdapter + auth factory come from integration.aiohttp
from botbuilder.integration.aiohttp.cloud_adapter import CloudAdapter
from botbuilder.integration.aiohttp.configuration_service_client_credential_factory import (
    ConfigurationServiceClientCredentialFactory,
)
from botbuilder.integration.aiohttp.configuration_bot_framework_authentication import (
    ConfigurationBotFrameworkAuthentication,
)
import asyncio
from collections import defaultdict, deque

load_dotenv()

app = Flask(__name__)

APP_ID = os.getenv("MICROSOFT_APP_ID")
APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD")
APP_TENANT_ID = os.getenv("MICROSOFT_APP_TENANT_ID")
APP_TYPE = os.getenv("MICROSOFT_APP_TYPE", "SingleTenant")
LAMBDA_URL = os.getenv("LAMBDA_URL")


creds_factory = ConfigurationServiceClientCredentialFactory({
    "MicrosoftAppId": APP_ID,
    "MicrosoftAppPassword": APP_PASSWORD,
    "MicrosoftAppTenantId": APP_TENANT_ID,
    "MicrosoftAppType": APP_TYPE,  # "SingleTenant" or "MultiTenant"
})

bot_auth = ConfigurationBotFrameworkAuthentication(configuration={}, credentials_factory=creds_factory)
adapter = CloudAdapter(bot_auth)  # inherits CloudAdapterBase → has process_activity(...)

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
