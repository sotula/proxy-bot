import os
import requests
from flask import Flask, request, Response
from dotenv import load_dotenv
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity
import asyncio

load_dotenv()

app = Flask(__name__)

APP_ID = os.getenv("MICROSOFT_APP_ID")
APP_PASSWORD = os.getenv("MICROSOFT_APP_PASSWORD")
LAMBDA_URL = os.getenv("LAMBDA_URL")

adapter_settings = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
adapter = BotFrameworkAdapter(adapter_settings)

class LambdaBot:
    async def on_turn(self, turn_context: TurnContext):
        if turn_context.activity.type == "message":
            user_input = turn_context.activity.text
            try:
                response = requests.post(LAMBDA_URL, json={"text": user_input})
                reply_text = response.text
            except Exception as e:
                reply_text = f"Lambda error: {str(e)}"
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
