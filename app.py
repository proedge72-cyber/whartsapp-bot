from flask import Flask, request
import requests
import openai
import os

app = Flask(__name__)

# 🔑 CONFIG
VERIFY_TOKEN = "12345"
WHATSAPP_TOKEN = "EAAdLJb4aLuABRPlWtIyfFHFElSTGjX5ZB4n5fWwGcIOHbCOX2I2EgjZBLBV63KbncWk0ZA4avwo1EyPJRFCZCxP9YnZC50uJzWwNb9YGzMUUHRnrpn2Sj9cm0o2HrFF7C5v5GpWTWGcidSrIH0qofsJoZCFDdAyEIZCtBMjMvwIk1327fD6lW3FHtLMbZBb1ZCmHLKOQXASt4YOlkhjM4ZA3AsM2yZASZAXt6ZBTJ92dZAUTGa6Nw2KJAFbbw2qHJQvWRo75ZAhzroJdxH1qWAZCA8v8dhHH"
PHONE_NUMBER_ID = "1103694872823232"

openai.api_key = os.getenv("OPENAI_API_KEY")

# 🧠 USER STATE MEMORY (use DB in production)
user_state = {}

# 📤 SEND MESSAGE
def send_message(to, message):
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "messaging_product": "whatsapp",
        "to": to,
        "text": {"body": message}
    }
    requests.post(url, headers=headers, json=data)

# 🧠 INIT USER
def init_user(user_id):
    if user_id not in user_state:
        user_state[user_id] = {
            "greeted": False,
            "intent": None,
            "waiting_for_order": False,
            "order_confirmed": False,
            "order_stage": None  # preparing / served
        }

# 🧠 ORDER DETECTION
def is_order_message(text):
    return "total" in text.lower() and "€" in text

# 🔁 VERIFY
@app.route("/webhook", methods=["GET"])
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge")
    return "error"

# 📩 MAIN WEBHOOK
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.get_json()

    try:
        message = data['entry'][0]['changes'][0]['value']['messages'][0]
        user_id = message['from']
        text = message['text']['body'].strip()

        init_user(user_id)
        state = user_state[user_id]

        # 🟢 GREETING (ONLY ONCE)
        if not state["greeted"]:
            send_message(user_id, """Welcome to Agnikara 🍽️

How can I assist you today?

1. Order Food  
2. Book a Table  
3. Check Reservation""")
            state["greeted"] = True
            return "ok"

        # 🟡 ORDER FLOW START
        if text == "1":
            state["intent"] = "order"
            state["waiting_for_order"] = True

            send_message(user_id, """Perfect 😌

Explore our menu:
https://agnikara.netlify.app/#menu

Add items → Checkout → Send order here.""")
            return "ok"

        # 🔵 ORDER RECEIVED
        if is_order_message(text):
            state["waiting_for_order"] = False

            send_message(user_id, f"""Nice choice 😏

I’ve received your order.

What would you like to do?

1. Confirm Order  
2. Add More Items  
3. Modify Order""")
            return "ok"

        # 🟣 CONFIRM ORDER
        if text == "1" and state["intent"] == "order":
            state["order_confirmed"] = True
            state["order_stage"] = "preparing"

            send_message(user_id, """Perfect. Your order is now being prepared 🍽️

⏳ Estimated time: 15 minutes

I’ll update you shortly.""")
            return "ok"

        # 🟠 PREPARING UPDATE
        if state["order_stage"] == "preparing":
            state["order_stage"] = "served"

            send_message(user_id, """Update 😌

Your order is being prepared in the kitchen.

Almost ready.""")
            return "ok"

        # 🔴 SERVED
        if state["order_stage"] == "served":
            send_message(user_id, """Your order is ready and served 🍽️

Enjoy your meal.

Let me know if you need anything else.""")
            state["order_stage"] = None
            return "ok"

        # ➕ ADD MORE
        if text == "2":
            send_message(user_id, """Sure 😌

Add more items here:
https://agnikara.netlify.app/#menu

Send updated order here.""")
            return "ok"

    except Exception as e:
        print("Error:", e)

    return "ok"

# 🚀 RUN
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
