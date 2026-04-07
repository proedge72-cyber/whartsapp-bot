from flask import Flask, request
import requests
import openai
import os

app = Flask(__name__)

# 🔑 Keys
VERIFY_TOKEN = "12345"
WHATSAPP_TOKEN = "EAAdLJb4aLuABRPlWtIyfFHFElSTGjX5ZB4n5fWwGcIOHbCOX2I2EgjZBLBV63KbncWk0ZA4avwo1EyPJRFCZCxP9YnZC50uJzWwNb9YGzMUUHRnrpn2Sj9cm0o2HrFF7C5v5GpWTWGcidSrIH0qofsJoZCFDdAyEIZCtBMjMvwIk1327fD6lW3FHtLMbZBb1ZCmHLKOQXASt4YOlkhjM4ZA3AsM2yZASZAXt6ZBTJ92dZAUTGa6Nw2KJAFbbw2qHJQvWRo75ZAhzroJdxH1qWAZCA8v8dhHH"
PHONE_NUMBER_ID = "1103694872823232"


openai.api_key = os.getenv("OPENAI_API_KEY")

# 🧠 Temporary memory (use DB in real system)
user_state = {}

# 📤 Send WhatsApp message
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


# 🧠 AI RESPONSE FUNCTION
def get_ai_response(user_id, user_message):

    state = user_state.get(user_id, {
        "has_table": None,
        "order_started": False
    })

    system_prompt = f"""
You are a professional restaurant receptionist AI for Agnikara.

STRICT RULES:
- Never show full menu in chat
- Always send this link for menu: https://agnikara.netlify.app/#menu
- Guide user step by step
- Be short, polite, premium tone
- If user sends order details, summarize and confirm
- If user already has table, don't ask to book table

USER STATE:
has_table = {state['has_table']}
order_started = {state['order_started']}
"""

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message}
        ]
    )

    return response['choices'][0]['message']['content']


# 🔁 WEBHOOK VERIFY
@app.route("/webhook", methods=["GET"])
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge")
    return "Verification failed"


# 📩 WEBHOOK RECEIVE MESSAGE
@app.route("/webhook", methods=["POST"])
def webhook():

    data = request.get_json()

    try:
        message = data['entry'][0]['changes'][0]['value']['messages'][0]
        user_id = message['from']
        text = message['text']['body'].lower()

        # 🧠 Initialize user
        if user_id not in user_state:
            user_state[user_id] = {
                "has_table": None,
                "order_started": False
            }

        # 🎯 FIRST MESSAGE LOGIC
        if text in ["hi", "hello", "hey"]:
            reply = """Welcome to Agnikara 🍽️

How may I assist you today?

1. View Menu & Order Food  
2. Book a Table  
3. Check Reservation  

Reply with 1, 2 or 3."""
            send_message(user_id, reply)
            return "ok"

        # 🍽️ USER CHOOSES ORDER
        if text == "1":
            user_state[user_id]["order_started"] = True

            reply = """Great choice 😌

Explore our menu here:
https://agnikara.netlify.app/#menu

👉 Add items to cart  
👉 Checkout  
👉 Send order here for confirmation"""
            send_message(user_id, reply)
            return "ok"

        # 🪑 USER CHOOSES TABLE
        if text == "2":
            user_state[user_id]["has_table"] = True

            reply = """Please share:

Name:
Date:
Time:
Guests:"""
            send_message(user_id, reply)
            return "ok"

        # 📦 DETECT ORDER MESSAGE (very simple detection)
        if "total" in text and "items" in text:
            state = user_state[user_id]

            reply = f"""Thank you 😌

I’ve received your order.

Would you like to:
1. Confirm Order ✅
2. Add More Items ➕
3. Cancel ❌"""

            send_message(user_id, reply)
            return "ok"

        # 🧠 FALLBACK → AI
        ai_reply = get_ai_response(user_id, text)
        send_message(user_id, ai_reply)

    except Exception as e:
        print("Error:", e)

    return "ok"


if __name__ == "__main__":
    app.run(port=5000)
