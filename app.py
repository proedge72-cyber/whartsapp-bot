from flask import Flask, request, jsonify
import requests
import os

app = Flask(__name__)

# ===== CONFIG =====
VERIFY_TOKEN = "12345"
WHATSAPP_TOKEN = "EAAdLJb4aLuABRL4EN0xBOzUrZCWzDk4iTBpl66Kq78CvsXSQrBMVMzuGh2FCQmhjq2E2gXZC1uraQHOA9OyP6GoUlhfanr5OnESCfANknwz3yxFJq4d0U4yaPN49O9DxjV1a2UZAW0tjSrUTbALmoEE3k14Dwq66iVkgjS2ZBzHfbtgoLZBXhLNCKtQmbJ7GXnZCYjEd8PzJ1mk2sfZAmxR6dFkuMRcridkgAP5dR0fuSZChwYnrTytf1HIZA5PdInCxEhcZBTorgWlrqUTadWqyW3"
PHONE_NUMBER_ID = "1103694872823232"

# ===== VERIFY WEBHOOK =====
@app.route('/webhook', methods=['GET'])
def verify():
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if token == VERIFY_TOKEN:
        return challenge
    return "Verification failed", 403


# ===== HANDLE INCOMING MESSAGES =====
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()

    try:
        message = data['entry'][0]['changes'][0]['value']['messages'][0]['text']['body']
        sender = data['entry'][0]['changes'][0]['value']['messages'][0]['from']

        print("Message:", message)

        # ===== SIMPLE LOGIC =====
        reply = f"Order received ✅\n\nYou said: {message}\n\nWe will confirm shortly."

        send_message(sender, reply)

    except Exception as e:
        print("Error:", e)

    return "OK", 200


# ===== SEND MESSAGE FUNCTION =====
def send_message(to, text):
    url = f"https://graph.facebook.com/v18.0/{PHONE_NUMBER_ID}/messages"

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json"
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {
            "body": text
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    print("Response:", response.text)


# ===== RUN SERVER =====
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)