from flask import Flask, request
import requests

app = Flask(__name__)

VERIFY_TOKEN = "12345"
WHATSAPP_TOKEN = "EAAdLJb4aLuABRL4EN0xBOzUrZCWzDk4iTBpl66Kq78CvsXSQrBMVMzuGh2FCQmhjq2E2gXZC1uraQHOA9OyP6GoUlhfanr5OnESCfANknwz3yxFJq4d0U4yaPN49O9DxjV1a2UZAW0tjSrUTbALmoEE3k14Dwq66iVkgjS2ZBzHfbtgoLZBXhLNCKtQmbJ7GXnZCYjEd8PzJ1mk2sfZAmxR6dFkuMRcridkgAP5dR0fuSZChwYnrTytf1HIZA5PdInCxEhcZBTorgWlrqUTadWqyW3"
PHONE_NUMBER_ID = "1103694872823232"


# ✅ VERIFY WEBHOOK (GET)
@app.route('/webhook', methods=['GET'])
def verify():
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge, 200
    return "Verification failed", 403


# ✅ RECEIVE MESSAGES (POST)
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()

    try:
        message = data['entry'][0]['changes'][0]['value']['messages'][0]['text']['body']
        sender = data['entry'][0]['changes'][0]['value']['messages'][0]['from']

        reply = f"Order received ✅\n\nYou said: {message}"

        send_message(sender, reply)

    except Exception as e:
        print("Error:", e)

    return "OK", 200


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
        "text": {"body": text}
    }

    requests.post(url, headers=headers, json=payload)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
