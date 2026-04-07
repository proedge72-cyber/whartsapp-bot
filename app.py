from flask import Flask, request
import requests
import os
from openai import OpenAI

app = Flask(__name__)

# ===== CONFIG =====
VERIFY_TOKEN = "12345"
WHATSAPP_TOKEN = "EAAdLJb4aLuABRL4EN0xBOzUrZCWzDk4iTBpl66Kq78CvsXSQrBMVMzuGh2FCQmhjq2E2gXZC1uraQHOA9OyP6GoUlhfanr5OnESCfANknwz3yxFJq4d0U4yaPN49O9DxjV1a2UZAW0tjSrUTbALmoEE3k14Dwq66iVkgjS2ZBzHfbtgoLZBXhLNCKtQmbJ7GXnZCYjEd8PzJ1mk2sfZAmxR6dFkuMRcridkgAP5dR0fuSZChwYnrTytf1HIZA5PdInCxEhcZBTorgWlrqUTadWqyW3"
PHONE_NUMBER_ID = "1103694872823232"

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# ===== FULL MENU (UNCHANGED — I DID NOT TOUCH IT) =====
MENU = {
    "Pantry & Pairings": {
        "Riso & Biryani": [
            {"name": "Steamed Basmati Rice", "price": "4"},
            {"name": "Jeera Rice", "price": "5"},
            {"name": "Veg Biryani", "price": "7"},
            {"name": "Chicken Biryani", "price": "8"},
            {"name": "Lamb Biryani", "price": "10"},
            {"name": "Prawn Biryani", "price": "10"}
        ],
        "Pane Indiano": [
            {"name": "Naan", "price": "1.50"},
            {"name": "Butter Naan", "price": "2"},
            {"name": "Garlic Naan", "price": "2.50"},
            {"name": "Cheese Naan", "price": "3"},
            {"name": "Aloo Kulcha", "price": "2.50"},
            {"name": "Amritsari Kulcha", "price": "3"}
        ],
        "Bevande": [
            {"name": "Acqua Naturale", "price": "3"},
            {"name": "Acqua Frizzante", "price": "3.50"},
            {"name": "Coca-Cola Zero", "price": "3"},
            {"name": "Coca-Cola", "price": "2.50"},
            {"name": "Redbull", "price": "3"},
            {"name": "Te Pesca/Limone", "price": "2.50"},
            {"name": "Birra Moretti", "price": "3/5"},
            {"name": "Corona", "price": "4"},
            {"name": "Kingfisher", "price": "4/6"},
            {"name": "Peroni", "price": "4/6"},
            {"name": "Mango Lassi", "price": "3"},
            {"name": "Sweet Lassi Classico", "price": "2.50"},
            {"name": "Roohafza", "price": "2.50"},
            {"name": "Lemon Soda", "price": "2"},
            {"name": "Masala Tea", "price": "2.50"}
        ],
        "Dolci": [
            {"name": "Tiramisu Classico", "price": "5"},
            {"name": "Gulab Jamun Caldo", "price": "4"},
            {"name": "Jalebi", "price": "3.50"},
            {"name": "Gelato", "price": "3"},
            {"name": "Gelato Vegano", "price": "3.50"}
        ]
    },

    "Curries & Classics": {
        "Piatti Vegetariani": [
            {"name": "Paneer Butter Masala", "price": "10"},
            {"name": "Shahi Paneer", "price": "10"},
            {"name": "Matar Paneer Classico", "price": "9"},
            {"name": "Kadhai Paneer", "price": "10"},
            {"name": "Palak Paneer Cremoso", "price": "11"},
            {"name": "Mushroom Masala", "price": "10"},
            {"name": "Punjabi Rajma", "price": "9"},
            {"name": "Malai Kofta Imperiale", "price": "12"},
            {"name": "Mix Veg Affumicato", "price": "9"},
            {"name": "Amritsari Chana Masala", "price": "8"},
            {"name": "Baingan Bharta", "price": "11"},
            {"name": "Dal Makhani Aromatico", "price": "8"},
            {"name": "Dal Punjabi", "price": "9"}
        ],
        "Piatti con Pollo": [
            {"name": "Butter Chicken", "price": "10"},
            {"name": "Tikka Masala Classico", "price": "11"},
            {"name": "Chicken Kadhai", "price": "10"},
            {"name": "Chicken Curry Classico", "price": "9"},
            {"name": "Chicken Korma Cremoso", "price": "10"},
            {"name": "Chicken Madras", "price": "12"}
        ],
        "Piatti con Agnello": [
            {"name": "Lamb Madras", "price": "12"},
            {"name": "Lamb Curry Classico", "price": "11"}
        ],
        "Piatti con Pesce": [
            {"name": "Fish Curry Classico", "price": "11"},
            {"name": "Prawn Curry Imperiale", "price": "12"}
        ]
    },

    "Street & Tandoor": {
        "Antipasti Non Vegetariani": [
            {"name": "Indo Chilli Chicken", "price": "9"},
            {"name": "Chicken Pakoda Dorato", "price": "7"},
            {"name": "Fish Pakoda Marino", "price": "8"},
            {"name": "Keema Samosa Imperiale", "price": "6"}
        ],
        "Antipasti Vegetariani": [
            {"name": "Indo Chilli Paneer", "price": "8"},
            {"name": "Paneer Pakoda Croccante", "price": "9"},
            {"name": "Mix Pakoda Croccante", "price": "5"},
            {"name": "Punjabi Samosa Classic", "price": "4"},
            {"name": "Honey Chilli Potato", "price": "4"},
            {"name": "Cheese Balls Croccanti", "price": "4"},
            {"name": "Agni Veg Platter", "price": "10"},
            {"name": "Spring Roll Croccanti", "price": "7"}
        ],
        "Agni's Street Specials": [
            {"name": "Desi Chowmin", "price": "7"},
            {"name": "Bombay Bhel Puri", "price": "8"},
            {"name": "Tikki Chaat Classico", "price": "5"},
            {"name": "Golgappe", "price": "5"}
        ],
        "Specialita al Tandoor": [
            {"name": "Chicken Tikka Classico", "price": "10"},
            {"name": "Tandoori Chicken", "price": "13"},
            {"name": "Malai Tikka Cremoso", "price": "11"},
            {"name": "Green Haryali Tikka", "price": "11"},
            {"name": "Fish Tikka Marino", "price": "12"},
            {"name": "King Prawns Imperiali", "price": "13"},
            {"name": "Paneer Tikka Affumicato", "price": "12"},
            {"name": "Mixed Grill Imperiale", "price": "15"}
        ]
    }
}

# ===== FORMAT MENU =====
def format_menu():
    text = ""
    for section, categories in MENU.items():
        text += f"\n=== {section} ===\n"
        for category, items in categories.items():
            text += f"\n{category}:\n"
            for item in items:
                text += f"- {item['name']} (₹{item['price']})\n"
    return text


# ===== AI FUNCTION =====
def generate_ai_reply(user_message):
    menu_text = format_menu()

    prompt = f"""
You are an AI restaurant assistant.

Rules:
- Only suggest items from menu
- Keep replies short
- Ask quantity if missing
- Confirm order clearly

MENU:
{menu_text}

Customer: {user_message}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "Professional restaurant assistant."},
            {"role": "user", "content": prompt}
        ]
    )

    return response.choices[0].message.content


# ===== VERIFY =====
@app.route('/webhook', methods=['GET'])
def verify():
    if request.args.get("hub.verify_token") == VERIFY_TOKEN:
        return request.args.get("hub.challenge"), 200
    return "Error", 403


# ===== HANDLE INCOMING MESSAGES (POST) =====
@app.route('/webhook', methods=['POST'])
def webhook():
    data = request.get_json()

    try:
        print("FULL DATA:", data)  # 🔥 DEBUG EVERYTHING

        if 'messages' in data['entry'][0]['changes'][0]['value']:
            message_data = data['entry'][0]['changes'][0]['value']['messages'][0]

            if 'text' in message_data:
                message = message_data['text']['body']
                sender = message_data['from']

                print("User message:", message)

                message = message.lower()

                # ===== CONTROL LOGIC =====
                if any(word in message for word in ["menu", "website", "link"]):
                    reply = """Visit our website:
https://agnikara.netlify.app/

Menu:
https://agnikara.netlify.app/#menu"""

                elif "not talk to human" in message:
                    reply = """Order directly here:
https://agnikara.netlify.app/#menu"""

                else:
                    reply = generate_ai_reply(message)

                send_message(sender, reply)

        else:
            print("No message found (status update or other event)")

    except Exception as e:
        print("ERROR:", str(e))

    return "OK", 200
# ===== SEND =====
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


# ===== RUN =====
if __name__ == '__main__':
    app.run(host="0.0.0.0", port=10000)
