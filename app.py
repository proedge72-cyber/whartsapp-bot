import json
import logging
import os
import re
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import requests
from flask import Flask, jsonify, request
from openai import OpenAI


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agnikara-ai")


MENU_URL = "https://agnikara.netlify.app/#menu"
DEFAULT_CURRENCY = os.getenv("CURRENCY_SYMBOL", "EUR")
WHATSAPP_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v20.0")
ORDER_PREP_MINUTES = int(os.getenv("ORDER_PREP_MINUTES", "15"))

VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID", "")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
RAZORPAY_BASE_URL = os.getenv("RAZORPAY_BASE_URL", "https://api.razorpay.com/v1/payment_links")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "")
SHEET_WEBHOOK_URL = os.getenv("SHEET_WEBHOOK_URL", "")

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


user_states: Dict[str, Dict[str, Any]] = {}
processed_message_ids = set()
state_lock = threading.Lock()


def create_default_state() -> Dict[str, Any]:
    return {
        "greeted": False,
        "intent": "none",
        "waiting_for_order": False,
        "order": {
            "name": "",
            "items": [],
            "total": Decimal("0.00"),
            "currency": DEFAULT_CURRENCY,
        },
        "order_confirmed": False,
        "payment_status": "pending",
        "payment_method": "",
        "payment_link": "",
        "payment_link_id": "",
        "awaiting_payment_choice": False,
        "awaiting_payment_confirmation": False,
        "order_stage": "none",
        "reservation_status": "none",
        "processed_messages": [],
        "last_message_at": None,
        "confirmed_at": None,
        "stage_updated_at": None,
    }


def get_state(user_id: str) -> Dict[str, Any]:
    with state_lock:
        if user_id not in user_states:
            user_states[user_id] = create_default_state()
        return user_states[user_id]


def normalize_text(text: str) -> str:
    return (text or "").strip()


def money_to_text(value: Decimal, currency: str) -> str:
    symbol = "\u20ac" if currency.upper() == "EUR" else currency
    quantized = value.quantize(Decimal("0.01"))
    if quantized == quantized.to_integral():
        return f"{symbol}{int(quantized)}"
    return f"{symbol}{quantized}"


def parse_decimal(raw_value: str) -> Decimal:
    cleaned = raw_value.replace(",", "").replace("\u20ac", "").replace("\u20b9", "").strip()
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return Decimal("0.00")


def detect_order_message(text: str) -> bool:
    lowered = text.lower()
    return (
        "\u20ac" in text
        or (
            "name:" in lowered
            and "items" in lowered
            and ("total" in lowered or "subtotal" in lowered)
        )
        or (
            "new restaurant order request" in lowered
            and "items:" in lowered
        )
    )


def parse_order_message(text: str) -> Optional[Dict[str, Any]]:
    normalized = normalize_text(text)
    if not normalized:
        return None

    name_match = re.search(r"Name\s*:\s*(.+)", normalized, flags=re.IGNORECASE)
    total_match = re.search(
        r"(?:Total|Subtotal)\s*:\s*[\u20ac\u20b9]?\s*([0-9]+(?:[.,][0-9]{1,2})?)",
        normalized,
        flags=re.IGNORECASE,
    )
    item_pattern = re.compile(
        r"^\s*[*\-\u2022]?\s*(?P<name>.+?)\s*x(?P<qty>\d+)(?:\s*\([\u20ac\u20b9]?[0-9]+(?:[.,][0-9]{1,2})?\s*each\))?\s*=\s*[\u20ac\u20b9]?\s*(?P<price>[0-9]+(?:[.,][0-9]{1,2})?)\s*$",
        flags=re.IGNORECASE,
    )

    items: List[Dict[str, Any]] = []
    for line in normalized.splitlines():
        match = item_pattern.match(line.strip())
        if not match:
            continue
        item_name = match.group("name").strip()
        qty = int(match.group("qty"))
        line_price = parse_decimal(match.group("price"))
        items.append(
            {
                "name": item_name,
                "qty": qty,
                "price": line_price,
            }
        )

    if not items:
        return None

    currency = "EUR" if "\u20ac" in normalized else DEFAULT_CURRENCY
    derived_total = sum((item["price"] for item in items), Decimal("0.00"))
    parsed_total = parse_decimal(total_match.group(1)) if total_match else derived_total

    return {
        "name": name_match.group(1).strip() if name_match else "",
        "items": items,
        "total": parsed_total if parsed_total > 0 else derived_total,
        "currency": currency,
    }


def merge_orders(existing_order: Dict[str, Any], incoming_order: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(existing_order)
    item_index = {item["name"].lower(): item for item in merged["items"]}

    if incoming_order.get("name"):
        merged["name"] = incoming_order["name"]
    if incoming_order.get("currency"):
        merged["currency"] = incoming_order["currency"]

    for new_item in incoming_order.get("items", []):
        key = new_item["name"].lower()
        if key in item_index:
            item_index[key]["qty"] += new_item["qty"]
            item_index[key]["price"] += new_item["price"]
        else:
            fresh_item = {
                "name": new_item["name"],
                "qty": new_item["qty"],
                "price": new_item["price"],
            }
            merged["items"].append(fresh_item)
            item_index[key] = fresh_item

    merged["total"] = sum((item["price"] for item in merged["items"]), Decimal("0.00"))
    return merged


def modify_order_from_text(order: Dict[str, Any], text: str) -> Tuple[Dict[str, Any], bool]:
    updated_order = deepcopy(order)
    lowered = text.lower()

    remove_match = re.search(r"remove\s+(.+)", lowered)
    if remove_match:
        target = remove_match.group(1).strip()
        original_count = len(updated_order["items"])
        updated_order["items"] = [
            item for item in updated_order["items"] if item["name"].lower() != target
        ]
        updated_order["total"] = sum((item["price"] for item in updated_order["items"]), Decimal("0.00"))
        return updated_order, len(updated_order["items"]) != original_count

    qty_match = re.search(r"(?:change|update|set)\s+(.+?)\s+to\s+(\d+)", lowered)
    if not qty_match:
        return updated_order, False

    target = qty_match.group(1).strip()
    new_qty = int(qty_match.group(2))

    for item in updated_order["items"]:
        if item["name"].lower() == target:
            unit_price = item["price"] / item["qty"] if item["qty"] else item["price"]
            item["qty"] = new_qty
            item["price"] = unit_price * new_qty
            updated_order["total"] = sum((line["price"] for line in updated_order["items"]), Decimal("0.00"))
            return updated_order, True

    return updated_order, False


def generate_order_summary(order: Dict[str, Any]) -> str:
    name = order.get("name") or "there"
    currency = order.get("currency", DEFAULT_CURRENCY)
    lines = [f"Here\u2019s your updated order, {name} \U0001f60f", ""]

    for item in order.get("items", []):
        lines.append(
            f"{item['name']} x{item['qty']} \u2014 {money_to_text(item['price'], currency)}"
        )

    lines.extend(
        [
            "",
            f"Total \u2014 {money_to_text(order.get('total', Decimal('0.00')), currency)}",
            "",
            "What would you like to do?",
            "1. Confirm Order",
            "2. Add More Items",
            "3. Modify Order",
        ]
    )
    return "\n".join(lines)


def truncate_response(text: str, max_words: int = 80) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).strip() + "..."


def send_whatsapp_message(to: str, body: str) -> None:
    if not WHATSAPP_TOKEN or not PHONE_NUMBER_ID:
        logger.warning("WhatsApp credentials missing. Skipping send to %s.", to)
        return

    url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"preview_url": True, "body": truncate_response(body)},
    }

    response = requests.post(url, headers=headers, json=payload, timeout=30)
    logger.info("WhatsApp send response %s: %s", response.status_code, response.text)
    response.raise_for_status()


def generate_payment_link(user_id: str, state: Dict[str, Any]) -> str:
    if state["payment_link"]:
        return state["payment_link"]

    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        fallback = f"{PUBLIC_BASE_URL.rstrip('/')}/pay/{user_id}" if PUBLIC_BASE_URL else "Payment link unavailable."
        state["payment_link"] = fallback
        state["payment_link_id"] = ""
        return fallback

    order = state["order"]
    amount_minor = int((order["total"] * Decimal("100")).quantize(Decimal("1")))
    payload = {
        "amount": amount_minor,
        "currency": order.get("currency", DEFAULT_CURRENCY),
        "accept_partial": False,
        "description": f"Agnikara order for {order.get('name') or user_id}",
        "customer": {
            "name": order.get("name") or "Guest",
        },
        "notify": {"sms": False, "email": False},
        "reference_id": f"agnikara-{user_id}-{int(datetime.now(timezone.utc).timestamp())}",
        "callback_method": "get",
    }

    response = requests.post(
        RAZORPAY_BASE_URL,
        auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
        json=payload,
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    state["payment_link"] = data.get("short_url", "")
    state["payment_link_id"] = data.get("id", "")
    return state["payment_link"]


def verify_online_payment(state: Dict[str, Any]) -> bool:
    payment_link_id = state.get("payment_link_id", "")
    if not payment_link_id or not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        return False

    verify_url = f"{RAZORPAY_BASE_URL.rstrip('/')}/{payment_link_id}"
    response = requests.get(
        verify_url,
        auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    status = str(data.get("status", "")).lower()
    payments = data.get("payments") or []

    if status == "paid":
        return True

    for payment in payments:
        if str(payment.get("status", "")).lower() == "captured":
            return True

    return False


def append_sheet_log(user_id: str, state: Dict[str, Any], event_type: str) -> None:
    if not SHEET_WEBHOOK_URL:
        return

    payload = {
        "user_id": user_id,
        "event_type": event_type,
        "state": serialize_state(state),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        requests.post(SHEET_WEBHOOK_URL, json=payload, timeout=15)
    except requests.RequestException as exc:
        logger.warning("Sheet webhook failed: %s", exc)


def serialize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    safe = deepcopy(state)
    safe["order"]["total"] = str(safe["order"]["total"])
    for item in safe["order"]["items"]:
        item["price"] = str(item["price"])
    for key in ("last_message_at", "confirmed_at", "stage_updated_at"):
        if safe.get(key):
            safe[key] = safe[key].isoformat()
    return safe


def should_ignore_duplicate(message_id: str) -> bool:
    with state_lock:
        if message_id in processed_message_ids:
            return True
        processed_message_ids.add(message_id)
        if len(processed_message_ids) > 10000:
            processed_message_ids.clear()
            processed_message_ids.add(message_id)
        return False


def classify_intent_with_ai(user_message: str) -> str:
    if not openai_client:
        return "none"

    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {
                    "role": "system",
                    "content": (
                        "Classify the restaurant message into exactly one word: "
                        "order, reservation, reservation_check, payment, status, none."
                    ),
                },
                {"role": "user", "content": user_message},
            ],
            max_output_tokens=10,
        )
        label = response.output_text.strip().lower()
        return label if label in {"order", "reservation", "reservation_check", "payment", "status", "none"} else "none"
    except Exception as exc:
        logger.warning("OpenAI intent classification failed: %s", exc)
        return "none"


def generate_ai_reply(user_message: str, state: Dict[str, Any]) -> Optional[str]:
    if not openai_client:
        return None

    system_prompt = (
        "You are Agnikara Restaurant AI, a premium WhatsApp receptionist. "
        "Keep replies under 60 words. Never show the full menu in chat. "
        f"Always use this menu link when menu is relevant: {MENU_URL}. "
        "Be calm, structured, and concise. Do not invent menu items or reservation details."
    )

    state_snapshot = json.dumps(serialize_state(state))

    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "system", "content": f"User state: {state_snapshot}"},
                {"role": "user", "content": user_message},
            ],
            max_output_tokens=120,
        )
        return truncate_response(response.output_text.strip(), max_words=60)
    except Exception as exc:
        logger.warning("OpenAI reply generation failed: %s", exc)
        return None


def refresh_order_stage(state: Dict[str, Any]) -> None:
    confirmed_at = state.get("confirmed_at")
    if not confirmed_at or state["order_stage"] == "served":
        return

    elapsed = datetime.now(timezone.utc) - confirmed_at
    if elapsed >= timedelta(minutes=ORDER_PREP_MINUTES):
        state["order_stage"] = "served"
    elif elapsed >= timedelta(minutes=1):
        state["order_stage"] = "preparing"


def greeting_message() -> str:
    return (
        "Welcome to Agnikara \U0001f37d\ufe0f\n\n"
        "How can I assist you today?\n\n"
        "1. Order Food\n"
        "2. Book a Table\n"
        "3. Check Reservation"
    )


def order_instruction_message() -> str:
    return (
        "Perfect \U0001f60c\n\n"
        f"Explore our menu:\n{MENU_URL}\n\n"
        "Add items \u2192 Checkout \u2192 Send your order here."
    )


def payment_prompt_message() -> str:
    return (
        "How would you like to pay?\n\n"
        "1. Pay Online \U0001f4b3\n"
        "2. Pay at Counter"
    )


def handle_payment_choice(user_id: str, state: Dict[str, Any], text: str) -> str:
    normalized = text.strip().lower()
    if normalized in {"1", "pay online", "online", "online payment"}:
        link = generate_payment_link(user_id, state)
        state["payment_method"] = "online"
        state["payment_status"] = "pending"
        state["awaiting_payment_choice"] = False
        state["awaiting_payment_confirmation"] = True
        if link == "Payment link unavailable.":
            state["awaiting_payment_confirmation"] = False
            state["awaiting_payment_choice"] = True
            return "Online payment is unavailable right now. Please choose 1 or 2."
        return f"Your secure payment link is ready \U0001f4b3\n{link}\n\nReply 'paid' or 'yes' once done."

    if normalized in {"2", "pay at counter", "counter"}:
        state["payment_method"] = "counter"
        state["payment_status"] = "done"
        state["awaiting_payment_choice"] = False
        state["awaiting_payment_confirmation"] = False
        finalize_confirmation(state)
        return "Perfect. Payment is marked for counter. Your order is now being prepared \U0001f37d\ufe0f\n\n\u23f3 Estimated time: 15 minutes"

    if normalized in {"paid", "yes", "done", "completed"}:
        if state.get("payment_method") == "online":
            try:
                if verify_online_payment(state):
                    state["payment_status"] = "done"
                    state["awaiting_payment_choice"] = False
                    state["awaiting_payment_confirmation"] = False
                    finalize_confirmation(state)
                    return "Payment received. Your order is now being prepared \U0001f37d\ufe0f\n\n\u23f3 Estimated time: 15 minutes"
            except requests.RequestException as exc:
                logger.warning("Payment verification failed: %s", exc)
                return "I couldn’t verify the payment yet. Please wait a moment and reply 'paid' again."

            return "I can’t confirm the payment yet. Please complete the payment link, then reply 'paid'."

        return "Please use the payment option shown above."

    return "Please choose 1 or 2 for payment."


def finalize_confirmation(state: Dict[str, Any]) -> None:
    state["order_confirmed"] = True
    state["order_stage"] = "preparing"
    state["awaiting_payment_choice"] = False
    state["awaiting_payment_confirmation"] = False
    now = datetime.now(timezone.utc)
    state["confirmed_at"] = now
    state["stage_updated_at"] = now
    state["waiting_for_order"] = False


def handle_status_request(state: Dict[str, Any]) -> str:
    refresh_order_stage(state)
    if state["order_stage"] == "none":
        return "I don\u2019t have an active order yet. Send your order after checkout and I\u2019ll take it from there."
    if state["order_stage"] == "preparing":
        return "Update \U0001f60c\n\nYour order is currently being prepared in the kitchen."
    return "Your order is ready and has been served \U0001f37d\ufe0f\n\nEnjoy your meal."


def handle_reservation_flow(state: Dict[str, Any], text: str) -> str:
    state["intent"] = "reservation"
    lowered = text.lower().strip()

    if lowered in {"2", "book a table", "table", "reservation"}:
        state["reservation_status"] = "pending_details"
        return "Please share your name, date, time, and guest count."

    if lowered in {"3", "check reservation", "reservation check"}:
        state["reservation_status"] = "checking"
        return "Please share your reservation name and date. I\u2019ll help you check it."

    ai_reply = generate_ai_reply(text, state)
    return ai_reply or "Please share your reservation details and I\u2019ll help you next."


def handle_order_flow(user_id: str, state: Dict[str, Any], text: str) -> str:
    lowered = text.lower().strip()
    refresh_order_stage(state)

    if state["awaiting_payment_choice"]:
        return handle_payment_choice(user_id, state, lowered)
    if state["awaiting_payment_confirmation"]:
        return handle_payment_choice(user_id, state, lowered)

    if lowered == "1" and not state["order"]["items"]:
        state["intent"] = "order"
        state["waiting_for_order"] = True
        return order_instruction_message()

    if lowered in {"2", "add more items"}:
        state["intent"] = "order"
        state["waiting_for_order"] = True
        return f"Of course.\n\nPlease add more items from {MENU_URL} and send the updated checkout here."

    if lowered in {"3", "modify order", "modify"}:
        state["intent"] = "order"
        return "Tell me the change in one line. Example: remove pasta alfredo or change margherita pizza to 1."

    if lowered in {"1", "confirm order", "confirm"} and state["order"]["items"] and not state["order_confirmed"]:
        state["awaiting_payment_choice"] = True
        state["awaiting_payment_confirmation"] = False
        return payment_prompt_message()

    if lowered.startswith(("remove ", "change ", "update ", "set ")):
        updated_order, changed = modify_order_from_text(state["order"], text)
        if changed:
            state["order"] = updated_order
            return generate_order_summary(state["order"])
        return "I couldn\u2019t match that item. Please use the exact item name from your checkout."

    if detect_order_message(text):
        parsed_order = parse_order_message(text)
        if not parsed_order:
            return "I couldn\u2019t read that order clearly. Please resend it in the checkout format with name, items, and total."

        state["intent"] = "order"
        state["waiting_for_order"] = False
        state["order"] = merge_orders(state["order"], parsed_order)
        state["order_confirmed"] = False
        state["payment_status"] = "pending"
        state["payment_method"] = ""
        state["payment_link"] = ""
        state["payment_link_id"] = ""
        state["awaiting_payment_choice"] = False
        state["awaiting_payment_confirmation"] = False
        append_sheet_log(user_id, state, "order_updated")
        return generate_order_summary(state["order"])

    if lowered in {"status", "track order", "where is my order", "preparing", "served"}:
        return handle_status_request(state)

    if state["order_confirmed"]:
        return handle_status_request(state)

    ai_reply = generate_ai_reply(text, state)
    return ai_reply or "If you\u2019d like to order, use the menu link, checkout, and send the order here."


def infer_intent(text: str) -> str:
    lowered = text.lower().strip()
    if lowered in {"1", "order", "order food"} or detect_order_message(text):
        return "order"
    if lowered in {"2", "book a table", "table", "reservation"}:
        return "reservation"
    if lowered in {"3", "check reservation", "reservation check"}:
        return "reservation_check"
    if lowered in {"status", "track order", "where is my order", "preparing", "served"}:
        return "status"
    return classify_intent_with_ai(text)


def build_reply(user_id: str, text: str) -> str:
    state = get_state(user_id)
    state["last_message_at"] = datetime.now(timezone.utc)

    if not state["greeted"]:
        state["greeted"] = True
        stripped = text.strip()
        if stripped.lower() in {"1", "2", "3"}:
            initial = greeting_message()
            follow_up = route_message_by_intent(user_id, stripped, state_override=state)
            return f"{initial}\n\n{follow_up}"
        return greeting_message()

    return route_message_by_intent(user_id, text)


def route_message_by_intent(user_id: str, text: str, state_override: Optional[Dict[str, Any]] = None) -> str:
    state = state_override or get_state(user_id)
    intent = infer_intent(text)
    if intent in {"order", "payment", "status"} or state["intent"] == "order" or state["order"]["items"]:
        return handle_order_flow(user_id, state, text)
    if intent in {"reservation", "reservation_check"}:
        return handle_reservation_flow(state, text)

    ai_reply = generate_ai_reply(text, state)
    return ai_reply or "Please reply with 1 to order food, 2 to book a table, or 3 to check a reservation."


def extract_message_payload(payload: Dict[str, Any]) -> Optional[Tuple[str, str, str]]:
    try:
        entry = payload.get("entry", [])
        changes = entry[0].get("changes", [])
        value = changes[0].get("value", {})
        messages = value.get("messages", [])
        if not messages:
            return None

        message = messages[0]
        message_id = message.get("id", "")
        sender = message.get("from", "")
        text = message.get("text", {}).get("body", "")
        if not sender or not text or not message_id:
            return None
        return message_id, sender, text
    except (IndexError, AttributeError, KeyError, TypeError):
        return None


@app.get("/")
def healthcheck() -> Tuple[Dict[str, str], int]:
    return jsonify({"status": "ok", "service": "Agnikara Restaurant AI"}), 200


@app.get("/webhook")
def verify_webhook() -> Tuple[str, int]:
    mode = request.args.get("hub.mode")
    token = request.args.get("hub.verify_token")
    challenge = request.args.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN:
        return challenge or "", 200
    return "Verification failed", 403


@app.post("/webhook")
def receive_webhook() -> Tuple[str, int]:
    payload = request.get_json(silent=True) or {}
    logger.info("Incoming payload: %s", payload)

    extracted = extract_message_payload(payload)
    if not extracted:
        return "EVENT_RECEIVED", 200

    message_id, sender, text = extracted
    if should_ignore_duplicate(message_id):
        logger.info("Duplicate message ignored: %s", message_id)
        return "EVENT_RECEIVED", 200

    try:
        reply = build_reply(sender, text)
        send_whatsapp_message(sender, reply)
        append_sheet_log(sender, get_state(sender), "message_handled")
    except requests.RequestException as exc:
        logger.exception("External API error: %s", exc)
    except Exception as exc:
        logger.exception("Unhandled webhook error: %s", exc)

    return "EVENT_RECEIVED", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
