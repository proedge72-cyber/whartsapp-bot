import json
import logging
import os
import re
import sqlite3
import threading
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
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
DB_PATH = Path(os.getenv("STATE_DB_PATH", "agnikara_state.db"))

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
HUMAN_HANDOFF_CONTACT = os.getenv("HUMAN_HANDOFF_CONTACT", "")

openai_client = OpenAI(api_key=OPENAI_API_KEY) if OPENAI_API_KEY else None


STAGE_MAIN_MENU = "awaiting_main_choice"
STAGE_CHECKOUT = "awaiting_checkout"
STAGE_ORDER_ACTION = "awaiting_order_action"
STAGE_PAYMENT_CHOICE = "awaiting_payment_choice"
STAGE_PAYMENT_CONFIRMATION = "awaiting_payment_confirmation"
STAGE_PREPARING = "preparing"
STAGE_SERVED = "served"
STAGE_RESERVATION_DETAILS = "reservation_pending_details"
STAGE_RESERVATION_CHECK = "reservation_check_pending"
STAGE_HUMAN_HANDOFF = "human_handoff"

SUPPORTED_ACTIONS = {
    "none",
    "show_greeting",
    "show_menu_link",
    "summarize_order",
    "confirm_order",
    "add_more_items",
    "modify_order",
    "remove_item",
    "update_quantity",
    "send_payment_link",
    "check_payment_status",
    "pay_at_counter",
    "check_order_stage",
    "book_table",
    "check_reservation",
    "handoff_to_human",
}

db_lock = threading.Lock()
processed_message_ids = set()


def get_db_connection() -> sqlite3.Connection:
    connection = sqlite3.connect(DB_PATH, check_same_thread=False)
    connection.row_factory = sqlite3.Row
    return connection


db = get_db_connection()


def initialize_db() -> None:
    with db_lock:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS user_states (
                user_id TEXT PRIMARY KEY,
                state_json TEXT NOT NULL,
                updated_at TEXT NOT NULL
            )
            """
        )
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS reservations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            )
            """
        )
        db.commit()


initialize_db()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def create_default_state() -> Dict[str, Any]:
    return {
        "greeted": False,
        "intent": "none",
        "stage": STAGE_MAIN_MENU,
        "previous_stage": STAGE_MAIN_MENU,
        "waiting_for_order": False,
        "checkout_mode": "fresh",
        "order": {
            "name": "",
            "items": [],
            "total": Decimal("0.00"),
            "currency": DEFAULT_CURRENCY,
        },
        "customer_profile": {
            "name": "",
            "mobile": "",
            "email": "",
            "service_type": "",
            "preferred_time": "",
            "guests": "",
        },
        "order_confirmed": False,
        "payment_status": "pending",
        "payment_method": "",
        "payment_link": "",
        "payment_link_id": "",
        "payment_verification_attempts": 0,
        "order_stage": "none",
        "reservation_status": "none",
        "reservation_details": {},
        "handoff_requested": False,
        "failure_count": 0,
        "last_ai_action": "",
        "response_counters": {},
        "last_message_at": None,
        "confirmed_at": None,
        "stage_updated_at": None,
    }


def serialize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    safe = deepcopy(state)
    safe["order"]["total"] = str(safe["order"]["total"])
    for item in safe["order"]["items"]:
        item["price"] = str(item["price"])
    for key in ("last_message_at", "confirmed_at", "stage_updated_at"):
        if safe.get(key):
            safe[key] = safe[key].isoformat()
    return safe


def deserialize_state(state_json: str) -> Dict[str, Any]:
    state = json.loads(state_json)
    state["order"]["total"] = Decimal(state["order"].get("total", "0.00"))
    for item in state["order"]["items"]:
        item["price"] = Decimal(item.get("price", "0.00"))
    for key in ("last_message_at", "confirmed_at", "stage_updated_at"):
        if state.get(key):
            state[key] = datetime.fromisoformat(state[key])
    return state


def save_state(user_id: str, state: Dict[str, Any]) -> None:
    payload = json.dumps(serialize_state(state))
    with db_lock:
        db.execute(
            """
            INSERT INTO user_states (user_id, state_json, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                state_json = excluded.state_json,
                updated_at = excluded.updated_at
            """,
            (user_id, payload, utc_now().isoformat()),
        )
        db.commit()


def get_state(user_id: str) -> Dict[str, Any]:
    with db_lock:
        row = db.execute(
            "SELECT state_json FROM user_states WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    if not row:
        state = create_default_state()
        save_state(user_id, state)
        return state
    return deserialize_state(row["state_json"])


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
        or ("name:" in lowered and "items" in lowered and ("total" in lowered or "subtotal" in lowered))
        or ("new restaurant order request" in lowered and "items:" in lowered)
    )


def parse_order_message(text: str) -> Optional[Dict[str, Any]]:
    normalized = normalize_text(text)
    if not normalized:
        return None

    fields = {
        "name": re.search(r"Name\s*:\s*(.+)", normalized, flags=re.IGNORECASE),
        "mobile": re.search(r"Mobile\s*:\s*(.+)", normalized, flags=re.IGNORECASE),
        "email": re.search(r"Email\s*:\s*(.+)", normalized, flags=re.IGNORECASE),
        "service_type": re.search(r"Service Type\s*:\s*(.+)", normalized, flags=re.IGNORECASE),
        "preferred_time": re.search(r"Preferred Time\s*:\s*(.+)", normalized, flags=re.IGNORECASE),
        "guests": re.search(r"Guests\s*:\s*(.+)", normalized, flags=re.IGNORECASE),
    }
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
        items.append(
            {
                "name": match.group("name").strip(),
                "qty": int(match.group("qty")),
                "price": parse_decimal(match.group("price")),
            }
        )

    if not items:
        return None

    currency = "EUR" if "\u20ac" in normalized else DEFAULT_CURRENCY
    derived_total = sum((item["price"] for item in items), Decimal("0.00"))
    parsed_total = parse_decimal(total_match.group(1)) if total_match else derived_total
    name_value = fields["name"].group(1).strip() if fields["name"] else ""

    return {
        "name": name_value,
        "items": items,
        "total": parsed_total if parsed_total > 0 else derived_total,
        "currency": currency,
        "profile": {
            "name": name_value,
            "mobile": fields["mobile"].group(1).strip() if fields["mobile"] else "",
            "email": fields["email"].group(1).strip() if fields["email"] else "",
            "service_type": fields["service_type"].group(1).strip() if fields["service_type"] else "",
            "preferred_time": fields["preferred_time"].group(1).strip() if fields["preferred_time"] else "",
            "guests": fields["guests"].group(1).strip() if fields["guests"] else "",
        },
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
            fresh = {
                "name": new_item["name"],
                "qty": new_item["qty"],
                "price": new_item["price"],
            }
            merged["items"].append(fresh)
            item_index[key] = fresh
    merged["total"] = sum((item["price"] for item in merged["items"]), Decimal("0.00"))
    return merged


def modify_order_from_text(order: Dict[str, Any], text: str) -> Tuple[Dict[str, Any], bool]:
    updated = deepcopy(order)
    lowered = text.lower()
    remove_match = re.search(r"remove\s+(.+)", lowered)
    if remove_match:
        target = remove_match.group(1).strip()
        original_count = len(updated["items"])
        updated["items"] = [item for item in updated["items"] if item["name"].lower() != target]
        updated["total"] = sum((item["price"] for item in updated["items"]), Decimal("0.00"))
        return updated, len(updated["items"]) != original_count

    qty_match = re.search(r"(?:change|update|set)\s+(.+?)\s+to\s+(\d+)", lowered)
    if not qty_match:
        return updated, False

    target = qty_match.group(1).strip()
    new_qty = int(qty_match.group(2))
    for item in updated["items"]:
        if item["name"].lower() == target:
            unit_price = item["price"] / item["qty"] if item["qty"] else item["price"]
            item["qty"] = new_qty
            item["price"] = unit_price * new_qty
            updated["total"] = sum((line["price"] for line in updated["items"]), Decimal("0.00"))
            return updated, True
    return updated, False


def update_customer_profile(state: Dict[str, Any], profile_data: Dict[str, str]) -> None:
    for key, value in profile_data.items():
        if value:
            state["customer_profile"][key] = value
    if state["customer_profile"].get("name") and not state["order"]["name"]:
        state["order"]["name"] = state["customer_profile"]["name"]


def generate_order_summary(order: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> str:
    name = order.get("name") or "there"
    currency = order.get("currency", DEFAULT_CURRENCY)
    if state is None:
        intro = f"Here\u2019s your updated order, {name} \U0001f60f"
    else:
        intro = choose_variant(
            state,
            "order_summary_intro",
            [
                f"Here\u2019s your updated order, {name} \U0001f60f",
                f"This is your updated order, {name} \U0001f60f",
                f"Your latest order looks like this, {name} \U0001f60f",
            ],
        )
    lines = [intro, ""]
    for item in order.get("items", []):
        lines.append(f"{item['name']} x{item['qty']} \u2014 {money_to_text(item['price'], currency)}")
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


def append_sheet_log(user_id: str, state: Dict[str, Any], event_type: str) -> None:
    if not SHEET_WEBHOOK_URL:
        return
    payload = {
        "user_id": user_id,
        "event_type": event_type,
        "state": serialize_state(state),
        "timestamp": utc_now().isoformat(),
    }
    try:
        requests.post(SHEET_WEBHOOK_URL, json=payload, timeout=15)
    except requests.RequestException as exc:
        logger.warning("Sheet webhook failed: %s", exc)


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
        "customer": {"name": order.get("name") or "Guest"},
        "notify": {"sms": False, "email": False},
        "reference_id": f"agnikara-{user_id}-{int(utc_now().timestamp())}",
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
    return any(str(payment.get("status", "")).lower() == "captured" for payment in payments)


def save_reservation_record(user_id: str, details: Dict[str, Any]) -> None:
    with db_lock:
        db.execute(
            "INSERT INTO reservations (user_id, payload_json, created_at) VALUES (?, ?, ?)",
            (user_id, json.dumps(details), utc_now().isoformat()),
        )
        db.commit()


def should_ignore_duplicate(message_id: str) -> bool:
    if message_id in processed_message_ids:
        return True
    processed_message_ids.add(message_id)
    if len(processed_message_ids) > 10000:
        processed_message_ids.clear()
        processed_message_ids.add(message_id)
    return False


def set_stage(state: Dict[str, Any], stage: str) -> None:
    current = state.get("stage", STAGE_MAIN_MENU)
    if current != stage:
        state["previous_stage"] = current
        state["stage"] = stage
        state["stage_updated_at"] = utc_now()


def choose_variant(state: Dict[str, Any], key: str, options: List[str]) -> str:
    counters = state.setdefault("response_counters", {})
    index = counters.get(key, 0) % len(options)
    counters[key] = counters.get(key, 0) + 1
    return options[index]


def greeting_message() -> str:
    return (
        "Welcome to Agnikara \U0001f37d\ufe0f\n\n"
        "How can I assist you today?\n\n"
        "1. Order Food\n"
        "2. Book a Table\n"
        "3. Check Reservation"
    )


def greeting_message_for_state(state: Dict[str, Any]) -> str:
    opener = choose_variant(
        state,
        "greeting",
        [
            "Welcome to Agnikara \U0001f37d\ufe0f",
            "A warm welcome to Agnikara \U0001f37d\ufe0f",
            "Good to have you at Agnikara \U0001f37d\ufe0f",
        ],
    )
    assist = choose_variant(
        state,
        "greeting_assist",
        [
            "How can I assist you today?",
            "How may I help you today?",
            "What can I help you with today?",
        ],
    )
    return (
        f"{opener}\n\n"
        f"{assist}\n\n"
        "1. Order Food\n"
        "2. Book a Table\n"
        "3. Check Reservation"
    )


def order_instruction_message(state: Dict[str, Any]) -> str:
    intro = choose_variant(
        state,
        "order_instruction_intro",
        [
            "Perfect \U0001f60c",
            "Lovely \U0001f60c",
            "Absolutely \U0001f60c",
        ],
    )
    closing = choose_variant(
        state,
        "order_instruction_closing",
        [
            "Add items \u2192 Checkout \u2192 Send your order here.",
            "Choose your items \u2192 Checkout \u2192 Send the order here.",
            "Build your order \u2192 Checkout \u2192 Share it here.",
        ],
    )
    return f"{intro}\n\nExplore our menu:\n{MENU_URL}\n\n{closing}"


def payment_prompt_message(state: Dict[str, Any]) -> str:
    intro = choose_variant(
        state,
        "payment_prompt",
        [
            "How would you like to pay?",
            "How would you prefer to pay?",
            "What payment option works best for you?",
        ],
    )
    return f"{intro}\n\n1. Pay Online \U0001f4b3\n2. Pay at Counter"


def handoff_message() -> str:
    if HUMAN_HANDOFF_CONTACT:
        return f"I’m bringing in our team for backup. You can also reach us at {HUMAN_HANDOFF_CONTACT}."
    return "I’m bringing in a teammate for backup. They’ll review this with priority."


def refresh_order_stage(state: Dict[str, Any]) -> None:
    confirmed_at = state.get("confirmed_at")
    if not confirmed_at or state["order_stage"] == "served":
        return
    elapsed = utc_now() - confirmed_at
    if elapsed >= timedelta(minutes=ORDER_PREP_MINUTES):
        state["order_stage"] = "served"
        state["stage"] = STAGE_SERVED
    elif elapsed >= timedelta(minutes=1):
        state["order_stage"] = "preparing"
        state["stage"] = STAGE_PREPARING


def check_order_stage_message(state: Dict[str, Any]) -> str:
    refresh_order_stage(state)
    if state["order_stage"] == "none":
        return "I don\u2019t have an active order yet. Send your checkout here and I\u2019ll take it from there."
    if state["order_stage"] == "preparing":
        return "Update \U0001f60c\n\nYour order is currently being prepared in the kitchen."
    return "Your order is ready and has been served \U0001f37d\ufe0f\n\nEnjoy your meal."


def infer_intent_rule(text: str, state: Dict[str, Any]) -> str:
    lowered = text.lower().strip()
    if detect_order_message(text):
        return "order_checkout"
    if lowered in {"1", "order", "order food"} and state["stage"] == STAGE_MAIN_MENU:
        return "order_start"
    if lowered in {"2", "book a table", "table", "reservation"} and state["stage"] == STAGE_MAIN_MENU:
        return "reservation"
    if lowered in {"3", "check reservation", "reservation check"} and state["stage"] == STAGE_MAIN_MENU:
        return "reservation_check"
    if lowered in {"1", "confirm order", "confirm"} and state["stage"] == STAGE_ORDER_ACTION:
        return "confirm_order"
    if lowered in {"2", "add more items"} and state["stage"] == STAGE_ORDER_ACTION:
        return "add_more_items"
    if lowered in {"3", "modify order", "modify"} and state["stage"] == STAGE_ORDER_ACTION:
        return "modify_order"
    if lowered in {"1", "pay online", "online", "online payment"} and state["stage"] == STAGE_PAYMENT_CHOICE:
        return "pay_online"
    if lowered in {"2", "pay at counter", "counter"} and state["stage"] == STAGE_PAYMENT_CHOICE:
        return "pay_at_counter"
    if lowered in {"paid", "yes", "done", "completed"} and state["stage"] == STAGE_PAYMENT_CONFIRMATION:
        return "payment_confirmation"
    if state["stage"] == STAGE_PAYMENT_CONFIRMATION and any(
        phrase in lowered
        for phrase in {"already paid", "already make", "already made", "i paid", "i have paid", "payment done"}
    ):
        return "payment_claim"
    if lowered in {"no", "not paid", "not yet"} and state["stage"] == STAGE_PAYMENT_CONFIRMATION:
        return "payment_pending"
    if lowered in {"status", "track order", "where is my order", "preparing", "served"}:
        return "order_status"
    if lowered.startswith(("remove ", "change ", "update ", "set ")):
        return "modify_inline"
    if any(phrase in lowered for phrase in {"human", "manager", "staff", "call me", "agent"}):
        return "handoff_to_human"
    if state["stage"] == STAGE_HUMAN_HANDOFF and any(
        phrase in lowered
        for phrase in {"dont want human", "don't want human", "talk to you", "not human", "stay with you"}
    ):
        return "cancel_handoff"
    return "none"


def json_from_text(text: str) -> Optional[Dict[str, Any]]:
    text = text.strip()
    if not text:
        return None
    match = re.search(r"\{.*\}", text, flags=re.DOTALL)
    candidate = match.group(0) if match else text
    try:
        data = json.loads(candidate)
        return data if isinstance(data, dict) else None
    except json.JSONDecodeError:
        return None


def ai_decide_action(user_message: str, state: Dict[str, Any]) -> Dict[str, Any]:
    if not openai_client:
        return {"action": "none", "intent": "none"}
    system_prompt = (
        "You are Agnikara Restaurant AI. Return only valid JSON. "
        "Choose one action from this set: "
        + ", ".join(sorted(SUPPORTED_ACTIONS))
        + ". Keep replies under 60 words. Never show the full menu in chat. "
        f"If menu is relevant, use this link: {MENU_URL}. "
        "Never claim payment is received unless the action is check_payment_status. "
        'Schema: {"intent":"string","action":"string","reply":"string","item_name":"string","quantity":0,"needs_handoff":false}.'
    )
    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "system", "content": f"State: {json.dumps(serialize_state(state))}"},
                {"role": "user", "content": user_message},
            ],
            max_output_tokens=180,
        )
        parsed = json_from_text(response.output_text) or {"action": "none", "intent": "none"}
        if parsed.get("action") not in SUPPORTED_ACTIONS:
            parsed["action"] = "none"
        return parsed
    except Exception as exc:
        logger.warning("OpenAI structured decision failed: %s", exc)
        return {"action": "none", "intent": "none"}


def classify_intent(text: str, state: Dict[str, Any]) -> Dict[str, Any]:
    rule_intent = infer_intent_rule(text, state)
    if rule_intent != "none":
        return {"intent": rule_intent, "action": "none"}
    return ai_decide_action(text, state)


def mark_handoff(state: Dict[str, Any]) -> str:
    state["handoff_requested"] = True
    set_stage(state, STAGE_HUMAN_HANDOFF)
    return handoff_message()


def finalize_confirmation(state: Dict[str, Any]) -> None:
    now = utc_now()
    state["order_confirmed"] = True
    state["payment_status"] = "done"
    set_stage(state, STAGE_PREPARING)
    state["order_stage"] = "preparing"
    state["confirmed_at"] = now
    state["stage_updated_at"] = now
    state["waiting_for_order"] = False
    state["failure_count"] = 0


def execute_action(user_id: str, state: Dict[str, Any], decision: Dict[str, Any], user_text: str) -> str:
    action = decision.get("action", "none")
    lowered = user_text.lower().strip()

    if decision.get("needs_handoff"):
        return mark_handoff(state)
    if action == "show_greeting":
        set_stage(state, STAGE_MAIN_MENU)
        state["failure_count"] = 0
        return greeting_message_for_state(state)
    if action == "show_menu_link":
        state["intent"] = "order"
        set_stage(state, STAGE_CHECKOUT)
        state["waiting_for_order"] = True
        state["checkout_mode"] = "fresh"
        state["failure_count"] = 0
        return order_instruction_message(state)
    if action == "summarize_order" and state["order"]["items"]:
        set_stage(state, STAGE_ORDER_ACTION)
        state["failure_count"] = 0
        return generate_order_summary(state["order"], state)
    if action == "confirm_order" and state["order"]["items"]:
        set_stage(state, STAGE_PAYMENT_CHOICE)
        state["failure_count"] = 0
        return payment_prompt_message(state)
    if action == "add_more_items":
        set_stage(state, STAGE_CHECKOUT)
        state["waiting_for_order"] = True
        state["checkout_mode"] = "replace"
        state["failure_count"] = 0
        return choose_variant(
            state,
            "add_more_items",
            [
                f"Of course.\n\nPlease add more items from {MENU_URL} and send the updated checkout here.",
                f"Absolutely.\n\nUpdate your cart on {MENU_URL} and send the latest checkout here.",
                f"Sure.\n\nAdd what you’d like on {MENU_URL}, then send me the refreshed checkout.",
            ],
        )
    if action == "modify_order":
        set_stage(state, STAGE_ORDER_ACTION)
        state["failure_count"] = 0
        return choose_variant(
            state,
            "modify_order",
            [
                "Tell me the change in one line. Example: remove pasta alfredo or change margherita pizza to 1.",
                "Share the change in one line. Example: remove pasta alfredo or change margherita pizza to 1.",
                "Just tell me the edit in one line. Example: remove pasta alfredo or change margherita pizza to 1.",
            ],
        )
    if action == "remove_item":
        item_name = (decision.get("item_name") or "").strip().lower()
        if not item_name:
            return "Tell me which item you want removed."
        updated, changed = modify_order_from_text(state["order"], f"remove {item_name}")
        if changed:
            state["order"] = updated
            set_stage(state, STAGE_ORDER_ACTION)
            state["failure_count"] = 0
            return generate_order_summary(state["order"], state)
        return "I couldn\u2019t match that item. Please use the exact item name from your checkout."
    if action == "update_quantity":
        item_name = (decision.get("item_name") or "").strip().lower()
        quantity = int(decision.get("quantity") or 0)
        if not item_name or quantity <= 0:
            return "Please tell me the exact item and quantity."
        updated, changed = modify_order_from_text(state["order"], f"change {item_name} to {quantity}")
        if changed:
            state["order"] = updated
            set_stage(state, STAGE_ORDER_ACTION)
            state["failure_count"] = 0
            return generate_order_summary(state["order"], state)
        return "I couldn\u2019t match that item. Please use the exact item name from your checkout."
    if action == "send_payment_link":
        state["payment_method"] = "online"
        state["payment_status"] = "pending"
        set_stage(state, STAGE_PAYMENT_CONFIRMATION)
        state["failure_count"] = 0
        link = generate_payment_link(user_id, state)
        if link == "Payment link unavailable.":
            set_stage(state, STAGE_PAYMENT_CHOICE)
            return "Online payment is unavailable right now. Please choose 1 or 2."
        return choose_variant(
            state,
            "payment_link",
            [
                f"Your secure payment link is ready \U0001f4b3\n{link}\n\nReply 'paid' or 'yes' once done.",
                f"Here’s your payment link \U0001f4b3\n{link}\n\nOnce it’s completed, reply 'paid' or 'yes'.",
                f"Your checkout link is ready \U0001f4b3\n{link}\n\nAfter payment, reply 'paid' or 'yes'.",
            ],
        )
    if action == "check_payment_status":
        state["payment_verification_attempts"] += 1
        if state.get("payment_method") != "online":
            return "Please use the payment option shown above."
        try:
            if verify_online_payment(state):
                finalize_confirmation(state)
                return "Payment received. Your order is now being prepared \U0001f37d\ufe0f\n\n\u23f3 Estimated time: 15 minutes"
        except requests.RequestException as exc:
            logger.warning("Payment verification failed: %s", exc)
            if state["payment_verification_attempts"] >= 3:
                return mark_handoff(state)
            return choose_variant(
                state,
                "payment_verify_error",
                [
                    "I couldn\u2019t verify the payment yet. Please wait a moment and reply 'paid' again.",
                    "The payment gateway hasn\u2019t confirmed it yet. Give it a moment, then reply 'paid' again.",
                ],
            )
        return choose_variant(
            state,
            "payment_not_found",
            [
                "I can\u2019t confirm the payment yet. Please complete the payment link, then reply 'paid'.",
                "I’m still waiting for payment confirmation from the gateway. Once it updates, reply 'paid' again.",
                "The payment hasn’t reflected on my side yet. Please give it a moment, then reply 'paid' again.",
            ],
        )
    if action == "pay_at_counter":
        state["payment_method"] = "counter"
        state["failure_count"] = 0
        finalize_confirmation(state)
        return "Perfect. Payment is marked for counter. Your order is now being prepared \U0001f37d\ufe0f\n\n\u23f3 Estimated time: 15 minutes"
    if action == "check_order_stage":
        return check_order_stage_message(state)
    if action == "book_table":
        state["intent"] = "reservation"
        set_stage(state, STAGE_RESERVATION_DETAILS)
        state["failure_count"] = 0
        return "Please share your name, date, time, and guest count."
    if action == "check_reservation":
        state["intent"] = "reservation"
        set_stage(state, STAGE_RESERVATION_CHECK)
        state["failure_count"] = 0
        return "Please share your reservation name and date. I\u2019ll help you check it."
    if action == "handoff_to_human":
        return mark_handoff(state)
    if state["stage"] == STAGE_HUMAN_HANDOFF:
        return choose_variant(
            state,
            "handoff_repeat",
            [
                "My teammate is still reviewing this for you.",
                "Our team is checking this now.",
                "A teammate is already on this and will follow up shortly.",
            ],
        )
    if state["stage"] in {STAGE_PREPARING, STAGE_SERVED}:
        return check_order_stage_message(state)
    if lowered in {"hello", "hi", "hey"} and not state["order"]["items"]:
        set_stage(state, STAGE_MAIN_MENU)
        return "Please reply with 1 to order food, 2 to book a table, or 3 to check a reservation."
    state["failure_count"] += 1
    if state["failure_count"] >= 3:
        return mark_handoff(state)
    return decision.get("reply") or "Please reply with 1, 2, or 3, or send your checkout here."


def handle_reservation_stage(user_id: str, state: Dict[str, Any], text: str) -> str:
    if state["stage"] == STAGE_RESERVATION_DETAILS:
        state["reservation_status"] = "requested"
        state["reservation_details"] = {"message": text, "type": "new"}
        save_reservation_record(user_id, state["reservation_details"])
        return "Thank you. Your table request has been noted. Our team will confirm it shortly."
    if state["stage"] == STAGE_RESERVATION_CHECK:
        state["reservation_status"] = "checking"
        state["reservation_details"] = {"message": text, "type": "check"}
        return "I’ve noted your reservation details. Our team will verify and update you shortly."
    return "Please share your reservation details and I’ll help you next."


def handle_rule_intent(user_id: str, state: Dict[str, Any], intent: str, text: str) -> Optional[str]:
    if intent == "none":
        return None
    if intent == "handoff_to_human":
        return mark_handoff(state)
    if intent == "order_start":
        return execute_action(user_id, state, {"action": "show_menu_link"}, text)
    if intent == "reservation":
        return execute_action(user_id, state, {"action": "book_table"}, text)
    if intent == "reservation_check":
        return execute_action(user_id, state, {"action": "check_reservation"}, text)
    if intent == "order_status":
        return execute_action(user_id, state, {"action": "check_order_stage"}, text)
    if intent == "confirm_order":
        return execute_action(user_id, state, {"action": "confirm_order"}, text)
    if intent == "add_more_items":
        return execute_action(user_id, state, {"action": "add_more_items"}, text)
    if intent == "modify_order":
        return execute_action(user_id, state, {"action": "modify_order"}, text)
    if intent == "pay_online":
        return execute_action(user_id, state, {"action": "send_payment_link"}, text)
    if intent == "pay_at_counter":
        return execute_action(user_id, state, {"action": "pay_at_counter"}, text)
    if intent == "payment_confirmation":
        return execute_action(user_id, state, {"action": "check_payment_status"}, text)
    if intent == "payment_claim":
        state["payment_verification_attempts"] += 1
        try:
            if verify_online_payment(state):
                finalize_confirmation(state)
                return "Thank you for waiting. I’ve confirmed the payment and your order is now being prepared \U0001f37d\ufe0f\n\n\u23f3 Estimated time: 15 minutes"
        except requests.RequestException as exc:
            logger.warning("Payment verification failed: %s", exc)
        return choose_variant(
            state,
            "payment_claim",
            [
                "I hear you. I’m re-checking the payment now. If the gateway is delayed, reply 'paid' again in a moment.",
                "Understood. I’m checking with the payment gateway again. If it still hasn’t updated, reply 'paid' in a moment.",
                "Thanks for flagging it. I’m checking the payment status again right now. If it’s still delayed, reply 'paid' shortly.",
            ],
        )
    if intent == "payment_pending":
        return "No problem. Complete the payment when you’re ready, then reply 'paid'."
    if intent == "cancel_handoff":
        if state.get("payment_method") == "online" and state.get("payment_status") != "done":
            state["handoff_requested"] = False
            set_stage(state, STAGE_PAYMENT_CONFIRMATION)
            return "I’m still here with you. I’ll keep checking the payment. Give it a minute, then reply 'paid' again."
        state["handoff_requested"] = False
        previous = state.get("previous_stage", STAGE_MAIN_MENU)
        set_stage(state, previous if previous != STAGE_HUMAN_HANDOFF else STAGE_MAIN_MENU)
        return "Of course. I’m with you. Let’s continue here."
    if intent == "modify_inline":
        updated, changed = modify_order_from_text(state["order"], text)
        if changed:
            state["order"] = updated
            set_stage(state, STAGE_ORDER_ACTION)
            state["failure_count"] = 0
            return generate_order_summary(state["order"], state)
        state["failure_count"] += 1
        return "I couldn\u2019t match that item. Please use the exact item name from your checkout."
    if intent == "order_checkout":
        parsed = parse_order_message(text)
        if not parsed:
            state["failure_count"] += 1
            return "I couldn\u2019t read that order clearly. Please resend it in the checkout format with name, items, and total."
        state["intent"] = "order"
        state["waiting_for_order"] = False
        if state.get("checkout_mode") == "replace":
            state["order"] = {
                "name": parsed.get("name", ""),
                "items": parsed.get("items", []),
                "total": parsed.get("total", Decimal("0.00")),
                "currency": parsed.get("currency", DEFAULT_CURRENCY),
            }
        else:
            state["order"] = merge_orders(state["order"], parsed)
        update_customer_profile(state, parsed.get("profile", {}))
        state["order_confirmed"] = False
        state["payment_status"] = "pending"
        state["payment_method"] = ""
        state["payment_link"] = ""
        state["payment_link_id"] = ""
        state["payment_verification_attempts"] = 0
        state["checkout_mode"] = "append"
        set_stage(state, STAGE_ORDER_ACTION)
        state["failure_count"] = 0
        append_sheet_log(user_id, state, "order_updated")
        return generate_order_summary(state["order"], state)
    if state["stage"] in {STAGE_RESERVATION_DETAILS, STAGE_RESERVATION_CHECK}:
        return handle_reservation_stage(user_id, state, text)
    return None


def build_reply(user_id: str, text: str) -> str:
    state = get_state(user_id)
    state["last_message_at"] = utc_now()
    refresh_order_stage(state)

    if not state["greeted"]:
        state["greeted"] = True
        state["stage"] = STAGE_MAIN_MENU
        if text.strip().lower() in {"1", "2", "3"}:
            initial = greeting_message_for_state(state)
            follow_up = handle_rule_intent(user_id, state, infer_intent_rule(text, state), text) or greeting_message_for_state(state)
            save_state(user_id, state)
            return f"{initial}\n\n{follow_up}"
        save_state(user_id, state)
        return greeting_message_for_state(state)

    decision = classify_intent(text, state)
    state["last_ai_action"] = decision.get("action", "")
    reply = handle_rule_intent(user_id, state, decision.get("intent", "none"), text)
    if reply is None:
        reply = execute_action(user_id, state, decision, text)
    save_state(user_id, state)
    return reply


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
def healthcheck() -> Tuple[Any, int]:
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
