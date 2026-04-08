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
import gspread
from google.oauth2.service_account import Credentials
from openai import OpenAI


app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("agnikara-ai")


MENU_URL = "https://agnikara.netlify.app/#menu"
DEFAULT_CURRENCY = os.getenv("CURRENCY_SYMBOL", "EUR")
WHATSAPP_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v20.0")
ORDER_PREP_MINUTES = int(os.getenv("ORDER_PREP_MINUTES", "15"))
DB_PATH = Path(os.getenv("STATE_DB_PATH", "agnikara_state.db"))
GOOGLE_SERVICE_ACCOUNT_FILE = Path(os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "creditional.json"))

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
GOOGLE_SHEETS_SPREADSHEET_ID = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID", "")
GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON", "")
GOOGLE_SHEETS_EVENT_SHEET = os.getenv("GOOGLE_SHEETS_EVENT_SHEET", "Events")
GOOGLE_SHEETS_RESERVATION_SHEET = os.getenv("GOOGLE_SHEETS_RESERVATION_SHEET", "Reservations")
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
payment_followup_timers: Dict[str, threading.Timer] = {}
payment_timer_lock = threading.Lock()
google_sheets_client = None
google_sheets_lock = threading.Lock()


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


def load_google_service_account_info() -> Tuple[Optional[Dict[str, Any]], str]:
    if GOOGLE_SERVICE_ACCOUNT_JSON.strip():
        try:
            return json.loads(GOOGLE_SERVICE_ACCOUNT_JSON), "GOOGLE_SERVICE_ACCOUNT_JSON"
        except json.JSONDecodeError as exc:
            logger.warning("Invalid GOOGLE_SERVICE_ACCOUNT_JSON: %s", exc)

    if GOOGLE_SERVICE_ACCOUNT_FILE.exists():
        try:
            return json.loads(GOOGLE_SERVICE_ACCOUNT_FILE.read_text(encoding="utf-8")), str(GOOGLE_SERVICE_ACCOUNT_FILE)
        except Exception as exc:
            logger.warning("Google service account file read failed (%s): %s", GOOGLE_SERVICE_ACCOUNT_FILE, exc)

    return None, ""


def initialize_google_sheets_client() -> Optional[gspread.Client]:
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        logger.warning("Google Sheets disabled: GOOGLE_SHEETS_SPREADSHEET_ID is not configured.")
        return None
    service_account_info, source = load_google_service_account_info()
    if not service_account_info:
        logger.warning(
            "Google Sheets disabled: no valid service account credentials found in GOOGLE_SERVICE_ACCOUNT_JSON or %s.",
            GOOGLE_SERVICE_ACCOUNT_FILE,
        )
        return None
    try:
        credentials = Credentials.from_service_account_info(
            service_account_info,
            scopes=[
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive",
            ],
        )
        logger.info("Google Sheets client initialized using %s.", source)
        return gspread.authorize(credentials)
    except Exception as exc:
        logger.warning("Google Sheets client initialization failed: %s", exc)
        return None


google_sheets_client = initialize_google_sheets_client()


def log_google_sheets_status() -> None:
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        return
    if not google_sheets_client:
        logger.warning("Google Sheets startup check skipped: client is unavailable.")
        return
    try:
        spreadsheet = google_sheets_client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        worksheet_titles = [worksheet.title for worksheet in spreadsheet.worksheets()]
        logger.info(
            "Google Sheets startup check passed: spreadsheet='%s', worksheets=%s",
            spreadsheet.title,
            worksheet_titles,
        )
    except Exception as exc:
        logger.warning("Google Sheets startup check failed: %s", exc)


log_google_sheets_status()


def create_default_state() -> Dict[str, Any]:
    return {
        "greeted": False,
        "intent": "none",
        "stage": STAGE_MAIN_MENU,
        "previous_stage": STAGE_MAIN_MENU,
        "waiting_for_order": False,
        "checkout_mode": "fresh",
        "response_seed": 0,
        "order_sequence": 0,
        "active_order_id": "",
        "last_completed_order_id": "",
        "orders": {},
        "order": {
            "order_id": "",
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
        "payment_pending_since": None,
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
    for order_id, record in safe.get("orders", {}).items():
        record["total"] = str(record.get("total", "0.00"))
        for item in record.get("items", []):
            item["price"] = str(item["price"])
        for key in ("created_at", "updated_at", "confirmed_at"):
            if record.get(key):
                record[key] = record[key].isoformat()
    for key in ("last_message_at", "confirmed_at", "stage_updated_at", "payment_pending_since"):
        if safe.get(key):
            safe[key] = safe[key].isoformat()
    return safe


def deserialize_state(state_json: str) -> Dict[str, Any]:
    state = json.loads(state_json)
    state["order"]["total"] = Decimal(state["order"].get("total", "0.00"))
    for item in state["order"]["items"]:
        item["price"] = Decimal(item.get("price", "0.00"))
    for order_id, record in state.get("orders", {}).items():
        record["total"] = Decimal(record.get("total", "0.00"))
        for item in record.get("items", []):
            item["price"] = Decimal(item.get("price", "0.00"))
        for key in ("created_at", "updated_at", "confirmed_at"):
            if record.get(key):
                record[key] = datetime.fromisoformat(record[key])
    for key in ("last_message_at", "confirmed_at", "stage_updated_at", "payment_pending_since"):
        if state.get(key):
            state[key] = datetime.fromisoformat(state[key])
    return state


def save_state(user_id: str, state: Dict[str, Any]) -> None:
    sync_active_order_record(state)
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
        state["response_seed"] = sum(ord(ch) for ch in user_id) % 17
        save_state(user_id, state)
        return state
    state = deserialize_state(row["state_json"])
    if "response_seed" not in state:
        state["response_seed"] = sum(ord(ch) for ch in user_id) % 17
    if "order_sequence" not in state:
        state["order_sequence"] = 0
    if "active_order_id" not in state:
        state["active_order_id"] = state.get("order", {}).get("order_id", "")
    if "last_completed_order_id" not in state:
        state["last_completed_order_id"] = ""
    if "orders" not in state:
        state["orders"] = {}
    return state


def empty_order() -> Dict[str, Any]:
    return {
        "order_id": "",
        "name": "",
        "items": [],
        "total": Decimal("0.00"),
        "currency": DEFAULT_CURRENCY,
    }


def generate_order_id(user_id: str, state: Dict[str, Any]) -> str:
    state["order_sequence"] = int(state.get("order_sequence", 0)) + 1
    suffix = "".join(ch for ch in user_id if ch.isdigit())[-4:] or "0000"
    return f"AGN-{suffix}-{state['order_sequence']:04d}"


def sync_active_order_record(state: Dict[str, Any]) -> None:
    order = state.get("order", {})
    order_id = order.get("order_id") or state.get("active_order_id", "")
    if not order_id:
        return
    state["active_order_id"] = order_id
    state.setdefault("orders", {})[order_id] = {
        "order_id": order_id,
        "name": order.get("name", ""),
        "items": deepcopy(order.get("items", [])),
        "total": order.get("total", Decimal("0.00")),
        "currency": order.get("currency", DEFAULT_CURRENCY),
        "payment_status": state.get("payment_status", "pending"),
        "payment_method": state.get("payment_method", ""),
        "order_stage": state.get("order_stage", "none"),
        "customer_profile": deepcopy(state.get("customer_profile", {})),
        "created_at": state.setdefault("orders", {}).get(order_id, {}).get("created_at", utc_now()),
        "updated_at": utc_now(),
        "confirmed_at": state.get("confirmed_at"),
    }


def start_new_order(state: Dict[str, Any], user_id: str) -> str:
    sync_active_order_record(state)
    new_order_id = generate_order_id(user_id, state)
    state["active_order_id"] = new_order_id
    state["order"] = empty_order()
    state["order"]["order_id"] = new_order_id
    state["order"]["name"] = state.get("customer_profile", {}).get("name", "")
    state["payment_status"] = "pending"
    state["payment_method"] = ""
    state["payment_link"] = ""
    state["payment_link_id"] = ""
    state["payment_verification_attempts"] = 0
    state["payment_pending_since"] = None
    state["order_confirmed"] = False
    state["order_stage"] = "none"
    state["confirmed_at"] = None
    state.setdefault("orders", {})[new_order_id] = {
        "order_id": new_order_id,
        "name": state["order"]["name"],
        "items": [],
        "total": Decimal("0.00"),
        "currency": DEFAULT_CURRENCY,
        "payment_status": "pending",
        "payment_method": "",
        "order_stage": "none",
        "customer_profile": deepcopy(state.get("customer_profile", {})),
        "created_at": utc_now(),
        "updated_at": utc_now(),
        "confirmed_at": None,
    }
    return new_order_id


def get_order_record(state: Dict[str, Any], order_id: str) -> Optional[Dict[str, Any]]:
    if not order_id:
        return None
    if state.get("active_order_id") == order_id:
        sync_active_order_record(state)
    return state.get("orders", {}).get(order_id)


def extract_order_id(text: str) -> Optional[str]:
    match = re.search(r"\bAGN-\d{4}-\d{4}\b", text.upper())
    return match.group(0) if match else None


def sorted_order_ids(state: Dict[str, Any]) -> List[str]:
    orders = state.get("orders", {})
    return sorted(
        orders.keys(),
        key=lambda order_id: (
            orders[order_id].get("created_at") or datetime.min.replace(tzinfo=timezone.utc),
            order_id,
        ),
    )


def resolve_order_reference(state: Dict[str, Any], text: str) -> Optional[str]:
    explicit = extract_order_id(text)
    if explicit:
        return explicit

    upper_text = text.upper()
    suffix_match = re.search(r"\b(\d{4})\b", upper_text)
    if suffix_match:
        suffix = suffix_match.group(1)
        for order_id in sorted_order_ids(state):
            if order_id.endswith(f"-{suffix}"):
                return order_id

    lowered = text.lower()
    order_ids = sorted_order_ids(state)
    if not order_ids:
        return None
    if any(phrase in lowered for phrase in {"first order", "old order", "older order", "previous order"}):
        return order_ids[0]
    if any(phrase in lowered for phrase in {"second order", "latest order", "new order", "recent order"}):
        return order_ids[-1]
    return None


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
    if order.get("order_id"):
        lines.append(f"Order ID: {order['order_id']}")
        lines.append("")
    if state is None:
        detail_style = 0
    else:
        detail_style = (
            int(state.get("response_counters", {}).get("order_detail_style", 0))
            + int(state.get("response_seed", 0))
            + int(state.get("order_sequence", 0))
        ) % 3
        state.setdefault("response_counters", {})["order_detail_style"] = state.get("response_counters", {}).get("order_detail_style", 0) + 1

    for item in order.get("items", []):
        if detail_style == 0:
            line = f"{item['name']} x{item['qty']} \u2014 {money_to_text(item['price'], currency)}"
        elif detail_style == 1:
            line = f"\u2022 {item['name']} | Qty {item['qty']} | {money_to_text(item['price'], currency)}"
        else:
            line = f"- {item['qty']} x {item['name']} for {money_to_text(item['price'], currency)}"
        lines.append(line)
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


def generate_final_order_summary(state: Dict[str, Any]) -> str:
    profile = state.get("customer_profile", {})
    order = state.get("order", {})
    currency = order.get("currency", DEFAULT_CURRENCY)
    header = choose_variant(
        state,
        "final_summary_header",
        [
            "Order summary for you:",
            "Your confirmed order summary:",
            "Here’s your final order summary:",
        ],
    )
    payment_label = {
        "online": "Paid Online",
        "counter": "Pay at Counter",
        "cash_on_table": "Cash on Table",
    }.get(state.get("payment_method", ""), "Pending")

    lines = [header, ""]
    if order.get("order_id"):
        lines.append(f"Order ID: {order['order_id']}")
        lines.append("")
    if profile.get("name"):
        lines.append(f"Name: {profile['name']}")
    if profile.get("mobile"):
        lines.append(f"Mobile: {profile['mobile']}")
    if profile.get("email"):
        lines.append(f"Email: {profile['email']}")
    if profile.get("service_type"):
        lines.append(f"Service: {profile['service_type']}")
    if profile.get("preferred_time"):
        lines.append(f"Time: {profile['preferred_time']}")
    if profile.get("guests"):
        lines.append(f"Guests: {profile['guests']}")

    lines.append("")
    lines.append("Items:")

    detail_style = (
        int(state.get("response_counters", {}).get("final_detail_style", 0))
        + int(state.get("response_seed", 0))
        + int(state.get("order_sequence", 0))
    ) % 3
    state.setdefault("response_counters", {})["final_detail_style"] = state.get("response_counters", {}).get("final_detail_style", 0) + 1

    for item in order.get("items", []):
        if detail_style == 0:
            lines.append(f"\u2022 {item['name']} x{item['qty']} \u2014 {money_to_text(item['price'], currency)}")
        elif detail_style == 1:
            lines.append(f"- {item['qty']} x {item['name']} | {money_to_text(item['price'], currency)}")
        else:
            lines.append(f"{item['name']} | Qty {item['qty']} | {money_to_text(item['price'], currency)}")

    lines.extend(
        [
            "",
            f"Total Amount: {money_to_text(order.get('total', Decimal('0.00')), currency)}",
            f"Payment Mode: {payment_label}",
            f"Order Status: {state.get('order_stage', 'none').title()}",
        ]
    )
    return "\n".join(lines)


def refresh_order_record_stage(record: Dict[str, Any]) -> None:
    confirmed_at = record.get("confirmed_at")
    if not confirmed_at or record.get("order_stage") == "served":
        return
    elapsed = utc_now() - confirmed_at
    if elapsed >= timedelta(minutes=ORDER_PREP_MINUTES):
        record["order_stage"] = "served"
    elif elapsed >= timedelta(minutes=1):
        record["order_stage"] = "preparing"


def generate_tracked_order_summary(record: Dict[str, Any], state: Dict[str, Any]) -> str:
    refresh_order_record_stage(record)
    currency = record.get("currency", DEFAULT_CURRENCY)
    lines = [f"Tracking details for {record.get('order_id', 'your order')}:", ""]
    profile = record.get("customer_profile", {})
    if profile.get("name"):
        lines.append(f"Name: {profile['name']}")
    lines.append(f"Order ID: {record.get('order_id', '')}")
    lines.append(f"Payment: {record.get('payment_method', '').replace('_', ' ').title() or 'Pending'}")
    lines.append(f"Status: {record.get('order_stage', 'none').title()}")
    lines.append("")
    lines.append("Items:")
    for item in record.get("items", []):
        lines.append(f"- {item['name']} x{item['qty']} | {money_to_text(item['price'], currency)}")
    lines.append("")
    lines.append(f"Total: {money_to_text(record.get('total', Decimal('0.00')), currency)}")
    return "\n".join(lines)


def recent_order_choices_message(state: Dict[str, Any]) -> str:
    order_ids = sorted_order_ids(state)
    if not order_ids:
        return "I don’t have any saved orders to track yet."

    recent_ids = list(reversed(order_ids[-3:]))
    lines = ["Which order would you like to track?", ""]
    for order_id in recent_ids:
        record = get_order_record(state, order_id) or {}
        status = str(record.get("order_stage", "none")).title()
        total = money_to_text(record.get("total", Decimal("0.00")), record.get("currency", DEFAULT_CURRENCY))
        lines.append(f"{order_id} — {status} — {total}")
    lines.append("")
    lines.append("Reply with the order ID or just the last 4 digits.")
    return "\n".join(lines)


def payment_success_message(state: Dict[str, Any]) -> str:
    return (
        "Payment received. Your order is now being prepared \U0001f37d\ufe0f\n\n"
        "\u23f3 Estimated time: 15 minutes\n\n"
        f"{generate_final_order_summary(state)}"
    )


def payment_counter_fallback_message(state: Dict[str, Any]) -> str:
    return (
        "Payment is not arrived in our gateway, so payment mode is now set to counter.\n\n"
        "If you already made the payment, please share your payment slip at the counter.\n\n"
        "Thanks for your order.\n\n"
        f"{generate_final_order_summary(state)}"
    )


def schedule_payment_followup(user_id: str, order_id: str) -> None:
    if not order_id:
        return
    cancel_payment_followup(user_id, order_id)

    def _run() -> None:
        key = payment_timer_key(user_id, order_id)
        try:
            state = get_state(user_id)
            if state.get("active_order_id") != order_id:
                return
            if state.get("payment_method") != "online" or state.get("payment_status") == "done":
                return
            state["payment_pending_since"] = state.get("payment_pending_since") or utc_now() - timedelta(seconds=60)
            try:
                if verify_online_payment(state):
                    finalize_confirmation(state)
                    save_state(user_id, state)
                    send_whatsapp_message(user_id, payment_success_message(state), max_words=220)
                    return
            except requests.RequestException as exc:
                logger.warning("Scheduled payment verification failed: %s", exc)
                return

            state["payment_method"] = "counter"
            state["payment_status"] = "done"
            finalize_confirmation(state)
            save_state(user_id, state)
            send_whatsapp_message(user_id, payment_counter_fallback_message(state), max_words=220)
        finally:
            with payment_timer_lock:
                payment_followup_timers.pop(key, None)

    timer = threading.Timer(60.0, _run)
    timer.daemon = True
    with payment_timer_lock:
        payment_followup_timers[payment_timer_key(user_id, order_id)] = timer
    timer.start()


def truncate_response(text: str, max_words: int = 80) -> str:
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).strip() + "..."


def send_whatsapp_message(to: str, body: str, max_words: int = 80) -> None:
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
        "text": {"preview_url": True, "body": truncate_response(body, max_words=max_words)},
    }
    response = requests.post(url, headers=headers, json=payload, timeout=30)
    logger.info("WhatsApp send response %s: %s", response.status_code, response.text)
    response.raise_for_status()


def append_sheet_log(user_id: str, state: Dict[str, Any], event_type: str) -> None:
    order = state.get("order", {})
    profile = state.get("customer_profile", {})
    append_google_sheet_row(
        GOOGLE_SHEETS_EVENT_SHEET,
        [
            utc_now().isoformat(),
            user_id,
            event_type,
            order.get("order_id", ""),
            profile.get("name", ""),
            profile.get("mobile", ""),
            profile.get("email", ""),
            profile.get("service_type", ""),
            profile.get("preferred_time", ""),
            profile.get("guests", ""),
            str(order.get("total", Decimal("0.00"))),
            order.get("currency", DEFAULT_CURRENCY),
            state.get("payment_status", ""),
            state.get("payment_method", ""),
            state.get("order_stage", ""),
            state.get("stage", ""),
            json.dumps(serialize_state(state)),
        ],
    )
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
    append_google_sheet_row(
        GOOGLE_SHEETS_RESERVATION_SHEET,
        [
            utc_now().isoformat(),
            user_id,
            details.get("type", ""),
            details.get("message", ""),
        ],
    )


def get_or_create_google_worksheet(sheet_name: str) -> Optional[gspread.Worksheet]:
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        logger.warning("Google Sheets append skipped: GOOGLE_SHEETS_SPREADSHEET_ID is not configured.")
        return None
    if not google_sheets_client:
        logger.warning("Google Sheets append skipped: client is unavailable. Check service-account credentials.")
        return None
    try:
        spreadsheet = google_sheets_client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        try:
            return spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            return spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)
    except Exception as exc:
        logger.warning("Google Sheets worksheet access failed: %s", exc)
        return None


def append_google_sheet_row(sheet_name: str, row: List[str]) -> None:
    worksheet = get_or_create_google_worksheet(sheet_name)
    if not worksheet:
        return
    try:
        with google_sheets_lock:
            worksheet.append_row(row, value_input_option="USER_ENTERED")
    except Exception as exc:
        logger.warning("Google Sheets append failed: %s", exc)


def should_ignore_duplicate(message_id: str) -> bool:
    if message_id in processed_message_ids:
        return True
    processed_message_ids.add(message_id)
    if len(processed_message_ids) > 10000:
        processed_message_ids.clear()
        processed_message_ids.add(message_id)
    return False


def payment_timer_key(user_id: str, order_id: str) -> str:
    return f"{user_id}:{order_id}"


def cancel_payment_followup(user_id: str, order_id: str) -> None:
    if not order_id:
        return
    key = payment_timer_key(user_id, order_id)
    with payment_timer_lock:
        timer = payment_followup_timers.pop(key, None)
    if timer:
        timer.cancel()


def set_stage(state: Dict[str, Any], stage: str) -> None:
    current = state.get("stage", STAGE_MAIN_MENU)
    if current != stage:
        state["previous_stage"] = current
        state["stage"] = stage
        state["stage_updated_at"] = utc_now()


def choose_variant(state: Dict[str, Any], key: str, options: List[str]) -> str:
    counters = state.setdefault("response_counters", {})
    seed = int(state.get("response_seed", 0)) + int(state.get("order_sequence", 0))
    index = (counters.get(key, 0) + seed) % len(options)
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
            "A warm welcome to Agnikara \U0001f525",
            "Good to have you at Agnikara \U0001f60c",
            "Hello from Agnikara \U0001f389",
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
            "Lovely \U0001f60a",
            "Absolutely \U0001f525",
            "Brilliant \U0001f44c",
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
            "How would you like to settle this order?",
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
    active_id = state.get("active_order_id") or state.get("last_completed_order_id")
    if state["order_stage"] == "none":
        return "I don\u2019t have an active order yet. Send your checkout here and I\u2019ll take it from there."
    if state["order_stage"] == "preparing":
        order_line = f"\nOrder ID: {active_id}" if active_id else ""
        return f"Update \U0001f60c\n\nYour order is currently being prepared in the kitchen.{order_line}"
    order_line = f"\nOrder ID: {active_id}" if active_id else ""
    return f"Your order is ready and has been served \U0001f37d\ufe0f\n\nEnjoy your meal.{order_line}"


def infer_intent_rule(text: str, state: Dict[str, Any]) -> str:
    lowered = text.lower().strip()
    if resolve_order_reference(state, text):
        return "track_specific_order"
    if detect_order_message(text):
        return "order_checkout"
    if lowered in {"1", "order", "order food", "new order", "order again", "another order"} and state["stage"] in {
        STAGE_MAIN_MENU,
        STAGE_PREPARING,
        STAGE_SERVED,
    }:
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
    if any(
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
    state["payment_pending_since"] = None
    if state.get("active_order_id"):
        state["last_completed_order_id"] = state["active_order_id"]
    sync_active_order_record(state)


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
        if not state.get("active_order_id") or state.get("payment_status") == "done" or state.get("order_stage") in {"preparing", "served"}:
            start_new_order(state, user_id)
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
        state["payment_pending_since"] = None
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
                return payment_success_message(state)
        except requests.RequestException as exc:
            logger.warning("Payment verification failed: %s", exc)
        pending_since = state.get("payment_pending_since")
        now = utc_now()
        if not pending_since:
            state["payment_pending_since"] = now
            schedule_payment_followup(user_id, state.get("active_order_id", ""))
            return choose_variant(
                state,
                "payment_wait_60",
                [
                    "I’m not seeing the payment in the gateway yet. Please wait 60 seconds. If the gateway updates it, I’ll confirm you right away.",
                    "The gateway hasn’t reflected the payment yet. Please give it 60 seconds. Once it updates, I’ll confirm it for you.",
                    "I can’t see the payment on the gateway yet. Please wait 60 seconds and I’ll confirm it as soon as it appears.",
                ],
            )
        if now - pending_since < timedelta(seconds=60):
            return choose_variant(
                state,
                "payment_waiting_window",
                [
                    "I’m still checking the gateway. Please allow the full 60 seconds and I’ll confirm it if the update arrives.",
                    "The 60-second gateway check is still running. Give me a little more time and I’ll confirm it if it comes through.",
                    "I’m still within the payment check window. Please wait a bit longer and I’ll confirm it if the gateway updates.",
                ],
            )

        state["payment_method"] = "counter"
        state["payment_status"] = "done"
        finalize_confirmation(state)
        cancel_payment_followup(user_id, state.get("active_order_id", ""))
        return payment_counter_fallback_message(state)
    if action == "pay_at_counter":
        state["payment_method"] = "counter"
        state["failure_count"] = 0
        cancel_payment_followup(user_id, state.get("active_order_id", ""))
        finalize_confirmation(state)
        return (
            "Perfect. Payment is marked for counter. Your order is now being prepared \U0001f37d\ufe0f\n\n"
            "\u23f3 Estimated time: 15 minutes\n\n"
            f"{generate_final_order_summary(state)}"
        )
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
    if intent == "track_specific_order":
        order_id = resolve_order_reference(state, text)
        record = get_order_record(state, order_id or "")
        if not record:
            return "I couldn’t find that order ID. Please check it and send it again."
        return generate_tracked_order_summary(record, state)
    if intent == "reservation":
        return execute_action(user_id, state, {"action": "book_table"}, text)
    if intent == "reservation_check":
        return execute_action(user_id, state, {"action": "check_reservation"}, text)
    if intent == "order_status":
        order_ids = sorted_order_ids(state)
        if len(order_ids) > 1 and not resolve_order_reference(state, text):
            return recent_order_choices_message(state)
        if len(order_ids) == 1:
            record = get_order_record(state, order_ids[0])
            if record:
                return generate_tracked_order_summary(record, state)
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
        if state.get("payment_status") == "done" and state.get("payment_method") == "counter":
            return choose_variant(
                state,
                "payment_claim_counter",
                [
                    "You’re all set. This order is already marked for counter payment and is being prepared now.",
                    "You’re good to go. This one is already set as pay at counter and the kitchen is on it.",
                    "No issue. This order is already marked for counter payment, and preparation is underway.",
                ],
            )
        if state.get("payment_status") == "done":
            return choose_variant(
                state,
                "payment_claim_done",
                [
                    "You’re all set. I already have this order marked as paid.",
                    "No worries, this order is already marked as paid on my side.",
                    "All good, I already have payment recorded for this order.",
                ],
            )
        state["payment_verification_attempts"] += 1
        try:
            if verify_online_payment(state):
                finalize_confirmation(state)
                return (
                    "Thank you for waiting. I’ve confirmed the payment and your order is now being prepared \U0001f37d\ufe0f\n\n"
                    "\u23f3 Estimated time: 15 minutes\n\n"
                    f"{generate_final_order_summary(state)}"
                )
        except requests.RequestException as exc:
            logger.warning("Payment verification failed: %s", exc)
        if state.get("payment_method") != "online":
            return choose_variant(
                state,
                "payment_claim_no_online",
                [
                    "I don’t have this order marked under online payment. If you used the payment link, give me a moment and I’ll keep checking.",
                    "This order isn’t currently tagged as an online payment on my side. If you paid through the link, I can keep rechecking it.",
                    "I’m not seeing this order under online payment yet. If you paid with the link, I’ll keep checking the gateway.",
                ],
            )
        pending_since = state.get("payment_pending_since")
        now = utc_now()
        if not pending_since:
            state["payment_pending_since"] = now
            schedule_payment_followup(user_id, state.get("active_order_id", ""))
            return choose_variant(
                state,
                "payment_claim_wait_60",
                [
                    "I hear you. The payment is not visible in the gateway yet. Please wait 60 seconds. If it updates, I’ll confirm you.",
                    "Understood. I’m not seeing it on the gateway yet. Please give it 60 seconds and I’ll confirm it if the update arrives.",
                    "Thanks for telling me. The gateway hasn’t reflected it yet. Please wait 60 seconds and I’ll confirm it if it appears.",
                ],
            )
        if now - pending_since < timedelta(seconds=60):
            return choose_variant(
                state,
                "payment_claim_waiting_window",
                [
                    "I’m still checking the gateway for your payment. Please allow the full 60 seconds.",
                    "The 60-second verification window is still active. I’m checking for the update now.",
                    "I’m still within the payment-check window. Give me a little more time to confirm it.",
                ],
            )

        state["payment_method"] = "counter"
        state["payment_status"] = "done"
        finalize_confirmation(state)
        cancel_payment_followup(user_id, state.get("active_order_id", ""))
        return payment_counter_fallback_message(state)
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
        if not state.get("active_order_id") or state.get("payment_status") == "done" or state.get("order_stage") in {"preparing", "served"}:
            start_new_order(state, user_id)
        state["intent"] = "order"
        state["waiting_for_order"] = False
        if state.get("checkout_mode") == "replace":
            state["order"] = {
                "order_id": state.get("active_order_id", state.get("order", {}).get("order_id", "")),
                "name": parsed.get("name", ""),
                "items": parsed.get("items", []),
                "total": parsed.get("total", Decimal("0.00")),
                "currency": parsed.get("currency", DEFAULT_CURRENCY),
            }
        else:
            state["order"] = merge_orders(state["order"], parsed)
            state["order"]["order_id"] = state.get("active_order_id", state["order"].get("order_id", ""))
        update_customer_profile(state, parsed.get("profile", {}))
        state["order"]["name"] = state.get("customer_profile", {}).get("name", state["order"].get("name", ""))
        state["order_confirmed"] = False
        state["payment_status"] = "pending"
        state["payment_method"] = ""
        state["payment_link"] = ""
        state["payment_link_id"] = ""
        state["payment_verification_attempts"] = 0
        state["checkout_mode"] = "append"
        set_stage(state, STAGE_ORDER_ACTION)
        state["failure_count"] = 0
        sync_active_order_record(state)
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
        max_words = 220 if "Order ID:" in reply or "Total Amount:" in reply or "Total:" in reply else 80
        send_whatsapp_message(sender, reply, max_words=max_words)
        append_sheet_log(sender, get_state(sender), "message_handled")
    except requests.RequestException as exc:
        logger.exception("External API error: %s", exc)
    except Exception as exc:
        logger.exception("Unhandled webhook error: %s", exc)

    return "EVENT_RECEIVED", 200


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
