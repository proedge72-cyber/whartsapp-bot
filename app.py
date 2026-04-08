import base64
import difflib
import json
import logging
import os
import random
import re
import sqlite3
import string
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
GOOGLE_SERVICE_ACCOUNT_JSON_B64 = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON_B64", "")
GOOGLE_SHEETS_EVENT_SHEET = os.getenv("GOOGLE_SHEETS_EVENT_SHEET", "Events")
GOOGLE_SHEETS_ORDER_SHEET = os.getenv("GOOGLE_SHEETS_ORDER_SHEET", "Orders")
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

FIXED_MENU_PAGES = {
    1: {
        "tab": "Pantry & Pairings",
        "label": "Pantry & Pairings",
        "title": "Riso, Pane, Bevande & Dolci",
        "categories": [
            {"name": "Riso & Biryani", "items": [
                {"name": "Steamed Basmati Rice", "description": "riso Basmati", "price": "4", "highlight": False},
                {"name": "Jeera Rice", "description": "riso con cumino Profumato", "price": "5", "highlight": True},
                {"name": "Veg Biryani", "description": "riso Basmati, verdure, spezie", "price": "7", "highlight": False},
                {"name": "Chicken Biryani", "description": "riso Basmati, verdure, spezie Ricco. Aromatico", "price": "8", "highlight": False},
                {"name": "Lamb Biryani", "description": "riso Basmati, agnello, spezie Profondo. Intenso", "price": "10", "highlight": False},
                {"name": "Prawn Biryani", "description": "riso Basmati, gamberi, spezie Elegante. Premium", "price": "10", "highlight": False}
            ]},
            {"name": "Pane Indiano", "items": [
                {"name": "Naan", "description": "farina, Acqua, lieveto", "price": "1.50", "highlight": False},
                {"name": "Butter Naan", "description": "Farina, aglio, burro", "price": "2", "highlight": False},
                {"name": "Garlic Naan", "description": "farina, pate, spezie", "price": "2.50", "highlight": False},
                {"name": "Cheese Naan", "description": "farina, pate, spezie", "price": "3", "highlight": False},
                {"name": "Aloo Kulcha", "description": "farina, pate, spezie Morbido. Semplice", "price": "2.50", "highlight": False},
                {"name": "Amritsari Kulcha", "description": "farina, patate, cipolla, spezie Croccante. Ricco", "price": "3", "highlight": False}
            ]},
            {"name": "Bevande", "items": [
                {"name": "Acqua Naturale", "price": "3", "highlight": False},
                {"name": "Acqua Frizzante", "price": "3.50", "highlight": False},
                {"name": "Coca-Cola Zero", "price": "3", "highlight": False},
                {"name": "Coca-Cola", "price": "2.50", "highlight": False},
                {"name": "Redbull", "price": "3", "highlight": False},
                {"name": "Te Pesca/Limone", "price": "2.50", "highlight": False},
                {"name": "Birra Moretti", "price": "3/5", "highlight": False},
                {"name": "Corona", "price": "4", "highlight": False},
                {"name": "Kingfisher", "price": "4/6", "highlight": False},
                {"name": "Peroni", "price": "4/6", "highlight": False},
                {"name": "Mango Lassi", "price": "3", "highlight": False},
                {"name": "Sweet Lassi Classico", "price": "2.50", "highlight": False},
                {"name": "Roohafza", "price": "2.50", "highlight": False},
                {"name": "Lemon Soda", "price": "2", "highlight": False},
                {"name": "Masala Tea", "price": "2.50", "highlight": False}
            ]},
            {"name": "Dolci", "items": [
                {"name": "Tiramisu Classico", "description": "Mascarpone, caffe, cacao", "price": "5", "highlight": False},
                {"name": "Gulab Jamun Caldo", "description": "Latte, sciroppo, zucchero", "price": "4", "highlight": False},
                {"name": "Jalebi", "description": "farina, yogurt, zucchero, sciroppo, zafferano", "price": "3.50", "highlight": False},
                {"name": "Gelato", "description": "cioccolato, mango, fragola", "price": "3", "highlight": False},
                {"name": "Gelato Vegano", "description": "vaniglia, mandorla, cioccolato", "price": "3.50", "highlight": False}
            ]}
        ]
    },
    2: {
        "tab": "Curries & Classics",
        "label": "Curries & Classics",
        "title": "Vegetariani, Pollo, Agnello & Pesce",
        "categories": [
            {"name": "Piatti Vegetariani", "items": [
                {"name": "Paneer Butter Masala", "description": "formaggio fresco indiano, Burro, Pomodoro Cremoso. Avvolgente", "price": "10", "highlight": True},
                {"name": "Shahi Paneer", "description": "formaggio fresco indiano, Pomodoro, Anacardi Ricco. Elegante", "price": "10", "highlight": False},
                {"name": "Matar Paneer Classico", "description": "formaggio fresco indiano, Piselli, Pomodoro Semplice. Confortevole", "price": "9", "highlight": False},
                {"name": "Kadhai Paneer", "description": "formaggio fresco indiano, Pepperoni, Spezie", "price": "10", "highlight": False},
                {"name": "Palak Paneer Cremoso", "description": "formaggio fresco indiano, Spinaci, Panna", "price": "11", "highlight": False},
                {"name": "Mushroom Masala", "description": "funghi, pomodoro, Cipolla, Spezie Morbido. Succoso. Cremoso", "price": "10", "highlight": True},
                {"name": "Punjabi Rajma", "description": "fagioli Rossi, cipolla, spezie", "price": "9", "highlight": False},
                {"name": "Malai Kofta Imperiale", "description": "polpette, panna, zafferano Morbido. Premium", "price": "12", "highlight": False},
                {"name": "Mix Veg Affumicato", "description": "verdure miste, Spezie, Burro/Panna Ricco. Aromatico", "price": "9", "highlight": True},
                {"name": "Amritsari Chana Masala", "description": "Ceci, Pomodoro, Spezie", "price": "8", "highlight": False},
                {"name": "Baingan Bharta", "description": "melanzane, pomodoro, spezie", "price": "11", "highlight": False},
                {"name": "Dal Makhani Aromatico", "description": "lenticchie nere, Burro, Panna", "price": "8", "highlight": False},
                {"name": "Dal Punjabi", "description": "lenticchie gialle, Aglio, Cumino", "price": "9", "highlight": False}
            ]},
            {"name": "Piatti con Pollo", "items": [
                {"name": "Butter Chicken", "description": "pollo, burro, pomodoro, panna Ricco. Cremoso. Iconico", "price": "10", "highlight": True},
                {"name": "Tikka Masala Classico", "description": "pollo, pomodoro, panna Morbido. Bilanciato", "price": "11", "highlight": True},
                {"name": "Chicken Kadhai", "description": "pollo, peperoni, spezie", "price": "10", "highlight": False},
                {"name": "Chicken Curry Classico", "description": "pollo, pomodoro, spezie", "price": "9", "highlight": False},
                {"name": "Chicken Korma Cremoso", "description": "pollo, anacardi, panna", "price": "10", "highlight": False},
                {"name": "Chicken Madras", "description": "pollo, vindaloo, semi di senape, foglie di curry", "price": "12", "highlight": False}
            ]},
            {"name": "Piatti con Agnello", "items": [
                {"name": "Lamb Madras", "description": "agnello, vindaloo, semi di senape, foglie di curry Profondo. Aromatico", "price": "12", "highlight": False},
                {"name": "Lamb Curry Classico", "description": "agnello, spezie", "price": "11", "highlight": False}
            ]},
            {"name": "Piatti con Pesce", "items": [
                {"name": "Fish Curry Classico", "description": "pesce, pomodoro, spezie", "price": "11", "highlight": False},
                {"name": "Prawn Curry Imperiale", "description": "gamberi, Spezie, Panna Ricco. Premium", "price": "12", "highlight": True}
            ]}
        ]
    },
    3: {
        "tab": "Street & Tandoor",
        "label": "Street & Tandoor",
        "title": "Antipasti, Street Food & Tandoor",
        "categories": [
            {"name": "Antipasti Non Vegetariani", "items": [
                {"name": "Indo Chilli Chicken", "description": "pollo, peperoncino, salsa di Soia Desio. Piccante. Avvolgente", "price": "9", "highlight": True},
                {"name": "Chicken Pakoda Dorato", "description": "pollo, farina di ceci, spezie", "price": "7", "highlight": False},
                {"name": "Fish Pakoda Marino", "description": "pesce orata, zenzero, aglio", "price": "8", "highlight": False},
                {"name": "Keema Samosa Imperiale", "description": "carne macinata, piselli, spezie", "price": "6", "highlight": False}
            ]},
            {"name": "Antipasti Vegetariani", "items": [
                {"name": "Indo Chilli Paneer", "description": "paneer, peperoncino, salsa di Soia Agrodolce. Intenso", "price": "8", "highlight": True},
                {"name": "Paneer Pakoda Croccante", "description": "formaggio fresco indiano, farina di ceci, peperoncino", "price": "9", "highlight": False},
                {"name": "Mix Pakoda Croccante", "description": "verdure mix, pastella speziata", "price": "5", "highlight": False},
                {"name": "Punjabi Samosa Classic", "description": "patate, piselli, tamarindo Croccante. Caldo. Tradizionale", "price": "4", "highlight": True},
                {"name": "Honey Chilli Potato", "description": "patate, miele, peperoncino, sesamo", "price": "4", "highlight": False},
                {"name": "Cheese Balls Croccanti", "description": "formaggio, panatura, dorata", "price": "4", "highlight": False},
                {"name": "Agni Veg Platter", "description": "samosa, pakoda, tikki Perfetto da condividere", "price": "10", "highlight": True},
                {"name": "Spring Roll Croccanti", "description": "verdure, sfoglia croccante", "price": "7", "highlight": False}
            ]},
            {"name": "Agni's Street Specials", "items": [
                {"name": "Desi Chowmin", "description": "noodles, verdure/pollo, salsa", "price": "7", "highlight": False},
                {"name": "Bombay Bhel Puri", "description": "riso soffiato, salsa dolce, salsa piccante, verdure", "price": "8", "highlight": False},
                {"name": "Tikki Chaat Classico", "description": "patate, ceci, yogurt", "price": "5", "highlight": False},
                {"name": "Golgappe", "description": "sfoglie croccanti, acqua speziata Fresco. Divertente. Iconico", "price": "5", "highlight": False}
            ]},
            {"name": "Specialita al Tandoor", "items": [
                {"name": "Chicken Tikka Classico", "description": "pollo, yogurt, spezie Morbido. Aromatico e Ben marinato", "price": "10", "highlight": True},
                {"name": "Tandoori Chicken", "description": "pollo, yogurt, spezie affumicate", "price": "13", "highlight": True},
                {"name": "Malai Tikka Cremoso", "description": "pollo, panna, anacardi", "price": "11", "highlight": False},
                {"name": "Green Haryali Tikka", "description": "pollo, menta, coriandolo", "price": "11", "highlight": False},
                {"name": "Fish Tikka Marino", "description": "pesce orata, erbe, spezie Leggero. Grigliato", "price": "12", "highlight": False},
                {"name": "King Prawns Imperiali", "description": "gamberi, zenzero, aglio Succoso. Premium", "price": "13", "highlight": True},
                {"name": "Paneer Tikka Affumicato", "description": "formaggio fresco indiano, Pepperoni, cipolla", "price": "12", "highlight": False},
                {"name": "Mixed Grill Imperiale", "description": "pollo, pesce, keema", "price": "15", "highlight": True}
            ]}
        ]
    }
}

db_lock = threading.Lock()
processed_message_ids = set()
payment_followup_timers: Dict[str, threading.Timer] = {}
payment_timer_lock = threading.Lock()
google_sheets_client = None
google_sheets_lock = threading.RLock()
google_order_ids_cache: set[str] = set()
google_order_ids_loaded = False

EVENT_SHEET_HEADERS = [
    "Timestamp",
    "User ID",
    "Event Type",
    "Order ID",
    "Customer Name",
    "Mobile",
    "Email",
    "Service Type",
    "Preferred Time",
    "Guests",
    "Total",
    "Currency",
    "Payment Status",
    "Payment Method",
    "Order Stage",
    "Conversation Stage",
    "State JSON",
]
ORDER_SHEET_HEADERS = [
    "Last Updated",
    "Order ID",
    "User ID",
    "Customer Name",
    "Mobile",
    "Email",
    "Service Type",
    "Preferred Time",
    "Guests",
    "Items Summary",
    "Total",
    "Currency",
    "Payment Status",
    "Payment Method",
    "Order Stage",
    "Conversation Stage",
    "Last Event Type",
]
RESERVATION_SHEET_HEADERS = [
    "Timestamp",
    "User ID",
    "Reservation Type",
    "Reservation Message",
]
SHEET_LAYOUTS: Dict[str, Dict[str, Any]] = {
    GOOGLE_SHEETS_EVENT_SHEET: {
        "headers": EVENT_SHEET_HEADERS,
        "widths": [190, 120, 120, 130, 170, 120, 220, 130, 130, 110, 90, 90, 120, 120, 110, 150, 420],
    },
    GOOGLE_SHEETS_ORDER_SHEET: {
        "headers": ORDER_SHEET_HEADERS,
        "widths": [190, 130, 120, 170, 120, 220, 130, 130, 110, 320, 90, 90, 120, 120, 110, 150, 120],
    },
    GOOGLE_SHEETS_RESERVATION_SHEET: {
        "headers": RESERVATION_SHEET_HEADERS,
        "widths": [190, 120, 140, 420],
    },
}


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


def normalize_google_service_account_info(service_account_info: Dict[str, Any]) -> Dict[str, Any]:
    normalized = dict(service_account_info)
    private_key = normalized.get("private_key")
    if isinstance(private_key, str):
        normalized["private_key"] = private_key.replace("\\n", "\n").strip()
    return normalized


def load_google_service_account_info() -> Tuple[Optional[Dict[str, Any]], str]:
    if GOOGLE_SERVICE_ACCOUNT_JSON.strip():
        try:
            return normalize_google_service_account_info(json.loads(GOOGLE_SERVICE_ACCOUNT_JSON)), "GOOGLE_SERVICE_ACCOUNT_JSON"
        except json.JSONDecodeError as exc:
            logger.warning("Invalid GOOGLE_SERVICE_ACCOUNT_JSON: %s", exc)

    if GOOGLE_SERVICE_ACCOUNT_JSON_B64.strip():
        try:
            decoded = base64.b64decode(GOOGLE_SERVICE_ACCOUNT_JSON_B64).decode("utf-8")
            return normalize_google_service_account_info(json.loads(decoded)), "GOOGLE_SERVICE_ACCOUNT_JSON_B64"
        except Exception as exc:
            logger.warning("Invalid GOOGLE_SERVICE_ACCOUNT_JSON_B64: %s", exc)

    if GOOGLE_SERVICE_ACCOUNT_FILE.exists():
        try:
            data = json.loads(GOOGLE_SERVICE_ACCOUNT_FILE.read_text(encoding="utf-8"))
            return normalize_google_service_account_info(data), str(GOOGLE_SERVICE_ACCOUNT_FILE)
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
            "Google Sheets disabled: no valid service account credentials found in GOOGLE_SERVICE_ACCOUNT_JSON, GOOGLE_SERVICE_ACCOUNT_JSON_B64, or %s.",
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


def ensure_google_worksheet_layout(worksheet: gspread.Worksheet, headers: List[str], widths: List[int]) -> None:
    current_headers = worksheet.row_values(1)
    if current_headers[: len(headers)] != headers:
        worksheet.update(f"A1:{gspread.utils.rowcol_to_a1(1, len(headers)).rstrip('1')}1", [headers], value_input_option="USER_ENTERED")

    requests_payload = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": worksheet.id,
                    "startRowIndex": 0,
                    "endRowIndex": 1,
                    "startColumnIndex": 0,
                    "endColumnIndex": len(headers),
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {"red": 0.09, "green": 0.34, "blue": 0.22},
                        "horizontalAlignment": "CENTER",
                        "textFormat": {
                            "bold": True,
                            "foregroundColor": {"red": 1, "green": 1, "blue": 1},
                        },
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": worksheet.id,
                    "gridProperties": {"frozenRowCount": 1},
                },
                "fields": "gridProperties.frozenRowCount",
            }
        },
        {
            "setBasicFilter": {
                "filter": {
                    "range": {
                        "sheetId": worksheet.id,
                        "startRowIndex": 0,
                        "startColumnIndex": 0,
                        "endColumnIndex": len(headers),
                    }
                }
            }
        },
    ]
    for index, width in enumerate(widths):
        requests_payload.append(
            {
                "updateDimensionProperties": {
                    "range": {
                        "sheetId": worksheet.id,
                        "dimension": "COLUMNS",
                        "startIndex": index,
                        "endIndex": index + 1,
                    },
                    "properties": {"pixelSize": width},
                    "fields": "pixelSize",
                }
            }
        )
    worksheet.spreadsheet.batch_update({"requests": requests_payload})


def ensure_google_sheet_structure(sheet_name: str, worksheet: gspread.Worksheet) -> None:
    layout = SHEET_LAYOUTS.get(sheet_name)
    if not layout:
        return
    ensure_google_worksheet_layout(worksheet, layout["headers"], layout["widths"])


def get_google_sheets_client() -> Optional[gspread.Client]:
    global google_sheets_client
    if google_sheets_client is not None:
        return google_sheets_client
    with google_sheets_lock:
        if google_sheets_client is None:
            logger.info("Retrying Google Sheets client initialization on demand.")
            google_sheets_client = initialize_google_sheets_client()
    return google_sheets_client


google_sheets_client = initialize_google_sheets_client()


def log_google_sheets_status() -> None:
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        return
    client = get_google_sheets_client()
    if not client:
        logger.warning("Google Sheets startup check skipped: client is unavailable.")
        return
    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        worksheet_titles = [worksheet.title for worksheet in spreadsheet.worksheets()]
        logger.info(
            "Google Sheets startup check passed: spreadsheet='%s', worksheets=%s",
            spreadsheet.title,
            worksheet_titles,
        )
        for sheet_name in (GOOGLE_SHEETS_EVENT_SHEET, GOOGLE_SHEETS_ORDER_SHEET, GOOGLE_SHEETS_RESERVATION_SHEET):
            get_or_create_google_worksheet(sheet_name)
    except Exception as exc:
        logger.warning("Google Sheets startup check failed: %s", exc)

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
            "insights": {
                "favorite_items": [],
                "avg_order_value": Decimal("0.00"),
                "order_frequency": 0,
                "preferred_time": "",
            },
            "preferences": {
                "item_counts": {},
                "category_counts": {},
                "modifiers": [],
                "time_slots": {},
            },
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
        "pending_suggested_item": "",
        "recent_suggestions": [],
        "response_counters": {},
        "last_message_at": None,
        "confirmed_at": None,
        "stage_updated_at": None,
    }


def serialize_state(state: Dict[str, Any]) -> Dict[str, Any]:
    safe = deepcopy(state)
    safe["order"]["total"] = str(safe["order"]["total"])
    insights = safe.get("customer_profile", {}).get("insights", {})
    if "avg_order_value" in insights:
        insights["avg_order_value"] = str(insights.get("avg_order_value", "0.00"))
    for item in safe["order"]["items"]:
        item["price"] = str(item["price"])
    for order_id, record in safe.get("orders", {}).items():
        record["total"] = str(record.get("total", "0.00"))
        record_insights = record.get("customer_profile", {}).get("insights", {})
        if "avg_order_value" in record_insights:
            record_insights["avg_order_value"] = str(record_insights.get("avg_order_value", "0.00"))
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
    insights = state.get("customer_profile", {}).get("insights", {})
    if "avg_order_value" in insights:
        insights["avg_order_value"] = Decimal(str(insights.get("avg_order_value", "0.00")))
    for item in state["order"]["items"]:
        item["price"] = Decimal(item.get("price", "0.00"))
    for order_id, record in state.get("orders", {}).items():
        record["total"] = Decimal(record.get("total", "0.00"))
        record_insights = record.get("customer_profile", {}).get("insights", {})
        if "avg_order_value" in record_insights:
            record_insights["avg_order_value"] = Decimal(str(record_insights.get("avg_order_value", "0.00")))
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
    if "pending_suggested_item" not in state:
        state["pending_suggested_item"] = ""
    if "recent_suggestions" not in state:
        state["recent_suggestions"] = []
    profile = state.setdefault("customer_profile", {})
    profile.setdefault("insights", {})
    profile["insights"].setdefault("favorite_items", [])
    profile["insights"].setdefault("avg_order_value", Decimal("0.00"))
    profile["insights"].setdefault("order_frequency", 0)
    profile["insights"].setdefault("preferred_time", "")
    profile.setdefault("preferences", {})
    profile["preferences"].setdefault("item_counts", {})
    profile["preferences"].setdefault("category_counts", {})
    profile["preferences"].setdefault("modifiers", [])
    profile["preferences"].setdefault("time_slots", {})
    return state


def empty_order() -> Dict[str, Any]:
    return {
        "order_id": "",
        "name": "",
        "items": [],
        "total": Decimal("0.00"),
        "currency": DEFAULT_CURRENCY,
    }


def generate_session_order_id(user_id: str, state: Dict[str, Any]) -> str:
    state["order_sequence"] = int(state.get("order_sequence", 0)) + 1
    suffix = "".join(ch for ch in user_id if ch.isdigit())[-4:] or "0000"
    return f"AGN-{suffix}-{state['order_sequence']:04d}"


def generate_order_id() -> str:
    date_part = datetime.now().strftime("%Y%m%d")
    random_part = "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
    return f"ORD-{date_part}-{random_part}"


def is_unique_order_id(order_id: str, existing_ids: set[str]) -> bool:
    return order_id not in existing_ids


def get_unique_order_id(existing_ids: set[str]) -> str:
    while True:
        order_id = generate_order_id()
        if is_unique_order_id(order_id, existing_ids):
            return order_id


def update_state_order_id(state: Dict[str, Any], new_order_id: str) -> None:
    previous_order_id = state.get("active_order_id") or state.get("order", {}).get("order_id", "")
    if previous_order_id == new_order_id:
        return

    orders = state.setdefault("orders", {})
    existing_record = deepcopy(orders.get(previous_order_id, {}))

    state["active_order_id"] = new_order_id
    state["last_completed_order_id"] = new_order_id
    state.setdefault("order", {})["order_id"] = new_order_id

    if previous_order_id and previous_order_id in orders:
        orders.pop(previous_order_id, None)

    if existing_record:
        existing_record["order_id"] = new_order_id
        orders[new_order_id] = existing_record


def build_order_sheet_row(user_id: str, state: Dict[str, Any], event_type: str, timestamp: Optional[str] = None) -> List[str]:
    order = state.get("order", {})
    profile = state.get("customer_profile", {})
    items_summary = ", ".join(
        f"{item.get('name', '')} x{item.get('qty', 0)}"
        + (f" [{item.get('notes', '')}]" if item.get("notes") else "")
        for item in order.get("items", [])
    )
    return [
        timestamp or utc_now().isoformat(),
        order.get("order_id", ""),
        user_id,
        profile.get("name", ""),
        profile.get("mobile", ""),
        profile.get("email", ""),
        profile.get("service_type", ""),
        profile.get("preferred_time", ""),
        profile.get("guests", ""),
        items_summary,
        str(order.get("total", Decimal("0.00"))),
        order.get("currency", DEFAULT_CURRENCY),
        state.get("payment_status", ""),
        state.get("payment_method", ""),
        state.get("order_stage", ""),
        state.get("stage", ""),
        event_type,
    ]


def fetch_existing_order_ids(worksheet: gspread.Worksheet) -> set[str]:
    order_ids = worksheet.col_values(2)
    return {order_id.strip() for order_id in order_ids[1:] if order_id.strip()}


def get_cached_existing_order_ids(worksheet: gspread.Worksheet, force_refresh: bool = False) -> set[str]:
    global google_order_ids_loaded
    with google_sheets_lock:
        if force_refresh or not google_order_ids_loaded:
            google_order_ids_cache.clear()
            google_order_ids_cache.update(fetch_existing_order_ids(worksheet))
            google_order_ids_loaded = True
        return set(google_order_ids_cache)


def reserve_unique_google_order_id(worksheet: gspread.Worksheet, previous_order_id: str = "") -> str:
    existing_ids = get_cached_existing_order_ids(worksheet)
    if previous_order_id:
        existing_ids.discard(previous_order_id)
    new_order_id = get_unique_order_id(existing_ids)
    with google_sheets_lock:
        google_order_ids_cache.add(new_order_id)
    return new_order_id


def save_confirmed_order_to_google_sheets(
    user_id: str,
    state: Dict[str, Any],
    previous_order_id: str = "",
    event_type: str = "order_confirmed",
) -> None:
    worksheet = get_or_create_google_worksheet(GOOGLE_SHEETS_ORDER_SHEET)
    if not worksheet:
        raise RuntimeError("worksheet_unavailable")

    timestamp = utc_now().isoformat()
    current_order_id = state.get("order", {}).get("order_id", "")

    with google_sheets_lock:
        order_ids = worksheet.col_values(2)
        row = build_order_sheet_row(user_id, state, event_type, timestamp)

        target_row_index = None
        for index, existing_order_id in enumerate(order_ids[1:], start=2):
            if existing_order_id == previous_order_id or existing_order_id == current_order_id:
                target_row_index = index
                break

        if target_row_index is None:
            worksheet.append_row(row, value_input_option="USER_ENTERED", table_range="A1")
        else:
            worksheet.update(f"A{target_row_index}:Q{target_row_index}", [row], value_input_option="USER_ENTERED")

        google_order_ids_cache.update(order_id.strip() for order_id in order_ids[1:] if order_id.strip())
        if state.get("order", {}).get("order_id"):
            google_order_ids_cache.add(state["order"]["order_id"])


def persist_confirmed_order_async(user_id: str, state_snapshot: Dict[str, Any], previous_order_id: str) -> None:
    try:
        save_confirmed_order_to_google_sheets(user_id, state_snapshot, previous_order_id=previous_order_id)
    except Exception as exc:
        logger.warning("Confirmed order save failed: %s", exc)
        try:
            send_whatsapp_message(user_id, "⚠️ Order could not be saved. Please try again.")
        except Exception as send_exc:
            logger.warning("Failed to send order save error message: %s", send_exc)


def confirm_order_and_store(user_id: str, state: Dict[str, Any]) -> Optional[str]:
    try:
        worksheet = get_or_create_google_worksheet(GOOGLE_SHEETS_ORDER_SHEET)
        if not worksheet:
            raise RuntimeError("worksheet_unavailable")
        previous_order_id = state.get("active_order_id") or state.get("order", {}).get("order_id", "")
        new_order_id = reserve_unique_google_order_id(worksheet, previous_order_id=previous_order_id)
        update_state_order_id(state, new_order_id)
    except Exception as exc:
        logger.warning("Confirmed order save failed: %s", exc)
        return "⚠️ Order could not be saved. Please try again."

    finalize_confirmation(state)
    state_snapshot = deepcopy(state)
    worker = threading.Thread(
        target=persist_confirmed_order_async,
        args=(user_id, state_snapshot, previous_order_id),
        daemon=True,
    )
    worker.start()
    return None


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
    new_order_id = generate_session_order_id(user_id, state)
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
    match = re.search(r"\b(?:AGN-\d{4}-\d{4}|ORD-\d{8}-[A-Z0-9]{5})\b", text.upper())
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
    suffix_match = re.search(r"\b([A-Z0-9]{4,5})\b", upper_text)
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


def normalize_menu_text(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (text or "").lower()).strip()


def normalize_price(price_string: str) -> Decimal:
    cleaned = str(price_string or "").strip().replace("\u20ac", "").replace("\u20b9", "").replace(",", "")
    if not cleaned:
        return Decimal("0.00")
    if "/" in cleaned:
        options = [parse_decimal(part) for part in cleaned.split("/") if part.strip()]
        return max(options) if options else Decimal("0.00")
    return parse_decimal(cleaned)


def parse_order(text: str) -> List[Dict[str, Any]]:
    parsed_map: Dict[str, Dict[str, Any]] = {}
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.lower().startswith("order:") or line.lower().startswith("total:"):
            continue

        match = re.match(r"^(?:(?P<qty>\d+)\s*x\s*)?(?P<body>.+)$", line, flags=re.IGNORECASE)
        if not match:
            continue

        quantity = int(match.group("qty") or 1)
        body = re.sub(r"^[*\-\u2022]\s*", "", match.group("body") or "").strip()
        price_match = re.search(r"[-:=]\s*([\u20ac\u20b9\?]?\s*\d+(?:[.,]\d{1,2})?(?:/\d+(?:[.,]\d{1,2})?)?)\s*$", body)
        user_price = normalize_price(price_match.group(1)) if price_match else Decimal("0.00")
        item_name = body[: price_match.start()].strip() if price_match else body
        if not item_name:
            continue
        key = normalize_menu_text(item_name)
        if key in parsed_map:
            parsed_map[key]["quantity"] += quantity
            parsed_map[key]["user_price"] += user_price
            continue
        parsed_map[key] = {
            "item_name": item_name,
            "quantity": quantity,
            "user_price": user_price,
        }
    return list(parsed_map.values())


def find_menu_item(name: str) -> Optional[Dict[str, Any]]:
    normalized_name = normalize_menu_text(name)
    if not normalized_name:
        return None

    all_items: List[Tuple[str, Dict[str, Any]]] = []
    for page in FIXED_MENU_PAGES.values():
        for category in page["categories"]:
            for item in category["items"]:
                all_items.append((normalize_menu_text(item["name"]), item))

    for candidate_name, item in all_items:
        if candidate_name == normalized_name:
            return item

    close_matches = difflib.get_close_matches(normalized_name, [candidate for candidate, _ in all_items], n=1, cutoff=0.8)
    if not close_matches:
        return None
    winner = close_matches[0]
    for candidate_name, item in all_items:
        if candidate_name == winner:
            return item
    return None


def calculate_total(valid_items: List[Dict[str, Any]]) -> Decimal:
    return sum((item["real_price"] * item["quantity"] for item in valid_items), Decimal("0.00"))


def validate_order(parsed_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    corrected_items: List[Dict[str, Any]] = []
    invalid_items: List[str] = []
    fraud_detected = False

    for parsed_item in parsed_items:
        menu_item = find_menu_item(parsed_item["item_name"])
        if not menu_item:
            invalid_items.append(parsed_item["item_name"])
            continue

        real_price = normalize_price(menu_item["price"])
        expected_total = (real_price * parsed_item["quantity"]).quantize(Decimal("0.01"))
        user_price = parsed_item["user_price"].quantize(Decimal("0.01"))
        if user_price > Decimal("0.00") and user_price != expected_total:
            fraud_detected = True

        corrected_items.append(
            {
                "item_name": menu_item["name"],
                "quantity": parsed_item["quantity"],
                "user_price": user_price,
                "real_price": real_price,
                "line_total": expected_total,
            }
        )

    return {
        "corrected_items": corrected_items,
        "invalid_items": invalid_items,
        "fraud_detected": fraud_detected,
        "submitted_total": sum((item["user_price"] for item in parsed_items), Decimal("0.00")),
    }


def detect_validatable_order_message(text: str) -> bool:
    lowered = text.lower()
    if "name:" in lowered and "items" in lowered:
        return False
    return bool(re.search(r"(?:^|\n)\s*(?:order\s*:)?", lowered)) and bool(
        re.search(r"(?:^|\n)\s*(?:[*\-\u2022]\s*)?(?:\d+\s*x\s*)?.+?(?:\s*[-:=]\s*[\u20ac\u20b9]?\s*\d)", text, flags=re.IGNORECASE)
    )


def build_validated_order_message(validated: Dict[str, Any], total: Decimal) -> str:
    corrected_items = validated["corrected_items"]
    invalid_items = validated["invalid_items"]
    fraud_detected = validated["fraud_detected"]
    submitted_total = validated.get("submitted_total", Decimal("0.00"))
    if not corrected_items:
        return "I couldn’t find any valid menu items in that order. Please use the menu link and send the item names again."

    header = "⚠️ Price Correction Applied" if fraud_detected else "✅ Order Verified"
    lines = [header]
    if fraud_detected:
        lines.extend(["", "Some item prices were incorrect and have been updated.", "", "🧾 Corrected Order:"])
    else:
        lines.extend(["", "🧾 Items:"])

    for item in corrected_items:
        lines.append(f"* {item['item_name']} x{item['quantity']} → {money_to_text(item['line_total'], DEFAULT_CURRENCY)}")

    if invalid_items:
        lines.extend(["", "❌ Some items were not found in our menu and were removed.", f"Removed: {', '.join(invalid_items)}"])

    if fraud_detected and submitted_total > Decimal("0.00"):
        lines.extend(["", f"Original subtotal: {money_to_text(submitted_total, DEFAULT_CURRENCY)} → Corrected total: {money_to_text(total, DEFAULT_CURRENCY)}"])

    lines.extend(["", f"💰 Total: {money_to_text(total, DEFAULT_CURRENCY)}", "", "Reply CONFIRM to place your order."])
    return "\n".join(lines)


def build_contextual_validated_order_message(validated: Dict[str, Any], total: Decimal, state: Dict[str, Any]) -> str:
    base_message = build_validated_order_message(validated, total)
    customer_name = state.get("customer_profile", {}).get("name", "").strip()
    modifiers = state.get("customer_profile", {}).get("preferences", {}).get("modifiers", [])
    temp_order = {
        "items": [
            {"name": item["item_name"], "qty": item["quantity"], "price": item["line_total"]}
            for item in validated.get("corrected_items", [])
        ]
    }
    suggestions = suggest_items(get_user_context(state), temp_order)

    extra_lines: List[str] = []
    if customer_name:
        extra_lines.append(f"{customer_name}, I’ve checked your order against the menu.")
    if modifiers:
        extra_lines.append(f"Saved preferences active: {', '.join(modifiers)}.")
    structured_suggestions = get_structured_suggestions(get_user_context(state), temp_order)
    if structured_suggestions:
        remember_suggested_item(state, structured_suggestions[0]["item_name"])
        extra_lines.append(structured_suggestions[0]["message"])
    elif suggestions:
        extra_lines.append(suggestions[0])
    if not extra_lines:
        return base_message
    return "\n".join(extra_lines + ["", base_message])


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
        r"(?mi)^\s*(?:Total|Subtotal)\s*:\s*[\u20ac\u20b9]?\s*([0-9]+(?:[.,][0-9]{1,2})?)\s*$",
        normalized,
        flags=re.IGNORECASE | re.MULTILINE,
    )
    item_pattern = re.compile(
        r"^\s*[*\-\u2022]?\s*(?P<name>.+?)\s*x(?P<qty>\d+)(?:\s*\([^\d]*?(?P<each>[0-9]+(?:[.,][0-9]{1,2})?(?:/[0-9]+(?:[.,][0-9]{1,2})?)?)\s*each\))?\s*=\s*[^\d]?\s*(?P<price>[0-9]+(?:[.,][0-9]{1,2})?)\s*$",
        flags=re.IGNORECASE,
    )

    items: List[Dict[str, Any]] = []
    invalid_items: List[str] = []
    fraud_detected = False
    submitted_items_total = Decimal("0.00")
    for line in normalized.splitlines():
        match = item_pattern.match(line.strip())
        if not match:
            continue
        item_name = match.group("name").strip()
        quantity = int(match.group("qty"))
        user_line_price = parse_decimal(match.group("price"))
        submitted_items_total += user_line_price
        menu_item = find_menu_item(item_name)
        if not menu_item:
            invalid_items.append(item_name)
            continue
        real_unit_price = normalize_price(menu_item["price"])
        corrected_line_price = (real_unit_price * quantity).quantize(Decimal("0.01"))
        if user_line_price.quantize(Decimal("0.01")) != corrected_line_price:
            fraud_detected = True
        items.append(
            {
                "name": menu_item["name"],
                "qty": quantity,
                "price": corrected_line_price,
            }
        )

    if not items:
        return None

    currency = "EUR" if "\u20ac" in normalized else DEFAULT_CURRENCY
    derived_total = sum((item["price"] for item in items), Decimal("0.00"))
    parsed_total = parse_decimal(total_match.group(1)) if total_match else submitted_items_total
    if parsed_total.quantize(Decimal("0.01")) != derived_total.quantize(Decimal("0.01")):
        fraud_detected = True
    name_value = fields["name"].group(1).strip() if fields["name"] else ""

    return {
        "name": name_value,
        "items": items,
        "total": derived_total,
        "currency": currency,
        "fraud_detected": fraud_detected,
        "invalid_items": invalid_items,
        "submitted_total": parsed_total,
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


def get_menu_category_map() -> Dict[str, str]:
    category_map: Dict[str, str] = {}
    for page in FIXED_MENU_PAGES.values():
        for category in page["categories"]:
            for item in category["items"]:
                category_map[item["name"]] = category["name"]
    return category_map


MENU_CATEGORY_MAP = get_menu_category_map()


def summarize_menu_for_ai(max_categories: int = 6, items_per_category: int = 4) -> str:
    summary_lines: List[str] = []
    for page in FIXED_MENU_PAGES.values():
        for category in page["categories"][:max_categories]:
            item_names = [item["name"] for item in category["items"][:items_per_category]]
            summary_lines.append(f"{category['name']}: {', '.join(item_names)}")
            if len(summary_lines) >= max_categories:
                return "\n".join(summary_lines)
    return "\n".join(summary_lines)


def get_last_completed_order(state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    last_order_id = state.get("last_completed_order_id", "")
    if last_order_id:
        return deepcopy(state.get("orders", {}).get(last_order_id) or {})
    order_ids = sorted_order_ids(state)
    for order_id in reversed(order_ids):
        record = state.get("orders", {}).get(order_id) or {}
        if record.get("confirmed_at"):
            return deepcopy(record)
    return None


def get_user_context(state: Dict[str, Any]) -> Dict[str, Any]:
    orders = state.get("orders", {})
    last_order = get_last_completed_order(state) or {}
    preferences = deepcopy(state.get("customer_profile", {}).get("preferences", {}))
    item_counts: Dict[str, int] = {}
    for record in orders.values():
        for item in record.get("items", []):
            item_name = item.get("name", "")
            if not item_name:
                continue
            item_counts[item_name] = item_counts.get(item_name, 0) + int(item.get("qty", 0) or 0)

    most_frequent_items = [
        item_name
        for item_name, _ in sorted(item_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:3]
    ]
    last_order_items = [
        {
            "name": item.get("name", ""),
            "qty": item.get("qty", 0),
            "notes": item.get("notes", ""),
        }
        for item in last_order.get("items", [])
    ]
    preference_modifiers = preferences.get("modifiers", [])
    preferred_categories = [
        category
        for category, _ in sorted(
            preferences.get("category_counts", {}).items(),
            key=lambda pair: (-pair[1], pair[0]),
        )[:2]
    ]
    return {
        "last_order_items": last_order_items,
        "most_frequent_items": most_frequent_items,
        "last_order_time": last_order.get("confirmed_at"),
        "recent_suggestions": list(state.get("recent_suggestions", [])[-5:]),
        "user_preferences": {
            "modifiers": preference_modifiers,
            "preferred_categories": preferred_categories,
            "favorite_items": most_frequent_items,
        },
    }


def analyze_customer_behavior(state: Dict[str, Any]) -> Dict[str, Any]:
    orders = state.get("orders", {})
    if not orders:
        return {
            "favorite_items": [],
            "avg_order_value": Decimal("0.00"),
            "order_frequency": 0,
            "preferred_time": "",
        }

    item_counts: Dict[str, int] = {}
    total_value = Decimal("0.00")
    recent_order_count = 0
    hour_counts: Dict[int, int] = {}
    now = utc_now()

    for record in orders.values():
        record_total = record.get("total", Decimal("0.00"))
        if isinstance(record_total, Decimal):
            total_value += record_total
        confirmed_at = record.get("confirmed_at") or record.get("created_at")
        if confirmed_at:
            if now - confirmed_at <= timedelta(days=7):
                recent_order_count += 1
            hour_counts[confirmed_at.hour] = hour_counts.get(confirmed_at.hour, 0) + 1
        for item in record.get("items", []):
            item_name = item.get("name", "")
            if not item_name:
                continue
            item_counts[item_name] = item_counts.get(item_name, 0) + int(item.get("qty", 0) or 0)

    favorite_items = [
        item_name
        for item_name, _ in sorted(item_counts.items(), key=lambda pair: (-pair[1], pair[0]))[:3]
    ]
    avg_order_value = (total_value / Decimal(len(orders))).quantize(Decimal("0.01")) if orders else Decimal("0.00")

    preferred_hour = ""
    if hour_counts:
        best_hour = sorted(hour_counts.items(), key=lambda pair: (-pair[1], pair[0]))[0][0]
        preferred_hour = datetime(2000, 1, 1, best_hour, 0).strftime("%I %p").lstrip("0")

    return {
        "favorite_items": favorite_items,
        "avg_order_value": avg_order_value,
        "order_frequency": recent_order_count,
        "preferred_time": preferred_hour,
    }


def extract_preference_note(message: str) -> str:
    lowered = normalize_text(message).lower()
    known_preferences = [
        "no spicy",
        "less spicy",
        "extra spicy",
        "less oil",
        "no onion",
        "no garlic",
        "no sugar",
        "less salt",
    ]
    for preference in known_preferences:
        if preference in lowered:
            return preference
    return ""


def attach_preference_to_order(state: Dict[str, Any], note: str) -> bool:
    if not note or not state.get("order", {}).get("items"):
        return False
    updated = False
    for item in state["order"]["items"]:
        existing_note = str(item.get("notes", "")).strip()
        if note in existing_note:
            continue
        item["notes"] = f"{existing_note}; {note}".strip("; ").strip()
        updated = True
    return updated


def store_user_preference(state: Dict[str, Any], note: str) -> None:
    if not note:
        return
    preferences = state.setdefault("customer_profile", {}).setdefault("preferences", {})
    modifiers = preferences.setdefault("modifiers", [])
    if note not in modifiers:
        modifiers.append(note)


def apply_saved_preferences_to_order(state: Dict[str, Any]) -> None:
    modifiers = state.get("customer_profile", {}).get("preferences", {}).get("modifiers", [])
    for note in modifiers:
        attach_preference_to_order(state, note)


def format_order_items_with_notes(items: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for item in items:
        note = str(item.get("notes", "")).strip()
        line = f"* {item.get('name', '')} x{item.get('qty', 0)}"
        if note:
            line += f" ({note})"
        lines.append(line)
    return lines


def format_reorder_message(state: Dict[str, Any]) -> Optional[str]:
    last_order = get_last_completed_order(state)
    if not last_order or not last_order.get("items"):
        return None
    lines = ["Repeating your last order:", ""]
    lines.extend(format_order_items_with_notes(last_order["items"]))
    lines.extend(["", "Reply YES to confirm or MODIFY to change."])
    return "\n".join(lines)


def load_last_order_into_cart(state: Dict[str, Any], user_id: str) -> bool:
    last_order = get_last_completed_order(state)
    if not last_order or not last_order.get("items"):
        return False
    if not state.get("active_order_id") or state.get("payment_status") == "done" or state.get("order_stage") in {"preparing", "served"}:
        start_new_order(state, user_id)
    state["intent"] = "order"
    state["waiting_for_order"] = False
    state["checkout_mode"] = "append"
    state["order"] = {
        "order_id": state.get("active_order_id", ""),
        "name": state.get("customer_profile", {}).get("name", last_order.get("name", "")),
        "items": deepcopy(last_order.get("items", [])),
        "total": last_order.get("total", Decimal("0.00")),
        "currency": last_order.get("currency", DEFAULT_CURRENCY),
    }
    apply_saved_preferences_to_order(state)
    state["order_confirmed"] = False
    state["payment_status"] = "pending"
    state["payment_method"] = ""
    state["payment_link"] = ""
    state["payment_link_id"] = ""
    state["payment_verification_attempts"] = 0
    set_stage(state, STAGE_ORDER_ACTION)
    sync_active_order_record(state)
    return True


def suggest_items(context: Dict[str, Any], current_order: Dict[str, Any]) -> List[str]:
    return [suggestion["message"] for suggestion in get_structured_suggestions(context, current_order)]


def build_rule_based_suggestion_candidates(context: Dict[str, Any], current_order: Dict[str, Any]) -> List[Dict[str, str]]:
    items = current_order.get("items", [])
    if not items:
        return []
    item_names = {str(item.get("name", "")) for item in items}
    item_categories = {MENU_CATEGORY_MAP.get(name, "") for name in item_names}
    favorite_items = set(context.get("most_frequent_items", []))
    recent_suggestions = set(context.get("recent_suggestions", []))
    candidate_groups: List[List[Dict[str, str]]] = []

    bread_candidates = [
        {"item_name": "Garlic Naan", "message": "Garlic Naan would round this order out nicely. Want me to add it?"},
        {"item_name": "Butter Naan", "message": "Butter Naan would go really well with these curries. Want me to add it?"},
        {"item_name": "Cheese Naan", "message": "Cheese Naan could make this feel extra satisfying. Want me to add it?"},
    ]
    rice_candidates = [
        {"item_name": "Jeera Rice", "message": "Jeera Rice would pair nicely with this order. Want me to add it?"},
        {"item_name": "Steamed Basmati Rice", "message": "A side of Steamed Basmati Rice would complete this plate well. Want me to add it?"},
    ]
    drink_candidates = [
        {"item_name": "Mango Lassi", "message": "Mango Lassi would balance these flavors really well. Want me to add it?"},
        {"item_name": "Lemon Soda", "message": "Lemon Soda would keep this order feeling fresh and balanced. Want me to add it?"},
        {"item_name": "Masala Tea", "message": "Masala Tea could be a lovely finish with this meal. Want me to add it?"},
    ]
    dessert_candidates = [
        {"item_name": "Gulab Jamun Caldo", "message": "Gulab Jamun Caldo would make this order feel complete. Want me to add it?"},
        {"item_name": "Tiramisu Classico", "message": "Tiramisu Classico would be a nice sweet finish here. Want me to add it?"},
        {"item_name": "Jalebi", "message": "A Jalebi on the side would add a cheerful sweet finish. Want me to add it?"},
    ]
    starter_candidates = [
        {"item_name": "Punjabi Samosa Classic", "message": "Punjabi Samosa Classic would make a nice starter with this order. Want me to add it?"},
        {"item_name": "Mix Pakoda Croccante", "message": "Mix Pakoda Croccante would be a good crunchy starter here. Want me to add it?"},
    ]

    has_curry = any(category in {"Piatti Vegetariani", "Piatti con Pollo", "Piatti con Agnello", "Piatti con Pesce"} for category in item_categories)
    has_biryani = any("Biryani" in name for name in item_names)
    has_bread = any(name in {"Naan", "Butter Naan", "Garlic Naan", "Cheese Naan", "Aloo Kulcha", "Amritsari Kulcha"} for name in item_names)
    has_rice = any(name in {"Jeera Rice", "Steamed Basmati Rice", "Veg Biryani", "Chicken Biryani", "Lamb Biryani", "Prawn Biryani"} for name in item_names)
    has_drink = any(MENU_CATEGORY_MAP.get(name) == "Bevande" for name in item_names)
    has_dessert = any(MENU_CATEGORY_MAP.get(name) == "Dolci" for name in item_names)
    has_starter = any(category in {"Antipasti Non Vegetariani", "Antipasti Vegetariani", "Agni's Street Specials"} for category in item_categories)
    total_value = current_order.get("total", Decimal("0.00"))

    if has_curry and not has_bread:
        candidate_groups.append(bread_candidates)
    if has_curry and not has_rice:
        candidate_groups.append(rice_candidates)
    if (has_biryani or total_value >= Decimal("18.00")) and not has_drink:
        candidate_groups.append(drink_candidates)
    if total_value >= Decimal("15.00") and not has_dessert:
        candidate_groups.append(dessert_candidates)
    if not has_starter and len(item_names) <= 2:
        candidate_groups.append(starter_candidates)

    ranked_suggestions: List[Dict[str, str]] = []
    seen_items: set[str] = set()
    for group in candidate_groups:
        sorted_group = sorted(
            group,
            key=lambda candidate: (
                candidate["item_name"] in recent_suggestions,
                candidate["item_name"] not in favorite_items,
                candidate["item_name"],
            ),
        )
        for candidate in sorted_group:
            item_name = candidate["item_name"]
            if item_name in item_names or item_name in seen_items:
                continue
            ranked_suggestions.append(candidate)
            seen_items.add(item_name)
            break
    return ranked_suggestions[:3]


def ai_select_menu_suggestion(context: Dict[str, Any], current_order: Dict[str, Any], candidates: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    if not openai_client or not candidates:
        return None
    payload = {
        "current_items": [item.get("name", "") for item in current_order.get("items", [])],
        "favorite_items": context.get("most_frequent_items", []),
        "preferences": context.get("user_preferences", {}),
        "candidates": [{"item_name": candidate["item_name"]} for candidate in candidates],
    }
    system_prompt = (
        "Choose the single best menu suggestion for this restaurant order. "
        "Return only valid JSON with schema "
        '{"item_name":"string","message":"string"}. '
        "Use only one candidate item_name from the provided candidates. "
        "The message should feel warm, natural, and specifically matched to the order. "
        "Do not mention prices. Do not invent menu items."
    )
    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "system", "content": f"Context: {json.dumps(payload, default=str)}"},
                {"role": "user", "content": "Pick the best matching suggestion for this order."},
            ],
            max_output_tokens=120,
        )
        parsed = json_from_text(response.output_text) or {}
        item_name = str(parsed.get("item_name", "")).strip()
        if not item_name:
            return None
        for candidate in candidates:
            if candidate["item_name"] == item_name:
                message = str(parsed.get("message", "")).strip()
                if not message:
                    return candidate
                return {"item_name": candidate["item_name"], "message": message}
    except Exception as exc:
        logger.warning("AI menu suggestion selection failed: %s", exc)
    return None


def get_structured_suggestions(context: Dict[str, Any], current_order: Dict[str, Any]) -> List[Dict[str, str]]:
    candidates = build_rule_based_suggestion_candidates(context, current_order)
    if not candidates:
        return []
    ai_choice = ai_select_menu_suggestion(context, current_order, candidates)
    if not ai_choice:
        return candidates[:2]
    remaining = [candidate for candidate in candidates if candidate["item_name"] != ai_choice["item_name"]]
    return [ai_choice] + remaining[:1]


def add_menu_item_to_current_order(state: Dict[str, Any], item_name: str, qty: int = 1) -> bool:
    menu_item = find_menu_item(item_name)
    if not menu_item:
        return False
    if not state.get("order", {}).get("items"):
        return False
    incoming_order = {
        "items": [
            {
                "name": menu_item["name"],
                "qty": qty,
                "price": normalize_price(menu_item["price"]) * qty,
            }
        ],
        "currency": state.get("order", {}).get("currency", DEFAULT_CURRENCY),
    }
    state["order"] = merge_orders(state["order"], incoming_order)
    state["order"]["order_id"] = state.get("active_order_id", state["order"].get("order_id", ""))
    apply_saved_preferences_to_order(state)
    set_stage(state, STAGE_ORDER_ACTION)
    sync_active_order_record(state)
    state["pending_suggested_item"] = ""
    return True


def remember_suggested_item(state: Dict[str, Any], item_name: str) -> None:
    if not item_name:
        return
    history = [item for item in state.get("recent_suggestions", []) if item != item_name]
    history.append(item_name)
    state["recent_suggestions"] = history[-6:]
    state["pending_suggested_item"] = item_name


def ai_match_suggested_item_reply(message: str, suggested_item: str) -> bool:
    if not openai_client or not suggested_item:
        return False
    system_prompt = (
        "Decide whether the user's message means yes, add, or accept for the pending suggested menu item. "
        "Return only valid JSON with schema {\"accept\":true|false}. "
        "Be strict enough to avoid false positives, but accept natural variants and typos."
    )
    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "system", "content": f"Pending suggested item: {suggested_item}"},
                {"role": "user", "content": message},
            ],
            max_output_tokens=40,
        )
        parsed = json_from_text(response.output_text) or {}
        return bool(parsed.get("accept"))
    except Exception as exc:
        logger.warning("AI suggested-item reply matching failed: %s", exc)
        return False


def maybe_accept_suggested_item(state: Dict[str, Any], message: str) -> Optional[str]:
    suggested_item = state.get("pending_suggested_item", "").strip()
    if not suggested_item or not state.get("order", {}).get("items"):
        return None
    lowered = normalize_text(message).lower()
    explicit_add_item = extract_explicit_add_item(message)
    has_affirmation = bool(re.search(r"\b(yes+|ok|okay|sure|yeah|yep|please|go ahead)\b", lowered))
    has_add_signal = bool(re.search(r"\badd\b", lowered))
    has_reference = (
        bool(re.search(r"\b(it|that|this)\b", lowered))
        or suggested_item.lower() in lowered
        or explicit_add_item == suggested_item
    )
    ai_accepts = ai_match_suggested_item_reply(message, suggested_item)
    if not ((has_add_signal and has_reference) or (has_affirmation and has_reference) or suggested_item.lower() in lowered or ai_accepts):
        return None
    if add_menu_item_to_current_order(state, suggested_item):
        return build_contextual_update_message(state, suggested_item) + "\n\n" + generate_order_summary(state["order"], state)
    return None


def extract_explicit_add_item(message: str) -> Optional[str]:
    lowered = normalize_text(message).lower()
    match = re.match(r"^(?:(?:yes+|ok|okay|sure|yeah|yep|please)\s+)*(?:please\s+)?add\s+(.+)$", lowered)
    if not match:
        return None
    candidate = match.group(1).strip()
    if candidate in {"it", "that", "this"}:
        return None
    menu_item = find_menu_item(candidate)
    if menu_item:
        return menu_item["name"]
    for page in FIXED_MENU_PAGES.values():
        for category in page["categories"]:
            for item in category["items"]:
                normalized_name = normalize_menu_text(item["name"])
                if candidate and candidate in normalized_name:
                    return item["name"]
    return menu_item["name"] if menu_item else None


def build_contextual_update_message(state: Dict[str, Any], item_name: str) -> str:
    customer_name = state.get("customer_profile", {}).get("name", "").strip()
    favorite_items = get_user_context(state).get("most_frequent_items", [])
    if customer_name and item_name in favorite_items:
        return f"Nice choice again, {customer_name}. I've added {item_name} to your order."
    if customer_name:
        return f"Nice choice, {customer_name}. I've added {item_name} to your order."
    return f"I've added {item_name} to your order."


def detect_intent_with_context(message: str, state: Dict[str, Any]) -> Dict[str, Any]:
    lowered = normalize_text(message).lower()
    preference_note = extract_preference_note(message)
    if preference_note:
        return {"intent": "remove_preference", "preference_note": preference_note}
    if lowered in {"repeat", "repeat last order", "same as last time", "reorder", "reorder last"}:
        return {"intent": "reorder_last"}
    if any(phrase in lowered for phrase in {"suggest", "recommend", "what should i add", "any suggestion"}):
        return {"intent": "suggest_items"}
    if lowered in {"hello", "hi", "hey"}:
        return {"intent": "greeting"}
    if lowered.startswith(("remove ", "change ", "update ", "set ")):
        return {"intent": "modify_existing_order"}
    if detect_order_message(message) or detect_validatable_order_message(message):
        return {"intent": "general_order"}
    if not openai_client:
        return {"intent": "unknown"}

    context = get_user_context(state)
    current_cart = {
        "items": [
            {"name": item.get("name", ""), "qty": item.get("qty", 0), "notes": item.get("notes", "")}
            for item in state.get("order", {}).get("items", [])
        ]
    }
    system_prompt = (
        "Classify the user's message for a restaurant ordering assistant. "
        "Return only JSON with schema "
        '{"intent":"reorder_last|remove_preference|suggest_items|modify_existing_order|general_order|greeting|unknown","preference_note":"string"}'
        ". AI must not change prices, bypass validation, or invent menu items."
    )
    try:
        response = openai_client.responses.create(
            model=OPENAI_MODEL,
            input=[
                {"role": "system", "content": system_prompt},
                {"role": "system", "content": f"Last order: {json.dumps(context.get('last_order_items', []), default=str)}"},
                {"role": "system", "content": f"Current cart: {json.dumps(current_cart, default=str)}"},
                {"role": "system", "content": f"Menu summary:\n{summarize_menu_for_ai()}"},
                {"role": "user", "content": message},
            ],
            max_output_tokens=120,
        )
        parsed = json_from_text(response.output_text) or {"intent": "unknown"}
        if parsed.get("intent") not in {
            "reorder_last",
            "remove_preference",
            "suggest_items",
            "modify_existing_order",
            "general_order",
            "greeting",
            "unknown",
        }:
            parsed["intent"] = "unknown"
        if not parsed.get("preference_note") and preference_note:
            parsed["preference_note"] = preference_note
        return parsed
    except Exception as exc:
        logger.warning("Context-aware intent detection failed: %s", exc)
        return {"intent": "unknown"}


def handle_context_intent(user_id: str, state: Dict[str, Any], message: str, context_intent: Dict[str, Any]) -> Optional[str]:
    intent = context_intent.get("intent", "unknown")
    if state.get("stage") in {STAGE_PAYMENT_CHOICE, STAGE_PAYMENT_CONFIRMATION, STAGE_RESERVATION_DETAILS, STAGE_RESERVATION_CHECK}:
        return None
    explicit_add_item = extract_explicit_add_item(message)
    if explicit_add_item and state.get("order", {}).get("items"):
        if add_menu_item_to_current_order(state, explicit_add_item):
            return build_contextual_update_message(state, explicit_add_item) + "\n\n" + generate_order_summary(state["order"], state)
    if intent == "greeting" and not state.get("order", {}).get("items"):
        return greeting_message_for_state(state)
    if intent == "reorder_last":
        return format_reorder_message(state) or "I don't have a completed order to repeat yet."
    if intent == "remove_preference":
        note = context_intent.get("preference_note") or extract_preference_note(message)
        if not note:
            return None
        store_user_preference(state, note)
        if attach_preference_to_order(state, note):
            sync_active_order_record(state)
            return f"Noted. I'll keep this order as '{note}'."
        return f"Noted. I'll remember '{note}' for your next order too."
    if intent == "suggest_items":
        suggestions = get_structured_suggestions(get_user_context(state), state.get("order", {}))
        if suggestions:
            remember_suggested_item(state, suggestions[0]["item_name"])
            return suggestions[0]["message"]
    if intent == "modify_existing_order" and state.get("order", {}).get("items"):
        updated, changed = modify_order_from_text(state["order"], message)
        if changed:
            state["order"] = updated
            set_stage(state, STAGE_ORDER_ACTION)
            sync_active_order_record(state)
            return generate_order_summary(state["order"], state)
    return None


def learn_from_confirmed_order(state: Dict[str, Any]) -> None:
    preferences = state.setdefault("customer_profile", {}).setdefault("preferences", {})
    item_counts = preferences.setdefault("item_counts", {})
    category_counts = preferences.setdefault("category_counts", {})
    time_slots = preferences.setdefault("time_slots", {})
    modifiers = preferences.setdefault("modifiers", [])

    for item in state.get("order", {}).get("items", []):
        item_name = item.get("name", "")
        if item_name:
            item_counts[item_name] = item_counts.get(item_name, 0) + int(item.get("qty", 0) or 0)
        category_name = MENU_CATEGORY_MAP.get(item_name)
        if category_name:
            category_counts[category_name] = category_counts.get(category_name, 0) + 1
        note = str(item.get("notes", "")).strip()
        if note and note not in modifiers:
            modifiers.append(note)

    preferred_time = state.get("customer_profile", {}).get("preferred_time", "").strip()
    if preferred_time:
        time_slots[preferred_time] = time_slots.get(preferred_time, 0) + 1
    state.setdefault("customer_profile", {})["insights"] = analyze_customer_behavior(state)


def generate_order_summary(order: Dict[str, Any], state: Optional[Dict[str, Any]] = None) -> str:
    name = order.get("name") or "there"
    currency = order.get("currency", DEFAULT_CURRENCY)
    if state is None:
        intro = f"Here\u2019s your updated order, {name} \U0001f60f"
    else:
        favorite_items = get_user_context(state).get("most_frequent_items", [])
        has_favorite = any(item.get("name") in favorite_items for item in order.get("items", []))
        intro = choose_variant(
            state,
            "order_summary_intro",
            [
                f"Nice choice, {name}. Here\u2019s your updated order \U0001f60f" if has_favorite else f"Here\u2019s your updated order, {name} \U0001f60f",
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
        note_suffix = f" ({item['notes']})" if item.get("notes") else ""
        if detail_style == 0:
            line = f"{item['name']}{note_suffix} x{item['qty']} \u2014 {money_to_text(item['price'], currency)}"
        elif detail_style == 1:
            line = f"\u2022 {item['name']}{note_suffix} | Qty {item['qty']} | {money_to_text(item['price'], currency)}"
        else:
            line = f"- {item['qty']} x {item['name']}{note_suffix} for {money_to_text(item['price'], currency)}"
        lines.append(line)
    lines.extend(
        [
            "",
            f"Total \u2014 {money_to_text(order.get('total', Decimal('0.00')), currency)}",
        ]
    )
    if state is not None:
        modifiers = state.get("customer_profile", {}).get("preferences", {}).get("modifiers", [])
        if modifiers:
            lines.extend(["", f"Saved preferences: {', '.join(modifiers)}"])
        structured_suggestions = get_structured_suggestions(get_user_context(state), order)
        if structured_suggestions:
            remember_suggested_item(state, structured_suggestions[0]["item_name"])
            lines.extend(["", structured_suggestions[0]["message"]])
    lines.extend(
        [
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
        note_suffix = f" ({item['notes']})" if item.get("notes") else ""
        if detail_style == 0:
            lines.append(f"\u2022 {item['name']}{note_suffix} x{item['qty']} \u2014 {money_to_text(item['price'], currency)}")
        elif detail_style == 1:
            lines.append(f"- {item['qty']} x {item['name']}{note_suffix} | {money_to_text(item['price'], currency)}")
        else:
            lines.append(f"{item['name']}{note_suffix} | Qty {item['qty']} | {money_to_text(item['price'], currency)}")

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
        note_suffix = f" ({item['notes']})" if item.get("notes") else ""
        lines.append(f"- {item['name']}{note_suffix} x{item['qty']} | {money_to_text(item['price'], currency)}")
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
        "\u23f3 Estimated time: 15-20 minutes\n\n"
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
                    error_message = confirm_order_and_store(user_id, state)
                    if error_message:
                        save_state(user_id, state)
                        send_whatsapp_message(user_id, error_message)
                        return
                    save_state(user_id, state)
                    send_whatsapp_message(user_id, payment_success_message(state), max_words=220)
                    return
            except requests.RequestException as exc:
                logger.warning("Scheduled payment verification failed: %s", exc)
                return

            state["payment_method"] = "counter"
            state["payment_status"] = "done"
            error_message = confirm_order_and_store(user_id, state)
            if error_message:
                save_state(user_id, state)
                send_whatsapp_message(user_id, error_message)
                return
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
    upsert_order_sheet_row(user_id, state, event_type)
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


def upsert_order_sheet_row(user_id: str, state: Dict[str, Any], event_type: str) -> None:
    order = state.get("order", {})
    order_id = order.get("order_id", "")
    if not order_id:
        return

    worksheet = get_or_create_google_worksheet(GOOGLE_SHEETS_ORDER_SHEET)
    if not worksheet:
        return

    row = build_order_sheet_row(user_id, state, event_type)

    with google_sheets_lock:
        order_ids = worksheet.col_values(2)
        target_row_index = None
        for index, existing_order_id in enumerate(order_ids[1:], start=2):
            if existing_order_id == order_id:
                target_row_index = index
                break
        if target_row_index is None:
            worksheet.append_row(row, value_input_option="USER_ENTERED", table_range="A1")
        else:
            worksheet.update(f"A{target_row_index}:Q{target_row_index}", [row], value_input_option="USER_ENTERED")


def get_or_create_google_worksheet(sheet_name: str) -> Optional[gspread.Worksheet]:
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        logger.warning("Google Sheets append skipped: GOOGLE_SHEETS_SPREADSHEET_ID is not configured.")
        return None
    client = get_google_sheets_client()
    if not client:
        logger.warning("Google Sheets append skipped: client is unavailable. Check service-account credentials.")
        return None
    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=30)
        ensure_google_sheet_structure(sheet_name, worksheet)
        return worksheet
    except Exception as exc:
        logger.warning("Google Sheets worksheet access failed: %s", exc)
        return None


def append_google_sheet_row(sheet_name: str, row: List[str]) -> None:
    worksheet = get_or_create_google_worksheet(sheet_name)
    if not worksheet:
        return
    try:
        with google_sheets_lock:
            worksheet.append_row(row, value_input_option="USER_ENTERED", table_range="A1")
        logger.info("Google Sheets append succeeded: sheet=%s, first_cell=%s", sheet_name, row[0] if row else "")
    except Exception as exc:
        logger.warning("Google Sheets append failed: %s", exc)


log_google_sheets_status()


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
    insights = state.get("customer_profile", {}).get("insights", {})
    favorite_items = insights.get("favorite_items", [])
    preferred_time = str(insights.get("preferred_time", "")).strip()
    order_frequency = int(insights.get("order_frequency", 0) or 0)
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
    lines = [opener]
    if favorite_items:
        usual_line = ", ".join(favorite_items[:2])
        lines.extend(["", f"Welcome back! Want your usual {usual_line} again?"])
    elif order_frequency > 0:
        lines.extend(["", "Welcome back. Ready for another order?"])
    if preferred_time:
        lines.extend(["", f"You usually order around {preferred_time} 😏"])
    if favorite_items:
        lines.extend(["", "Your favorites:", *[f"* {item}" for item in favorite_items[:3]], "Reply YES to reorder instantly."])
    else:
        lines.extend(["", assist])
    lines.extend(["", "1. Order Food", "2. Book a Table", "3. Check Reservation"])
    return "\n".join(lines)


def order_instruction_message(state: Dict[str, Any]) -> str:
    customer_name = state.get("customer_profile", {}).get("name", "").strip()
    favorite_items = get_user_context(state).get("most_frequent_items", [])
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
    lines = [intro]
    if customer_name:
        lines.extend(["", f"Welcome back, {customer_name}."])
    if favorite_items:
        lines.extend(["", f"Your usual picks include {', '.join(favorite_items[:2])}."])
    lines.extend(["", f"Explore our menu:\n{MENU_URL}", "", closing])
    return "\n".join(lines)


def payment_prompt_message(state: Dict[str, Any]) -> str:
    customer_name = state.get("customer_profile", {}).get("name", "").strip()
    total = money_to_text(state.get("order", {}).get("total", Decimal("0.00")), state.get("order", {}).get("currency", DEFAULT_CURRENCY))
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
    if customer_name:
        intro = f"{customer_name}, {intro.lower()}"
    return f"{intro}\n\nCurrent total: {total}\n\n1. Pay Online \U0001f4b3\n2. Pay at Counter"


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
    if detect_validatable_order_message(text):
        return "order_validation"
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
    learn_from_confirmed_order(state)
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
        state["pending_suggested_item"] = ""
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
        state["pending_suggested_item"] = ""
        set_stage(state, STAGE_PAYMENT_CHOICE)
        state["failure_count"] = 0
        return payment_prompt_message(state)
    if action == "add_more_items":
        state["pending_suggested_item"] = ""
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
        state["pending_suggested_item"] = ""
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
                error_message = confirm_order_and_store(user_id, state)
                if error_message:
                    return error_message
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
        error_message = confirm_order_and_store(user_id, state)
        if error_message:
            return error_message
        cancel_payment_followup(user_id, state.get("active_order_id", ""))
        return payment_counter_fallback_message(state)
    if action == "pay_at_counter":
        state["payment_method"] = "counter"
        state["failure_count"] = 0
        cancel_payment_followup(user_id, state.get("active_order_id", ""))
        error_message = confirm_order_and_store(user_id, state)
        if error_message:
            return error_message
        return (
            "Perfect. Payment is marked for counter. Your order is now being prepared \U0001f37d\ufe0f\n\n"
            "\u23f3 Estimated time: 15-20 minutes\n\n"
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
    if intent == "order_validation":
        parsed_items = parse_order(text)
        validated = validate_order(parsed_items)
        corrected_items = validated["corrected_items"]
        if not corrected_items:
            state["failure_count"] += 1
            return build_contextual_validated_order_message(validated, Decimal("0.00"), state)

        if not state.get("active_order_id") or state.get("payment_status") == "done" or state.get("order_stage") in {"preparing", "served"}:
            start_new_order(state, user_id)

        state["intent"] = "order"
        state["waiting_for_order"] = False
        state["checkout_mode"] = "append"
        state["order"] = {
            "order_id": state.get("active_order_id", state.get("order", {}).get("order_id", "")),
            "name": state.get("customer_profile", {}).get("name", state.get("order", {}).get("name", "")),
            "items": [
                {"name": item["item_name"], "qty": item["quantity"], "price": item["line_total"]}
                for item in corrected_items
            ],
            "total": calculate_total(corrected_items),
            "currency": DEFAULT_CURRENCY,
        }
        apply_saved_preferences_to_order(state)
        state["order_confirmed"] = False
        state["payment_status"] = "pending"
        state["payment_method"] = ""
        state["payment_link"] = ""
        state["payment_link_id"] = ""
        state["payment_verification_attempts"] = 0
        set_stage(state, STAGE_ORDER_ACTION)
        state["failure_count"] = 0
        sync_active_order_record(state)
        append_sheet_log(user_id, state, "order_validated")
        return build_contextual_validated_order_message(validated, state["order"]["total"], state)
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
                error_message = confirm_order_and_store(user_id, state)
                if error_message:
                    return error_message
                return (
                    "Thank you for waiting. I’ve confirmed the payment and your order is now being prepared \U0001f37d\ufe0f\n\n"
                    "\u23f3 Estimated time: 15-20 minutes\n\n"
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
        error_message = confirm_order_and_store(user_id, state)
        if error_message:
            return error_message
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
        apply_saved_preferences_to_order(state)
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
        event_type = "order_corrected" if parsed.get("fraud_detected") else "order_updated"
        append_sheet_log(user_id, state, event_type)
        if parsed.get("fraud_detected") or parsed.get("invalid_items"):
            validated_payload = {
                "corrected_items": [
                    {
                        "item_name": item["name"],
                        "quantity": item["qty"],
                        "user_price": Decimal("0.00"),
                        "real_price": (item["price"] / item["qty"]) if item["qty"] else item["price"],
                        "line_total": item["price"],
                    }
                    for item in state["order"]["items"]
                ],
                "invalid_items": parsed.get("invalid_items", []),
                "fraud_detected": parsed.get("fraud_detected", False),
                "submitted_total": parsed.get("submitted_total", Decimal("0.00")),
            }
            return build_contextual_validated_order_message(validated_payload, state["order"]["total"], state)
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

    suggestion_reply = maybe_accept_suggested_item(state, text)
    if suggestion_reply:
        state["last_ai_action"] = "accept_suggested_item"
        save_state(user_id, state)
        return suggestion_reply

    lowered = text.strip().lower()
    if lowered == "yes" and state.get("last_ai_action") == "reorder_last":
        if load_last_order_into_cart(state, user_id):
            save_state(user_id, state)
            return generate_order_summary(state["order"], state)
    if lowered == "modify" and state.get("last_ai_action") == "reorder_last":
        if load_last_order_into_cart(state, user_id):
            set_stage(state, STAGE_ORDER_ACTION)
            save_state(user_id, state)
            return "I’ve loaded your last order. Tell me the change in one line."

    context_intent = detect_intent_with_context(text, state)
    context_reply = handle_context_intent(user_id, state, text, context_intent)
    if context_reply:
        state["last_ai_action"] = context_intent.get("intent", "")
        save_state(user_id, state)
        return context_reply

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


@app.get("/debug/sheets")
def debug_sheets() -> Tuple[Any, int]:
    token = request.args.get("token", "")
    if not VERIFY_TOKEN or token != VERIFY_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    client = get_google_sheets_client()
    result: Dict[str, Any] = {
        "spreadsheet_id_configured": bool(GOOGLE_SHEETS_SPREADSHEET_ID),
        "service_account_json_configured": bool(GOOGLE_SERVICE_ACCOUNT_JSON.strip()),
        "service_account_file": str(GOOGLE_SERVICE_ACCOUNT_FILE),
        "service_account_file_exists": GOOGLE_SERVICE_ACCOUNT_FILE.exists(),
        "client_available": client is not None,
    }

    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        return jsonify(result), 200

    if not client:
        result["status"] = "client_unavailable"
        return jsonify(result), 200

    try:
        spreadsheet = client.open_by_key(GOOGLE_SHEETS_SPREADSHEET_ID)
        result["status"] = "ok"
        result["spreadsheet_title"] = spreadsheet.title
        result["worksheets"] = [worksheet.title for worksheet in spreadsheet.worksheets()]
        return jsonify(result), 200
    except Exception as exc:
        result["status"] = "access_failed"
        result["error"] = str(exc)
        return jsonify(result), 200


@app.post("/debug/sheets/append")
def debug_sheets_append() -> Tuple[Any, int]:
    token = request.args.get("token", "")
    if not VERIFY_TOKEN or token != VERIFY_TOKEN:
        return jsonify({"status": "forbidden"}), 403

    client = get_google_sheets_client()
    if not GOOGLE_SHEETS_SPREADSHEET_ID:
        return jsonify({"status": "spreadsheet_id_missing"}), 200
    if not client:
        return jsonify({"status": "client_unavailable"}), 200

    test_row = [
        utc_now().isoformat(),
        "render-debug",
        "debug_append_test",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "0.00",
        DEFAULT_CURRENCY,
        "",
        "",
        "",
        "",
        json.dumps({"source": "debug_sheets_append"}),
    ]

    try:
        append_google_sheet_row(GOOGLE_SHEETS_EVENT_SHEET, test_row)
        return jsonify({"status": "ok", "sheet": GOOGLE_SHEETS_EVENT_SHEET, "first_cell": test_row[0]}), 200
    except Exception as exc:
        return jsonify({"status": "append_failed", "error": str(exc)}), 200


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
