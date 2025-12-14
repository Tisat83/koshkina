"""
notifications.py — утилиты для отправки уведомлений (Telegram и т.п.)

Пока здесь:
- низкоуровневая отправка сообщений в Telegram;
- заготовки функций под разные события парковки.
"""

from __future__ import annotations

import os
from urllib.parse import urlencode
from urllib.request import urlopen


# --- Конфигурация Telegram-бота ---

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN)


def send_telegram_message(chat_id: str, text: str) -> bool:
    """
    Отправка простого текстового сообщения в Telegram-бота.

    Используем для:
    - уведомлений о парковке (перекрыли, освободили, истёк срок);
    - общих рассылок от админа;
    - уведомлений гостям.
    """
    if not TELEGRAM_ENABLED:
        return False

    chat_id = str(chat_id or "").strip()
    text = (text or "").strip()
    if not chat_id or not text:
        return False

    try:
        data = urlencode(
            {
                "chat_id": chat_id,
                "text": text,
            }
        ).encode("utf-8")
        url = f"{TELEGRAM_API_BASE}/sendMessage"
        with urlopen(url, data=data, timeout=5) as resp:
            resp.read()
        return True
    except Exception:
        # TODO: при необходимости добавить логирование в файл / sentry
        return False


# --- Заготовки под «семантические» уведомления парковки ---


def notify_parking_blocked(blocked_chat_id: str, spot_label: str, by_apartment: str | None = None) -> bool:
    """
    Уведомление: вашу машину перекрыли.
    spot_label — человекочитаемый номер / название места (например, "Место 5").
    by_apartment — квартира, которая перекрыла (опционально).
    """
    if not blocked_chat_id:
        return False

    if by_apartment:
        text = f"Ваше парковочное место {spot_label} перекрыто машиной от квартиры {by_apartment}."
    else:
        text = f"Ваше парковочное место {spot_label} сейчас перекрыто другой машиной."
    return send_telegram_message(blocked_chat_id, text)


def notify_parking_expired(chat_id: str, spot_label: str) -> bool:
    """
    Уведомление: истекло время вашей брони.
    """
    if not chat_id:
        return False
    text = f"Ваша бронь парковочного места {spot_label} завершилась. Вы уже уехали?"
    return send_telegram_message(chat_id, text)


def notify_parking_call_owner(chat_id: str, spot_label: str) -> bool:
    """
    Уведомление: кто-то запросил «подвинуть машину» на вашем месте.
    """
    if not chat_id:
        return False
    text = f"По вашему парковочному месту {spot_label} поступил запрос «подвинуть машину»."
    return send_telegram_message(chat_id, text)


def notify_parking_freed_subscribers(subscriber_chat_ids: list[str], spot_label: str) -> int:
    """
    Уведомление всем подписчикам: место освободилось.
    Возвращает количество успешно отправленных сообщений.
    """
    if not subscriber_chat_ids:
        return 0

    text = f"Парковочное место {spot_label} освобождено."
    sent = 0
    for cid in subscriber_chat_ids:
        if send_telegram_message(cid, text):
            sent += 1
    return sent


def notify_admin_broadcast(chat_ids: list[str], text: str) -> int:
    """
    Массовая рассылка от админа всем подписчикам бот-уведомлений.
    Возвращает количество успешно отправленных сообщений.
    """
    if not chat_ids or not text:
        return 0

    sent = 0
    for cid in chat_ids:
        if send_telegram_message(cid, text):
            sent += 1
    return sent
