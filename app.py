from __future__ import annotations

from pathlib import Path
import os
import json
import shutil
from contextlib import contextmanager

try:
    import fcntl  # только Linux/macOS
except Exception:
    fcntl = None

import secrets
from functools import wraps
from datetime import date, datetime
from urllib.parse import urlparse, urlencode
import time
from urllib.request import urlopen
from werkzeug.utils import secure_filename

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    flash,
    jsonify,
)

from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from notifications import notify_parking_expired, notify_parking_freed_subscribers


# ---------------- Paths / config ----------------

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
UPLOAD_DIR = BASE_DIR / "static" / "img" / "news"

ALLOWED_EXTENSIONS = {"jpg", "jpeg", "png", "gif", "webp"}

INVITES_FILE = DATA_DIR / "invites.json"
REACTIONS_FILE = DATA_DIR / "reactions.json"
PARKING_STATE_FILE = DATA_DIR / "parking_state.json"
GUESTS_FILE = DATA_DIR / "guests.json"  # заявки гостей на парковку
GUEST_PHOTOS_DIR = BASE_DIR / "static" / "img" / "guest_photos"

# Сколько новостей выводить на странице /news
POSTS_PER_PAGE = 5

# Реакции как в Telegram (можно менять набор)
REACTION_EMOJIS = ["👍", "❤️", "🔥", "🎉", "👏", "😁", "😢", "🤔"]

# Квартиры администраторов (можно переопределить через переменную окружения)
# Пример: set ADMIN_APARTMENTS=501,12
ADMIN_APARTMENTS = os.getenv("ADMIN_APARTMENTS", "501")
ADMINS = {a.strip() for a in ADMIN_APARTMENTS.split(",") if a.strip()}

# Аварийный вход по телефону (только для восстановления админа).
# Включить: set ALLOW_PHONE_FALLBACK=1
ALLOW_PHONE_FALLBACK = os.getenv("ALLOW_PHONE_FALLBACK", "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}

# Telegram-бот для уведомлений
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_API_BASE = (
    f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""
)
TELEGRAM_ENABLED = bool(TELEGRAM_BOT_TOKEN)

app = Flask(__name__)
# В проде вынести в .env
app.secret_key = "change_this_secret_key_for_production"


# ---------------- Jinja filters ----------------


@app.template_filter("ru_date")
def ru_date(value: str) -> str:
    """'YYYY-MM-DD' -> 'DD-MM-YYYY'."""
    try:
        if not value:
            return ""
        y, m, d = str(value).split("-")
        return f"{d.zfill(2)}-{m.zfill(2)}-{y}"
    except Exception:
        return value


# алиас для старых шаблонов: |date_ru
app.jinja_env.filters["date_ru"] = ru_date


# ---------------- JSON helpers ----------------


@contextmanager
def _json_lock(path: Path):
    """
    Блокировка на время чтения/записи JSON.
    На Windows fcntl нет — там просто работаем без flock (локальная разработка).
    """
    lock_path = path.with_suffix(path.suffix + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_f = lock_path.open("a+", encoding="utf-8")
    try:
        if fcntl:
            fcntl.flock(lock_f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            if fcntl:
                fcntl.flock(lock_f.fileno(), fcntl.LOCK_UN)
        finally:
            lock_f.close()


def load_json(path: Path, default):
    if not path.exists():
        return default

    def _read(p: Path):
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)

    with _json_lock(path):
        try:
            # пустой файл тоже считаем битым
            if path.stat().st_size == 0:
                raise json.JSONDecodeError("empty file", "", 0)
            return _read(path)
        except (json.JSONDecodeError, OSError):
            # пробуем восстановиться из .bak
            bak = path.with_suffix(path.suffix + ".bak")
            if bak.exists():
                try:
                    if bak.stat().st_size == 0:
                        raise json.JSONDecodeError("empty bak", "", 0)
                    return _read(bak)
                except Exception:
                    pass
            return default


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)

    with _json_lock(path):
        bak = path.with_suffix(path.suffix + ".bak")
        tmp = path.with_suffix(path.suffix + f".tmp.{secrets.token_hex(6)}")

        try:
            # 1) сначала делаем бэкап текущего файла
            if path.exists():
                try:
                    shutil.copy2(path, bak)
                except Exception:
                    pass

            # 2) пишем во временный файл + fsync
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
                f.flush()
                os.fsync(f.fileno())

            # 3) атомарно подменяем
            os.replace(tmp, path)

        finally:
            # на всякий случай убираем tmp, если что-то пошло не так
            try:
                if tmp.exists():
                    tmp.unlink()
            except Exception:
                pass



def load_users() -> dict:
    return load_json(DATA_DIR / "users.json", {})


def save_users(users: dict):
    save_json(DATA_DIR / "users.json", users)


def load_posts() -> list:
    return load_json(DATA_DIR / "posts.json", [])


def save_posts(posts: list):
    save_json(DATA_DIR / "posts.json", posts)


def load_subscriptions() -> dict:
    return load_json(DATA_DIR / "subscriptions.json", {})


def save_subscriptions(subs: dict):
    save_json(DATA_DIR / "subscriptions.json", subs)


def load_info_items() -> list:
    return load_json(DATA_DIR / "info.json", [])


def load_parking() -> dict:
    """Конфигурация парковки (список мест, базовые типы)."""
    return load_json(DATA_DIR / "parking.json", {"spots": []})


def save_parking(parking: dict):
    """Позже пригодится для сохранения занятости мест."""
    save_json(DATA_DIR / "parking.json", parking)


def load_parking_state() -> dict:
    """
    Текущее состояние занятости мест (по умолчанию пусто),
    с авто-очисткой просроченных мест и уведомлениями.
    """
    state = load_json(PARKING_STATE_FILE, {"spots": {}, "subscriptions": {}})
    spots = state.get("spots", {})
    subscriptions = state.get("subscriptions", {})
    # берём локальное время сервера, а не UTC — иначе при datetime-local из браузера
    # время может "не наступить" из-за часового пояса
    now = datetime.now()
    changed = False

    # подгружаем конфиг для человекочитаемых названий мест
    parking_cfg = load_parking()
    id_to_label = {
        str(s.get("id")): (s.get("label") or f"место {s.get('id')}")
        for s in parking_cfg.get("spots", [])
    }

    for sid, info in list(spots.items()):
        until = (info.get("until") or "").strip()
        if not until:
            continue
        try:
            # ожидаем формат вида "YYYY-MM-DDTHH:MM"
            dt = datetime.fromisoformat(until)
        except Exception:
            # если вдруг формат нестандартный — просто пропускаем, ничего не ломаем
            continue
        if dt < now:
            label = id_to_label.get(sid, f"место {sid}")

            # перед тем как освободить, пытаемся отправить уведомление владельцу
            chat_id = (info.get("telegram_chat_id") or "").strip()
            if chat_id:
                try:
                    notify_parking_expired(chat_id, label)
                except Exception:
                    # уведомление не должно ломать очистку
                    pass

            # и уведомления всем подписчикам на это место
            subs_for_spot = subscriptions.get(sid) or []
            if subs_for_spot:
                try:
                    notify_parking_freed_subscribers(subs_for_spot, label)
                except Exception:
                    pass
                # подписки для этого места больше не актуальны
                subscriptions.pop(sid, None)
                changed = True

            # время вышло — считаем место свободным
            spots.pop(sid, None)
            changed = True

    if changed:
        state["spots"] = spots
        state["subscriptions"] = subscriptions
        save_json(PARKING_STATE_FILE, state)

    return state


def save_parking_state(state: dict):
    save_json(PARKING_STATE_FILE, state)


def load_invites() -> dict:
    return load_json(INVITES_FILE, {})


def save_invites(invites: dict):
    save_json(INVITES_FILE, invites)


def load_reactions() -> dict:
    return load_json(REACTIONS_FILE, {})


def save_reactions(reactions: dict):
    save_json(REACTIONS_FILE, reactions)


def load_guests() -> dict:
    """
    Заявки гостей на парковку.
    Формат:
    {
      "guests": [
        {
          "id": 1,
          "created_at": "...",
          "name": "...",
          "phone": "...",
          "car_number": "...",
          "comment": "...",
          "status": "pending/approved/rejected",
          "source": "site/telegram"
        },
        ...
      ]
    }
    """
    return load_json(GUESTS_FILE, {"guests": []})


def save_guests(data: dict):
    if "guests" not in data or not isinstance(data["guests"], list):
        data["guests"] = []
    save_json(GUESTS_FILE, data)

def normalize_phone(raw: str) -> str:
    """
    Нормализуем телефон к виду 7XXXXXXXXXX (только цифры).
    Принимаем +7, 8, 7, 10-значные.
    """
    s = "".join(ch for ch in str(raw or "") if ch.isdigit())
    if len(s) == 11 and s.startswith("8"):
        s = "7" + s[1:]
    if len(s) == 10:
        s = "7" + s
    return s

def find_guest_by_phone(phone: str):
    """
    Поиск гостя по телефону.
    Важно: если по этому телефону есть хотя бы один approved-гость — возвращаем его (самый свежий).
    Иначе возвращаем самый свежий любой статус (pending/rejected), чтобы логин корректно сказал "не одобрено".
    """
    phone_norm = normalize_phone(phone)
    guests_data = load_guests()
    guests = guests_data.get("guests") or []

    matches = []
    for g in guests:
        if normalize_phone(g.get("phone")) == phone_norm:
            matches.append(g)

    if not matches:
        return None

    def _sort_key(g):
        created_at = g.get("created_at") or ""
        try:
            gid = int(g.get("id") or 0)
        except Exception:
            gid = 0
        return (created_at, gid)

    approved = []
    for g in matches:
        status = (g.get("status") or "").strip().lower()
        if status == "approved":
            approved.append(g)

    pool = approved or matches
    pool.sort(key=_sort_key, reverse=True)
    return pool[0]


# ---------------- Sidebar visibility ----------------


def _normalize_show_on(v):
    if not v:
        return None
    if isinstance(v, str):
        return [v.strip()]
    if isinstance(v, list):
        out = []
        for x in v:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out or None
    return None


def info_item_visible(item: dict, place: str) -> bool:
    """
    Поддерживаем ключи:
      show_on: ["index", "news"] / ["all"]
      placement/placements/pages/where (на всякий случай)
    Если ключа нет -> показываем везде.
    """
    v = (
        item.get("show_on")
        or item.get("placement")
        or item.get("placements")
        or item.get("pages")
        or item.get("where")
    )
    show_on = _normalize_show_on(v)
    if not show_on:
        return True
    s = set(show_on)
    return ("all" in s) or (place in s)


def get_sidebar_items(place: str, limit: int = 3) -> list:
    items = load_info_items()
    visible = [it for it in items if info_item_visible(it, place)]
    visible.sort(key=lambda x: x.get("order", 10_000))
    return visible[:limit] if limit else visible


# ---------------- Upload helpers ----------------


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def ensure_upload_dir():
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


def save_uploaded_file(file_storage):
    if not file_storage or not file_storage.filename:
        return None

    filename = secure_filename(file_storage.filename)
    if not allowed_file(filename):
        return None

    ensure_upload_dir()
    dest = UPLOAD_DIR / filename
    base, ext = os.path.splitext(filename)
    counter = 1
    while dest.exists():
        filename = f"{base}_{counter}{ext}"
        dest = UPLOAD_DIR / filename
        counter += 1

    file_storage.save(dest)
    return f"news/{filename}".replace("\\", "/")


def download_image_from_url(url: str):
    try:
        parsed = urlparse(url)
        filename = os.path.basename(parsed.path)
        if not filename:
            return None

        filename = secure_filename(filename)
        if not allowed_file(filename):
            return None

        ensure_upload_dir()
        dest = UPLOAD_DIR / filename
        base, ext = os.path.splitext(filename)
        counter = 1
        while dest.exists():
            filename = f"{base}_{counter}{ext}"
            dest = UPLOAD_DIR / filename
            counter += 1

        with urlopen(url) as resp, dest.open("wb") as f:
            f.write(resp.read())

        return f"news/{filename}".replace("\\", "/")
    except Exception:
        return None


# ---------------- Auth / PIN helpers ----------------


def _is_legacy_sha256_hash(s: str) -> bool:
    if not isinstance(s, str) or len(s) != 64:
        return False
    return all(c in "0123456789abcdef" for c in s.lower())


def hash_pin(pin: str) -> str:
    """Надёжный хеш с солью (werkzeug)."""
    return generate_password_hash(pin)


def check_pin(pin: str, stored_hash: str) -> bool:
    """Поддерживаем и новый хеш werkzeug, и старый sha256 (на случай старых записей)."""
    try:
        if _is_legacy_sha256_hash(stored_hash):
            import hashlib, hmac

            candidate = hashlib.sha256(pin.encode("utf-8")).hexdigest()
            return hmac.compare_digest(candidate, stored_hash)
        return check_password_hash(stored_hash, pin)
    except Exception:
        return False


def login_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "user" not in session:
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapper


def is_admin_for(apartment: str, user_record: dict | None) -> bool:
    if str(apartment) in ADMINS:
        return True
    if isinstance(user_record, dict) and bool(user_record.get("is_admin")):
        return True
    return False


def admin_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        user = session.get("user")
        if not user or not user.get("is_admin"):
            flash("У вас нет доступа к этому разделу.", "error")
            return redirect(url_for("news"))
        return view_func(*args, **kwargs)

    return wrapper


def get_user_key() -> str:
    user = session.get("user") or {}
    apt = (user.get("apartment") or "").strip()
    name = (user.get("name") or "").strip()
    return f"{apt}:{name}" if name else apt


def user_has_any_pin(user_record: dict | None) -> bool:
    if not isinstance(user_record, dict):
        return False
    if user_record.get("pin_hash"):
        return True
    residents = user_record.get("residents")
    if isinstance(residents, list):
        return any(isinstance(r, dict) and bool(r.get("pin_hash")) for r in residents)
    return False


def current_user_parking_flags():
    """
    Общий хелпер: текущий пользователь + флаги доступа к парковке.
    can_use_parking: можно ли пользоваться парковкой
    can_subscribe_parking: можно ли подписываться на уведомления
    """
    sess_user = session.get("user") or {}
    apartment = str(sess_user.get("apartment") or "").strip()
    users = load_users()
    record = users.get(apartment) if apartment and isinstance(users, dict) else {}
    if not isinstance(record, dict):
        record = {}
    is_admin = bool(sess_user.get("is_admin"))
    is_guest = bool(sess_user.get("is_guest"))

    # Гостю (пока) даём доступ к парковке через сессию, без users.json
    if is_guest:
        can_use_parking = True
        can_subscribe_parking = False
        return sess_user, apartment, record, can_use_parking, can_subscribe_parking

    # Жильцы: доступ только если can_use_parking=true (или админ)
    can_use_parking = bool(record.get("can_use_parking", False) or is_admin)
    can_subscribe_parking = bool(record.get("can_subscribe_parking", False) or is_admin)
    return sess_user, apartment, record, can_use_parking, can_subscribe_parking

@app.context_processor
def inject_nav_flags():
    """
    Флаги для навигации в base.html.
    """
    try:
        _, _, _, can_use_parking, _ = current_user_parking_flags()
    except Exception:
        can_use_parking = False

    return {
        "nav_can_use_parking": bool(can_use_parking),
    }

# ---------------- Pagination ----------------


def paginate(items: list, page: int, per_page: int):
    total = len(items)
    if total == 0:
        return [], 1, 1, 0
    pages = (total + per_page - 1) // per_page
    page = max(1, min(page, pages))
    start = (page - 1) * per_page
    end = start + per_page
    return items[start:end], page, pages, total


# ---------------- Routes ----------------


@app.route("/")
def index():
    if (session.get("user") or {}).get("is_guest"):
        return redirect(url_for("parking"))
    posts = load_posts()

    public_posts = [
        p for p in posts if bool(p.get("is_public")) and not bool(p.get("is_archived"))
    ]
    public_sorted = sorted(public_posts, key=lambda p: p.get("date", ""), reverse=True)

    sidebar_items = get_sidebar_items("index", limit=3)

    return render_template(
        "index.html",
        public_posts=public_sorted[:5],
        sidebar_items=sidebar_items,
    )


@app.route("/p/guest")
def parking_guest():
    """
    Гостевая страница парковки.
    Доступна без авторизации, используется для QR-ссылок.
    """
    # грузим текущую конфигурацию парковки (если понадобится дальше)
    parking_data = load_parking()

    # грузим текущее состояние занятости
    state = load_parking_state()
    state_spots = state.get("spots", {}) or {}

    # список занятых мест в виде строк "1", "2", ...
    disabled_spots = [
        str(spot_id)
        for spot_id, spot_info in state_spots.items()
        if spot_info  # если словарь не пустой — место занято
    ]

    return render_template(
        "parking_guest.html",
        telegram_bot_url="#",
        disabled_spots=disabled_spots,
    )

@app.route("/admin/guests", methods=["GET", "POST"])
@login_required
@admin_required
def admin_guests():
    """
    Простая админка для просмотра и изменения статусов гостевых заявок.
    При одобрении заявки с указанием spot_id пытаемся занять это место за гостя.
    """
    guests_data = load_guests()
    guests = guests_data.get("guests") or []

    # Сначала более новые
    guests_sorted = sorted(
        guests,
        key=lambda g: (g.get("created_at") or "", g.get("id") or 0),
        reverse=True,
    )

    if request.method == "POST":
        guest_id_str = (request.form.get("guest_id") or "").strip()
        action = (request.form.get("action") or "").strip()

        try:
            guest_id = int(guest_id_str)
        except ValueError:
            guest_id = None

        if guest_id is not None and action:
            target_index = None
            for idx, g in enumerate(guests):
                try:
                    gid = int(g.get("id") or 0)
                except (TypeError, ValueError):
                    continue
                if gid == guest_id:
                    target_index = idx
                    break

            if target_index is not None:
                g = guests[target_index]

                if action == "approve":
                    g["status"] = "approved"

                    # если в заявке указано место — попробуем занять его за гостя
                    spot_id = g.get("spot_id")
                    try:
                        spot_id_int = int(spot_id) if spot_id is not None else None
                    except (TypeError, ValueError):
                        spot_id_int = None

                    if spot_id_int:
                        # проверяем, что такое место вообще есть в конфиге парковки
                        parking_cfg = load_parking()
                        spot_ids = {
                            int(s.get("id", 0))
                            for s in parking_cfg.get("spots", [])
                            if s.get("id") is not None
                        }

                        if spot_id_int in spot_ids:
                            state = load_parking_state()
                            spots_state = state.setdefault("spots", {})
                            sid = str(spot_id_int)

                            # если место свободно — занимаем его гостем
                            if not spots_state.get(sid):

                                guest_id = g.get("id")
                                guest_apartment = f"g{guest_id}" if guest_id else "гость"

                                spots_state[sid] = {
                                    "occupied": True,
                                    "apartment": guest_apartment,          # теперь гость = владелец
                                    "guest_id": guest_id,
                                    "is_guest": True,

                                    "name": (g.get("name") or "").strip(),
                                    "phone": (g.get("phone") or "").strip(),
                                    "car_code": (g.get("car_number") or "").strip(),
                                    "until": (g.get("until") or ""),

                                    "long_term": False,
                                    "show_phone": True,
                                    "guest_photo": g.get("photo") or "",
                                    "updated_at": datetime.utcnow().isoformat(timespec="minutes"),
                                }

                                save_parking_state(state)

                            else:
                                flash(
                                    f"Заявка гостя №{guest_id} одобрена, "
                                    f"но место {spot_id_int} уже занято.",
                                    "warning",
                                )

                    flash(f"Заявка гостя №{guest_id} одобрена.", "success")

                elif action == "reject":
                    g["status"] = "rejected"
                    flash(f"Заявка гостя №{guest_id} отклонена.", "info")

                elif action == "reset":
                    g["status"] = "pending"
                    flash(
                        f"Заявка гостя №{guest_id} снова помечена как 'ожидает решения'.",
                        "success",
                    )

                elif action == "delete":
                    guests.pop(target_index)
                    flash(f"Заявка гостя №{guest_id} удалена.", "success")

                guests_data["guests"] = guests
                save_guests(guests_data)

        return redirect(url_for("admin_guests"))

    return render_template("admin_guests.html", guests=guests_sorted)


@app.route("/parking/guest/demo")
def parking_guest_demo():
    """Старый демо-адрес, теперь просто редирект на /p/guest."""
    return redirect(url_for("parking_guest"))


@app.route("/p/guest/register", methods=["POST"])
def parking_guest_register():
    """Приём заявки гостя с формы на /p/guest."""
    # --- Основные поля формы ---
    name = (request.form.get("name") or "").strip()
    phone = (request.form.get("phone") or "").strip()
    car_number = (request.form.get("car_number") or "").strip()
    comment = (request.form.get("comment") or "").strip()
    spot_id_raw = (request.form.get("spot_id") or "").strip()
    until_raw = (request.form.get("until") or "").strip()  # datetime-local из формы
    # --- PIN гостя (для будущего входа через сайт/бота) ---
    pin1 = (request.form.get("pin1") or "").strip()
    pin2 = (request.form.get("pin2") or "").strip()
    pin_hash = None

    if pin1 or pin2:
        # оба поля должны быть заполнены одинаково
        if pin1 != pin2:
            return jsonify({"ok": False, "error": "pin_mismatch"}), 400

        if not pin1.isdigit() or not (4 <= len(pin1) <= 8):
            return jsonify({"ok": False, "error": "bad_pin_format"}), 400

        pin_hash = hash_pin(pin1)

    # --- Номер места ---
    try:
        spot_id = int(spot_id_raw) if spot_id_raw else None
    except ValueError:
        spot_id = None

    # --- Время до (может быть пустым) ---
    until_iso = None
    if until_raw:
        try:
            dt = datetime.fromisoformat(until_raw)
            until_iso = dt.isoformat(timespec="minutes")
        except ValueError:
            until_iso = None

    # --- Фото ---
    photo_file = request.files.get("photo")
    photo_rel_path = None
    if photo_file and photo_file.filename:
        GUEST_PHOTOS_DIR.mkdir(parents=True, exist_ok=True)
        filename = secure_filename(f"{int(time.time())}_{photo_file.filename}")
        full_path = GUEST_PHOTOS_DIR / filename
        photo_file.save(full_path)
        # относительный путь от папки static/
        photo_rel_path = f"img/guest_photos/{filename}"

    # --- Загрузка и сохранение guests.json ---
    guests_data = load_guests()
    guests = guests_data.get("guests") or []

    new_id = max((int(g.get("id", 0)) for g in guests), default=0) + 1

    guest = {
        "id": new_id,
        "name": name,
        "phone": phone,
        "car_number": car_number,
        "spot_id": spot_id,
        "until": until_iso,
        "comment": comment,
        "status": "pending",
        "photo": photo_rel_path,  # сюда кладём путь к фото
        "pin_hash": pin_hash,     # храним хеш PIN гостя (может быть None)
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "source": "site",
    }

    guests.append(guest)
    guests_data["guests"] = guests
    save_guests(guests_data)

    # --- Уведомление админам в Telegram ---
    if TELEGRAM_ENABLED:
        try:
            users = load_users()
            lines = [
                "Новая гостевая заявка на парковку:",
                f"ID: {new_id}",
                f"Имя: {name or '—'}",
                f"Телефон: {phone or '—'}",
                f"Номер машины: {car_number or '—'}",
            ]
            if spot_id is not None:
                lines.append(f"Место: {spot_id}")
            if until_iso:
                lines.append(f"Примерно до: {until_iso}")
            if comment:
                lines.append(f"Комментарий: {comment}")
            lines.append("")
            lines.append(
                "Подтвердите или отклоните заявку в админке сайта (раздел гостей)."
            )
            text = "\n".join(lines)

            for apt, rec in users.items():
                if not isinstance(rec, dict):
                    continue
                # админ — либо в списке ADMINS, либо флаг is_admin
                if not (is_admin_for(str(apt), rec) or rec.get("is_admin")):
                    continue
                chat_id = (rec.get("telegram_chat_id") or "").strip()
                if not chat_id:
                    continue
                send_telegram_message(chat_id, text)
        except Exception:
            # не ломаем регистрацию, если телега недоступна
            pass

    # фронт ждёт JSON
        # Автологин гостя сразу после отправки заявки (даже если pending)
    session["user"] = {
        "apartment": f"g{new_id}",
        "name": (name or "Гость").strip(),
        "is_admin": False,
        "is_guest": True,
        "guest_id": new_id,
        "guest_status": "pending",
        # для предзаполнения на парковке
        "phone": phone,
        "car_code": (car_number or "").strip(),
    }

    return jsonify({"ok": True, "guest_id": new_id, "status": "pending", "redirect": url_for("parking")})


@app.route("/api/guest/login", methods=["POST"])
def api_guest_login():
    """
    Логин гостя по телефону + PIN.

    Ожидает JSON:
    {
      "phone": "+79991234567",
      "pin": "1234"
    }
    """
    data = request.get_json(silent=True) or {}

    phone = (data.get("phone") or "").strip()
    secret = (data.get("pin") or data.get("pin_code") or "").strip()

    if not phone or not secret:
        return (
            jsonify(
                {"ok": False, "error": "missing_phone_or_pin", "message": "Нужны телефон и PIN"}
            ),
            400,
        )

    # Ищем ВСЕ заявки этого телефона и проверяем PIN по ним.
    guests_data = load_guests()
    guests = guests_data.get("guests") or []
    phone_norm = normalize_phone(phone)

    matches = []
    for g in guests:
        if normalize_phone(g.get("phone")) == phone_norm:
            matches.append(g)

    if not matches:
        return (
            jsonify(
                {"ok": False, "error": "guest_not_found", "message": "Гость с таким телефоном не найден"}
            ),
            404,
        )


    def _sort_key(g):
        created_at = g.get("created_at") or ""
        try:
            gid = int(g.get("id") or 0)
        except Exception:
            gid = 0
        return (created_at, gid)

    def _status(g):
        return (g.get("status") or "").strip().lower()

    def _pin_ok(g):
        h = g.get("pin_hash")
        return bool(h) and check_pin(secret, h)

    pin_ok = [g for g in matches if _pin_ok(g)]

    # Если у всех записей нет PIN — отдельная ошибка
    if not pin_ok:
        if all(not (g.get("pin_hash") or "") for g in matches):
            return (
                jsonify(
                    {"ok": False, "error": "pin_not_set", "message": "Для этого гостя ещё не задан PIN"}
                ),
                400,
            )
        return (
            jsonify(
                {"ok": False, "error": "wrong_pin", "message": "Неверный PIN"}
            ),
            403,
        )

    # Разрешаем вход и для pending — доступ к парковке ограничим оверлеем на /parking
    pin_ok.sort(key=_sort_key, reverse=True)
    guest = pin_ok[0]

    status = (_status(guest) or "pending").strip().lower()
    if status == "rejected":
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "guest_rejected",
                    "message": "Заявка была отклонена администратором. Подайте заявку снова.",
                }
            ),
            403,
        )


    guest_id = guest.get("id")
    guest_name = (guest.get("name") or "Гость").strip()
    guest_phone = (guest.get("phone") or phone).strip()
    guest_car = (guest.get("car_number") or "").strip()

    # ВАЖНО: apartment должен быть непустым, иначе /parking вас не пустит
    # Делаем уникальный ключ на гостя, чтобы работало правило "1 пользователь = 1 место"
    guest_apartment = f"g{guest_id}" if guest_id else f"g{secrets.token_hex(3)}"

    session["user"] = {
        "apartment": guest_apartment,
        "name": guest_name,
        "is_admin": False,
        "is_guest": True,
        "guest_id": guest_id,
        "guest_status": status,

        # полезно для предзаполнения на парковке
        "phone": guest_phone,
        "car_code": guest_car,
    }

    return jsonify(
        {
            "ok": True,
            "guest_id": guest_id,
            "name": guest_name,
            "phone": guest_phone,
            "redirect": url_for("parking"),
        }
    )

@app.route("/api/guest/status")
@login_required
def api_guest_status():
    """
    Возвращает статус текущего гостя (pending/approved/rejected).
    Используется для модального оверлея на /parking.
    """
    u = session.get("user") or {}
    if not u.get("is_guest"):
        return jsonify({"ok": False, "error": "not_guest"}), 403

    guest_id = u.get("guest_id")
    phone = (u.get("phone") or "").strip()

    guests_data = load_guests()
    guests = guests_data.get("guests") or []

    def _status(g):
        return (g.get("status") or "pending").strip().lower()

    found = None

    # 1) пытаемся найти по guest_id
    if guest_id is not None:
        for g in guests:
            if str(g.get("id")) == str(guest_id):
                found = g
                break

    # 2) запасной вариант — по телефону (если вдруг id нет)
    if not found and phone:
        phone_norm = normalize_phone(phone)
        for g in guests:
            if normalize_phone(g.get("phone")) == phone_norm:
                found = g
                break

    status = _status(found) if found else "pending"

    # Обновляем сессию (важно для /api/parking/spots и для кнопок)
    u["guest_status"] = status
    session["user"] = u

    return jsonify({"ok": True, "status": status, "approved": status == "approved"})


@app.route("/login", methods=["GET", "POST"])
def login():
    """
    Вход: номер квартиры + PIN.

    Телефон как пароль:
      - разрешён, если у квартиры ещё НЕ задан PIN (старый режим)
      - или если включен аварийный режим ALLOW_PHONE_FALLBACK=1 (для админа)
    """
    if request.method == "POST":
        apartment = (request.form.get("apartment") or "").strip()
        secret = (request.form.get("pin") or "").strip()

        users = load_users()
        user = users.get(apartment)

        if user and secret:
            admin = is_admin_for(apartment, user)

            # 1) Несколько жильцов (residents)
            residents = user.get("residents")
            if isinstance(residents, list) and residents:
                for r in residents:
                    if not isinstance(r, dict):
                        continue
                    stored = r.get("pin_hash")
                    if stored and check_pin(secret, stored):
                        session["user"] = {
                            "apartment": apartment,
                            "name": r.get("name", ""),
                            "is_admin": admin,
                        }
                        flash("Вы успешно вошли!", "success")
                        return redirect(url_for("news"))

            # 2) Старый формат: один PIN на квартиру
            stored = user.get("pin_hash")
            if stored and check_pin(secret, stored):
                session["user"] = {
                    "apartment": apartment,
                    "name": user.get("name", ""),
                    "is_admin": admin,
                }
                flash("Вы успешно вошли!", "success")
                return redirect(url_for("news"))

            # 3) Телефон как "пароль" (только если PIN ещё не задан или аварийно включён)
            has_pin = user_has_any_pin(user)
            allow_phone = (not has_pin) or ALLOW_PHONE_FALLBACK
            if allow_phone:
                phones = []
                if isinstance(user.get("phone"), str) and user["phone"]:
                    phones.append(user["phone"].strip())
                if isinstance(user.get("phones"), list):
                    for p in user["phones"]:
                        if p:
                            phones.append(str(p).strip())

                if secret in phones:
                    session["user"] = {
                        "apartment": apartment,
                        "name": user.get("name", ""),
                        "is_admin": admin,
                    }
                    if has_pin and ALLOW_PHONE_FALLBACK:
                        flash(
                            "ВНИМАНИЕ: включён аварийный вход по телефону. "
                            "После восстановления выключите режим.",
                            "info",
                        )
                    else:
                        flash(
                            "Вы вошли по старой схеме (телефон). "
                            "Попросите администратора выдать ссылку и задать PIN.",
                            "info",
                        )
                    return redirect(url_for("news"))

        flash("Неверный номер квартиры или PIN.", "error")

    return render_template("login.html")


@app.route("/forgot-pin")
def forgot_pin():
    return render_template("forgot_pin.html")


@app.route("/logout")
def logout():
    session.pop("user", None)
    flash("Вы вышли из системы.", "info")
    return redirect(url_for("index"))


@app.route("/profile", methods=["GET", "POST"])
@login_required
def profile():
    """Профиль жильца: ФИО, телефоны, машина, смена PIN."""
    users = load_users()
    sess_user = session.get("user") or {}
    apartment = (sess_user.get("apartment") or "").strip()
    # --- Гость: показываем профиль из session (не из users.json) ---
    if bool(sess_user.get("is_guest")):
        guest_phone = (sess_user.get("phone") or "").strip()
        guest_car = (sess_user.get("car_code") or "").strip()
        guest_name = (sess_user.get("name") or "").strip()

        if request.method == "POST":
            flash("Профиль гостя пока нельзя редактировать на сайте.", "info")
            return redirect(url_for("profile"))

        return render_template(
            "profile.html",
            is_guest=True,
            guest_id=sess_user.get("guest_id"),
            login_value=guest_phone or "",
            apartment=apartment,          # технический ключ (g17)
            last_name="",
            first_name=guest_name,
            middle_name="",
            phone1=guest_phone,
            phone2="",
            car_number=guest_car,
            can_use_parking=True,
            can_subscribe_parking=False,
        )

    if not apartment:
        flash("Не удалось определить квартиру.", "error")
        return redirect(url_for("news"))

    record = users.get(apartment, {}) if isinstance(users, dict) else {}

    # --- читаем существующие данные профиля ---
    last_name = (record.get("last_name") or "").strip()
    first_name = (record.get("first_name") or "").strip() or (
        sess_user.get("name") or ""
    ).strip()
    middle_name = (record.get("middle_name") or "").strip()

    # телефоны
    phones = []
    if isinstance(record.get("phones"), list):
        for p in record["phones"]:
            if p:
                phones.append(str(p).strip())
    elif isinstance(record.get("phone"), str) and record["phone"].strip():
        phones.append(record["phone"].strip())

    phone1 = phones[0] if len(phones) > 0 else ""
    phone2 = phones[1] if len(phones) > 1 else ""

    # номер машины (для совместимости с парковкой берём и car_number, и car_code)
    car_number = (record.get("car_number") or record.get("car_code") or "").strip()

    # флаги доступа к парковке (пока только для чтения, править будем позже)
    can_use_parking = bool(record.get("can_use_parking", True))
    can_subscribe_parking = bool(record.get("can_subscribe_parking", False))

    if request.method == "POST":
        # --- читаем форму ---
        last_name = (request.form.get("last_name") or "").strip()
        first_name = (request.form.get("first_name") or "").strip()
        middle_name = (request.form.get("middle_name") or "").strip()
        phone1 = (request.form.get("phone1") or "").strip()
        phone2 = (request.form.get("phone2") or "").strip()
        car_number = (request.form.get("car_number") or "").strip()

        current_pin = (request.form.get("current_pin") or "").strip()
        new_pin1 = (request.form.get("pin1") or "").strip()
        new_pin2 = (request.form.get("pin2") or "").strip()

        # --- сохраняем ФИО / телефоны / машину ---
        record["last_name"] = last_name
        record["first_name"] = first_name
        record["middle_name"] = middle_name

        new_phones = []
        if phone1:
            new_phones.append(phone1)
        if phone2:
            new_phones.append(phone2)
        record["phones"] = new_phones
        # на всякий случай дублируем первый телефон в старое поле
        if new_phones:
            record["phone"] = new_phones[0]

        if car_number:
            record["car_number"] = car_number
            # дублируем для парковки, если там ожидается car_code
            record["car_code"] = car_number
        else:
            record.pop("car_number", None)

        # --- смена PIN (опционально) ---
        if current_pin or new_pin1 or new_pin2:
            # проверяем новый PIN
            if new_pin1 != new_pin2:
                flash("Новый PIN в обоих полях должен совпадать.", "error")
                return redirect(url_for("profile"))

            if not new_pin1:
                flash("Укажите новый PIN.", "error")
                return redirect(url_for("profile"))

            if not new_pin1.isdigit() or not (4 <= len(new_pin1) <= 8):
                flash("Новый PIN должен состоять из 4–8 цифр.", "error")
                return redirect(url_for("profile"))

            # нужно ли проверять текущий PIN
            has_pin = user_has_any_pin(record)
            if has_pin:
                if not current_pin:
                    flash("Введите текущий PIN, чтобы его сменить.", "error")
                    return redirect(url_for("profile"))

                hashes = []
                if record.get("pin_hash"):
                    hashes.append(record["pin_hash"])
                residents = record.get("residents")
                if isinstance(residents, list):
                    for r in residents:
                        if isinstance(r, dict) and r.get("pin_hash"):
                            hashes.append(r["pin_hash"])

                ok_old = any(check_pin(current_pin, h) for h in hashes)
                if not ok_old:
                    flash("Текущий PIN указан неверно.", "error")
                    return redirect(url_for("profile"))

            # сохраняем новый PIN для текущего жильца
            residents = record.get("residents")
            if not isinstance(residents, list):
                residents = []

            display_name = (
                first_name or sess_user.get("name") or f"Житель кв. {apartment}"
            )

            updated = False
            for r in residents:
                if not isinstance(r, dict):
                    continue
                if (
                    r.get("name") == sess_user.get("name")
                    or r.get("name") == display_name
                ):
                    r["name"] = display_name
                    r["pin_hash"] = hash_pin(new_pin1)
                    updated = True
                    break

            if not updated:
                residents.append({"name": display_name, "pin_hash": hash_pin(new_pin1)})

            record["residents"] = residents
            # убираем старый pin_hash на квартиру, чтобы всё было через residents
            record.pop("pin_hash", None)

            # обновляем имя в сессии
            session["user"]["name"] = display_name
            flash("Профиль и PIN сохранены.", "success")
        else:
            flash("Профиль сохранён.", "success")

        users[apartment] = record
        save_users(users)
        return redirect(url_for("profile"))

    # GET — просто отдаём текущие значения в шаблон
    return render_template(
        "profile.html",
        apartment=apartment,
        last_name=last_name,
        first_name=first_name,
        middle_name=middle_name,
        phone1=phone1,
        phone2=phone2,
        car_number=car_number,
        can_use_parking=can_use_parking,
        can_subscribe_parking=can_subscribe_parking,
    )


@app.route("/parking")
@login_required
def parking():
    """Закрытая страница интерактивной парковки."""
    parking_data = load_parking()
    spots = parking_data.get("spots", [])

    (
        sess_user,
        apartment,
        user_row,
        can_use_parking,
        can_subscribe_parking,
    ) = current_user_parking_flags()
    if not apartment:
        flash("Не удалось определить квартиру.", "error")
        return redirect(url_for("news"))

    if not can_use_parking:
        flash(
            "Доступ к парковке для вашей квартиры пока не открыт. Обратитесь к администратору.",
            "error",
        )
        return redirect(url_for("news"))

    phone = ""
    car_code = ""

    # Подтягиваем телефон и номер авто из users.json по квартире
    if apartment and user_row:
        # телефон может быть строкой или списком
        if isinstance(user_row.get("phone"), str) and user_row["phone"].strip():
            phone = user_row["phone"].strip()
        elif isinstance(user_row.get("phones"), list):
            for p in user_row["phones"]:
                if p:
                    phone = str(p).strip()
                    if phone:
                        break

        # номер машины (если поле car_code есть в users.json)
        if isinstance(user_row.get("car_code"), str) and user_row["car_code"].strip():
            car_code = user_row["car_code"].strip()

    # обогащаем данные пользователя, не ломая существующую структуру
    user = dict(sess_user)
    if phone and "phone" not in user:
        user["phone"] = phone
    if car_code and "car_code" not in user:
        user["car_code"] = car_code
    user["can_use_parking"] = can_use_parking
    user["can_subscribe_parking"] = can_subscribe_parking

    return render_template(
        "parking.html",
        spots=spots,
        user=user,
    )


@app.route("/p")
@login_required
def parking_short():
    """Короткий адрес для страницы парковки (/p вместо /parking)."""
    return parking()

@app.route("/api/parking/spots")
def api_parking_spots():
    """Отдаём все места + текущее состояние занятости.

    Для авторизованных пользователей с правом парковки возвращаем occupant с деталями.
    Для гостей и тех, кому парковка запрещена, возвращаем только occupied без персональных данных.
    """
    sess_user, apartment, user_record, can_use_parking, _ = current_user_parking_flags()

    config = load_parking()
    state = load_parking_state()

    spots_cfg = config.get("spots", [])
    state_spots = state.get("spots", {})

    merged = []
    for spot in spots_cfg:
        sid = str(spot.get("id"))
        sstate = state_spots.get(sid, {})
        occupied = bool(sstate)

        # occupant показываем только тем, у кого парковка разрешена
        occupant = sstate or None
        if not can_use_parking:
            occupant = None
                    # Pending-гость не должен видеть детали занятости
        if sess_user.get("is_guest") and (sess_user.get("guest_status") or "pending") != "approved":
            occupant = None


        merged.append({
            "id": spot.get("id"),
            "label": spot.get("label"),
            "type": spot.get("type"),
            "description": spot.get("description"),
            "occupied": occupied,
            "occupant": occupant,
        })

    return jsonify({"spots": merged})


@app.route("/api/parking/spot/<int:spot_id>/occupy", methods=["POST"])
@login_required
def api_parking_occupy(spot_id: int):
    """
    Занять место: привязываем к текущему пользователю.

    Дополнительно:
      - флаг long_term (надолго) может менять только админ.
      - если админ ставит место "надолго" (для брошенной машины и т.п.),
        квартиру админа в карточке не показываем (apartment оставляем пустым).
    """
    sess_user, apartment, _, can_use_parking, _ = current_user_parking_flags()
    user = sess_user
    if user.get("is_guest") and (user.get("guest_status") or "pending") != "approved":
        return jsonify({"ok": False, "error": "guest_not_approved"}), 403

    if not apartment:
        return jsonify({"ok": False, "error": "no_user"}), 400

    is_admin = bool(user.get("is_admin"))
    if not can_use_parking and not is_admin:
        return jsonify({"ok": False, "error": "parking_not_allowed"}), 403

    # Проверяем, что такое место вообще есть в конфиге парковки
    config = load_parking()
    if not any(int(s.get("id", 0)) == spot_id for s in config.get("spots", [])):
        return jsonify({"ok": False, "error": "unknown_spot"}), 404

    payload = request.get_json(silent=True) or {}
    until = (payload.get("until") or "").strip()  # ISO-строка, опционально
    phone = (payload.get("phone") or "").strip()
    car_code = (payload.get("car_code") or "").strip()
    show_phone = bool(payload.get("show_phone", True))
    payload_long_term = bool(payload.get("long_term", False))

    if until:
        try:
            datetime.fromisoformat(until)
        except Exception:
            return jsonify({"ok": False, "error": "bad_until"}), 400

    state = load_parking_state()
    spots = state.setdefault("spots", {})
    subscriptions = state.setdefault("subscriptions", {})
    sid = str(spot_id)

    # если не админ — проверяем, не занято ли уже другое место этой же квартирой
    if not is_admin:
        for other_sid, info in spots.items():
            if other_sid == sid:
                continue
            if str(info.get("apartment") or "").strip() == apartment:
                # у пользователя уже есть другое занятое место
                return (
                    jsonify(
                        {
                            "ok": False,
                            "error": "already_has_spot",
                            "current_spot_id": int(other_sid),
                        }
                    ),
                    409,
                )

    existing = spots.get(sid)
    # Чужое занятое место трогать нельзя, кроме админа
    if existing and existing.get("apartment") != apartment and not is_admin:
        return jsonify({"ok": False, "error": "spot_busy"}), 409

    # long_term: админ может выставлять/снимать, обычный пользователь не трогает
    if is_admin:
        long_term = (
            payload_long_term
            if "long_term" in payload
            else bool(existing.get("long_term")) if existing else payload_long_term
        )
    else:
        long_term = bool(existing.get("long_term")) if existing else False

    # Квартира в записи:
    # - обычный пользователь всегда пишет свою квартиру;
    # - админ, если ставит "надолго", не светит свою квартиру (для брошенных машин).
    if is_admin and long_term:
        occupant_apartment = ""
    else:
        occupant_apartment = apartment

    # пробуем подтянуть telegram_chat_id из профиля пользователя
    telegram_chat_id = ""
    try:
        users = load_users()
        user_row = users.get(apartment)
        if isinstance(user_row, dict):
            telegram_chat_id = (user_row.get("telegram_chat_id") or "").strip()
    except Exception:
        telegram_chat_id = ""

    spots[sid] = {
        "apartment": occupant_apartment,
        "name": user.get("name") or "",
        "car_code": car_code,
        "phone": phone if show_phone else "",
        "show_phone": show_phone,
        "until": until,
        "long_term": long_term,
        "updated_at": datetime.utcnow().isoformat(timespec="minutes"),
        "telegram_chat_id": telegram_chat_id,
    }
    state["spots"] = spots
    state["subscriptions"] = subscriptions
    save_parking_state(state)
    return jsonify({"ok": True, "spot_id": spot_id})


@app.route("/api/parking/spot/<int:spot_id>/free", methods=["POST"])
@login_required
def api_parking_free(spot_id: int):
    """Освободить место: может владелец или админ."""
    sess_user, apartment, _, can_use_parking, _ = current_user_parking_flags()
    user = sess_user
    is_admin = bool(user.get("is_admin"))
    if user.get("is_guest") and (user.get("guest_status") or "pending") != "approved":
        return jsonify({"ok": False, "error": "guest_not_approved"}), 403


    if not apartment:
        return jsonify({"ok": False, "error": "no_user"}), 400

    if not can_use_parking and not is_admin:
        return jsonify({"ok": False, "error": "parking_not_allowed"}), 403

    state = load_parking_state()
    spots = state.setdefault("spots", {})
    subscriptions = state.setdefault("subscriptions", {})
    sid = str(spot_id)
    existing = spots.get(sid)

    if not existing:
        # если вдруг были подписки на уже свободное место — подчистим
        if sid in subscriptions:
            subscriptions.pop(sid, None)
            save_parking_state(state)
        return jsonify({"ok": True, "spot_id": spot_id})  # уже свободно

    if existing.get("apartment") != apartment and not is_admin:
        return jsonify({"ok": False, "error": "forbidden"}), 403

    # человекочитаемое название места
    parking_cfg = load_parking()
    label = f"место {sid}"
    for s in parking_cfg.get("spots", []):
        if str(s.get("id")) == sid:
            label = s.get("label") or label
            break

    # уведомим подписчиков, что место освободилось
    subs_for_spot = subscriptions.get(sid) or []
    if subs_for_spot:
        try:
            notify_parking_freed_subscribers(subs_for_spot, label)
        except Exception:
            pass
        subscriptions.pop(sid, None)

    spots.pop(sid, None)
    state["spots"] = spots
    state["subscriptions"] = subscriptions
    save_parking_state(state)
    return jsonify({"ok": True, "spot_id": spot_id})


@app.route("/api/parking/spot/<int:spot_id>/subscribe", methods=["POST"])
@login_required
def api_parking_subscribe(spot_id: int):
    """
    Подписаться на уведомление, когда конкретное место освободится.
    Используем telegram_chat_id из профиля пользователя.
    """
    (
        sess_user,
        apartment,
        record,
        can_use_parking,
        can_subscribe_parking,
    ) = current_user_parking_flags()
    if not apartment:
        return jsonify({"ok": False, "error": "no_user"}), 400

    if not can_use_parking and not sess_user.get("is_admin"):
        return jsonify({"ok": False, "error": "parking_not_allowed"}), 403

    if not can_subscribe_parking and not sess_user.get("is_admin"):
        return jsonify({"ok": False, "error": "subscribe_not_allowed"}), 403

    if not isinstance(record, dict):
        return jsonify({"ok": False, "error": "no_user_record"}), 400

    chat_id = (record.get("telegram_chat_id") or "").strip()
    if not chat_id:
        return jsonify({"ok": False, "error": "no_telegram_chat_id"}), 400

    # проверяем, что место вообще существует в конфиге
    config = load_parking()
    if not any(int(s.get("id", 0)) == spot_id for s in config.get("spots", [])):
        return jsonify({"ok": False, "error": "unknown_spot"}), 404

    state = load_parking_state()
    subscriptions = state.setdefault("subscriptions", {})
    sid = str(spot_id)
    subs_for_spot = subscriptions.get(sid) or []

    if chat_id in subs_for_spot:
        return jsonify({"ok": True, "already": True})

    subs_for_spot.append(chat_id)
    subscriptions[sid] = subs_for_spot
    state["subscriptions"] = subscriptions
    save_parking_state(state)

    return jsonify({"ok": True, "spot_id": spot_id})


@app.route("/api/parking/spot/<int:spot_id>/unsubscribe", methods=["POST"])
@login_required
def api_parking_unsubscribe(spot_id: int):
    """Отписаться от уведомлений по конкретному месту."""
    (
        sess_user,
        apartment,
        record,
        can_use_parking,
        can_subscribe_parking,
    ) = current_user_parking_flags()
    if not apartment:
        return jsonify({"ok": False, "error": "no_user"}), 400

    if not can_use_parking and not sess_user.get("is_admin"):
        return jsonify({"ok": False, "error": "parking_not_allowed"}), 403

    if not can_subscribe_parking and not sess_user.get("is_admin"):
        return jsonify({"ok": False, "error": "subscribe_not_allowed"}), 403

    if not isinstance(record, dict):
        return jsonify({"ok": False, "error": "no_user_record"}), 400

    chat_id = (record.get("telegram_chat_id") or "").strip()
    if not chat_id:
        return jsonify({"ok": False, "error": "no_telegram_chat_id"}), 400

    state = load_parking_state()
    subscriptions = state.setdefault("subscriptions", {})
    sid = str(spot_id)
    subs_for_spot = subscriptions.get(sid) or []

    if chat_id in subs_for_spot:
        subs_for_spot = [c for c in subs_for_spot if c != chat_id]
        if subs_for_spot:
            subscriptions[sid] = subs_for_spot
        else:
            subscriptions.pop(sid, None)
        state["subscriptions"] = subscriptions
        save_parking_state(state)

    return jsonify({"ok": True, "spot_id": spot_id})


@app.route("/news", methods=["GET", "POST"])
@login_required
def news():
    if (session.get("user") or {}).get("is_guest"):
        return redirect(url_for("parking"))
    posts_all = load_posts()

    active_posts = [p for p in posts_all if not bool(p.get("is_archived"))]
    posts_sorted = sorted(active_posts, key=lambda p: p.get("date", ""), reverse=True)

    # подписки
    subs_all = load_subscriptions()
    apartment = session["user"]["apartment"]
    user_subs = subs_all.get(apartment, {"house": True, "district": True})

    if request.method == "POST":
        house_on = bool(request.form.get("sub_house"))
        district_on = bool(request.form.get("sub_district"))
        user_subs = {"house": house_on, "district": district_on}
        subs_all[apartment] = user_subs
        save_subscriptions(subs_all)
        flash("Настройки подписки сохранены.", "success")
        return redirect(url_for("news"))

    # пагинация (чтобы news.html не падал: pagination is undefined)
    page = request.args.get("page", 1, type=int)
    page_posts, page, pages, total = paginate(posts_sorted, page, POSTS_PER_PAGE)
    pagination = {
        "page": page,
        "pages": pages,
        "total": total,
        "per_page": POSTS_PER_PAGE,
        "endpoint": "news",
    }

    sidebar_items = get_sidebar_items("news", limit=3)

    reactions = load_reactions()
    me = get_user_key()
    user_reactions = {}
    for pid_str, per_emoji in reactions.items():
        if not isinstance(per_emoji, dict):
            continue
        for emoji, users_list in per_emoji.items():
            if isinstance(users_list, list) and me in users_list:
                try:
                    user_reactions[int(pid_str)] = emoji
                except Exception:
                    pass
                break

    return render_template(
        "news.html",
        posts=page_posts,
        user_subs=user_subs,
        sidebar_items=sidebar_items,
        reactions=reactions,
        reaction_emojis=REACTION_EMOJIS,
        user_reactions=user_reactions,
        is_admin=bool(session["user"].get("is_admin", False)),
        pagination=pagination,
        is_archive_view=False,
        has_archived=any(bool(p.get("is_archived")) for p in posts_all),
    )


@app.route("/news/archive")
@login_required
def news_archive():
    if (session.get("user") or {}).get("is_guest"):
        return redirect(url_for("parking"))
    posts_all = load_posts()
    archived = [p for p in posts_all if bool(p.get("is_archived"))]
    posts_sorted = sorted(archived, key=lambda p: p.get("date", ""), reverse=True)

    subs_all = load_subscriptions()
    apartment = session["user"]["apartment"]
    user_subs = subs_all.get(apartment, {"house": True, "district": True})

    page = request.args.get("page", 1, type=int)
    page_posts, page, pages, total = paginate(posts_sorted, page, POSTS_PER_PAGE)
    pagination = {
        "page": page,
        "pages": pages,
        "total": total,
        "per_page": POSTS_PER_PAGE,
        "endpoint": "news_archive",
    }

    sidebar_items = get_sidebar_items("news", limit=3)

    reactions = load_reactions()
    me = get_user_key()
    user_reactions = {}
    for pid_str, per_emoji in reactions.items():
        if not isinstance(per_emoji, dict):
            continue
        for emoji, users_list in per_emoji.items():
            if isinstance(users_list, list) and me in users_list:
                try:
                    user_reactions[int(pid_str)] = emoji
                except Exception:
                    pass
                break

    return render_template(
        "news.html",
        posts=page_posts,
        user_subs=user_subs,
        sidebar_items=sidebar_items,
        reactions=reactions,
        reaction_emojis=REACTION_EMOJIS,
        user_reactions=user_reactions,
        is_admin=bool(session["user"].get("is_admin", False)),
        pagination=pagination,
        is_archive_view=True,
        has_archived=True,
    )


@app.route("/news/<int:post_id>/react", methods=["POST"])
@login_required
def react(post_id: int):
    if (session.get("user") or {}).get("is_guest"):
        return redirect(url_for("parking"))
    emoji = (request.form.get("emoji") or "").strip()
    if emoji not in REACTION_EMOJIS:
        return redirect(url_for("news"))

    reactions = load_reactions()
    pid = str(post_id)
    post_map = reactions.get(pid)
    if not isinstance(post_map, dict):
        post_map = {}

    me = get_user_key()
    already = me in (post_map.get(emoji) or [])

    # убираем старую реакцию пользователя для этого поста
    for e in list(post_map.keys()):
        lst = post_map.get(e)
        if not isinstance(lst, list):
            post_map.pop(e, None)
            continue
        if me in lst:
            lst = [x for x in lst if x != me]
            if lst:
                post_map[e] = lst
            else:
                post_map.pop(e, None)

    # если нажал ту же — отключаем; иначе ставим новую
    if not already:
        post_map.setdefault(emoji, []).append(me)

    reactions[pid] = post_map
    save_reactions(reactions)
    return redirect(url_for("news") + f"#post-{post_id}")


@app.route("/info")
@login_required
def info():
    if (session.get("user") or {}).get("is_guest"):
        return redirect(url_for("parking"))
    items = get_sidebar_items("news", limit=0)

    return render_template("info.html", items=items)


# ---------------- Admin: news CRUD ----------------


def _handle_news_form(existing_post: dict | None):
    title = (request.form.get("title") or "").strip()
    date_str = (request.form.get("date") or "").strip() or date.today().isoformat()
    category = (request.form.get("category") or "").strip() or "Дом"
    text = (request.form.get("text") or "").strip()

    is_public = bool(request.form.get("is_public"))
    is_archived = bool(request.form.get("is_archived"))

    if not title or not text:
        flash("Заголовок и текст новости обязательны.", "error")
        return None

    sources = []
    if request.form.get("src_telegram"):
        sources.append("telegram")
    if request.form.get("src_max"):
        sources.append("max")
    if request.form.get("src_site"):
        sources.append("site")

    # превью
    image_path = existing_post.get("image") if existing_post else None
    image_file = request.files.get("image_file")
    image_text = (request.form.get("image") or "").strip()

    if image_file and image_file.filename:
        saved = save_uploaded_file(image_file)
        if not saved:
            flash("Не удалось сохранить превью (jpg/png/gif/webp).", "error")
            return None
        image_path = saved
    elif image_text:
        if image_text.lower().startswith(("http://", "https://")):
            downloaded = download_image_from_url(image_text)
            if not downloaded:
                flash("Не удалось скачать превью по ссылке.", "error")
                return None
            image_path = downloaded
        else:
            image_path = image_text.replace("\\", "/")

    # галерея
    existing_gallery = existing_post.get("gallery") if existing_post else None
    new_gallery = []

    for gf in request.files.getlist("gallery_files"):
        if gf and gf.filename:
            saved = save_uploaded_file(gf)
            if saved:
                new_gallery.append(saved)

    gallery_raw = (request.form.get("gallery") or "").strip()
    if gallery_raw:
        for part in gallery_raw.split(","):
            token = part.strip()
            if not token:
                continue
            if token.lower().startswith(("http://", "https://")):
                downloaded = download_image_from_url(token)
                if downloaded:
                    new_gallery.append(downloaded)
            else:
                new_gallery.append(token.replace("\\", "/"))

    gallery = new_gallery if new_gallery else existing_gallery
    if gallery == []:
        gallery = None

    return {
        "date": date_str,
        "title": title,
        "category": category,
        "source": sources,
        "text": text,
        "image": image_path,
        "gallery": gallery,
        "is_public": is_public,
        "is_archived": is_archived,
    }


@app.route("/admin/news/new", methods=["GET", "POST"])
@login_required
@admin_required
def admin_news_new():
    if request.method == "POST":
        posts = load_posts()
        payload = _handle_news_form(existing_post=None)
        if payload is None:
            return redirect(url_for("admin_news_new"))

        new_id = max((int(p.get("id", 0)) for p in posts), default=0) + 1
        payload["id"] = new_id
        posts.append(payload)
        save_posts(posts)

        flash("Новость успешно добавлена.", "success")
        return redirect(url_for("news") + f"#post-{new_id}")

    return render_template(
        "admin_news_new.html", today=date.today().isoformat(), post=None
    )


@app.route("/admin/news/<int:post_id>/edit", methods=["GET", "POST"])
@login_required
@admin_required
def admin_news_edit(post_id: int):
    posts = load_posts()
    post = next((p for p in posts if int(p.get("id", 0)) == post_id), None)
    if not post:
        flash("Новость не найдена.", "error")
        return redirect(url_for("news"))

    if request.method == "POST":
        payload = _handle_news_form(existing_post=post)
        if payload is None:
            return redirect(url_for("admin_news_edit", post_id=post_id))
        for k, v in payload.items():
            post[k] = v
        save_posts(posts)
        flash("Новость обновлена.", "success")
        return redirect(url_for("news") + f"#post-{post_id}")

    return render_template(
        "admin_news_new.html",
        today=post.get("date", date.today().isoformat()),
        post=post,
    )


@app.route("/admin/news/<int:post_id>/delete", methods=["POST"])
@login_required
@admin_required
def admin_news_delete(post_id: int):
    posts = load_posts()
    new_posts = [p for p in posts if int(p.get("id", 0)) != post_id]
    if len(new_posts) == len(posts):
        flash("Новость не найдена.", "error")
        return redirect(url_for("news"))

    save_posts(new_posts)

    reactions = load_reactions()
    reactions.pop(str(post_id), None)
    save_reactions(reactions)

    flash("Новость удалена.", "info")
    return redirect(url_for("news"))


def send_telegram_message(chat_id: str, text: str) -> bool:
    """
    Отправка простого текстового сообщения в Telegram-бота.
    Используется для уведомлений (парковка, общий инфопоток и т.п.).
    """
    if not TELEGRAM_ENABLED:
        return False

    chat_id = str(chat_id or "").strip()
    text = (text or "").strip()
    if not chat_id or not text:
        return False

    try:
        data = urlencode({"chat_id": chat_id, "text": text}).encode("utf-8")
        url = f"{TELEGRAM_API_BASE}/sendMessage"
        with urlopen(url, data=data, timeout=5) as resp:
            resp.read()  # просто чтобы запрос завершился
        return True
    except Exception:
        # логирование можно добавить позже
        return False


# ---------------- Admin: invites & registration ----------------


@app.route("/admin/invites", methods=["GET", "POST"])
@login_required
@admin_required
def admin_invites():
    invites = load_invites()

    if request.method == "POST":
        apartment = (request.form.get("apartment") or "").strip()
        if not apartment:
            flash("Укажите номер квартиры.", "error")
            return redirect(url_for("admin_invites"))

        token = secrets.token_urlsafe(16)
        invites[token] = {
            "apartment": apartment,
            "created_at": datetime.utcnow().isoformat(timespec="seconds"),
            "used": False,
        }
        save_invites(invites)
        flash("Ссылка приглашения создана.", "success")
        return redirect(url_for("admin_invites"))

    invite_list = [
        {
            "token": token,
            "apartment": info.get("apartment"),
            "created_at": info.get("created_at"),
            "used": info.get("used", False),
        }
        for token, info in invites.items()
    ]
    invite_list.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    base_url = request.host_url.rstrip("/")

    return render_template(
        "admin_invites.html", invites=invite_list, base_url=base_url
    )


@app.route("/register/<token>", methods=["GET", "POST"])
def register(token: str):
    invites = load_invites()
    invite = invites.get(token)

    if not invite or invite.get("used"):
        flash("Эта ссылка недействительна или уже была использована.", "error")
        return redirect(url_for("login"))

    apartment = invite.get("apartment")

    if request.method == "POST":
        name = (request.form.get("name") or "").strip()
        phone = (request.form.get("phone") or "").strip()
        pin1 = (request.form.get("pin1") or "").strip()
        pin2 = (request.form.get("pin2") or "").strip()

        if not name or not pin1 or not pin2:
            flash("Заполните имя и PIN.", "error")
            return redirect(url_for("register", token=token))

        if pin1 != pin2:
            flash("PIN в обоих полях должен совпадать.", "error")
            return redirect(url_for("register", token=token))

        if not pin1.isdigit() or not (4 <= len(pin1) <= 8):
            flash("PIN должен состоять из 4–8 цифр.", "error")
            return redirect(url_for("register", token=token))

        users = load_users()
        existing = users.get(apartment, {})

        # телефоны как справочная инфа (может быть несколько)
        phones = []
        if phone:
            phones.append(phone.strip())
        if isinstance(existing.get("phones"), list):
            for p in existing["phones"]:
                if p:
                    phones.append(str(p).strip())
        elif isinstance(existing.get("phone"), str):
            phones.append(existing["phone"].strip())

        # перенос старого формата pin_hash -> residents
        residents = existing.get("residents")
        if not isinstance(residents, list):
            residents = []
            old_pin = existing.get("pin_hash")
            old_name = existing.get("name")
            if old_pin:
                residents.append(
                    {
                        "name": old_name or f"Житель кв. {apartment}",
                        "pin_hash": old_pin,
                    }
                )

        residents.append({"name": name, "pin_hash": hash_pin(pin1)})
        admin = is_admin_for(str(apartment), existing)

        users[apartment] = {
            "residents": residents,
            "phones": phones,
            "is_admin": admin or bool(existing.get("is_admin", False)),
        }
        save_users(users)

        invite["used"] = True
        invites[token] = invite
        save_invites(invites)

        session["user"] = {"apartment": apartment, "name": name, "is_admin": admin}
        flash("Регистрация завершена. Добро пожаловать!", "success")
        return redirect(url_for("news"))

    return render_template("register.html", apartment=apartment)


@app.route("/api/debug/telegram")
@login_required
@admin_required
def api_debug_telegram():
    """
    Простой тест-эндпоинт для проверки связи с Telegram-ботом.
    Использование (только для админа):
      /api/debug/telegram?chat_id=XXX&text=Привет
    """
    if not TELEGRAM_ENABLED:
        return jsonify({"ok": False, "error": "telegram_disabled"}), 500

    chat_id = (request.args.get("chat_id") or "").strip()
    text = (request.args.get("text") or "Тестовое уведомление с сайта парковки").strip()

    if not chat_id:
        return jsonify({"ok": False, "error": "no_chat_id"}), 400

    ok = send_telegram_message(chat_id, text)
    return jsonify({"ok": ok})


if __name__ == "__main__":
    # host="0.0.0.0" — чтобы можно было открыть с телефона в той же сети
    app.run(debug=True, host="0.0.0.0")
