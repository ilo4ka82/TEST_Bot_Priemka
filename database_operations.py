import sqlite3
import secrets
import string
import logging
from datetime import datetime, timedelta
import pytz

from config import DATABASE_PATH

logger = logging.getLogger(__name__)
MOSCOW_TZ = pytz.timezone("Europe/Moscow")


def get_db_connection():
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # --- users ---
        # user_id — внутренний первичный ключ
        # telegram_id / vk_id — внешние идентификаторы, оба опциональны
        # link_code — постоянный код привязки аккаунтов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id             INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id         INTEGER UNIQUE,
                vk_id               INTEGER UNIQUE,
                link_code           TEXT UNIQUE,
                username            TEXT,
                first_name          TEXT,
                last_name           TEXT,
                application_full_name  TEXT,
                application_department TEXT,
                is_authorized       BOOLEAN DEFAULT FALSE,
                is_admin            BOOLEAN DEFAULT FALSE,
                registration_date   DATETIME DEFAULT CURRENT_TIMESTAMP,
                application_status  TEXT DEFAULT 'none'
            )
        """)
        logger.info("Таблица 'users' проверена/создана.")

        # --- work_sessions ---
        # user_id — внутренний FK, не telegram_id
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS work_sessions (
                session_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL,
                check_in_time   DATETIME NOT NULL,
                check_out_time  DATETIME,
                duration_minutes INTEGER,
                latitude        REAL,
                longitude       REAL,
                checkin_type    TEXT NOT NULL DEFAULT 'geo',
                sector_id       TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
        """)
        logger.info("Таблица 'work_sessions' проверена/создана.")

        # --- manual_checkin_requests ---
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS manual_checkin_requests (
                request_id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id                 INTEGER NOT NULL,
                requested_checkin_time  TEXT NOT NULL,
                request_timestamp       TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                status                  TEXT NOT NULL DEFAULT 'pending',
                admin_user_id           INTEGER,
                processed_timestamp     TEXT,
                final_checkin_time      TEXT,
                FOREIGN KEY (user_id)       REFERENCES users(user_id),
                FOREIGN KEY (admin_user_id) REFERENCES users(user_id)
            )
        """)
        logger.info("Таблица 'manual_checkin_requests' проверена/создана.")

        conn.commit()
    except sqlite3.Error as e:
        logger.error(f"Ошибка при инициализации БД: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()


# ------------------------------------------------------------------ #
# Генерация кода привязки                                             #
# ------------------------------------------------------------------ #

def _generate_link_code() -> str:
    """Генерирует уникальный 6-символьный код привязки."""
    alphabet = string.ascii_uppercase + string.digits
    while True:
        code = "".join(secrets.choice(alphabet) for _ in range(6))
        conn = get_db_connection()
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM users WHERE link_code = ?", (code,))
            if not cursor.fetchone():
                return code
        finally:
            conn.close()


# ------------------------------------------------------------------ #
# Поиск пользователя (внутренний хелпер)                              #
# ------------------------------------------------------------------ #

def _get_user_by_field(field: str, value) -> dict | None:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM users WHERE {field} = ?", (value,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        logger.error(f"Ошибка при поиске пользователя по {field}={value}: {e}")
        return None
    finally:
        conn.close()


# ------------------------------------------------------------------ #
# Публичные функции поиска пользователя                               #
# ------------------------------------------------------------------ #

def get_user_by_telegram_id(telegram_id: int) -> dict | None:
    return _get_user_by_field("telegram_id", telegram_id)


def get_user_by_vk_id(vk_id: int) -> dict | None:
    return _get_user_by_field("vk_id", vk_id)


def get_user_by_user_id(user_id: int) -> dict | None:
    return _get_user_by_field("user_id", user_id)


def get_user_by_link_code(link_code: str) -> dict | None:
    return _get_user_by_field("link_code", link_code.upper().strip())


# Обратная совместимость — используется в bot_main.py
def get_user(telegram_id: int) -> dict | None:
    return get_user_by_telegram_id(telegram_id)


# ------------------------------------------------------------------ #
# Регистрация / обновление                                            #
# ------------------------------------------------------------------ #

def add_or_update_user(
    telegram_id: int = None,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
    vk_id: int = None,
) -> dict | None:
    """
    Добавляет нового пользователя или обновляет существующего.
    При создании автоматически генерирует link_code.
    Возвращает словарь с данными пользователя.
    """
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        reg_time = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")

        # Ищем существующего пользователя
        existing = None
        if telegram_id:
            existing = get_user_by_telegram_id(telegram_id)
        if not existing and vk_id:
            existing = get_user_by_vk_id(vk_id)

        if existing:
            # Обновляем данные
            cursor.execute("""
                UPDATE users SET
                    username   = COALESCE(?, username),
                    first_name = COALESCE(?, first_name),
                    last_name  = COALESCE(?, last_name),
                    telegram_id = COALESCE(?, telegram_id),
                    vk_id       = COALESCE(?, vk_id)
                WHERE user_id = ?
            """, (username, first_name, last_name, telegram_id, vk_id, existing["user_id"]))
            conn.commit()
            logger.info(f"Пользователь user_id={existing['user_id']} обновлён.")
            return get_user_by_user_id(existing["user_id"])
        else:
            # Создаём нового
            link_code = _generate_link_code()
            cursor.execute("""
                INSERT INTO users
                    (telegram_id, vk_id, link_code, username, first_name, last_name, registration_date)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (telegram_id, vk_id, link_code, username, first_name, last_name, reg_time))
            conn.commit()
            user_id = cursor.lastrowid
            logger.info(f"Новый пользователь создан: user_id={user_id}, link_code={link_code}.")
            return get_user_by_user_id(user_id)

    except sqlite3.Error as e:
        logger.error(f"Ошибка при добавлении/обновлении пользователя: {e}", exc_info=True)
        return None
    finally:
        conn.close()


# ------------------------------------------------------------------ #
# Привязка аккаунтов по коду                                          #
# ------------------------------------------------------------------ #

def link_account_by_code(code: str, vk_id: int = None, telegram_id: int = None) -> tuple[bool, str]:
    """
    Привязывает VK или Telegram аккаунт к существующему пользователю по коду.

    Сценарий: пользователь зарегистрирован в TG, хочет привязать VK.
    Вводит код в VK → вызываем link_account_by_code(code, vk_id=12345)
    """
    if not vk_id and not telegram_id:
        return False, "Не указан ни VK ID, ни Telegram ID для привязки."

    target = get_user_by_link_code(code)
    if not target:
        return False, "❌ Код не найден. Проверьте правильность ввода."

    # Проверяем что этот аккаунт ещё не привязан
    if vk_id and target.get("vk_id"):
        return False, "❌ К этому аккаунту уже привязан VK."
    if telegram_id and target.get("telegram_id"):
        return False, "❌ К этому аккаунту уже привязан Telegram."

    # Проверяем что vk_id/telegram_id не принадлежат другому пользователю
    if vk_id:
        existing_vk = get_user_by_vk_id(vk_id)
        if existing_vk and existing_vk["user_id"] != target["user_id"]:
            return False, "❌ Этот VK аккаунт уже зарегистрирован в системе."
    if telegram_id:
        existing_tg = get_user_by_telegram_id(telegram_id)
        if existing_tg and existing_tg["user_id"] != target["user_id"]:
            return False, "❌ Этот Telegram аккаунт уже зарегистрирован в системе."

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        if vk_id:
            cursor.execute("UPDATE users SET vk_id = ? WHERE user_id = ?", (vk_id, target["user_id"]))
        if telegram_id:
            cursor.execute("UPDATE users SET telegram_id = ? WHERE user_id = ?", (telegram_id, target["user_id"]))
        conn.commit()
        name = target.get("application_full_name") or target.get("first_name") or str(target["user_id"])
        logger.info(f"Аккаунт привязан: user_id={target['user_id']}, vk_id={vk_id}, telegram_id={telegram_id}.")
        return True, f"✅ Аккаунт успешно привязан! Добро пожаловать, {name}."
    except sqlite3.Error as e:
        logger.error(f"Ошибка при привязке аккаунта: {e}", exc_info=True)
        return False, "❌ Ошибка базы данных при привязке аккаунта."
    finally:
        conn.close()

def merge_users_on_link(code: str, vk_id: int = None, telegram_id: int = None) -> tuple[bool, str]:
    """
    Привязывает аккаунты с мержем если оба уже существуют в БД.
    """
    if not vk_id and not telegram_id:
        return False, "Не указан ни VK ID, ни Telegram ID для привязки."

    target = get_user_by_link_code(code)
    if not target:
        return False, "❌ Код не найден. Проверьте правильность ввода."

    if vk_id and target.get("vk_id"):
        return False, "❌ К этому аккаунту уже привязан VK."
    if telegram_id and target.get("telegram_id"):
        return False, "❌ К этому аккаунту уже привязан Telegram."

    existing = None
    if vk_id:
        existing = get_user_by_vk_id(vk_id)
    if telegram_id:
        existing = get_user_by_telegram_id(telegram_id)

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        if existing and existing["user_id"] != target["user_id"]:
            existing_uid = existing["user_id"]
            target_uid   = target["user_id"]

            cursor.execute(
                "UPDATE work_sessions SET user_id=? WHERE user_id=?",
                (target_uid, existing_uid)
            )
            cursor.execute(
                "UPDATE manual_checkin_requests SET user_id=? WHERE user_id=?",
                (target_uid, existing_uid)
            )
            if not target.get("application_full_name") and existing.get("application_full_name"):
                cursor.execute("""
                    UPDATE users SET
                        application_full_name = ?,
                        application_department = ?,
                        application_status = ?,
                        is_authorized = ?
                    WHERE user_id = ?
                """, (
                    existing["application_full_name"],
                    existing["application_department"],
                    existing["application_status"],
                    existing["is_authorized"],
                    target_uid
                ))
            cursor.execute("DELETE FROM users WHERE user_id=?", (existing_uid,))
            logger.info(f"Мерж: user_id={existing_uid} → user_id={target_uid}")

        if vk_id:
            cursor.execute("UPDATE users SET vk_id=? WHERE user_id=?", (vk_id, target["user_id"]))
        if telegram_id:
            cursor.execute("UPDATE users SET telegram_id=? WHERE user_id=?", (telegram_id, target["user_id"]))

        conn.commit()
        name = target.get("application_full_name") or target.get("first_name") or str(target["user_id"])
        return True, f"✅ Аккаунты успешно привязаны! Добро пожаловать, {name}."

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при мерже пользователей: {e}", exc_info=True)
        return False, "❌ Ошибка базы данных при привязке аккаунта."
    finally:
        if conn:
            conn.close()


# ------------------------------------------------------------------ #
# Авторизация                                                         #
# ------------------------------------------------------------------ #

def is_user_authorized(telegram_id: int) -> bool:
    user = get_user_by_telegram_id(telegram_id)
    return bool(user and user.get("is_authorized"))


def is_user_authorized_by_vk(vk_id: int) -> bool:
    user = get_user_by_vk_id(vk_id)
    return bool(user and user.get("is_authorized"))


def authorize_user(telegram_id_to_authorize: int, authorizing_admin_id: int) -> tuple[bool, str]:
    """Авторизует пользователя по его telegram_id. Обратная совместимость с bot_main.py."""
    target = get_user_by_telegram_id(telegram_id_to_authorize)
    if not target:
        return False, f"Пользователь с Telegram ID {telegram_id_to_authorize} не найден."
    return _authorize_by_user_id(target["user_id"], authorizing_admin_id)


def authorize_user_by_vk(vk_id: int, admin_user_id: int) -> tuple[bool, str]:
    """Авторизует пользователя по его vk_id."""
    target = get_user_by_vk_id(vk_id)
    if not target:
        return False, f"Пользователь с VK ID {vk_id} не найден."
    return _authorize_by_user_id(target["user_id"], admin_user_id)


def _authorize_by_user_id(user_id: int, admin_user_id: int) -> tuple[bool, str]:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users SET is_authorized = TRUE, application_status = 'approved'
            WHERE user_id = ?
        """, (user_id,))
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"user_id={user_id} авторизован администратором user_id={admin_user_id}.")
            return True, f"Пользователь ID {user_id} успешно авторизован."
        return False, f"Не удалось авторизовать пользователя ID {user_id}."
    except sqlite3.Error as e:
        logger.error(f"Ошибка при авторизации user_id={user_id}: {e}", exc_info=True)
        return False, "Ошибка базы данных при авторизации."
    finally:
        conn.close()


# ------------------------------------------------------------------ #
# Заявки на доступ                                                    #
# ------------------------------------------------------------------ #

def submit_application(telegram_id: int, full_name: str, department: str) -> tuple[bool, str]:
    user = get_user_by_telegram_id(telegram_id)
    if not user:
        return False, "Произошла ошибка. Пожалуйста, попробуйте сначала /start."
    return _submit_application_by_user_id(user["user_id"], full_name, department)


def submit_application_vk(vk_id: int, full_name: str, department: str) -> tuple[bool, str]:
    user = get_user_by_vk_id(vk_id)
    if not user:
        return False, "Произошла ошибка. Пожалуйста, попробуйте сначала /start."
    return _submit_application_by_user_id(user["user_id"], full_name, department)


def _submit_application_by_user_id(user_id: int, full_name: str, department: str) -> tuple[bool, str]:
    user = get_user_by_user_id(user_id)
    if user["application_status"] == "pending":
        return False, "Вы уже подали заявку. Ожидайте подтверждения администратором."
    if user["application_status"] == "approved" or user["is_authorized"]:
        return False, "Ваша заявка уже одобрена, и вы авторизованы."

    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users SET application_status = 'pending',
                application_full_name = ?, application_department = ?
            WHERE user_id = ?
        """, (full_name, department, user_id))
        conn.commit()
        if cursor.rowcount > 0:
            logger.info(f"user_id={user_id} подал заявку: {full_name}, {department}.")
            return True, "✅ Ваша заявка на доступ успешно подана! Администратор рассмотрит ее в ближайшее время."
        return False, "Не удалось подать заявку. Попробуйте ещё раз."
    except sqlite3.Error as e:
        logger.error(f"Ошибка при подаче заявки user_id={user_id}: {e}", exc_info=True)
        return False, "Ошибка базы данных при подаче заявки."
    finally:
        conn.close()


def list_pending_users() -> list:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id, telegram_id, vk_id, link_code, username, first_name, last_name,
                   registration_date, application_full_name, application_department
            FROM users
            WHERE application_status = 'pending' AND is_authorized = FALSE
            ORDER BY registration_date ASC
        """)
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении ожидающих пользователей: {e}", exc_info=True)
        return []
    finally:
        conn.close()


def reject_application(telegram_id_to_reject: int, rejecting_admin_id: int, reason: str = None) -> tuple[bool, str]:
    user = get_user_by_telegram_id(telegram_id_to_reject)
    if not user:
        return False, f"Пользователь с ID {telegram_id_to_reject} не найден."
    if user["is_authorized"]:
        return False, f"Пользователь уже авторизован."
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            UPDATE users SET application_status = 'rejected', is_authorized = FALSE,
                application_full_name = NULL, application_department = NULL
            WHERE user_id = ? AND application_status = 'pending'
        """, (user["user_id"],))
        conn.commit()
        if cursor.rowcount > 0:
            return True, f"Заявка пользователя ID {telegram_id_to_reject} успешно отклонена."
        return False, "Не удалось отклонить заявку."
    except sqlite3.Error as e:
        logger.error(f"Ошибка при отклонении заявки: {e}", exc_info=True)
        return False, "Ошибка базы данных."
    finally:
        conn.close()


# ------------------------------------------------------------------ #
# Check-in / Check-out                                                #
# ------------------------------------------------------------------ #

def _get_internal_user_id(telegram_id: int = None, vk_id: int = None) -> int | None:
    """Возвращает внутренний user_id по telegram_id или vk_id."""
    if telegram_id:
        user = get_user_by_telegram_id(telegram_id)
    elif vk_id:
        user = get_user_by_vk_id(vk_id)
    else:
        return None
    return user["user_id"] if user else None


def record_check_in(
    user_id: int,
    latitude: float,
    longitude: float,
    vk_id: int = None,
) -> tuple[bool, str]:
    """
    Записывает check-in. user_id может быть telegram_id или vk_id.
    Если передан vk_id=True — ищем по VK.
    """
    internal_id = _get_internal_user_id(
        telegram_id=None if vk_id else user_id,
        vk_id=vk_id or (user_id if vk_id is not None else None),
    ) if vk_id else _get_internal_user_id(telegram_id=user_id)

    if not internal_id:
        return False, "❌ Пользователь не найден в базе данных."

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT session_id FROM work_sessions WHERE user_id = ? AND check_out_time IS NULL",
            (internal_id,)
        )
        if cursor.fetchone():
            return False, "❌ Вы уже отметились на приходе. Сначала нужно отметить уход."

        cursor.execute(
            "SELECT application_department FROM users WHERE user_id = ?", (internal_id,)
        )
        row = cursor.fetchone()
        department = row["application_department"] if row else None

        now_msk = datetime.now(MOSCOW_TZ)
        checkin_str = now_msk.strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT INTO work_sessions (user_id, check_in_time, checkin_type, latitude, longitude, sector_id)
            VALUES (?, ?, 'geo', ?, ?, ?)
        """, (internal_id, checkin_str, latitude, longitude, department))
        conn.commit()

        time_str = now_msk.strftime("%H:%M:%S")
        logger.info(f"Check-in: user_id={internal_id}, время={checkin_str}, сектор={department}.")
        return True, f"✅ Вы успешно отметили приход в {time_str}!"

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка check-in для user_id={internal_id}: {e}", exc_info=True)
        return False, "❌ Ошибка базы данных при отметке прихода."
    finally:
        if conn:
            conn.close()


def record_check_in_vk(vk_id: int, latitude: float, longitude: float) -> tuple[bool, str]:
    """Check-in через VK."""
    internal_id = _get_internal_user_id(vk_id=vk_id)
    if not internal_id:
        return False, "❌ Пользователь не найден в базе данных."
    return _record_check_in_internal(internal_id, latitude, longitude)


def _record_check_in_internal(internal_id: int, latitude: float, longitude: float) -> tuple[bool, str]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute(
            "SELECT session_id FROM work_sessions WHERE user_id = ? AND check_out_time IS NULL",
            (internal_id,)
        )
        if cursor.fetchone():
            return False, "❌ Вы уже отметились на приходе. Сначала нужно отметить уход."

        cursor.execute("SELECT application_department FROM users WHERE user_id = ?", (internal_id,))
        row = cursor.fetchone()
        department = row["application_department"] if row else None

        now_msk = datetime.now(MOSCOW_TZ)
        checkin_str = now_msk.strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            INSERT INTO work_sessions (user_id, check_in_time, checkin_type, latitude, longitude, sector_id)
            VALUES (?, ?, 'geo', ?, ?, ?)
        """, (internal_id, checkin_str, latitude, longitude, department))
        conn.commit()

        time_str = now_msk.strftime("%H:%M:%S")
        logger.info(f"Check-in: user_id={internal_id}, время={checkin_str}.")
        return True, f"✅ Вы успешно отметили приход в {time_str}!"

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка check-in user_id={internal_id}: {e}", exc_info=True)
        return False, "❌ Ошибка базы данных при отметке прихода."
    finally:
        if conn:
            conn.close()


def record_check_out(user_id: int) -> tuple[bool, str]:
    """Check-out по telegram_id (обратная совместимость)."""
    internal_id = _get_internal_user_id(telegram_id=user_id)
    if not internal_id:
        return False, "❌ Пользователь не найден."
    return _record_check_out_internal(internal_id)


def record_check_out_vk(vk_id: int) -> tuple[bool, str]:
    """Check-out по vk_id."""
    internal_id = _get_internal_user_id(vk_id=vk_id)
    if not internal_id:
        return False, "❌ Пользователь не найден."
    return _record_check_out_internal(internal_id)


def _record_check_out_internal(internal_id: int) -> tuple[bool, str]:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT session_id, check_in_time FROM work_sessions
            WHERE user_id = ? AND check_out_time IS NULL
            ORDER BY check_in_time DESC LIMIT 1
        """, (internal_id,))
        row = cursor.fetchone()
        if not row:
            return False, "❌ Вы не были отмечены на приходе. Невозможно отметить уход."

        session_id = row["session_id"]
        check_in_naive = datetime.strptime(row["check_in_time"], "%Y-%m-%d %H:%M:%S")
        check_in_msk = MOSCOW_TZ.localize(check_in_naive)

        now_msk = datetime.now(MOSCOW_TZ)
        duration = now_msk - check_in_msk
        total_sec = int(duration.total_seconds())
        hours, rem = divmod(total_sec, 3600)
        minutes, _ = divmod(rem, 60)

        checkout_str = now_msk.strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute(
            "UPDATE work_sessions SET check_out_time = ? WHERE session_id = ?",
            (checkout_str, session_id)
        )
        conn.commit()

        logger.info(f"Check-out: user_id={internal_id}, сессия={session_id}, длительность={hours}ч {minutes}мин.")
        return True, (
            f"✅ Вы успешно отметили уход в {now_msk.strftime('%H:%M:%S')}.\n\n"
            f"⏱️ Продолжительность сессии: {hours} ч {minutes:02d} мин\n\n"
            f"Хорошего вечера!"
        )

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка check-out user_id={internal_id}: {e}", exc_info=True)
        return False, "❌ Ошибка базы данных при отметке ухода."
    finally:
        if conn:
            conn.close()


# ------------------------------------------------------------------ #
# Ручные отметки                                                      #
# ------------------------------------------------------------------ #

def add_manual_checkin_request(user_id: int, requested_checkin_time: datetime, vk_id: int = None) -> bool:
    internal_id = _get_internal_user_id(
        telegram_id=None if vk_id else user_id,
        vk_id=vk_id,
    ) if vk_id else _get_internal_user_id(telegram_id=user_id)
    if not internal_id:
        return False

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        time_str = requested_checkin_time.strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            INSERT INTO manual_checkin_requests (user_id, requested_checkin_time, request_timestamp, status)
            VALUES (?, ?, datetime('now', 'localtime'), 'pending')
        """, (internal_id, time_str))
        conn.commit()
        logger.info(f"Заявка на ручную отметку: user_id={internal_id}, время={time_str}.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Ошибка при добавлении ручной заявки: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()


def get_pending_manual_checkin_requests() -> list:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT req.request_id, req.user_id, req.requested_checkin_time, req.request_timestamp,
                   u.application_full_name, u.username, u.telegram_id, u.vk_id
            FROM manual_checkin_requests req
            LEFT JOIN users u ON req.user_id = u.user_id
            WHERE req.status = 'pending'
            ORDER BY req.request_timestamp ASC
        """)
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении ручных заявок: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


def get_manual_checkin_request_by_id(request_id: int) -> dict | None:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT mcr.request_id, mcr.user_id, mcr.requested_checkin_time, mcr.status,
                   u.application_full_name, u.username, u.application_department,
                   u.telegram_id, u.vk_id
            FROM manual_checkin_requests mcr
            LEFT JOIN users u ON mcr.user_id = u.user_id
            WHERE mcr.request_id = ?
        """, (request_id,))
        row = cursor.fetchone()
        return dict(row) if row else None
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении заявки {request_id}: {e}", exc_info=True)
        return None
    finally:
        if conn:
            conn.close()


def approve_manual_checkin_request(
    request_id: int,
    admin_id: int,
    final_checkin_time_local: datetime,
    user_id: int,
    user_sector_key: str,
) -> bool:
    admin_internal = _get_internal_user_id(telegram_id=admin_id)
    admin_internal = admin_internal or admin_id

    internal_user = _get_internal_user_id(telegram_id=user_id) or user_id
    logger.info(f"approve: request_id={request_id}, admin_id={admin_id}→{admin_internal}, user_id={user_id}→{internal_user}")

    conn_check = get_db_connection()
    exists = conn_check.execute("SELECT 1 FROM users WHERE user_id=?", (internal_user,)).fetchone()
    conn_check.close()
    logger.info(f"approve: exists={exists}")
    if not exists:
        return False

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        checkin_str = final_checkin_time_local.strftime("%Y-%m-%d %H:%M:%S")
        processed_str = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")

        cursor.execute("""
            UPDATE manual_checkin_requests
            SET status = 'approved', admin_user_id = ?, processed_timestamp = ?, final_checkin_time = ?
            WHERE request_id = ? AND status = 'pending'
        """, (admin_internal, processed_str, checkin_str, request_id))

        if cursor.rowcount == 0:
            conn.rollback()
            logger.warning(f"approve: rowcount=0 для request_id={request_id}, возможно уже обработана")
            return False

        cursor.execute("""
            INSERT INTO work_sessions (user_id, check_in_time, checkin_type, sector_id)
            VALUES (?, ?, 'manual_admin', ?)
        """, (internal_user, checkin_str, user_sector_key))

        conn.commit()
        logger.info(f"Заявка {request_id} одобрена. Сессия создана для user_id={internal_user}.")
        return True

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Ошибка при одобрении заявки {request_id}: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()


def approve_all_pending_manual_checkins(admin_id: int) -> tuple[list, int]:
    """admin_id — telegram_id администратора (обратная совместимость)."""
    admin_internal = _get_internal_user_id(telegram_id=admin_id) or admin_id

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM manual_checkin_requests WHERE status = 'pending'")
        pending = [dict(row) for row in cursor.fetchall()]

        if not pending:
            return [], 0

        approved = []
        failed = 0
        processed_str = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")

        for req in pending:
            try:
                cursor.execute("""
                    UPDATE manual_checkin_requests
                    SET status = 'approved', admin_user_id = ?, processed_timestamp = ?, final_checkin_time = ?
                    WHERE request_id = ?
                """, (admin_internal, processed_str, req["requested_checkin_time"], req["request_id"]))

                cursor.execute("SELECT application_department FROM users WHERE user_id = ?", (req["user_id"],))
                u = cursor.fetchone()
                sector = u["application_department"] if u else "unknown"

                cursor.execute("""
                    INSERT INTO work_sessions (user_id, check_in_time, checkin_type, sector_id)
                    VALUES (?, ?, 'manual_admin', ?)
                """, (req["user_id"], req["requested_checkin_time"], sector))

                approved.append({"user_id": req["user_id"], "checkin_time_str": req["requested_checkin_time"]})
            except sqlite3.Error as e:
                logger.error(f"Ошибка при массовом одобрении заявки {req.get('request_id')}: {e}")
                failed += 1

        conn.commit()
        return approved, failed

    except sqlite3.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"Критическая ошибка при массовом одобрении: {e}", exc_info=True)
        return [], len(pending) if "pending" in locals() else 0
    finally:
        if conn:
            conn.close()


def reject_manual_checkin_request(request_id: int, admin_id: int) -> bool:
    admin_internal = _get_internal_user_id(telegram_id=admin_id) or admin_id
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        processed_str = datetime.now(MOSCOW_TZ).strftime("%Y-%m-%d %H:%M:%S")
        cursor.execute("""
            UPDATE manual_checkin_requests
            SET status = 'rejected', admin_user_id = ?, processed_timestamp = ?
            WHERE request_id = ? AND status = 'pending'
        """, (admin_internal, processed_str, request_id))
        conn.commit()
        return cursor.rowcount > 0
    except sqlite3.Error as e:
        logger.error(f"Ошибка при отклонении заявки {request_id}: {e}", exc_info=True)
        return False
    finally:
        if conn:
            conn.close()


# ------------------------------------------------------------------ #
# Отчёты и статистика                                                 #
# ------------------------------------------------------------------ #

def get_attendance_data_for_period(start_date: datetime, end_date: datetime, sector_key: str = None) -> list:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        query = """
            SELECT
                u.application_full_name,
                u.username,
                u.application_department,
                ws.check_in_time  AS session_start_time,
                ws.check_out_time AS session_end_time
            FROM work_sessions ws
            JOIN users u ON ws.user_id = u.user_id
            WHERE ws.check_in_time BETWEEN ? AND ?
        """
        params = [start_date, end_date]
        if sector_key and sector_key.upper() != "ALL":
            query += " AND UPPER(u.application_department) = ? "
            params.append(sector_key.upper())
        query += " ORDER BY u.application_full_name ASC, ws.check_in_time ASC"

        cursor.execute(query, tuple(params))
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении данных посещаемости: {e}", exc_info=True)
        return []
    finally:
        conn.close()


def get_unique_departments() -> list:
    conn = get_db_connection()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT application_department FROM users
            WHERE application_department IS NOT NULL AND application_department != ''
            ORDER BY application_department ASC
        """)
        return [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении секторов: {e}", exc_info=True)
        return []
    finally:
        conn.close()


def get_unique_user_departments() -> list:
    return get_unique_departments()


def get_active_users_by_department(department: str) -> list:
    conn = None
    try:
        now_msk = datetime.now(MOSCOW_TZ)
        cutoff = now_msk.replace(hour=5, minute=0, second=0, microsecond=0)
        if now_msk < cutoff:
            cutoff -= timedelta(days=1)
        cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT u.application_full_name, u.username, u.application_department,
                   t.max_check_in_time AS check_in_time
            FROM (
                SELECT user_id, MAX(check_in_time) AS max_check_in_time
                FROM work_sessions
                WHERE check_out_time IS NULL AND check_in_time >= ?
                GROUP BY user_id
            ) AS t
            JOIN users u ON t.user_id = u.user_id
            WHERE (? = 'ALL' OR u.application_department = ?)
            ORDER BY u.application_department, u.application_full_name ASC
        """, (cutoff_str, department, department))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении активных пользователей: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


def find_users_by_name(name_part: str) -> list:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id, telegram_id, vk_id, application_full_name
            FROM users
            WHERE LOWER(application_full_name) LIKE LOWER(?) AND is_authorized = 1
            ORDER BY application_full_name
        """, (f"%{name_part}%",))
        return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logger.error(f"Ошибка при поиске по имени '{name_part}': {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


def get_completed_sessions_for_user(user_id: int, period: str) -> list:
    """user_id здесь — telegram_id (обратная совместимость)."""
    internal_id = _get_internal_user_id(telegram_id=user_id)
    if not internal_id:
        return []

    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT session_id, check_in_time, check_out_time
            FROM work_sessions
            WHERE user_id = ? AND check_out_time IS NOT NULL
            ORDER BY check_in_time DESC
        """, (internal_id,))
        all_sessions = [dict(row) for row in cursor.fetchall()]

        if period == "last5":
            return all_sessions[:5]

        days = 7 if period == "week" else 30
        cutoff = datetime.now(MOSCOW_TZ) - timedelta(days=days)

        result = []
        for s in all_sessions:
            try:
                dt = MOSCOW_TZ.localize(datetime.strptime(s["check_in_time"], "%Y-%m-%d %H:%M:%S"))
                if dt >= cutoff:
                    result.append(s)
            except (ValueError, TypeError):
                continue
        return result
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении сессий: {e}", exc_info=True)
        return []
    finally:
        if conn:
            conn.close()


def update_session_checkout_time(session_id: int, new_checkout_time_str: str) -> bool:
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT check_in_time FROM work_sessions WHERE session_id = ?", (session_id,))
        row = cursor.fetchone()
        if not row:
            return False

        check_in = datetime.strptime(row["check_in_time"], "%Y-%m-%d %H:%M:%S")
        check_out = datetime.strptime(new_checkout_time_str, "%Y-%m-%d %H:%M:%S")
        duration_min = int((check_out - check_in).total_seconds() / 60)

        cursor.execute("""
            UPDATE work_sessions SET check_out_time = ?, duration_minutes = ?
            WHERE session_id = ?
        """, (new_checkout_time_str, duration_min, session_id))
        conn.commit()
        return cursor.rowcount > 0
    except (sqlite3.Error, ValueError, TypeError) as e:
        logger.error(f"Ошибка при обновлении сессии {session_id}: {e}", exc_info=True)
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            conn.close()