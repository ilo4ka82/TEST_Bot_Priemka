import logging
import database_operations as db

logger = logging.getLogger(__name__)


def register_or_update(
    user_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
) -> None:
    """
    Добавляет пользователя в БД или обновляет его данные.
    Вызывается при любом первом контакте с ботом.
    """
    db.add_or_update_user(user_id, username, first_name, last_name)
    logger.info(f"Пользователь {user_id} (@{username}) зарегистрирован/обновлён.")


def ensure_admin_exists(
    user_id: int,
    username: str = None,
    first_name: str = None,
    last_name: str = None,
) -> None:
    """
    Если администратор ещё не в БД — добавляет и сразу авторизует.
    Используется при первом обращении администратора к боту.
    """
    user_data = db.get_user(user_id)
    if not user_data or not user_data.get("is_authorized"):
        db.add_or_update_user(
            user_id,
            username or "N/A",
            first_name or "N/A",
            last_name or "N/A",
        )
        db.authorize_user(user_id, user_id)
        logger.info(f"Администратор {user_id} автоматически добавлен и авторизован.")


def submit_application(
    user_id: int,
    full_name: str,
    department: str,
) -> tuple[bool, str]:
    """
    Подаёт заявку на доступ.
    Возвращает (success, message) из db.submit_application.
    """
    return db.submit_application(user_id, full_name, department)


def authorize_user(
    user_id: int,
    admin_id: int,
) -> tuple[bool, str]:
    """
    Авторизует пользователя от имени администратора.
    Возвращает (success, message) из db.authorize_user.
    """
    return db.authorize_user(user_id, admin_id)


def get_user(user_id: int) -> dict | None:
    """Возвращает данные пользователя из БД или None."""
    return db.get_user(user_id)


def is_authorized(user_id: int) -> bool:
    """Проверяет, авторизован ли пользователь."""
    return db.is_user_authorized(user_id)


def list_pending() -> list:
    """Возвращает список пользователей, ожидающих авторизации."""
    return db.list_pending_users()