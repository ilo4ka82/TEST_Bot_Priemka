import logging
from config import THE_OFFICE_ZONE
import database_operations as db

logger = logging.getLogger(__name__)


def is_within_office_zone(latitude: float, longitude: float) -> bool:
    """
    Проверяет, находятся ли координаты внутри THE_OFFICE_ZONE.
    Возвращает False если зона не настроена.
    """
    required_keys = ("min_latitude", "max_latitude", "min_longitude", "max_longitude")
    if not THE_OFFICE_ZONE or not all(k in THE_OFFICE_ZONE for k in required_keys):
        logger.error("Конфигурация THE_OFFICE_ZONE не определена или неполная.")
        return False

    in_zone = (
        THE_OFFICE_ZONE["min_latitude"] <= latitude <= THE_OFFICE_ZONE["max_latitude"]
        and THE_OFFICE_ZONE["min_longitude"] <= longitude <= THE_OFFICE_ZONE["max_longitude"]
    )

    if in_zone:
        logger.info(f"Координаты ({latitude}, {longitude}) — ВНУТРИ разрешённой зоны.")
    else:
        logger.info(f"Координаты ({latitude}, {longitude}) — ВНЕ разрешённой зоны.")

    return in_zone


def checkin(user_id: int, latitude: float, longitude: float) -> tuple[bool, str]:
    """
    Выполняет check-in пользователя.

    Возвращает (success, message):
      - (False, ...) если пользователь вне геозоны
      - результат db.record_check_in если в зоне
    """
    if not is_within_office_zone(latitude, longitude):
        logger.info(f"Пользователь {user_id} вне геозоны — check-in отклонён.")
        return False, (
            "❌ Вы находитесь слишком далеко от офиса для отметки прихода. "
            "Пожалуйста, подойдите ближе и попробуйте снова."
        )

    logger.info(f"Пользователь {user_id} в геозоне — выполняем check-in.")
    return db.record_check_in(user_id, latitude, longitude)


def checkout(user_id: int) -> tuple[bool, str]:
    """
    Выполняет check-out пользователя.
    Возвращает (success, message) из db.record_check_out.
    """
    return db.record_check_out(user_id)