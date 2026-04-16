"""
StateManager — замена ConversationHandler из python-telegram-bot для VK-бота.

В Telegram PTB сам хранит:
  - текущее состояние диалога (например, ASK_FULL_NAME)
  - временные данные диалога (context.user_data)

Здесь мы храним то же самое в памяти, привязывая всё к user_id.
Ключи хранятся с префиксом платформы (tg_ / vk_), чтобы оба бота
могли работать с одним экземпляром без пересечений.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Константа — диалог завершён (аналог ConversationHandler.END)
END = -1


class StateManager:
    """
    Хранит состояния и временные данные диалогов для всех пользователей.

    Использование в VK-боте:
        sm = StateManager()

        # Установить состояние
        sm.set_state("vk", user_id, ASK_FULL_NAME)

        # Получить текущее состояние
        state = sm.get_state("vk", user_id)

        # Сохранить временные данные диалога
        sm.set_data("vk", user_id, "application_full_name", "Иванов Иван")

        # Прочитать данные
        name = sm.get_data("vk", user_id, "application_full_name")

        # Завершить диалог и очистить данные
        sm.clear("vk", user_id)
    """

    def __init__(self):
        # { "vk_12345": {"state": ASK_FULL_NAME, "data": {...}} }
        self._sessions: dict[str, dict] = {}

    def _key(self, platform: str, user_id: int) -> str:
        return f"{platform}_{user_id}"

    # ------------------------------------------------------------------ #
    # Состояния                                                            #
    # ------------------------------------------------------------------ #

    def set_state(self, platform: str, user_id: int, state: int) -> None:
        key = self._key(platform, user_id)
        if key not in self._sessions:
            self._sessions[key] = {"state": None, "data": {}}
        self._sessions[key]["state"] = state
        logger.debug(f"[{key}] state → {state}")

    def get_state(self, platform: str, user_id: int) -> int | None:
        return self._sessions.get(self._key(platform, user_id), {}).get("state")

    def has_active_dialog(self, platform: str, user_id: int) -> bool:
        """Возвращает True если у пользователя есть незавершённый диалог."""
        state = self.get_state(platform, user_id)
        return state is not None and state != END

    # ------------------------------------------------------------------ #
    # Временные данные диалога (аналог context.user_data в PTB)           #
    # ------------------------------------------------------------------ #

    def set_data(self, platform: str, user_id: int, key: str, value: Any) -> None:
        session_key = self._key(platform, user_id)
        if session_key not in self._sessions:
            self._sessions[session_key] = {"state": None, "data": {}}
        self._sessions[session_key]["data"][key] = value
        logger.debug(f"[{session_key}] data[{key}] = {value!r}")

    def get_data(self, platform: str, user_id: int, key: str, default: Any = None) -> Any:
        return self._sessions.get(self._key(platform, user_id), {}).get("data", {}).get(key, default)

    def get_all_data(self, platform: str, user_id: int) -> dict:
        """Возвращает все временные данные диалога."""
        return self._sessions.get(self._key(platform, user_id), {}).get("data", {})

    # ------------------------------------------------------------------ #
    # Управление сессией                                                   #
    # ------------------------------------------------------------------ #

    def clear(self, platform: str, user_id: int) -> None:
        """Завершает диалог и удаляет все временные данные."""
        key = self._key(platform, user_id)
        if key in self._sessions:
            del self._sessions[key]
        logger.debug(f"[{key}] сессия очищена")

    def active_count(self) -> int:
        """Количество активных диалогов (для мониторинга)."""
        return len(self._sessions)