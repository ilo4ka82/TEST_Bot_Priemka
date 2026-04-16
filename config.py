import os
from dotenv import load_dotenv

load_dotenv()

# --- TELEGRAM ---
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
if not TELEGRAM_BOT_TOKEN:
    raise ValueError("Критическая ошибка: TELEGRAM_BOT_TOKEN не найден в .env файле!")

admin_ids_str = os.getenv("ADMIN_TELEGRAM_IDS", "")
cleaned_ids_str = admin_ids_str.strip().strip('[]')
ADMIN_TELEGRAM_IDS = [int(admin_id.strip()) for admin_id in cleaned_ids_str.split(',') if admin_id.strip()]

DATABASE_PATH = os.getenv("DATABASE_PATH")
if not DATABASE_PATH:
    raise ValueError("Критическая ошибка: DATABASE_PATH не найден в .env файле!")


ITEMS_PER_PAGE = 7
THE_OFFICE_ZONE = {
    "min_latitude": 55.753988,
    "max_latitude": 55.756340,
    "min_longitude": 37.710915,
    "max_longitude": 37.716277
}
SECTOR_WEEKLY_NORMS = {
    "СС": 40,
    "ВИ": 40,
    "ОП": 40,
    "DEFAULT_NORM": 40
}
SECTOR_SCHEDULES = {
    'ОП': {'start_hour': 9, 'end_hour': 18},
    'ВИ': {'start_hour': 9, 'end_hour': 18},
    'СС': {'start_hour': 10, 'end_hour': 19},
}

# --- VK ---
VK_GROUP_TOKEN = os.getenv("VK_GROUP_TOKEN", "")
vk_admin_ids_str = os.getenv("VK_ADMIN_IDS", "")
VK_ADMIN_IDS = [int(i.strip()) for i in vk_admin_ids_str.split(",") if i.strip()]

PREDEFINED_SECTORS = ["Сектор СС", "Сектор ВИ", "Сектор ОП"]
