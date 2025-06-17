# config.py
TELEGRAM_BOT_TOKEN = "8077716714:AAFjWQjHKN5_gtXr12xwWu2JW1qIdBPksk8"
ADMIN_TELEGRAM_IDS = [1125354576, 511913237]
DATABASE_NAME = "time_tracker.db"
ITEMS_PER_PAGE = 7
THE_OFFICE_ZONE = {
    "min_latitude": 55.753988,  # Южная 
    "max_latitude": 55.756340,  # Северная 
    "min_longitude": 37.710915, # Западная  
    "max_longitude": 37.716277  # Восточная 
}
SECTOR_WEEKLY_NORMS = {
    "СС": 40,  
    "ВИ": 40,  
    "ОП": 40,
    # Важно, чтобы ключи (названия секторов) точно совпадали с тем, 
    # как они хранятся в поле application_department таблицы users.
    "DEFAULT_NORM": 40 
}

