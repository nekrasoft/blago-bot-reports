# Точка входа для запуска бота
# Использование: python run.py
# Платформа задаётся через BOT_PLATFORM в .env: telegram (по умолчанию) или max

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

if __name__ == "__main__":
    platform = os.environ.get("BOT_PLATFORM", "telegram").lower()
    if platform == "max":
        from max_bot import main
    else:
        from bot import main
    main()
