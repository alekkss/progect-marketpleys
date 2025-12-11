"""
Telegram бот для синхронизации маркетплейсов
Главный файл инициализации
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config.config import TELEGRAM_BOT_TOKEN
from utils.logger_config import setup_logger

# Импорт регистраторов обработчиков
from bot.handlers.common import register_common_handlers
from bot.handlers.upload import register_upload_handlers
from bot.handlers.schema_create import register_schema_create_handlers
from bot.handlers.schema_edit import register_schema_edit_handlers
from bot.handlers.schema_update import register_schema_update_handlers
from bot.handlers.schema_delete import register_schema_delete_handlers
from bot.handlers.stats import register_stats_handlers

logger = setup_logger('bot')
logging.basicConfig(level=logging.INFO)


def create_bot():
    """
    Создание и настройка бота
    
    Returns:
        tuple: (bot, dispatcher)
    """
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    # Регистрация всех обработчиков
    register_common_handlers(dp)
    register_upload_handlers(dp, bot)
    register_schema_create_handlers(dp, bot)
    register_schema_edit_handlers(dp, bot)
    register_schema_update_handlers(dp, bot)
    register_schema_delete_handlers(dp)
    register_stats_handlers(dp)
    
    return bot, dp


async def start_bot():
    """Запуск бота"""
    bot, dp = create_bot()
    print("🚀 Telegram бот запущен!")
    await dp.start_polling(bot)
