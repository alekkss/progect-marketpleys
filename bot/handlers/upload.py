"""
Обработчики загрузки и обработки файлов
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
import asyncio
import os
import logging
from datetime import datetime
from aiogram import types, F
from aiogram.types import FSInputFile, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext

from bot.states import UploadStates
from bot.keyboards import (
    get_main_menu_keyboard,
    get_cancel_keyboard,
    get_process_keyboard,
    get_schema_list_keyboard
)
from bot.storage import user_files, db
from bot.utils import download_file
from bot.handlers.common import cmd_start

from config.config import FILE_CONFIGS
from utils.excel_writer import ExcelWriter
from services.synchronizer import DataSynchronizer
from services.ai_comparator import AIComparator
from utils.logger_config import setup_logger

logger = setup_logger('upload')


async def select_schema_for_upload(message: types.Message, state: FSMContext):
    """Выбор схемы для загрузки файлов"""
    user_id = message.from_user.id
    schemas = db.get_user_schemas(user_id)
    
    if not schemas:
        await message.answer(
            "❌ У тебя нет схем!\n\n"
            "Сначала создай схему через 📋 Управление схемами"
        )
        return
    
    keyboard = get_schema_list_keyboard(schemas)
    
    if not keyboard:
        await message.answer(
            "❌ У тебя нет валидных схем!\n\n"
            "Создай новую схему через 📋 Управление схемами"
        )
        return
    
    await state.set_state(UploadStates.selecting_schema)
    await message.answer("Выбери схему для синхронизации:", reply_markup=keyboard)


async def schema_selected(message: types.Message, state: FSMContext, bot):
    """Схема выбрана, начинаем загрузку файлов"""
    if message.text == "❌ Отмена":
        await cmd_start(message, state)
        return
    
    user_id = message.from_user.id
    schema = db.get_schema(user_id, message.text)
    
    if not schema:
        await message.answer("❌ Схема не найдена. Выбери из списка.")
        return
    
    # Сохраняем выбранную схему
    await state.update_data(selected_schema_id=schema['id'])
    user_files[user_id] = {}
    
    await state.set_state(UploadStates.waiting_for_files)
    await message.answer(
        f"✅ Схема '{message.text}' выбрана\n\n"
        "Отправь 3 файла Excel",
        reply_markup=ReplyKeyboardRemove()
    )


async def handle_file(message: types.Message, state: FSMContext, bot):
    """Обработка загруженного файла"""
    user_id = message.from_user.id
    
    if user_id not in user_files:
        user_files[user_id] = {}
    
    # НОВОЕ: Проверяем, не обработали ли мы уже все файлы
    data = await state.get_data()
    if data.get('files_processed'):
        return  # Уже обработали, игнорируем дубликаты
    
    file_path, file_name, marketplace = await download_file(bot, message, user_id)
    
    if not marketplace:
        await message.answer("❌ Переименуй файл (добавь wb/ozon/yandex)")
        return
    
    if marketplace in user_files[user_id]:
        await message.answer(f"⚠️ {marketplace.upper()} уже загружен")
        return
    
    user_files[user_id][marketplace] = file_path
    await message.answer(f"✅ {marketplace.upper()} ({len(user_files[user_id])}/3)")
    
    if len(user_files[user_id]) == 3:
        # КРИТИЧНО: Двойная проверка флага
        data = await state.get_data()
        if data.get('files_processed'):
            return
        
        await state.update_data(files_processed=True)
        
        await message.answer(
            "✅ Все файлы загружены!",
            reply_markup=get_process_keyboard()
        )


async def process_files(message: types.Message, state: FSMContext, bot):
    """Обрабатывает файлы (запускает в фоновом режиме)"""
    user_id = message.from_user.id
    
    if user_id not in user_files or len(user_files[user_id]) != 3:
        await message.answer("⚠️ Загрузите все 3 файла!")
        return
    
    # Получаем данные из состояния
    data = await state.get_data()
    schema_id = data.get('selected_schema_id')
    
    if not schema_id:
        await message.answer("❌ Схема не выбрана!")
        return
    
    # Создаем ID обработки
    processing_id = db.start_processing(user_id)
    
    # Отправляем начальное сообщение с прогрессом
    progress_msg = await message.answer(
        "⏳ <b>Начинаю обработку...</b>\n\n"
        "▱▱▱▱▱▱▱▱▱▱ 0%",
        parse_mode="HTML"
    )
    
    # Подготавливаем пути
    file_paths = user_files[user_id]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"output/{user_id}_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    output_sync_paths = {
        'wildberries': f"{output_dir}/WB.xlsx",
        'ozon': f"{output_dir}/Ozon.xlsx",
        'yandex': f"{output_dir}/Яндекс.xlsx"
    }
    report_path = f"{output_dir}/Отчет_{timestamp}.xlsx"
    
    # Регистрируем файлы в БД
    for marketplace, filepath in file_paths.items():
        db.add_file(user_id, processing_id, marketplace, 
                   os.path.basename(filepath), filepath)
    
    # 🆕 Запускаем обработку в фоне
    from services.processor import BackgroundProcessor
    processor = BackgroundProcessor(bot, db)
    
    # Создаем задачу
    task = asyncio.create_task(
        processor.process_files(
            user_id=user_id,
            chat_id=message.chat.id,
            processing_id=processing_id,
            schema_id=schema_id,
            file_paths=file_paths,
            output_paths=output_sync_paths,
            report_path=report_path,
            progress_message_id=progress_msg.message_id
        )
    )
    
    # Сохраняем задачу
    processor.active_tasks[processing_id] = task
    
    # Очищаем состояние
    user_files[user_id] = {}
    await state.clear()


async def cancel_processing_callback(callback: types.CallbackQuery, bot):
    """Обрабатывает отмену обработки"""
    # Получаем message_id из callback_data
    message_id = int(callback.data.split('_')[1])
    
    # Находим processing_id по message_id (нужно сохранять маппинг)
    # Временное решение - ищем последнюю активную обработку пользователя
    user_id = callback.from_user.id
    
    conn = db.get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id FROM processing_history
        WHERE user_id = ? AND status != 'completed' AND status != 'failed'
        ORDER BY started_at DESC LIMIT 1
    """, (user_id,))
    result = cursor.fetchone()
    conn.close()
    
    if result:
        processing_id = result[0]
        
        # Отменяем обработку
        from services.processor import BackgroundProcessor
        processor = BackgroundProcessor(bot, db)
        processor.cancel_processing(processing_id)
        
        await callback.answer("⏹ Отмена обработки...")
    else:
        await callback.answer("❌ Обработка не найдена", show_alert=True)

def register_upload_handlers(dp, bot):
    from functools import partial
    
    dp.message.register(select_schema_for_upload, F.text == "📤 Загрузить файлы")
    dp.message.register(partial(schema_selected, bot=bot), UploadStates.selecting_schema)
    dp.message.register(partial(handle_file, bot=bot), UploadStates.waiting_for_files, F.document)
    dp.message.register(partial(process_files, bot=bot), F.text == "✅ Обработать файлы")
    
    # 🆕 Добавь обработчик отмены
    dp.callback_query.register(partial(cancel_processing_callback, bot=bot), F.data.startswith("cancel_"))

