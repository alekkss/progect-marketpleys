"""
Обработчики обновления схем
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import logging
from aiogram import types, F
from aiogram.types import ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext

from bot.states import SchemaStates
from bot.keyboards import (
    get_main_menu_keyboard,
    get_cancel_keyboard,
    get_update_schema_keyboard,
    get_schema_list_keyboard
)
from bot.storage import user_schemas, db
from bot.utils import download_file
from bot.handlers.common import schema_management

from config.config import FILE_CONFIGS
from utils.excel_reader import ExcelReader
from services.ai_comparator import AIComparator


async def update_schema_start(message: types.Message, state: FSMContext):
    """Начало обновления схемы"""
    user_id = message.from_user.id
    schemas = db.get_user_schemas(user_id)
    
    if not schemas:
        await message.answer("❌ У тебя нет схем!")
        return
    
    keyboard = get_schema_list_keyboard(schemas)
    
    if not keyboard:
        await message.answer("❌ У тебя нет валидных схем!")
        return
    
    await state.set_state(SchemaStates.selecting_schema_to_update)
    await message.answer("Выбери схему для обновления:", reply_markup=keyboard)


async def schema_selected_for_update(message: types.Message, state: FSMContext):
    """Схема выбрана для обновления"""
    if message.text == "❌ Отмена":
        await schema_management(message, state)
        return
    
    user_id = message.from_user.id
    schema = db.get_schema(user_id, message.text)
    
    if not schema:
        await message.answer("❌ Схема не найдена")
        return
    
    # Сохраняем id и название схемы
    await state.update_data(
        update_schema_id=schema['id'],
        update_schema_name=schema['name']
    )
    
    user_schemas[user_id] = {}
    await state.set_state(SchemaStates.waiting_update_files)
    
    await message.answer(
        f"✅ Схема '{schema['name']}' выбрана\n\n"
        "Отправь 3 файла Excel для повторного анализа",
        reply_markup=ReplyKeyboardRemove()
    )


async def handle_update_file(message: types.Message, state: FSMContext, bot):
    """Обработка файла при обновлении схемы"""
    user_id = message.from_user.id
    
    if user_id not in user_schemas:
        user_schemas[user_id] = {}
    
    # НОВОЕ: Проверяем, не обработали ли мы уже все файлы
    data = await state.get_data()
    if data.get('files_processed'):
        return  # Уже обработали, игнорируем дубликаты
    
    file_path, file_name, marketplace = await download_file(bot, message, user_id)
    
    if not marketplace:
        await message.answer("❌ Переименуй файл (добавь wb/ozon/yandex)")
        return
    
    if marketplace in user_schemas[user_id]:
        await message.answer(f"⚠️ {marketplace.upper()} уже загружен")
        return
    
    user_schemas[user_id][marketplace] = file_path
    await message.answer(f"✅ {marketplace.upper()} ({len(user_schemas[user_id])}/3)")
    
    if len(user_schemas[user_id]) == 3:
        # КРИТИЧНО: Двойная проверка флага
        data = await state.get_data()
        if data.get('files_processed'):
            return
        
        await state.update_data(files_processed=True)
        
        await message.answer(
            "✅ Все файлы загружены!",
            reply_markup=get_update_schema_keyboard()
        )


async def finalize_schema_update(message: types.Message, state: FSMContext):
    """Финализация обновления схемы"""
    # Проверяем состояние
    current_state = await state.get_state()
    if current_state != SchemaStates.waiting_update_files:
        await message.answer("❌ Сначала выбери схему для обновления")
        return
    
    user_id = message.from_user.id
    
    if user_id not in user_schemas or len(user_schemas[user_id]) != 3:
        await message.answer("❌ Загрузи 3 файла!")
        return
    
    data = await state.get_data()
    schema_id = data.get('update_schema_id')
    schema_name = data.get('update_schema_name')
    
    if not schema_id or not schema_name:
        await message.answer(
            f"❌ Ошибка: данные схемы потеряны\n"
            f"Начни обновление заново"
        )
        return
    
    await message.answer(f"⏳ Анализирую столбцы для схемы '{schema_name}'...")
    
    try:
        file_paths = user_schemas[user_id]
        
        # Читаем ВСЕ столбцы из файлов
        reader = ExcelReader()
        all_columns = {}
        
        for marketplace, file_path in file_paths.items():
            config = FILE_CONFIGS[marketplace]
            all_columns[marketplace] = reader.get_column_names(
                file_path,
                config['sheet_name'],
                config['header_row']
            )
        
        # Получаем СУЩЕСТВУЮЩИЕ сопоставления из схемы
        existing_matches = db.get_schema_matches(schema_id)
        
        # Формируем множества УЖЕ сопоставленных столбцов
        matched_wb = set()
        matched_ozon = set()
        matched_yandex = set()
        
        for match in existing_matches.get('matches_all_three', []):
            if match.get('column_1'):
                matched_wb.add(match['column_1'])
            if match.get('column_2'):
                matched_ozon.add(match['column_2'])
            if match.get('column_3'):
                matched_yandex.add(match['column_3'])
        
        # Фильтруем ТОЛЬКО несопоставленные столбцы
        remaining_columns = {
            'wildberries': [col for col in all_columns['wildberries'] if col not in matched_wb],
            'ozon': [col for col in all_columns['ozon'] if col not in matched_ozon],
            'yandex': [col for col in all_columns['yandex'] if col not in matched_yandex]
        }
        
        total_remaining = (
            len(remaining_columns['wildberries']) +
            len(remaining_columns['ozon']) +
            len(remaining_columns['yandex'])
        )
        
        if total_remaining == 0:
            user_schemas[user_id] = {}
            await state.clear()
            
            await message.answer(
                f"ℹ️ Все столбцы уже сопоставлены!\n\n"
                f"Схема '{schema_name}' не требует обновления",
                reply_markup=get_main_menu_keyboard()
            )
            return
        
        await message.answer(
            f"🔍 Найдено несопоставленных столбцов:\n"
            f"• WB: {len(remaining_columns['wildberries'])}\n"
            f"• Ozon: {len(remaining_columns['ozon'])}\n"
            f"• Яндекс: {len(remaining_columns['yandex'])}\n\n"
            f"🤖 AI ищет новые совпадения..."
        )
        
        # Сравниваем ТОЛЬКО оставшиеся столбцы
        comparator = AIComparator()
        new_comparison_result = comparator.compare_columns(
            remaining_columns['wildberries'],
            remaining_columns['ozon'],
            remaining_columns['yandex']
        )
        
        # Добавляем новые совпадения с уверенностью >= 85%
        new_count = 0
        skipped_count = 0
        
        for match in new_comparison_result.get('matches_all_three', []):
            confidence = match.get('confidence', 0)
            
            # ФИЛЬТР: только >= 85%
            if confidence < 0.85:
                skipped_count += 1
                continue
            
            # Проверяем что это действительно новое совпадение
            key = (match.get('column_1'), match.get('column_2'), match.get('column_3'))
            is_new = True
            
            for existing in existing_matches['matches_all_three']:
                existing_key = (existing.get('column_1'), existing.get('column_2'), existing.get('column_3'))
                if key == existing_key:
                    is_new = False
                    break
            
            if is_new:
                existing_matches['matches_all_three'].append(match)
                new_count += 1
        
        # Сохраняем обновленную схему
        if new_count > 0:
            db.save_schema_matches(schema_id, existing_matches)
        
        user_schemas[user_id] = {}
        await state.clear()
        
        if new_count > 0:
            total_matches = len(existing_matches['matches_all_three'])
            message_text = f"✅ Схема '{schema_name}' обновлена!\n\n"
            message_text += f"➕ Добавлено новых совпадений: {new_count}\n"
            message_text += f"📊 Всего столбцов в схеме: {total_matches}"
            
            if skipped_count > 0:
                message_text += f"\n⚠️ Пропущено (< 85%): {skipped_count}"
            
            await message.answer(message_text, reply_markup=get_main_menu_keyboard())
        else:
            message_text = f"ℹ️ Новых совпадений не найдено\n\n"
            message_text += f"AI не нашел подходящих пар (>= 85%) среди оставшихся {total_remaining} столбцов"
            
            if skipped_count > 0:
                message_text += f"\n⚠️ Пропущено (< 85%): {skipped_count}"
            
            await message.answer(message_text, reply_markup=get_main_menu_keyboard())
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {str(e)}")
        logging.error(f"Error updating schema: {e}", exc_info=True)


def register_schema_update_handlers(dp, bot):
    """Регистрация обработчиков обновления схем"""
    from functools import partial
    
    dp.message.register(update_schema_start, F.text == "🔄 Обновить схему")
    dp.message.register(schema_selected_for_update, SchemaStates.selecting_schema_to_update)
    dp.message.register(partial(handle_update_file, bot=bot), SchemaStates.waiting_update_files, F.document)
    dp.message.register(finalize_schema_update, F.text == "✅ Обновить схему")
