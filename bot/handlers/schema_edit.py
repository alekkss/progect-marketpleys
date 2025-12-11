"""
Обработчики редактирования схем (просмотр и изменение сопоставлений)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from aiogram import types, F
from aiogram.types import ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext

from bot.states import SchemaStates
from bot.keyboards import (
    get_schema_edit_keyboard,
    get_cancel_keyboard,
    get_edit_column_keyboard,
    get_back_to_edit_keyboard,
    get_schema_list_keyboard,
    get_edit_match_menu_keyboard
)
from bot.storage import user_schemas, db
from bot.utils import download_file
from bot.handlers.common import cmd_start

from config.config import FILE_CONFIGS
from utils.excel_reader import ExcelReader


async def edit_schema_start(message: types.Message, state: FSMContext):
    """Меню редактирования схемы"""
    await message.answer(
        "Редактирование схемы:\n\n"
        "Выбери действие:",
        reply_markup=get_schema_edit_keyboard()
    )


# ===== ПРОСМОТР СОПОСТАВЛЕНИЙ =====

async def view_matches_start(message: types.Message, state: FSMContext):
    """Выбор схемы для просмотра"""
    user_id = message.from_user.id
    schemas = db.get_user_schemas(user_id)
    
    if not schemas:
        await message.answer("❌ У тебя нет схем!")
        return
    
    keyboard = get_schema_list_keyboard(schemas)
    
    if not keyboard:
        await message.answer("❌ У тебя нет валидных схем!")
        return
    
    await state.set_state(SchemaStates.selecting_schema_to_view)
    await message.answer("Выбери схему для просмотра:", reply_markup=keyboard)


async def show_schema_matches(message: types.Message, state: FSMContext):
    """Отображение сопоставлений выбранной схемы"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    user_id = message.from_user.id
    schema = db.get_schema(user_id, message.text)
    
    if not schema:
        await message.answer("❌ Схема не найдена")
        return
    
    schema_id = schema['id']
    schema_name = schema['name']
    
    # Получаем сопоставления
    matches_data = db.get_schema_matches(schema_id)
    matches = matches_data.get('matches_all_three', [])
    
    if not matches:
        await state.clear()
        await message.answer(
            f"📋 Схема '{schema_name}'\n\n"
            "⚠️ Нет сопоставлений",
            reply_markup=get_back_to_edit_keyboard()
        )
        return
    
    # Формируем красивый вывод
    text = f"📋 Схема: {schema_name}\n"
    text += f"📊 Всего сопоставлений: {len(matches)}\n\n"
    text += "─" * 40 + "\n\n"
    
    for i, match in enumerate(matches, 1):
        wb_col = match.get('column_1', '—')
        ozon_col = match.get('column_2', '—')
        yandex_col = match.get('column_3', '—')
        confidence = match.get('confidence', 0)
        description = match.get('description', '')
        
        text += f"#{i}\n"
        text += f"🔹 WB: {wb_col}\n"
        text += f"🔸 Ozon: {ozon_col}\n"
        text += f"🔹 Яндекс: {yandex_col}\n"
        text += f"📈 Уверенность: {confidence:.0%}\n"
        
        if description:
            text += f"💬 {description}\n"
        
        text += "\n"
        
        # Разбиваем на части если слишком длинное
        if len(text) > 3500:
            await message.answer(text)
            text = ""
    
    # Отправляем остаток
    if text:
        await message.answer(text)
    
    await state.clear()
    await message.answer("✅ Просмотр завершен", reply_markup=get_back_to_edit_keyboard())


# ===== ИЗМЕНЕНИЕ СОПОСТАВЛЕНИЙ =====

async def edit_match_start(message: types.Message, state: FSMContext):
    """Выбор схемы для редактирования"""
    user_id = message.from_user.id
    schemas = db.get_user_schemas(user_id)
    
    if not schemas:
        await message.answer("❌ У тебя нет схем!")
        return
    
    keyboard = get_schema_list_keyboard(schemas)
    
    if not keyboard:
        await message.answer("❌ У тебя нет валидных схем!")
        return
    
    await state.set_state(SchemaStates.selecting_schema_to_edit)
    await message.answer("Выбери схему для редактирования:", reply_markup=keyboard)


async def schema_selected_for_edit(message: types.Message, state: FSMContext):
    """Схема выбрана, запрашиваем файлы для валидации"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    user_id = message.from_user.id
    schema = db.get_schema(user_id, message.text)
    
    if not schema:
        await message.answer("❌ Схема не найдена")
        return
    
    schema_id = schema['id']
    schema_name = schema['name']
    
    # Получаем сопоставления
    matches_data = db.get_schema_matches(schema_id)
    matches = matches_data.get('matches_all_three', [])
    
    if not matches:
        await state.clear()
        await message.answer(
            f"📋 Схема '{schema_name}'\n\n"
            "⚠️ Нет сопоставлений для редактирования"
        )
        await edit_schema_start(message, state)
        return
    
    # Сохраняем в state
    await state.update_data(
        edit_schema_id=schema_id,
        edit_schema_name=schema_name,
        edit_matches=matches
    )
    
    # Запрашиваем загрузку файлов для валидации
    user_schemas[user_id] = {}

    await state.update_data(files_processed=False)
    
    await message.answer(
        f"📋 Схема '{schema_name}' выбрана\n\n"
        "📤 Для валидации столбцов загрузи 3 актуальных файла Excel\n"
        "(wb, ozon, yandex)",
        reply_markup=ReplyKeyboardRemove()
    )
    
    await state.set_state(SchemaStates.waiting_edit_files)


async def handle_edit_validation_file(message: types.Message, state: FSMContext, bot):
    """Загрузка файлов для валидации при редактировании"""
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
    
    # ВАЖНО: Проверяем ровно == 3 и ЕЩЁ РАЗ проверяем флаг
    if len(user_schemas[user_id]) == 3:
        # КРИТИЧНО: Сразу устанавливаем флаг ДО любой обработки
        data = await state.get_data()
        if data.get('files_processed'):
            return  # Другой обработчик уже начал обработку
        
        await state.update_data(files_processed=True)
        
        # Читаем столбцы из файлов
        try:
            reader = ExcelReader()
            available_columns = {}
            
            for marketplace, file_path in user_schemas[user_id].items():
                config = FILE_CONFIGS[marketplace]
                available_columns[marketplace] = reader.get_column_names(
                    file_path,
                    config['sheet_name'],
                    config['header_row']
                )
            
            # Сохраняем доступные столбцы
            await state.update_data(available_columns=available_columns)
            
            # Показываем список сопоставлений
            data = await state.get_data()  # Перечитываем data
            matches = data.get('edit_matches', [])
            schema_name = data.get('edit_schema_name')
            
            # Формируем ОДИН большой текст
            text_parts = []
            text_parts.append(f"✅ Файлы загружены!\n\n")
            text_parts.append(f"📋 Схема: {schema_name}\n")
            text_parts.append(f"📊 Всего сопоставлений: {len(matches)}\n\n")
            
            for i, match in enumerate(matches, 1):
                wb_col = match.get('column_1', '—')
                ozon_col = match.get('column_2', '—')
                yandex_col = match.get('column_3', '—')
                
                text_parts.append(f"#{i}: {wb_col} | {ozon_col} | {yandex_col}\n")
            
            # Объединяем весь текст
            full_text = ''.join(text_parts)
            
            # Разбиваем на части по 3500 символов
            max_length = 3500
            if len(full_text) <= max_length:
                await message.answer(full_text)
            else:
                # Разбиваем на части
                parts = []
                current_part = ""
                for line in full_text.split('\n'):
                    if len(current_part) + len(line) + 1 > max_length:
                        parts.append(current_part)
                        current_part = line + '\n'
                    else:
                        current_part += line + '\n'
                
                if current_part:
                    parts.append(current_part)
                
                # Отправляем все части
                for part in parts:
                    await message.answer(part)
            
            # ВАЖНО: Переходим в состояние выбора действия
            from bot.keyboards import get_edit_match_menu_keyboard
            await state.set_state(SchemaStates.choosing_edit_action)
            
            # ОДНО сообщение с меню
            await message.answer(
                "Выбери действие:",
                reply_markup=get_edit_match_menu_keyboard()
            )
            
        except Exception as e:
            await message.answer(f"❌ Ошибка чтения файлов: {str(e)}")
            await edit_schema_start(message, state)

async def edit_action_selected(message: types.Message, state: FSMContext):
    """Выбор действия: изменить или добавить сопоставление"""
    if message.text == "❌ Отмена":
        # Очищаем временные данные
        user_id = message.from_user.id
        if user_id in user_schemas:
            user_schemas[user_id] = {}
        await edit_schema_start(message, state)
        return
    
    data = await state.get_data()
    
    if message.text == "✏️ Редактировать сопоставление":  
        # Переход к изменению существующего
        matches = data.get('edit_matches', [])
        
        await state.set_state(SchemaStates.entering_match_number)
        await message.answer(
            f"Введи номер сопоставления для редактирования (1-{len(matches)}):",
            reply_markup=get_cancel_keyboard()
        )
    
    elif message.text == "➕ Добавить сопоставление":
        # Переход к добавлению нового
        await add_new_match_start(message, state)
    
    else:
        await message.answer("❌ Неизвестная команда. Используй кнопки ниже.")



async def match_number_entered(message: types.Message, state: FSMContext):
    """Номер введен, показываем детали"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    # Проверяем что это число
    try:
        match_number = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введи число!")
        return
    
    data = await state.get_data()
    matches = data.get('edit_matches', [])
    
    if match_number < 1 or match_number > len(matches):
        await message.answer(f"❌ Номер должен быть от 1 до {len(matches)}")
        return
    
    # Получаем выбранное сопоставление
    selected_match = matches[match_number - 1]
    
    # Сохраняем номер
    await state.update_data(
        edit_match_index=match_number - 1,
        edit_match_data=selected_match
    )
    
    # Показываем текущее сопоставление
    wb_col = selected_match.get('column_1', '—')
    ozon_col = selected_match.get('column_2', '—')
    yandex_col = selected_match.get('column_3', '—')
    confidence = selected_match.get('confidence', 0)
    description = selected_match.get('description', '')
    
    text = f"📋 Сопоставление #{match_number}\n\n"
    text += f"🔹 WB: {wb_col}\n"
    text += f"🔸 Ozon: {ozon_col}\n"
    text += f"🔹 Яндекс: {yandex_col}\n"
    text += f"📈 Уверенность: {confidence:.0%}\n"
    if description:
        text += f"💬 {description}\n"
    
    await message.answer(text)
    
    await state.set_state(SchemaStates.selecting_column_to_edit)
    await message.answer("Что хочешь изменить?", reply_markup=get_edit_column_keyboard())


async def column_selected_for_edit(message: types.Message, state: FSMContext):
    """Выбран столбец для редактирования"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    if message.text == "🗑 Удалить сопоставление":
        await delete_match_confirm(message, state)
        return
    
    # Определяем какой столбец редактируем
    if message.text == "📝 Изменить WB столбец":
        marketplace = 'wildberries'
        column_key = 'column_1'
        display_name = 'WB'
    elif message.text == "📝 Изменить Ozon столбец":
        marketplace = 'ozon'
        column_key = 'column_2'
        display_name = 'Ozon'
    elif message.text == "📝 Изменить Яндекс столбец":
        marketplace = 'yandex'
        column_key = 'column_3'
        display_name = 'Яндекс'
    else:
        await message.answer("❌ Неизвестная команда")
        return
    
    data = await state.get_data()
    available_columns = data.get('available_columns', {})
    columns_list = available_columns.get(marketplace, [])
    
    if not columns_list:
        await message.answer("❌ Не удалось загрузить список столбцов")
        return
    
    await state.update_data(
        edit_marketplace=marketplace,
        edit_column_key=column_key,
        edit_display_name=display_name
    )
    
    # Показываем список доступных столбцов
    text = f"📋 Доступные столбцы {display_name} ({len(columns_list)}):\n\n"
    
    for i, col in enumerate(columns_list, 1):
        text += f"{i}. {col}\n"
        
        # Разбиваем на части
        if i % 30 == 0:
            await message.answer(text)
            text = ""
    
    if text:
        await message.answer(text)
    
    await message.answer(
        f"Введи название столбца из списка выше или номер (1-{len(columns_list)}):",
        reply_markup=get_cancel_keyboard()
    )
    
    await state.set_state(SchemaStates.selecting_new_column_value)


async def new_column_value_entered(message: types.Message, state: FSMContext):
    """Новое значение введено, валидируем и сохраняем"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    user_input = message.text.strip()
    
    if not user_input:
        await message.answer("❌ Название столбца не может быть пустым!")
        return
    
    data = await state.get_data()
    marketplace = data.get('edit_marketplace')
    available_columns = data.get('available_columns', {})
    columns_list = available_columns.get(marketplace, [])
    
    # Валидация
    new_value = None
    
    # Проверяем, может это номер
    try:
        col_number = int(user_input)
        if 1 <= col_number <= len(columns_list):
            new_value = columns_list[col_number - 1]
    except ValueError:
        # Не номер, ищем по точному совпадению
        if user_input in columns_list:
            new_value = user_input
        else:
            # Ищем похожее (case-insensitive)
            user_lower = user_input.lower()
            for col in columns_list:
                if col.lower() == user_lower:
                    new_value = col
                    break
    
    if not new_value:
        await message.answer(
            f"❌ Столбец '{user_input}' не найден в шаблоне {data.get('edit_display_name')}!\n\n"
            f"Введи точное название или номер из списка."
        )
        return
    
    # Продолжаем с валидированным значением
    schema_id = data.get('edit_schema_id')
    schema_name = data.get('edit_schema_name')
    matches = data.get('edit_matches', [])
    match_index = data.get('edit_match_index')
    column_key = data.get('edit_column_key')
    display_name = data.get('edit_display_name')
    
    # Обновляем значение
    old_value = matches[match_index].get(column_key, '—')
    matches[match_index][column_key] = new_value
    
    # Сохраняем в БД
    matches_data = {'matches_all_three': matches}
    db.save_schema_matches(schema_id, matches_data)
    
    # Очищаем временные файлы
    user_id = message.from_user.id
    if user_id in user_schemas:
        user_schemas[user_id] = {}
    
    await state.clear()
    
    text = f"✅ Сопоставление обновлено!\n\n"
    text += f"📋 Схема: {schema_name}\n"
    text += f"📝 Столбец {display_name}:\n"
    text += f"   Было: {old_value}\n"
    text += f"   Стало: {new_value}"
    
    await message.answer(text)
    
    # Возвращаемся к меню редактирования
    await edit_schema_start(message, state)


async def delete_match_confirm(message: types.Message, state: FSMContext):
    """Удаление сопоставления"""
    data = await state.get_data()
    schema_id = data.get('edit_schema_id')
    schema_name = data.get('edit_schema_name')
    matches = data.get('edit_matches', [])
    match_index = data.get('edit_match_index')
    
    # Удаляем сопоставление
    deleted_match = matches.pop(match_index)
    
    # Сохраняем в БД
    matches_data = {'matches_all_three': matches}
    db.save_schema_matches(schema_id, matches_data)
    
    await state.clear()
    
    text = f"✅ Сопоставление удалено!\n\n"
    text += f"📋 Схема: {schema_name}\n"
    text += f"🗑 Удалено:\n"
    text += f"   WB: {deleted_match.get('column_1', '—')}\n"
    text += f"   Ozon: {deleted_match.get('column_2', '—')}\n"
    text += f"   Яндекс: {deleted_match.get('column_3', '—')}"
    
    await message.answer(text)
    
    # Возвращаемся к меню редактирования
    await edit_schema_start(message, state)

# ===== ДОБАВЛЕНИЕ НОВОГО СОПОСТАВЛЕНИЯ =====

async def add_new_match_start(message: types.Message, state: FSMContext):
    """Начало добавления нового сопоставления"""
    data = await state.get_data()
    available_columns = data.get('available_columns', {})
    wb_columns = available_columns.get('wildberries', [])
    
    if not wb_columns:
        await message.answer("❌ Не удалось загрузить столбцы WB")
        return
    
    # Показываем список доступных столбцов WB
    text = f"📋 Доступные столбцы WB ({len(wb_columns)}):\n\n"
    
    for i, col in enumerate(wb_columns, 1):
        text += f"{i}. {col}\n"
        
        if i % 30 == 0:
            await message.answer(text)
            text = ""
    
    if text:
        await message.answer(text)
    
    await state.set_state(SchemaStates.selecting_wb_column)
    await message.answer(
        f"Шаг 1/3: Выбери столбец WB\n\n"
        f"Введи название или номер (1-{len(wb_columns)}):",
        reply_markup=get_cancel_keyboard()
    )


async def wb_column_selected(message: types.Message, state: FSMContext):
    """Столбец WB выбран"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    data = await state.get_data()
    available_columns = data.get('available_columns', {})
    wb_columns = available_columns.get('wildberries', [])
    
    # Валидация выбора
    wb_value = None
    user_input = message.text.strip()
    
    try:
        col_number = int(user_input)
        if 1 <= col_number <= len(wb_columns):
            wb_value = wb_columns[col_number - 1]
    except ValueError:
        if user_input in wb_columns:
            wb_value = user_input
        else:
            user_lower = user_input.lower()
            for col in wb_columns:
                if col.lower() == user_lower:
                    wb_value = col
                    break
    
    if not wb_value:
        await message.answer(
            f"❌ Столбец '{user_input}' не найден!\n\n"
            f"Введи точное название или номер из списка."
        )
        return
    
    # Сохраняем выбор
    await state.update_data(new_match_wb=wb_value)
    
    # Переходим к Ozon
    ozon_columns = available_columns.get('ozon', [])
    
    text = f"✅ WB: {wb_value}\n\n"
    text += f"📋 Доступные столбцы Ozon ({len(ozon_columns)}):\n\n"
    
    for i, col in enumerate(ozon_columns, 1):
        text += f"{i}. {col}\n"
        
        if i % 30 == 0:
            await message.answer(text)
            text = ""
    
    if text:
        await message.answer(text)
    
    await state.set_state(SchemaStates.selecting_ozon_column)
    await message.answer(
        f"Шаг 2/3: Выбери столбец Ozon\n\n"
        f"Введи название или номер (1-{len(ozon_columns)}):",
        reply_markup=get_cancel_keyboard()
    )


async def ozon_column_selected(message: types.Message, state: FSMContext):
    """Столбец Ozon выбран"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    data = await state.get_data()
    available_columns = data.get('available_columns', {})
    ozon_columns = available_columns.get('ozon', [])
    
    # Валидация выбора
    ozon_value = None
    user_input = message.text.strip()
    
    try:
        col_number = int(user_input)
        if 1 <= col_number <= len(ozon_columns):
            ozon_value = ozon_columns[col_number - 1]
    except ValueError:
        if user_input in ozon_columns:
            ozon_value = user_input
        else:
            user_lower = user_input.lower()
            for col in ozon_columns:
                if col.lower() == user_lower:
                    ozon_value = col
                    break
    
    if not ozon_value:
        await message.answer(
            f"❌ Столбец '{user_input}' не найден!\n\n"
            f"Введи точное название или номер из списка."
        )
        return
    
    # Сохраняем выбор
    await state.update_data(new_match_ozon=ozon_value)
    
    # Переходим к Яндекс
    yandex_columns = available_columns.get('yandex', [])
    
    text = f"✅ WB: {data.get('new_match_wb')}\n"
    text += f"✅ Ozon: {ozon_value}\n\n"
    text += f"📋 Доступные столбцы Яндекс ({len(yandex_columns)}):\n\n"
    
    for i, col in enumerate(yandex_columns, 1):
        text += f"{i}. {col}\n"
        
        if i % 30 == 0:
            await message.answer(text)
            text = ""
    
    if text:
        await message.answer(text)
    
    await state.set_state(SchemaStates.selecting_yandex_column)
    await message.answer(
        f"Шаг 3/3: Выбери столбец Яндекс\n\n"
        f"Введи название или номер (1-{len(yandex_columns)}):",
        reply_markup=get_cancel_keyboard()
    )


async def yandex_column_selected(message: types.Message, state: FSMContext):
    """Столбец Яндекс выбран, сохраняем сопоставление"""
    if message.text == "❌ Отмена":
        await edit_schema_start(message, state)
        return
    
    data = await state.get_data()
    available_columns = data.get('available_columns', {})
    yandex_columns = available_columns.get('yandex', [])
    
    # Валидация выбора
    yandex_value = None
    user_input = message.text.strip()
    
    try:
        col_number = int(user_input)
        if 1 <= col_number <= len(yandex_columns):
            yandex_value = yandex_columns[col_number - 1]
    except ValueError:
        if user_input in yandex_columns:
            yandex_value = user_input
        else:
            user_lower = user_input.lower()
            for col in yandex_columns:
                if col.lower() == user_lower:
                    yandex_value = col
                    break
    
    if not yandex_value:
        await message.answer(
            f"❌ Столбец '{user_input}' не найден!\n\n"
            f"Введи точное название или номер из списка."
        )
        return
    
    # Собираем новое сопоставление
    wb_value = data.get('new_match_wb')
    ozon_value = data.get('new_match_ozon')
    
    new_match = {
        'column_1': wb_value,
        'column_2': ozon_value,
        'column_3': yandex_value,
        'confidence': 1.0,  # Ручное сопоставление = 100%
        'description': 'Добавлено вручную'
    }
    
    # Проверяем дубликаты
    schema_id = data.get('edit_schema_id')
    schema_name = data.get('edit_schema_name')
    matches = data.get('edit_matches', [])
    
    is_duplicate = False
    for match in matches:
        if (match.get('column_1') == wb_value and 
            match.get('column_2') == ozon_value and 
            match.get('column_3') == yandex_value):
            is_duplicate = True
            break
    
    if is_duplicate:
        await message.answer(
            "⚠️ Такое сопоставление уже существует!\n\n"
            f"WB: {wb_value}\n"
            f"Ozon: {ozon_value}\n"
            f"Яндекс: {yandex_value}"
        )
        await edit_schema_start(message, state)
        return
    
    # Добавляем новое сопоставление
    matches.append(new_match)
    
    # Сохраняем в БД
    matches_data = {'matches_all_three': matches}
    db.save_schema_matches(schema_id, matches_data)
    
    # Очищаем временные данные
    user_id = message.from_user.id
    if user_id in user_schemas:
        user_schemas[user_id] = {}
    
    await state.clear()
    
    text = f"✅ Новое сопоставление добавлено!\n\n"
    text += f"📋 Схема: {schema_name}\n"
    text += f"📊 Всего сопоставлений: {len(matches)}\n\n"
    text += f"Добавлено:\n"
    text += f"🔹 WB: {wb_value}\n"
    text += f"🔸 Ozon: {ozon_value}\n"
    text += f"🔹 Яндекс: {yandex_value}"
    
    await message.answer(text)
    
    # Возвращаемся к меню редактирования
    await edit_schema_start(message, state)



def register_schema_edit_handlers(dp, bot):
    """Регистрация обработчиков редактирования схем"""
    from functools import partial
    
    dp.message.register(edit_schema_start, F.text == "✏️ Редактировать схему")
    
    # Просмотр
    dp.message.register(view_matches_start, F.text == "👁 Просмотреть текущие сопоставления")
    dp.message.register(show_schema_matches, SchemaStates.selecting_schema_to_view)
    
    # Редактирование - выбор схемы
    dp.message.register(edit_match_start, F.text == "✏️ Изменить сопоставление")
    dp.message.register(schema_selected_for_edit, SchemaStates.selecting_schema_to_edit)
    
    # Загрузка файлов для валидации
    dp.message.register(partial(handle_edit_validation_file, bot=bot), SchemaStates.waiting_edit_files, F.document)
    
    # ВАЖНО: Выбор действия ПОСЛЕ загрузки файлов (в специальном состоянии)
    dp.message.register(edit_action_selected, SchemaStates.choosing_edit_action)
    
    # Изменение существующего сопоставления
    dp.message.register(match_number_entered, SchemaStates.entering_match_number)
    dp.message.register(column_selected_for_edit, SchemaStates.selecting_column_to_edit)
    dp.message.register(new_column_value_entered, SchemaStates.selecting_new_column_value)
    
    # Добавление нового сопоставления
    dp.message.register(wb_column_selected, SchemaStates.selecting_wb_column)
    dp.message.register(ozon_column_selected, SchemaStates.selecting_ozon_column)
    dp.message.register(yandex_column_selected, SchemaStates.selecting_yandex_column)
