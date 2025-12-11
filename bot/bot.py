"""
Telegram бот для синхронизации маркетплейсов
"""
import asyncio
import os
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile, ReplyKeyboardRemove
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config.config import FILE_CONFIGS, TELEGRAM_BOT_TOKEN
from utils.excel_reader import ExcelReader
from services.ai_comparator import AIComparator
from utils.excel_writer import ExcelWriter
from services.data_synchronizer import DataSynchronizer
from database.database import Database
import logging
from utils.logger_config import setup_logger
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

logger = setup_logger('bot')

logging.basicConfig(level=logging.INFO)

class UploadStates(StatesGroup):
    waiting_for_files = State()
    selecting_schema = State()

class SchemaStates(StatesGroup):
    creating_schema = State()
    waiting_schema_name = State()
    waiting_schema_files = State()
    managing_schema = State()
    selecting_schema_to_update = State()
    waiting_update_files = State()
    selecting_schema_to_delete = State()
    selecting_schema_to_view = State()       # Для просмотра сопоставлений
    viewing_schema_matches = State()          # Для навигации по сопоставлениям
    selecting_schema_to_edit = State()        # Для выбора схемы для редактирования
    waiting_edit_files = State()
    entering_match_number = State()           # Ввод номера сопоставления
    selecting_column_to_edit = State()        # Выбор какой столбец менять
    selecting_new_column_value = State()      # Выбор нового значения

user_files = {}
user_schemas = {}  # Временное хранилище для создания схем
db = Database()

def create_bot():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message, state: FSMContext):
        await state.clear()
        user_files[message.from_user.id] = {}
        
        # Регистрируем пользователя
        db.add_user(
            message.from_user.id,
            message.from_user.username,
            message.from_user.first_name,
            message.from_user.last_name
        )
        
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📤 Загрузить файлы")],
                [KeyboardButton(text="📋 Управление схемами")],
                [KeyboardButton(text="📊 Моя статистика")]
            ],
            resize_keyboard=True
        )
        
        await message.answer(
            "🤖 Бот синхронизации маркетплейсов\n\n"
            "📤 Загрузить файлы - синхронизация по схеме\n"
            "📋 Управление схемами - создать/обновить/удалить",
            reply_markup=keyboard
        )
    
    # === ЗАГРУЗКА ФАЙЛОВ С ВЫБОРОМ СХЕМЫ ===
    
    @dp.message(F.text == "📤 Загрузить файлы")
    async def select_schema_for_upload(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        schemas = db.get_user_schemas(user_id)
        
        if not schemas:
            await message.answer(
                "❌ У тебя нет схем!\n\n"
                "Сначала создай схему через 📋 Управление схемами"
            )
            return
        
        # Формируем список схем
        keyboard_buttons = []
        for schema in schemas:
            # Проверяем что name не None
            if schema.get('name'):
                keyboard_buttons.append([KeyboardButton(text=schema['name'])])
        
        # Если после фильтрации пусто
        if not keyboard_buttons:
            await message.answer(
                "❌ У тебя нет валидных схем!\n\n"
                "Создай новую схему через 📋 Управление схемами"
            )
            return
        
        keyboard_buttons.append([KeyboardButton(text="❌ Отмена")])
        
        keyboard = ReplyKeyboardMarkup(keyboard=keyboard_buttons, resize_keyboard=True)
        
        await state.set_state(UploadStates.selecting_schema)
        await message.answer("Выбери схему для синхронизации:", reply_markup=keyboard)
    
    @dp.message(UploadStates.selecting_schema)
    async def schema_selected(message: types.Message, state: FSMContext):
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
    
    @dp.message(UploadStates.waiting_for_files, F.document)
    async def handle_file(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        
        if user_id not in user_files:
            user_files[user_id] = {}
        
        file = await bot.get_file(message.document.file_id)
        file_name = message.document.file_name
        
        os.makedirs(f"uploads/{user_id}", exist_ok=True)
        file_path = f"uploads/{user_id}/{file_name}"
        await bot.download_file(file.file_path, file_path)
        
        fn = file_name.lower()
        if 'wb' in fn or 'wildberries' in fn:
            marketplace = 'wildberries'
        elif 'ozon' in fn or 'озон' in fn:
            marketplace = 'ozon'
        elif 'yandex' in fn or 'яндекс' in fn or 'market' in fn:
            marketplace = 'yandex'
        else:
            await message.answer("❌ Переименуй файл (добавь wb/ozon/yandex)")
            return
        
        if marketplace in user_files[user_id]:
            await message.answer(f"⚠️ {marketplace.upper()} уже загружен")
            return
            
        user_files[user_id][marketplace] = file_path
        await message.answer(f"✅ {marketplace.upper()} ({len(user_files[user_id])}/3)")
        
        if len(user_files[user_id]) == 3:
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="🚀 Обработать")]],
                resize_keyboard=True
            )
            await message.answer("✅ Все файлы загружены!", reply_markup=keyboard)
    
    @dp.message(F.text == "🚀 Обработать")
    async def process_files(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        
        if user_id not in user_files or len(user_files[user_id]) != 3:
            await message.answer("❌ Загрузи 3 файла!")
            return
        
        # Получаем выбранную схему
        data = await state.get_data()
        schema_id = data.get('selected_schema_id')
        
        if not schema_id:
            await message.answer("❌ Схема не выбрана!")
            return
        
        processing_id = db.start_processing(user_id)
        await message.answer("⏳ Обработка по схеме...")
        
        try:
            file_paths = user_files[user_id]
            
            for marketplace, file_path in file_paths.items():
                db.add_file(user_id, processing_id, marketplace, os.path.basename(file_path), file_path)
            
            await message.answer("📖 Читаю файлы...")
            
            # Получаем сопоставления из схемы
            comparison_result = db.get_schema_matches(schema_id)
            
            await message.answer(f"🔄 Синхронизирую по схеме ({len(comparison_result['matches_all_three'])} столбцов)...")
            
            # ДОБАВЛЕНО: Создаем AI comparator для validation проверок
            comparator = AIComparator()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = f"output/{user_id}_{timestamp}"
            os.makedirs(output_dir, exist_ok=True)
            
            output_sync_paths = {
                'wildberries': f"{output_dir}/WB_синхронизировано.xlsx",
                'ozon': f"{output_dir}/Ozon_синхронизировано.xlsx",
                'yandex': f"{output_dir}/Яндекс_синхронизировано.xlsx"
            }
            
            # ✅ ПЕРЕМЕСТИЛИ НАВЕРХУ!
            report_path = f"{output_dir}/результат_{timestamp}.xlsx"
            
            # Теперь передаем comparator в DataSynchronizer
            synchronizer = DataSynchronizer(comparison_result, ai_comparator=comparator)
            
            # ✅ ТЕПЕРЬ report_path существует!
            synced_dfs, changes_log = synchronizer.synchronize_data(
                file_paths, 
                output_sync_paths, 
                report_path=report_path
            )
            
            await message.answer("📊 Создаю отчет...")
            
            report_path = f"{output_dir}/результат_{timestamp}.xlsx"
            writer = ExcelWriter()
            writer.create_report_with_changes(comparison_result, changes_log, report_path)

            # ДОБАВИТЕ ЭТИ 3 СТРОКИ:
            if hasattr(synchronizer, 'ai_validation_log') and synchronizer.ai_validation_log:
                logger.info(f"📋 Создаю лист с AI-логами ({len(synchronizer.ai_validation_log)} записей)...")
                synchronizer._create_ai_log_sheet_in_report(report_path)
            
            wb_count = len(synced_dfs['wildberries'])
            ozon_count = len(synced_dfs['ozon'])
            yandex_count = len(synced_dfs['yandex'])
            total_synced = sum(len(changes_log[mp]) for mp in changes_log)
            
            db.complete_processing(processing_id, wb_count, ozon_count, yandex_count, total_synced)
            
            await message.answer("📤 Отправляю результаты...")
            
            for marketplace, path in output_sync_paths.items():
                doc = FSInputFile(path)
                await message.answer_document(doc)
            
            report_doc = FSInputFile(report_path)
            await message.answer_document(report_doc, caption="📊 Отчет")
            
            user_files[user_id] = {}
            await state.clear()
            
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📤 Загрузить файлы")],
                    [KeyboardButton(text="📋 Управление схемами")],
                    [KeyboardButton(text="📊 Моя статистика")]
                ],
                resize_keyboard=True
            )
            
            await message.answer(
                f"✅ Готово!\n\n"
                f"📦 Обработано товаров:\n"
                f"• WB: {wb_count}\n"
                f"• Ozon: {ozon_count}\n"
                f"• Яндекс: {yandex_count}\n\n"
                f"🔄 Синхронизировано ячеек: {total_synced}",
                reply_markup=keyboard
            )
            
        except Exception as e:
            db.fail_processing(processing_id, str(e))
            await message.answer(f"❌ Ошибка: {str(e)}")
            logging.error(f"Error: {e}", exc_info=True)
    
    # === УПРАВЛЕНИЕ СХЕМАМИ ===
    
    @dp.message(F.text == "📋 Управление схемами")
    async def schema_management(message: types.Message, state: FSMContext):
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="➕ Создать схему")],
                [KeyboardButton(text="✏️ Редактировать схему")],  # ← ДОБАВЬ ЭТУ СТРОКУ
                [KeyboardButton(text="🔄 Обновить схему")],
                [KeyboardButton(text="🗑 Удалить схему")],
                [KeyboardButton(text="📋 Мои схемы")],
                [KeyboardButton(text="◀️ Назад")]
            ],
            resize_keyboard=True
        )
        
        await message.answer("Управление схемами:", reply_markup=keyboard)
    
    @dp.message(F.text == "📋 Мои схемы")
    async def list_schemas(message: types.Message):
        user_id = message.from_user.id
        schemas = db.get_user_schemas(user_id)
        
        if not schemas:
            await message.answer("У тебя пока нет схем")
            return
        
        text = "📋 Твои схемы:\n\n"
        for i, schema in enumerate(schemas, 1):
            if schema.get('name'):  # <-- ДОБАВЬ ПРОВЕРКУ
                text += f"{i}. {schema['name']}\n"
                text += f"   📊 Столбцов: {schema.get('matches_count', 0)}\n"
                text += f"   📅 Создана: {schema.get('created_at', '')[:10]}\n\n"
        
        await message.answer(text)

        # РЕДАКТИРОВАНИЕ СХЕМЫ
    
    @dp.message(F.text == "✏️ Редактировать схему")
    async def edit_schema_start(message: types.Message, state: FSMContext):
        """Меню редактирования схемы"""
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="👁 Просмотреть текущие сопоставления")],
                [KeyboardButton(text="✏️ Изменить сопоставление")],  # НОВАЯ КНОПКА
                [KeyboardButton(text="◀️ Назад")]
            ],
            resize_keyboard=True
        )
        
        await message.answer(
            "Редактирование схемы:\n\n"
            "Выбери действие:",
            reply_markup=keyboard
        )
    
    @dp.message(F.text == "👁 Просмотреть текущие сопоставления")
    async def view_matches_start(message: types.Message, state: FSMContext):
        """Выбор схемы для просмотра сопоставлений"""
        user_id = message.from_user.id
        schemas = db.get_user_schemas(user_id)
        
        if not schemas:
            await message.answer("❌ У тебя нет схем!")
            return
        
        keyboard_buttons = []
        for schema in schemas:
            if schema.get('name'):
                keyboard_buttons.append([KeyboardButton(text=schema['name'])])
        
        if not keyboard_buttons:
            await message.answer("❌ У тебя нет валидных схем!")
            return
        
        keyboard_buttons.append([KeyboardButton(text="❌ Отмена")])
        
        keyboard = ReplyKeyboardMarkup(keyboard=keyboard_buttons, resize_keyboard=True)
        
        await state.set_state(SchemaStates.selecting_schema_to_view)
        await message.answer("Выбери схему для просмотра:", reply_markup=keyboard)


    @dp.message(SchemaStates.selecting_schema_to_view)
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
        
        # Получаем сопоставления из БД
        matches_data = db.get_schema_matches(schema_id)
        matches = matches_data.get('matches_all_three', [])
        
        if not matches:
            await state.clear()
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="✏️ Редактировать схему")],
                    [KeyboardButton(text="◀️ Назад")]
                ],
                resize_keyboard=True
            )
            await message.answer(
                f"📋 Схема '{schema_name}'\n\n"
                "⚠️ Нет сопоставлений",
                reply_markup=keyboard
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
            
            # Telegram имеет лимит 4096 символов на сообщение
            # Разбиваем на части если слишком длинное
            if len(text) > 3500:  # Оставляем запас
                await message.answer(text)
                text = ""
        
        # Отправляем остаток
        if text:
            await message.answer(text)
        
        await state.clear()
        
        # Возвращаемся к меню редактирования
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="✏️ Редактировать схему")],
                [KeyboardButton(text="◀️ Назад")]
            ],
            resize_keyboard=True
        )
        
        await message.answer(
            "✅ Просмотр завершен",
            reply_markup=keyboard
        )
    
    @dp.message(F.text == "✏️ Изменить сопоставление")
    async def edit_match_start(message: types.Message, state: FSMContext):
        """Выбор схемы для редактирования сопоставления"""
        user_id = message.from_user.id
        schemas = db.get_user_schemas(user_id)
        
        if not schemas:
            await message.answer("❌ У тебя нет схем!")
            return
        
        keyboard_buttons = []
        for schema in schemas:
            if schema.get('name'):
                keyboard_buttons.append([KeyboardButton(text=schema['name'])])
        
        if not keyboard_buttons:
            await message.answer("❌ У тебя нет валидных схем!")
            return
        
        keyboard_buttons.append([KeyboardButton(text="❌ Отмена")])
        
        keyboard = ReplyKeyboardMarkup(keyboard=keyboard_buttons, resize_keyboard=True)
        
        await state.set_state(SchemaStates.selecting_schema_to_edit)
        await message.answer("Выбери схему для редактирования:", reply_markup=keyboard)
    
    @dp.message(SchemaStates.selecting_schema_to_edit)
    async def schema_selected_for_edit(message: types.Message, state: FSMContext):
        """Схема выбрана, запрашиваем файлы для валидации столбцов"""
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
        
        # НОВОЕ: Запрашиваем загрузку файлов для валидации
        user_schemas[user_id] = {}
        
        await message.answer(
            f"📋 Схема '{schema_name}' выбрана\n\n"
            "📤 Для валидации столбцов загрузи 3 актуальных файла Excel\n"
            "(wb, ozon, yandex)",
            reply_markup=ReplyKeyboardRemove()
        )
        
        await state.set_state(SchemaStates.waiting_edit_files)
    
    @dp.message(SchemaStates.waiting_edit_files, F.document)
    async def handle_edit_validation_file(message: types.Message, state: FSMContext):
        """Загрузка файлов для валидации при редактировании"""
        user_id = message.from_user.id
        
        if user_id not in user_schemas:
            user_schemas[user_id] = {}
        
        file = await bot.get_file(message.document.file_id)
        file_name = message.document.file_name
        
        os.makedirs(f"uploads/{user_id}", exist_ok=True)
        file_path = f"uploads/{user_id}/{file_name}"
        await bot.download_file(file.file_path, file_path)
        
        fn = file_name.lower()
        if 'wb' in fn or 'wildberries' in fn:
            marketplace = 'wildberries'
        elif 'ozon' in fn or 'озон' in fn:
            marketplace = 'ozon'
        elif 'yandex' in fn or 'яндекс' in fn or 'market' in fn:
            marketplace = 'yandex'
        else:
            await message.answer("❌ Переименуй файл (добавь wb/ozon/yandex)")
            return
        
        if marketplace in user_schemas[user_id]:
            await message.answer(f"⚠️ {marketplace.upper()} уже загружен")
            return
            
        user_schemas[user_id][marketplace] = file_path
        await message.answer(f"✅ {marketplace.upper()} ({len(user_schemas[user_id])}/3)")
        
        if len(user_schemas[user_id]) == 3:
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
                
                # Теперь показываем список сопоставлений
                data = await state.get_data()
                matches = data.get('edit_matches', [])
                schema_name = data.get('edit_schema_name')
                
                text = f"✅ Файлы загружены!\n\n"
                text += f"📋 Схема: {schema_name}\n"
                text += f"📊 Всего сопоставлений: {len(matches)}\n\n"
                
                for i, match in enumerate(matches, 1):
                    wb_col = match.get('column_1', '—')
                    ozon_col = match.get('column_2', '—')
                    yandex_col = match.get('column_3', '—')
                    
                    text += f"#{i}: {wb_col} | {ozon_col} | {yandex_col}\n"
                    
                    if i % 20 == 0:
                        await message.answer(text)
                        text = ""
                
                if text:
                    await message.answer(text)
                
                keyboard = ReplyKeyboardMarkup(
                    keyboard=[[KeyboardButton(text="❌ Отмена")]],
                    resize_keyboard=True
                )
                
                await state.set_state(SchemaStates.entering_match_number)
                await message.answer(
                    f"Введи номер сопоставления для редактирования (1-{len(matches)}):",
                    reply_markup=keyboard
                )
                
            except Exception as e:
                await message.answer(f"❌ Ошибка чтения файлов: {str(e)}")
                await edit_schema_start(message, state)
    
    @dp.message(SchemaStates.entering_match_number)
    async def match_number_entered(message: types.Message, state: FSMContext):
        """Номер введен, показываем детали и варианты редактирования"""
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
        
        # Получаем выбранное сопоставление (индекс = номер - 1)
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
        
        # Предлагаем что изменить
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📝 Изменить WB столбец")],
                [KeyboardButton(text="📝 Изменить Ozon столбец")],
                [KeyboardButton(text="📝 Изменить Яндекс столбец")],
                [KeyboardButton(text="🗑 Удалить сопоставление")],
                [KeyboardButton(text="❌ Отмена")]
            ],
            resize_keyboard=True
        )
        
        await state.set_state(SchemaStates.selecting_column_to_edit)
        await message.answer("Что хочешь изменить?", reply_markup=keyboard)
    
    @dp.message(SchemaStates.selecting_column_to_edit)
    async def column_selected_for_edit(message: types.Message, state: FSMContext):
        """Выбран столбец для редактирования, показываем доступные варианты"""
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
        
        # НОВОЕ: Показываем список доступных столбцов
        text = f"📋 Доступные столбцы {display_name} ({len(columns_list)}):\n\n"
        
        for i, col in enumerate(columns_list, 1):
            text += f"{i}. {col}\n"
            
            # Telegram имеет лимит, разбиваем на части
            if i % 30 == 0:
                await message.answer(text)
                text = ""
        
        if text:
            await message.answer(text)
        
        await message.answer(
            f"Введи название столбца из списка выше или номер (1-{len(columns_list)}):",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
        
        await state.set_state(SchemaStates.selecting_new_column_value)
    
    @dp.message(SchemaStates.selecting_new_column_value)
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
        
        # НОВОЕ: Валидация
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
        match_data = data.get('edit_match_data')
        
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






    

    
    # СОЗДАНИЕ СХЕМЫ
    
    @dp.message(F.text == "➕ Создать схему")
    async def create_schema_start(message: types.Message, state: FSMContext):
        await state.set_state(SchemaStates.waiting_schema_name)
        await message.answer(
            "Введи название схемы:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="❌ Отмена")]],
                resize_keyboard=True
            )
        )
    
    @dp.message(SchemaStates.waiting_schema_name)
    async def schema_name_entered(message: types.Message, state: FSMContext):
        if message.text == "❌ Отмена":
            await schema_management(message, state)
            return
        
        schema_name = message.text.strip()
        user_id = message.from_user.id
        
        # Проверяем, не существует ли уже
        if db.get_schema(user_id, schema_name):
            await message.answer("❌ Схема с таким названием уже существует. Введи другое название:")
            return
        
        # Сохраняем название
        await state.update_data(schema_name=schema_name)
        
        user_schemas[user_id] = {}
        await state.set_state(SchemaStates.waiting_schema_files)
        
        await message.answer(
            f"✅ Название схемы: '{schema_name}'\n\n"
            "Теперь отправь 3 файла Excel для определения столбцов",
            reply_markup=ReplyKeyboardRemove()
        )
    
    @dp.message(SchemaStates.waiting_schema_files, F.document)
    async def handle_schema_file(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        
        if user_id not in user_schemas:
            user_schemas[user_id] = {}
        
        file = await bot.get_file(message.document.file_id)
        file_name = message.document.file_name
        
        os.makedirs(f"uploads/{user_id}", exist_ok=True)
        file_path = f"uploads/{user_id}/{file_name}"
        await bot.download_file(file.file_path, file_path)
        
        fn = file_name.lower()
        if 'wb' in fn or 'wildberries' in fn:
            marketplace = 'wildberries'
        elif 'ozon' in fn or 'озон' in fn:
            marketplace = 'ozon'
        elif 'yandex' in fn or 'яндекс' in fn or 'market' in fn:
            marketplace = 'yandex'
        else:
            await message.answer("❌ Переименуй файл (добавь wb/ozon/yandex)")
            return
        
        if marketplace in user_schemas[user_id]:
            await message.answer(f"⚠️ {marketplace.upper()} уже загружен")
            return
            
        user_schemas[user_id][marketplace] = file_path
        await message.answer(f"✅ {marketplace.upper()} ({len(user_schemas[user_id])}/3)")
        
        if len(user_schemas[user_id]) == 3:
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ Создать схему")]],
                resize_keyboard=True
            )
            await message.answer("✅ Все файлы загружены!", reply_markup=keyboard)
    
    @dp.message(F.text == "✅ Создать схему")
    async def finalize_schema_creation(message: types.Message, state: FSMContext):
        # Проверяем состояние
        current_state = await state.get_state()
        if current_state != SchemaStates.waiting_schema_files:
            await message.answer("❌ Сначала начни создание схемы через '➕ Создать схему'")
            return
        
        user_id = message.from_user.id
        
        if user_id not in user_schemas or len(user_schemas[user_id]) != 3:
            await message.answer("❌ Загрузи 3 файла!")
            return
        
        data = await state.get_data()
        schema_name = data.get('schema_name')
        
        if not schema_name:
            await message.answer("❌ Название схемы потеряно. Начни заново.")
            return
        
        await message.answer("⏳ Анализирую столбцы...")
        
        try:
            file_paths = user_schemas[user_id]
            
            # Читаем столбцы
            reader = ExcelReader()
            columns = {}
            
            for marketplace, file_path in file_paths.items():
                config = FILE_CONFIGS[marketplace]
                columns[marketplace] = reader.get_column_names(
                    file_path,
                    config['sheet_name'],
                    config['header_row']
                )
            
            await message.answer("🤖 AI сравнивает столбцы...")
            
            # Сравниваем с помощью AI
            comparator = AIComparator()
            comparison_result = comparator.compare_columns(
                columns['wildberries'],
                columns['ozon'],
                columns['yandex']
            )
            
            # Фильтруем совпадения по уверенности >= 85%
            total_matches = len(comparison_result.get('matches_all_three', []))
            filtered_matches = [
                match for match in comparison_result.get('matches_all_three', [])
                if match.get('confidence', 0) >= 0.85
            ]
            
            # Заменяем на отфильтрованные
            comparison_result['matches_all_three'] = filtered_matches
            matches_count = len(filtered_matches)
            skipped_count = total_matches - matches_count
            
            # Создаем схему в БД
            schema_id = db.create_schema(user_id, schema_name)
            
            if not schema_id:
                await message.answer("❌ Схема с таким названием уже существует!")
                return
            
            # Сохраняем сопоставления (уже отфильтрованные)
            db.save_schema_matches(schema_id, comparison_result)
            
            user_schemas[user_id] = {}
            await state.clear()
            
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📤 Загрузить файлы")],
                    [KeyboardButton(text="📋 Управление схемами")],
                    [KeyboardButton(text="📊 Моя статистика")]
                ],
                resize_keyboard=True
            )
            
            message_text = f"✅ Схема '{schema_name}' создана!\n\n"
            message_text += f"📊 Сохранено совпадений: {matches_count}"
            
            if skipped_count > 0:
                message_text += f"\n⚠️ Пропущено (< 85%): {skipped_count}"
            
            await message.answer(message_text, reply_markup=keyboard)
            
        except Exception as e:
            await message.answer(f"❌ Ошибка: {str(e)}")
            logging.error(f"Error creating schema: {e}", exc_info=True)
    
    # ОБНОВЛЕНИЕ СХЕМЫ
    
    @dp.message(F.text == "🔄 Обновить схему")
    async def update_schema_start(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        schemas = db.get_user_schemas(user_id)
        
        if not schemas:
            await message.answer("❌ У тебя нет схем!")
            return
        
        keyboard_buttons = []
        for schema in schemas:
            if schema.get('name'):  # <-- ДОБАВЬ ПРОВЕРКУ
                keyboard_buttons.append([KeyboardButton(text=schema['name'])])
        
        if not keyboard_buttons:  # <-- ДОБАВЬ ПРОВЕРКУ
            await message.answer("❌ У тебя нет валидных схем!")
            return
        
        keyboard_buttons.append([KeyboardButton(text="❌ Отмена")])
        
        keyboard = ReplyKeyboardMarkup(keyboard=keyboard_buttons, resize_keyboard=True)
        
        await state.set_state(SchemaStates.selecting_schema_to_update)
        await message.answer("Выбери схему для обновления:", reply_markup=keyboard)
    
    @dp.message(SchemaStates.selecting_schema_to_update)
    async def schema_selected_for_update(message: types.Message, state: FSMContext):
        if message.text == "❌ Отмена":
            await schema_management(message, state)
            return
        
        user_id = message.from_user.id
        schema = db.get_schema(user_id, message.text)
        
        if not schema:
            await message.answer("❌ Схема не найдена")
            return
        
        # Сохраняем И id И название схемы
        await state.update_data(
            update_schema_id=schema['id'], 
            update_schema_name=schema['name']  # <-- Убедись что сохраняется schema['name'] а не message.text
        )
        
        user_schemas[user_id] = {}
        await state.set_state(SchemaStates.waiting_update_files)
        
        await message.answer(
            f"✅ Схема '{schema['name']}' выбрана\n\n"  # <-- Используй schema['name']
            "Отправь 3 файла Excel для повторного анализа",
            reply_markup=ReplyKeyboardRemove()
        )
    
    @dp.message(SchemaStates.waiting_update_files, F.document)
    async def handle_update_file(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        
        if user_id not in user_schemas:
            user_schemas[user_id] = {}
        
        file = await bot.get_file(message.document.file_id)
        file_name = message.document.file_name
        
        os.makedirs(f"uploads/{user_id}", exist_ok=True)
        file_path = f"uploads/{user_id}/{file_name}"
        await bot.download_file(file.file_path, file_path)
        
        fn = file_name.lower()
        if 'wb' in fn or 'wildberries' in fn:
            marketplace = 'wildberries'
        elif 'ozon' in fn or 'озон' in fn:
            marketplace = 'ozon'
        elif 'yandex' in fn or 'яндекс' in fn or 'market' in fn:
            marketplace = 'yandex'
        else:
            await message.answer("❌ Переименуй файл (добавь wb/ozon/yandex)")
            return
        
        if marketplace in user_schemas[user_id]:
            await message.answer(f"⚠️ {marketplace.upper()} уже загружен")
            return
            
        user_schemas[user_id][marketplace] = file_path
        await message.answer(f"✅ {marketplace.upper()} ({len(user_schemas[user_id])}/3)")
        
        if len(user_schemas[user_id]) == 3:
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="✅ Обновить схему")]],  # <-- ИЗМЕНИЛ С "Создать" на "Обновить"
                resize_keyboard=True
            )
            await message.answer("✅ Все файлы загружены!", reply_markup=keyboard)
    
    @dp.message(F.text == "✅ Обновить схему")
    async def finalize_schema_update(message: types.Message, state: FSMContext):
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
                
                keyboard = ReplyKeyboardMarkup(
                    keyboard=[
                        [KeyboardButton(text="📤 Загрузить файлы")],
                        [KeyboardButton(text="📋 Управление схемами")],
                        [KeyboardButton(text="📊 Моя статистика")]
                    ],
                    resize_keyboard=True
                )
                
                await message.answer(
                    f"ℹ️ Все столбцы уже сопоставлены!\n\n"
                    f"Схема '{schema_name}' не требует обновления",
                    reply_markup=keyboard
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
            
            keyboard = ReplyKeyboardMarkup(
                keyboard=[
                    [KeyboardButton(text="📤 Загрузить файлы")],
                    [KeyboardButton(text="📋 Управление схемами")],
                    [KeyboardButton(text="📊 Моя статистика")]
                ],
                resize_keyboard=True
            )
            
            if new_count > 0:
                total_matches = len(existing_matches['matches_all_three'])
                message_text = f"✅ Схема '{schema_name}' обновлена!\n\n"
                message_text += f"➕ Добавлено новых совпадений: {new_count}\n"
                message_text += f"📊 Всего столбцов в схеме: {total_matches}"
                
                if skipped_count > 0:
                    message_text += f"\n⚠️ Пропущено (< 85%): {skipped_count}"
                
                await message.answer(message_text, reply_markup=keyboard)
            else:
                message_text = f"ℹ️ Новых совпадений не найдено\n\n"
                message_text += f"AI не нашел подходящих пар (>= 85%) среди оставшихся {total_remaining} столбцов"
                
                if skipped_count > 0:
                    message_text += f"\n⚠️ Пропущено (< 85%): {skipped_count}"
                
                await message.answer(message_text, reply_markup=keyboard)
            
        except Exception as e:
            await message.answer(f"❌ Ошибка: {str(e)}")
            logging.error(f"Error updating schema: {e}", exc_info=True)
    
    # УДАЛЕНИЕ СХЕМЫ
    
    @dp.message(F.text == "🗑 Удалить схему")
    async def delete_schema_start(message: types.Message, state: FSMContext):
        user_id = message.from_user.id
        schemas = db.get_user_schemas(user_id)
        
        if not schemas:
            await message.answer("❌ У тебя нет схем!")
            return
        
        keyboard_buttons = []
        for schema in schemas:
            if schema.get('name'):  # <-- ДОБАВЬ ПРОВЕРКУ
                keyboard_buttons.append([KeyboardButton(text=schema['name'])])
        
        if not keyboard_buttons:  # <-- ДОБАВЬ ПРОВЕРКУ
            await message.answer("❌ У тебя нет валидных схем!")
            return
        
        keyboard_buttons.append([KeyboardButton(text="❌ Отмена")])
        
        keyboard = ReplyKeyboardMarkup(keyboard=keyboard_buttons, resize_keyboard=True)
        
        await state.set_state(SchemaStates.selecting_schema_to_delete)
        await message.answer("Выбери схему для удаления:", reply_markup=keyboard)
    
    @dp.message(SchemaStates.selecting_schema_to_delete)
    async def schema_selected_for_deletion(message: types.Message, state: FSMContext):
        if message.text == "❌ Отмена":
            await schema_management(message, state)
            return
        
        user_id = message.from_user.id
        schema_name = message.text
        
        deleted = db.delete_schema(user_id, schema_name)
        
        await state.clear()
        
        keyboard = ReplyKeyboardMarkup(
            keyboard=[
                [KeyboardButton(text="📤 Загрузить файлы")],
                [KeyboardButton(text="📋 Управление схемами")],
                [KeyboardButton(text="📊 Моя статистика")]
            ],
            resize_keyboard=True
        )
        
        if deleted:
            await message.answer(
                f"✅ Схема '{schema_name}' удалена",
                reply_markup=keyboard
            )
        else:
            await message.answer(
                "❌ Не удалось удалить схему",
                reply_markup=keyboard
            )
    
    @dp.message(F.text == "◀️ Назад")
    async def go_back(message: types.Message, state: FSMContext):
        await cmd_start(message, state)
    
    @dp.message(F.text == "📊 Моя статистика")
    async def show_stats(message: types.Message):
        user_id = message.from_user.id
        stats = db.get_user_stats(user_id)
        
        if stats:
            text = f"""
📊 Твоя статистика:

✅ Всего обработок: {stats['total_processings']}
🎯 Успешных: {stats['successful']}
❌ С ошибками: {stats['failed']}
🔄 Синхронизировано ячеек: {stats['total_synced_cells']}
📅 Зарегистрирован: {stats['registered_at'][:10]}
"""
            await message.answer(text)
        else:
            await message.answer("Статистика не найдена")
    
    return bot, dp

async def start_bot():
    bot, dp = create_bot()
    print("🚀 Telegram бот запущен!")
    await dp.start_polling(bot)
