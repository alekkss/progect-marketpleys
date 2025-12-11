"""
Telegram бот для синхронизации маркетплейсов
"""
import asyncio
import os
import logging
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, FSInputFile
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

from config import FILE_CONFIGS, TELEGRAM_BOT_TOKEN
from excel_reader import ExcelReader
from ai_comparator import AIComparator
from excel_writer import ExcelWriter
from data_synchronizer import DataSynchronizer

logging.basicConfig(level=logging.INFO)

class UploadStates(StatesGroup):
    waiting_for_files = State()

user_files = {}

def create_bot():
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    
    @dp.message(Command("start"))
    async def cmd_start(message: types.Message, state: FSMContext):
        await state.clear()
        user_files[message.from_user.id] = {}
        
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="📤 Загрузить файлы")]],
            resize_keyboard=True
        )
        
        await message.answer(
            "🤖 Бот синхронизации маркетплейсов\n\nЗагрузи 3 Excel файла",
            reply_markup=keyboard
        )

    @dp.message(F.text == "📤 Загрузить файлы")
    async def start_upload(message: types.Message, state: FSMContext):
        user_files[message.from_user.id] = {}
        await state.set_state(UploadStates.waiting_for_files)
        await message.answer("Отправь 3 файла Excel")

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
            await state.clear()

    @dp.message(F.text == "🚀 Обработать")
    async def process_files(message: types.Message):
        user_id = message.from_user.id
        
        if user_id not in user_files or len(user_files[user_id]) != 3:
            await message.answer("❌ Загрузи 3 файла!")
            return
        
        await message.answer("⏳ Обработка...")
        
        try:
            file_paths = user_files[user_id]
            
            await message.answer("📖 Читаю файлы...")
            reader = ExcelReader()
            columns = {}
            
            for marketplace, file_path in file_paths.items():
                config = FILE_CONFIGS[marketplace]
                columns[marketplace] = reader.get_column_names(
                    file_path,
                    config['sheet_name'],
                    config['header_row']
                )
            
            await message.answer("🤖 AI сравнивает...")
            comparator = AIComparator()
            comparison_result = comparator.compare_columns(
                columns['wildberries'],
                columns['ozon'],
                columns['yandex']
            )
            
            await message.answer("🔄 Синхронизирую...")
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_dir = f"output/{user_id}_{timestamp}"
            os.makedirs(output_dir, exist_ok=True)
            
            output_sync_paths = {
                'wildberries': f"{output_dir}/WB_синхронизировано.xlsx",
                'ozon': f"{output_dir}/Ozon_синхронизировано.xlsx",
                'yandex': f"{output_dir}/Яндекс_синхронизировано.xlsx"
            }
            
            synchronizer = DataSynchronizer(comparison_result)
            synced_dfs, changes_log = synchronizer.synchronize_data(file_paths, output_sync_paths)
            
            await message.answer("📊 Создаю отчет...")
            report_path = f"{output_dir}/результат_{timestamp}.xlsx"
            writer = ExcelWriter()
            writer.create_report_with_changes(comparison_result, changes_log, report_path)
            
            await message.answer("📤 Отправляю результаты...")
            
            for marketplace, path in output_sync_paths.items():
                doc = FSInputFile(path)
                await message.answer_document(doc)
            
            report_doc = FSInputFile(report_path)
            await message.answer_document(report_doc, caption="📊 Отчет")
            
            user_files[user_id] = {}
            
            keyboard = ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="📤 Загрузить файлы")]],
                resize_keyboard=True
            )
            await message.answer("✅ Готово!", reply_markup=keyboard)
            
        except Exception as e:
            await message.answer(f"❌ Ошибка: {str(e)}")
            logging.error(f"Error: {e}", exc_info=True)
    
    return bot, dp

async def start_bot():
    bot, dp = create_bot()
    print("🚀 Telegram бот запущен!")
    await dp.start_polling(bot)
