"""
Фоновая обработка файлов с поддержкой прогресса и отмены
"""

import asyncio
import logging
from typing import Dict, Callable, Optional
from datetime import datetime
from aiogram import Bot
from aiogram.types import FSInputFile
from bot.keyboards import get_main_menu_keyboard

from services.synchronizer import DataSynchronizer
from services.ai_comparator import AIComparator
from utils.excel_writer import ExcelWriter
from database.database import Database
from config.config import FILE_CONFIGS


logger = logging.getLogger('processor')


class ProcessingCancelled(Exception):
    """Исключение для отмены обработки"""
    pass


class BackgroundProcessor:
    """Процессор для фоновой обработки файлов"""
    
    def __init__(self, bot: Bot, db: Database):
        self.bot = bot
        self.db = db
        self.active_tasks = {}  # {processing_id: Task}
    
    async def process_files(
        self,
        user_id: int,
        chat_id: int,
        processing_id: int,
        schema_id: int,
        file_paths: Dict[str, str],
        output_paths: Dict[str, str],
        report_path: str,
        progress_message_id: int
    ):
        """
        Обрабатывает файлы в фоновом режиме с обновлением прогресса
        """
        try:
            logger.info(f"[Processing {processing_id}] Начало фоновой обработки")
            
            # Этап 1: Загрузка схемы (5%)
            await self._update_progress(processing_id, 5, "Загрузка схемы...")
            await self._edit_progress_message(chat_id, progress_message_id, 5, "Загрузка схемы...")
            
            if await self._is_cancelled(processing_id):
                raise ProcessingCancelled("Обработка отменена пользователем")
            
            comparison_result = self.db.get_schema_matches(schema_id)
            
            # Этап 2: Инициализация AI (10%)
            await self._update_progress(processing_id, 10, "Инициализация AI...")
            await self._edit_progress_message(chat_id, progress_message_id, 10, "Инициализация AI...")
            
            comparator = AIComparator()
            
            # Этап 3: Создание синхронизатора (15%)
            await self._update_progress(processing_id, 15, "Подготовка к синхронизации...")
            await self._edit_progress_message(chat_id, progress_message_id, 15, "Подготовка к синхронизации...")
            
            if await self._is_cancelled(processing_id):
                raise ProcessingCancelled("Обработка отменена пользователем")
            
            synchronizer = DataSynchronizer(comparison_result, ai_comparator=comparator)
            
            # Этап 4: Синхронизация (20% → 80%)
            await self._update_progress(processing_id, 20, "Синхронизация данных...")
            await self._edit_progress_message(chat_id, progress_message_id, 20, "Синхронизация данных...")
            
            # 🆕 Запускаем синхронизацию с более частым обновлением
            sync_task = asyncio.create_task(
                self._run_sync_with_cancel_check(
                    synchronizer, file_paths, output_paths, report_path, processing_id
                )
            )
            
            # Обновляем прогресс чаще и быстрее
            progress = 20
            update_interval = 2  # 🔧 Уменьшено с 5 до 2 секунд
            progress_step = 10    # 🔧 Увеличено с 5 до 10%
            
            while not sync_task.done():
                await asyncio.sleep(update_interval)
                
                if await self._is_cancelled(processing_id):
                    sync_task.cancel()
                    raise ProcessingCancelled("Обработка отменена пользователем")
                
                # Увеличиваем прогресс (до 80%)
                progress = min(progress + progress_step, 80)
                await self._update_progress(processing_id, progress, "Синхронизация данных...")
                await self._edit_progress_message(chat_id, progress_message_id, progress, "Синхронизация данных...")
            
            # Получаем результат
            synced_dfs, changes_log = await sync_task
            
            # Этап 5: Создание отчета (85%)
            await self._update_progress(processing_id, 85, "Создание отчета...")
            await self._edit_progress_message(chat_id, progress_message_id, 85, "Создание отчета...")
            
            if await self._is_cancelled(processing_id):
                raise ProcessingCancelled("Обработка отменена пользователем")
            
            # Создаем отчет
            writer = ExcelWriter()
            writer.create_report_with_changes(comparison_result, changes_log, report_path)
            
            # Добавляем AI логи если есть
            if hasattr(synchronizer, 'ai_validation_log') and synchronizer.ai_validation_log:
                logger.info(f"AI-логов найдено: {len(synchronizer.ai_validation_log)}")
                synchronizer._create_ai_log_sheet_in_report(report_path)
            
            # Этап 6: Подсчет результатов (90%)
            await self._update_progress(processing_id, 90, "Подготовка файлов...")
            await self._edit_progress_message(chat_id, progress_message_id, 90, "Подготовка файлов...")
            
            wb_count = len(synced_dfs['wildberries'])
            ozon_count = len(synced_dfs['ozon'])
            yandex_count = len(synced_dfs['yandex'])
            total_synced = sum(len(changes_log[mp]) for mp in changes_log)
            
            self.db.complete_processing(processing_id, wb_count, ozon_count, yandex_count, total_synced)
            
            # Этап 7: Отправка файлов (95%)
            await self._update_progress(processing_id, 95, "Отправка файлов...")
            await self._edit_progress_message(chat_id, progress_message_id, 95, "Отправка файлов...")
            
            # Отправляем файлы
            for marketplace, path in output_paths.items():
                doc = FSInputFile(path)
                await self.bot.send_document(chat_id=chat_id, document=doc)
            
            # Отправляем отчет
            report_doc = FSInputFile(report_path)
            await self.bot.send_document(
                chat_id=chat_id, 
                document=report_doc, 
                caption="📊 Отчет о синхронизации"
            )
            
            # Этап 8: Завершено (100%)
            await self._update_progress(processing_id, 100, "Завершено")
            
            # Финальное сообщение
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_message_id,
                text=(
                    f"✅ <b>Обработка завершена!</b>\n\n"
                    f"📊 Обработано товаров:\n"
                    f"  • WB: {wb_count}\n"
                    f"  • Ozon: {ozon_count}\n"
                    f"  • Яндекс: {yandex_count}\n\n"
                    f"🔄 Синхронизировано значений: {total_synced}"
                ),
                parse_mode="HTML"
            )
            
            # 🆕 ДОБАВЬ: Возвращаем главное меню
            await self.bot.send_message(
                chat_id=chat_id,
                text="Что дальше? 👇",
                reply_markup=get_main_menu_keyboard()
            )

            logger.info(f"[Processing {processing_id}] Обработка завершена успешно")
            
        except ProcessingCancelled as e:
            logger.info(f"[Processing {processing_id}] Обработка отменена: {e}")
            self.db.fail_processing(processing_id, str(e))
            
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_message_id,
                text="⏹ <b>Обработка отменена</b>",
                parse_mode="HTML"
            )

            # 🆕 НОВОЕ: Возвращаем главное меню
            await self.bot.send_message(
                chat_id=chat_id,
                text="Главное меню:",
                reply_markup=get_main_menu_keyboard()
            )
            
        except Exception as e:
            logger.error(f"[Processing {processing_id}] Ошибка: {e}", exc_info=True)
            self.db.fail_processing(processing_id, str(e))
            
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=progress_message_id,
                text=f"❌ <b>Ошибка при обработке:</b>\n\n<code>{str(e)}</code>",
                parse_mode="HTML"
            )

            # 🆕 НОВОЕ: Возвращаем главное меню
            await self.bot.send_message(
                chat_id=chat_id,
                text="Главное меню:",
                reply_markup=get_main_menu_keyboard()
            )
        
        finally:
            # Удаляем из активных задач
            if processing_id in self.active_tasks:
                del self.active_tasks[processing_id]
    
    async def _run_sync_with_cancel_check(
        self, 
        synchronizer: DataSynchronizer,
        file_paths: Dict,
        output_paths: Dict,
        report_path: str,
        processing_id: int
    ):
        """Запускает синхронизацию в executor с проверкой отмены"""
        loop = asyncio.get_event_loop()
        
        # Запускаем в executor (чтобы не блокировать event loop)
        return await loop.run_in_executor(
            None,
            synchronizer.synchronize_data,
            file_paths,
            output_paths,
            report_path
        )
    
    async def _update_progress(self, processing_id: int, progress: int, status: str):
        """Обновляет прогресс в БД"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE processing_history
            SET progress = ?, status = ?
            WHERE id = ?
        """, (progress, status, processing_id))
        
        conn.commit()
        conn.close()
    
    async def _edit_progress_message(
        self, 
        chat_id: int, 
        message_id: int, 
        progress: int, 
        status: str
    ):
        """Обновляет сообщение с прогрессом"""
        try:
            # Создаем прогресс-бар
            bar_length = 10
            filled = int(progress / 10)
            bar = "▰" * filled + "▱" * (bar_length - filled)
            
            text = (
                f"⏳ <b>{status}</b>\n\n"
                f"{bar} {progress}%"
            )
            
            # Добавляем кнопку отмены если прогресс < 95%
            from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
            
            keyboard = None
            if progress < 95:
                keyboard = InlineKeyboardMarkup(inline_keyboard=[
                    [InlineKeyboardButton(text="⏹ Отменить обработку", callback_data=f"cancel_{message_id}")]
                ])
            
            await self.bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode="HTML",
                reply_markup=keyboard
            )
        except Exception as e:
            # Игнорируем ошибки редактирования (например, если сообщение не изменилось)
            logger.debug(f"Ошибка обновления прогресса: {e}")
    
    async def _is_cancelled(self, processing_id: int) -> bool:
        """Проверяет, отменена ли обработка"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT can_cancel FROM processing_history WHERE id = ?
        """, (processing_id,))
        
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return result[0] == 0  # can_cancel = 0 означает отменено
        
        return False
    
    def cancel_processing(self, processing_id: int):
        """Отменяет обработку"""
        conn = self.db.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE processing_history
            SET can_cancel = 0
            WHERE id = ?
        """, (processing_id,))
        
        conn.commit()
        conn.close()
        
        logger.info(f"[Processing {processing_id}] Запрошена отмена")
