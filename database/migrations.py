"""
Система миграций базы данных
"""

import sqlite3
from typing import List, Tuple
from utils.logger_config import setup_logger

logger = setup_logger('migrations')


class Migration:
    """Базовый класс для миграции"""
    
    version: int
    description: str
    
    def up(self, cursor: sqlite3.Cursor):
        """Применить миграцию"""
        raise NotImplementedError
    
    def down(self, cursor: sqlite3.Cursor):
        """Откатить миграцию"""
        raise NotImplementedError


class Migration001AddIndexes(Migration):
    """Добавление индексов для оптимизации запросов"""
    
    version = 1
    description = "Добавление индексов на user_id и составных индексов"
    
    def up(self, cursor: sqlite3.Cursor):
        """Создаем индексы"""
        logger.info(f"[Migration {self.version}] {self.description}")
        
        # 1. Индекс на processing_history.user_id
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_processing_history_user_id 
            ON processing_history(user_id)
        """)
        logger.info("  ✅ Создан индекс: idx_processing_history_user_id")
        
        # 2. Составной индекс на processing_history(user_id, status)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_processing_history_user_status 
            ON processing_history(user_id, status)
        """)
        logger.info("  ✅ Создан индекс: idx_processing_history_user_status")
        
        # 3. Индекс на files.user_id
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_user_id 
            ON files(user_id)
        """)
        logger.info("  ✅ Создан индекс: idx_files_user_id")
        
        # 4. Индекс на files.processing_id
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_files_processing_id 
            ON files(processing_id)
        """)
        logger.info("  ✅ Создан индекс: idx_files_processing_id")
        
        # 5. Индекс на schemas.user_id
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_schemas_user_id 
            ON schemas(user_id)
        """)
        logger.info("  ✅ Создан индекс: idx_schemas_user_id")
        
        logger.info(f"[Migration {self.version}] Завершена успешно")
    
    def down(self, cursor: sqlite3.Cursor):
        """Удаляем индексы"""
        logger.info(f"[Migration {self.version}] Откат миграции")
        
        cursor.execute("DROP INDEX IF EXISTS idx_processing_history_user_id")
        cursor.execute("DROP INDEX IF EXISTS idx_processing_history_user_status")
        cursor.execute("DROP INDEX IF EXISTS idx_files_user_id")
        cursor.execute("DROP INDEX IF EXISTS idx_files_processing_id")
        cursor.execute("DROP INDEX IF EXISTS idx_schemas_user_id")
        
        logger.info(f"[Migration {self.version}] Откат завершен")


class Migration002RemoveLegacyTable(Migration):
    """Удаление legacy таблицы schema_matches"""
    
    version = 2
    description = "Удаление таблицы schema_matches (данные в JSON)"
    
    def up(self, cursor: sqlite3.Cursor):
        """Удаляем таблицу"""
        logger.info(f"[Migration {self.version}] {self.description}")
        
        # Проверяем что данные мигрированы в JSON
        cursor.execute("SELECT COUNT(*) FROM schemas WHERE full_comparison_json IS NULL OR full_comparison_json = ''")
        empty_count = cursor.fetchone()[0]
        
        if empty_count > 0:
            logger.warning(f"  ⚠️ Найдено {empty_count} схем без JSON! Миграция пропущена")
            logger.warning("  ℹ️ Сначала пересоздайте схемы или обновите их")
            return
        
        # Удаляем таблицу
        cursor.execute("DROP TABLE IF EXISTS schema_matches")
        logger.info("  ✅ Таблица schema_matches удалена")
        
        logger.info(f"[Migration {self.version}] Завершена успешно")
    
    def down(self, cursor: sqlite3.Cursor):
        """Восстанавливаем таблицу (без данных)"""
        logger.info(f"[Migration {self.version}] Откат миграции")
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                schema_id INTEGER,
                wb_column TEXT,
                ozon_column TEXT,
                yandex_column TEXT,
                confidence REAL,
                is_mandatory BOOLEAN DEFAULT 0,
                FOREIGN KEY (schema_id) REFERENCES schemas (id) ON DELETE CASCADE
            )
        """)
        
        logger.info(f"[Migration {self.version}] Откат завершен (таблица создана без данных)")

class Migration003AddProcessingProgress(Migration):
    """Добавление полей для отслеживания прогресса"""
    
    version = 3
    description = "Добавление progress и can_cancel в processing_history"
    
    def up(self, cursor: sqlite3.Cursor):
        """Добавляем поля"""
        logger.info(f"[Migration {self.version}] {self.description}")
        
        # Проверяем существование столбцов
        cursor.execute("PRAGMA table_info(processing_history)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'progress' not in columns:
            cursor.execute("""
                ALTER TABLE processing_history 
                ADD COLUMN progress INTEGER DEFAULT 0
            """)
            logger.info("  ✅ Добавлено поле: progress")
        
        if 'can_cancel' not in columns:
            cursor.execute("""
                ALTER TABLE processing_history 
                ADD COLUMN can_cancel BOOLEAN DEFAULT 1
            """)
            logger.info("  ✅ Добавлено поле: can_cancel")
        
        logger.info(f"[Migration {self.version}] Завершена успешно")
    
    def down(self, cursor: sqlite3.Cursor):
        """SQLite не поддерживает DROP COLUMN, создаем таблицу заново"""
        logger.info(f"[Migration {self.version}] Откат миграции")
        logger.warning("  ⚠️ SQLite не поддерживает DROP COLUMN")
        logger.warning("  ℹ️ Поля останутся, но не будут использоваться")

class MigrationManager:
    """Менеджер миграций"""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.migrations: List[Migration] = [
            Migration001AddIndexes(),
            Migration002RemoveLegacyTable(),
            Migration003AddProcessingProgress()
        ]
        self._init_migrations_table()
    
    def _init_migrations_table(self):
        """Создает таблицу для отслеживания миграций"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                description TEXT,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.commit()
        conn.close()
    
    def get_current_version(self) -> int:
        """Получает текущую версию БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("SELECT MAX(version) FROM schema_migrations")
        result = cursor.fetchone()[0]
        
        conn.close()
        return result or 0
    
    def get_pending_migrations(self) -> List[Migration]:
        """Получает список непримененных миграций"""
        current_version = self.get_current_version()
        return [m for m in self.migrations if m.version > current_version]
    
    def migrate(self) -> int:
        """
        Применяет все непримененные миграции
        
        Returns:
            Количество примененных миграций
        """
        pending = self.get_pending_migrations()
        
        if not pending:
            logger.info("✅ База данных актуальна, миграции не требуются")
            return 0
        
        logger.info(f"📦 Найдено {len(pending)} новых миграций")
        
        conn = sqlite3.connect(self.db_path)
        applied_count = 0
        
        try:
            for migration in pending:
                cursor = conn.cursor()
                
                # Применяем миграцию
                migration.up(cursor)
                
                # Записываем версию
                cursor.execute(
                    "INSERT INTO schema_migrations (version, description) VALUES (?, ?)",
                    (migration.version, migration.description)
                )
                
                conn.commit()
                applied_count += 1
            
            logger.info(f"✅ Применено {applied_count} миграций")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при применении миграций: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
        
        return applied_count
    
    def rollback(self, target_version: int = None):
        """
        Откатывает миграции до указанной версии
        
        Args:
            target_version: версия, до которой откатить (или None для полного отката)
        """
        current_version = self.get_current_version()
        
        if target_version is None:
            target_version = 0
        
        if target_version >= current_version:
            logger.info("✅ Откат не требуется")
            return
        
        # Получаем миграции для отката (в обратном порядке)
        to_rollback = [m for m in reversed(self.migrations) if target_version < m.version <= current_version]
        
        logger.info(f"🔄 Откат {len(to_rollback)} миграций (до версии {target_version})")
        
        conn = sqlite3.connect(self.db_path)
        
        try:
            for migration in to_rollback:
                cursor = conn.cursor()
                
                # Откатываем миграцию
                migration.down(cursor)
                
                # Удаляем запись о версии
                cursor.execute("DELETE FROM schema_migrations WHERE version = ?", (migration.version,))
                
                conn.commit()
                logger.info(f"  ✅ Откачена миграция {migration.version}")
            
            logger.info(f"✅ Откат завершен")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при откате: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()
    
    def status(self):
        """Выводит статус миграций"""
        current_version = self.get_current_version()
        pending = self.get_pending_migrations()
        
        logger.info("="*60)
        logger.info("СТАТУС МИГРАЦИЙ")
        logger.info("="*60)
        logger.info(f"Текущая версия: {current_version}")
        logger.info(f"Доступно миграций: {len(self.migrations)}")
        logger.info(f"Ожидают применения: {len(pending)}")
        
        if pending:
            logger.info("\nНепримененные миграции:")
            for m in pending:
                logger.info(f"  • v{m.version}: {m.description}")
        else:
            logger.info("\n✅ Все миграции применены")
        
        logger.info("="*60)
