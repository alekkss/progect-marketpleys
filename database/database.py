"""
Модуль для работы с базой данных SQLite
"""
import sqlite3
from datetime import datetime
from typing import Optional, Dict, List
import os
import sys
from pathlib import Path
from utils.logger_config import setup_logger
sys.path.insert(0, str(Path(__file__).parent.parent))

class Database:
    def __init__(self, db_path: str = "marketplace_sync.db"):
        self.db_path = db_path
        self.init_db()
    
    def get_connection(self):
        return sqlite3.connect(self.db_path)
    
    def init_db(self):
        """Инициализация базы данных"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Таблица пользователей
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                total_processings INTEGER DEFAULT 0
            )
        """)
        
        # Таблица истории обработок
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS processing_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                wb_products_count INTEGER DEFAULT 0,
                ozon_products_count INTEGER DEFAULT 0,
                yandex_products_count INTEGER DEFAULT 0,
                synced_cells_count INTEGER DEFAULT 0,
                status TEXT,
                error_message TEXT,
                FOREIGN KEY (user_id) REFERENCES users (user_id)
            )
        """)
        
        # Таблица файлов
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                processing_id INTEGER,
                marketplace TEXT,
                original_filename TEXT,
                file_path TEXT,
                uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                FOREIGN KEY (processing_id) REFERENCES processing_history (id)
            )
        """)
        
        # НОВАЯ таблица для схем
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS schemas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                schema_name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id),
                UNIQUE(user_id, schema_name)
            )
        """)
        
        # НОВАЯ таблица для сопоставлений столбцов в схеме
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
        
        conn.commit()
        conn.close()
    
    def add_user(self, user_id: int, username: str = None, 
                 first_name: str = None, last_name: str = None):
        """Добавляет пользователя или обновляет его данные"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO users (user_id, username, first_name, last_name, registered_at, total_processings)
            VALUES (?, ?, ?, ?, COALESCE((SELECT registered_at FROM users WHERE user_id = ?), CURRENT_TIMESTAMP),
                    COALESCE((SELECT total_processings FROM users WHERE user_id = ?), 0))
        """, (user_id, username, first_name, last_name, user_id, user_id))
        
        conn.commit()
        conn.close()
    
    def start_processing(self, user_id: int) -> int:
        """Начинает новую обработку, возвращает processing_id"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO processing_history (user_id, started_at, status)
            VALUES (?, ?, 'processing')
        """, (user_id, datetime.now()))
        
        processing_id = cursor.lastrowid
        conn.commit()
        conn.close()
        
        return processing_id
    
    def complete_processing(self, processing_id: int, 
                          wb_count: int, ozon_count: int, yandex_count: int,
                          synced_cells: int):
        """Завершает обработку успешно"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE processing_history
            SET completed_at = ?,
                wb_products_count = ?,
                ozon_products_count = ?,
                yandex_products_count = ?,
                synced_cells_count = ?,
                status = 'completed'
            WHERE id = ?
        """, (datetime.now(), wb_count, ozon_count, yandex_count, synced_cells, processing_id))
        
        # Увеличиваем счетчик обработок у пользователя
        cursor.execute("""
            UPDATE users
            SET total_processings = total_processings + 1
            WHERE user_id = (SELECT user_id FROM processing_history WHERE id = ?)
        """, (processing_id,))
        
        conn.commit()
        conn.close()
    
    def fail_processing(self, processing_id: int, error_message: str):
        """Завершает обработку с ошибкой"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            UPDATE processing_history
            SET completed_at = ?,
                status = 'failed',
                error_message = ?
            WHERE id = ?
        """, (datetime.now(), error_message, processing_id))
        
        conn.commit()
        conn.close()
    
    def add_file(self, user_id: int, processing_id: int, 
                 marketplace: str, original_filename: str, file_path: str):
        """Добавляет информацию о загруженном файле"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO files (user_id, processing_id, marketplace, original_filename, file_path)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, processing_id, marketplace, original_filename, file_path))
        
        conn.commit()
        conn.close()
    
    def get_user_stats(self, user_id: int) -> Optional[Dict]:
        """Получает статистику пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                u.total_processings,
                u.registered_at,
                COUNT(CASE WHEN ph.status = 'completed' THEN 1 END) as successful,
                COUNT(CASE WHEN ph.status = 'failed' THEN 1 END) as failed,
                SUM(CASE WHEN ph.status = 'completed' THEN ph.synced_cells_count ELSE 0 END) as total_synced
            FROM users u
            LEFT JOIN processing_history ph ON u.user_id = ph.user_id
            WHERE u.user_id = ?
            GROUP BY u.user_id
        """, (user_id,))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'total_processings': row[0],
                'registered_at': row[1],
                'successful': row[2],
                'failed': row[3],
                'total_synced_cells': row[4] or 0
            }
        return None
    
    def get_user_history(self, user_id: int, limit: int = 10) -> List[Dict]:
        """Получает историю обработок пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                id,
                started_at,
                completed_at,
                wb_products_count,
                ozon_products_count,
                yandex_products_count,
                synced_cells_count,
                status,
                error_message
            FROM processing_history
            WHERE user_id = ?
            ORDER BY started_at DESC
            LIMIT ?
        """, (user_id, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        history = []
        for row in rows:
            history.append({
                'id': row[0],
                'started_at': row[1],
                'completed_at': row[2],
                'wb_count': row[3],
                'ozon_count': row[4],
                'yandex_count': row[5],
                'synced_cells': row[6],
                'status': row[7],
                'error': row[8]
            })
        
        return history
    
    def create_schema(self, user_id: int, schema_name: str) -> int:
        """Создает новую схему"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                INSERT INTO schemas (user_id, schema_name)
                VALUES (?, ?)
            """, (user_id, schema_name))
            
            schema_id = cursor.lastrowid
            conn.commit()
            return schema_id
        except sqlite3.IntegrityError:
            return None  # Схема с таким именем уже существует
        finally:
            conn.close()

    def get_user_schemas(self, user_id: int) -> List[Dict]:
        """Получает список схем пользователя"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, schema_name, created_at, updated_at,
                (SELECT COUNT(*) FROM schema_matches WHERE schema_id = schemas.id) as matches_count
            FROM schemas
            WHERE user_id = ?
            ORDER BY updated_at DESC
        """, (user_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        schemas = []
        for row in rows:
            schemas.append({
                'id': row[0],
                'name': row[1],
                'created_at': row[2],
                'updated_at': row[3],
                'matches_count': row[4]
            })
        
        return schemas

    def get_schema(self, user_id: int, schema_name: str) -> Optional[Dict]:
        """Получает схему по имени"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT id, schema_name, created_at, updated_at
            FROM schemas
            WHERE user_id = ? AND schema_name = ?
        """, (user_id, schema_name))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'id': row[0],
                'name': row[1],
                'created_at': row[2],
                'updated_at': row[3]
            }
        return None

    def delete_schema(self, user_id: int, schema_name: str) -> bool:
        """Удаляет схему"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM schemas
            WHERE user_id = ? AND schema_name = ?
        """, (user_id, schema_name))
        
        deleted = cursor.rowcount > 0
        conn.commit()
        conn.close()
        
        return deleted

    def save_schema_matches(self, schema_id: int, comparison_result: Dict):
        """Сохраняет сопоставления столбцов в схему (только с уверенностью >= 85%)"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        # Очищаем старые сопоставления
        cursor.execute("DELETE FROM schema_matches WHERE schema_id = ?", (schema_id,))
        
        saved_count = 0
        skipped_count = 0
        
        # Сохраняем только совпадения с уверенностью >= 0.85
        for match in comparison_result.get('matches_all_three', []):
            confidence = match.get('confidence', 0)
            
            # ФИЛЬТР: только >= 85%
            if confidence >= 0.85:
                cursor.execute("""
                    INSERT INTO schema_matches (schema_id, wb_column, ozon_column, yandex_column, confidence, is_mandatory)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    schema_id,
                    match.get('column_1'),
                    match.get('column_2'),
                    match.get('column_3'),
                    confidence,
                    match.get('mandatory', False)
                ))
                saved_count += 1
            else:
                skipped_count += 1
        
        # Обновляем время изменения схемы
        cursor.execute("""
            UPDATE schemas
            SET updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
        """, (schema_id,))
        
        conn.commit()
        conn.close()
        
        print(f"[DB] Сохранено совпадений: {saved_count}, пропущено (confidence < 85%): {skipped_count}")

    def get_schema_matches(self, schema_id: int) -> Dict:
        """Получает сопоставления столбцов из схемы"""
        conn = self.get_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT wb_column, ozon_column, yandex_column, confidence, is_mandatory
            FROM schema_matches
            WHERE schema_id = ?
        """, (schema_id,))
        
        rows = cursor.fetchall()
        conn.close()
        
        matches = []
        for row in rows:
            matches.append({
                'column_1': row[0],
                'column_2': row[1],
                'column_3': row[2],
                'confidence': row[3],
                'mandatory': row[4]
            })
        
        # Возвращаем в формате comparison_result
        return {
            'matches_all_three': matches,
            'matches_1_2': [],
            'matches_1_3': [],
            'matches_2_3': [],
            'only_in_first': [],
            'only_in_second': [],
            'only_in_third': []
        }

    def update_schema_matches(self, schema_id: int, new_comparison_result: Dict):
        """Обновляет схему, добавляя новые совпадения"""
        # Получаем существующие совпадения
        existing_matches = self.get_schema_matches(schema_id)
        
        # Создаем set существующих комбинаций столбцов
        existing_set = set()
        for match in existing_matches['matches_all_three']:
            key = (match['column_1'], match['column_2'], match['column_3'])
            existing_set.add(key)
        
        # Добавляем новые совпадения
        new_count = 0
        for match in new_comparison_result.get('matches_all_three', []):
            key = (match.get('column_1'), match.get('column_2'), match.get('column_3'))
            if key not in existing_set:
                existing_matches['matches_all_three'].append(match)
                new_count += 1
        
        # Сохраняем обновленную схему
        if new_count > 0:
            self.save_schema_matches(schema_id, existing_matches)
        
        return new_count
