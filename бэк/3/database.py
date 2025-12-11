"""
Модуль для работы с базой данных SQLite
"""
import sqlite3
from datetime import datetime
from typing import Optional, Dict, List
import os

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
