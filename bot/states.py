"""
FSM состояния для бота
"""
from aiogram.fsm.state import State, StatesGroup


class UploadStates(StatesGroup):
    """Состояния для загрузки файлов"""
    waiting_for_files = State()
    selecting_schema = State()


class SchemaStates(StatesGroup):
    """Состояния для работы со схемами"""
    # Создание
    creating_schema = State()
    waiting_schema_name = State()
    waiting_schema_files = State()
    
    # Управление
    managing_schema = State()
    
    # Обновление
    selecting_schema_to_update = State()
    waiting_update_files = State()
    
    # Удаление
    selecting_schema_to_delete = State()
    
    # Просмотр
    selecting_schema_to_view = State()
    viewing_schema_matches = State()
    
    # Редактирование
    selecting_schema_to_edit = State()
    waiting_edit_files = State()
    entering_match_number = State()
    selecting_column_to_edit = State()
    selecting_new_column_value = State()

    # НОВОЕ: Добавление нового сопоставления
    adding_new_match = State()              # Режим добавления
    selecting_wb_column = State()           # Выбор столбца WB
    selecting_ozon_column = State()         # Выбор столбца Ozon
    selecting_yandex_column = State()       # Выбор столбца Яндекс
