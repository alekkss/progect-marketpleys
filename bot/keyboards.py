"""
Клавиатуры для бота
"""
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove


def get_main_menu_keyboard():
    """Главное меню"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📤 Загрузить файлы")],
            [KeyboardButton(text="📋 Управление схемами")],
            [KeyboardButton(text="📊 Моя статистика")]
        ],
        resize_keyboard=True
    )


def get_schema_management_keyboard():
    """Меню управления схемами"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать схему")],
            [KeyboardButton(text="✏️ Редактировать схему")],
            [KeyboardButton(text="🔄 Обновить схему")],
            [KeyboardButton(text="🗑 Удалить схему")],
            [KeyboardButton(text="📋 Мои схемы")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )


def get_schema_edit_keyboard():
    """Меню редактирования схемы"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="👁 Просмотреть текущие сопоставления")],
            [KeyboardButton(text="✏️ Изменить сопоставление")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )


def get_cancel_keyboard():
    """Кнопка отмены"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )


def get_process_keyboard():
    """Кнопка обработки"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="🚀 Обработать")]],
        resize_keyboard=True
    )


def get_create_schema_keyboard():
    """Кнопка создания схемы"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ Создать схему")]],
        resize_keyboard=True
    )


def get_update_schema_keyboard():
    """Кнопка обновления схемы"""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="✅ Обновить схему")]],
        resize_keyboard=True
    )


def get_edit_column_keyboard():
    """Меню выбора столбца для редактирования"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📝 Изменить WB столбец")],
            [KeyboardButton(text="📝 Изменить Ozon столбец")],
            [KeyboardButton(text="📝 Изменить Яндекс столбец")],
            [KeyboardButton(text="🗑 Удалить сопоставление")],
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )


def get_back_to_edit_keyboard():
    """Возврат к редактированию"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✏️ Редактировать схему")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def get_edit_match_menu_keyboard():
    """Меню после загрузки файлов для редактирования"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✏️ Изменить сопоставление")],
            [KeyboardButton(text="➕ Добавить сопоставление")],  # НОВАЯ КНОПКА
            [KeyboardButton(text="❌ Отмена")]
        ],
        resize_keyboard=True
    )



def get_schema_list_keyboard(schemas):
    """Клавиатура со списком схем"""
    keyboard_buttons = []
    for schema in schemas:
        if schema.get('name'):
            keyboard_buttons.append([KeyboardButton(text=schema['name'])])
    
    if not keyboard_buttons:
        return None
    
    keyboard_buttons.append([KeyboardButton(text="❌ Отмена")])
    return ReplyKeyboardMarkup(keyboard=keyboard_buttons, resize_keyboard=True)
