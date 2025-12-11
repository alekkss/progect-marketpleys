"""
Конфигурация приложения
"""

import os
from dotenv import load_dotenv

load_dotenv()

# OpenRouter API настройки
OPENROUTER_API_KEY = "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"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
AI_MODEL = "google/gemini-2.5-flash-preview-09-2025"
AI_TEMPERATURE = 0.1

# Конфигурация файлов для каждого маркетплейса
FILE_CONFIGS = {
    "wildberries": {
        "sheet_name": "Товары",
        "header_row": 3,
        "display_name": "WB Товары"
    },
    "ozon": {
        "sheet_name": "Шаблон",
        "header_row": 2,
        "display_name": "Ozon Шаблон"
    },
    "yandex": {
        "sheet_name": "Данные о товарах",
        "header_row": 4,
        "display_name": "Яндекс Данные"
    }
}

# Обязательные совпадения (всегда должны быть сопоставлены)
MANDATORY_MATCHES = [
    {
        "column_1": "Артикул продавца",
        "column_2": "Артикул*",
        "column_3": "Ваш SKU *",
        "description": "Уникальный артикул товара"
    },
    {
        "column_1": "Баркоды",
        "column_2": "Штрихкод (Серийный номер / EAN)",
        "column_3": "Штрихкод *",
        "description": "Штрихкод товара"
    },
    {
        "column_1": "Бренд",
        "column_2": "Бренд*",
        "column_3": "Бренд *",
        "description": "Бренд производителя"
    },
    {
        "column_1": "Наименование",
        "column_2": "Название товара",
        "column_3": "Название товара *",
        "description": "Название товара"
    },
    {
        "column_1": "Описание",
        "column_2": "Аннотация",
        "column_3": "Описание товара *",
        "description": "Описание товара"
    },
    {
        "column_1": "Вес с упаковкой (кг)",
        "column_2": "Вес в упаковке, г*",
        "column_3": "Вес с упаковкой, кг",
        "description": "Вес товара с упаковкой"
    },
    # ИСКЛЮЧЕНО: Цена больше не обязательное совпадение
    # {
    #     "column_1": "Цена",
    #     "column_2": "Цена, руб.*",
    #     "column_3": "Цена *",
    #     "description": "Цена товара"
    # },
    {
        "column_1": "Фото",
        "column_2": "Ссылки на дополнительные фото",
        "column_3": "Ссылка на изображение *",
        "description": "Фотографии товара"
    }
]

# НОВОЕ: Список столбцов-исключений (не сравнивать и не синхронизировать)
EXCLUDED_COLUMNS = [
    # Цены (каждый маркетплейс устанавливает свои цены)
    "Цена",
    "Цена, руб.*",
    "Цена *",
    "Розничная цена",
    "Цена до скидки",
    "Старая цена",
    "Rich-контент JSON",
    
    # Можно добавить другие столбцы, например:
    # "Остаток",
    # "Количество",
    # "Скидка",
]

# Функция для проверки, является ли столбец исключенным
def is_excluded_column(column_name: str) -> bool:
    """
    Проверяет, находится ли столбец в списке исключений
    
    Args:
        column_name: название столбца
    
    Returns:
        True если столбец исключен, False в противном случае
    """
    if not column_name:
        return False
    
    column_lower = column_name.strip().lower()
    
    for excluded in EXCLUDED_COLUMNS:
        if excluded.strip().lower() == column_lower:
            return True
    
    return False