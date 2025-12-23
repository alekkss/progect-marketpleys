"""
Модуль синхронизации данных между маркетплейсами

Основные компоненты:
- DataSynchronizer: оркестратор синхронизации
- DimensionsSynchronizer: синхронизация габаритов
- ArticleAligner: выравнивание артикулов
- ValidationChain: валидация значений (5 уровней)
- ValueConverter: конвертация единиц измерения
"""

from .core import DataSynchronizer
from .dimensions import DimensionsSynchronizer
from .alignment import ArticleAligner
from .validation import ValidationChain
from .converters import ValueConverter
from .constants import ARTICLE_COLUMNS, DIMENSIONS_MAPPING, VALUE_SEPARATORS

__all__ = [
    'DataSynchronizer',
    'DimensionsSynchronizer',
    'ArticleAligner',
    'ValidationChain',
    'ValueConverter',
    'ARTICLE_COLUMNS',
    'DIMENSIONS_MAPPING',
    'VALUE_SEPARATORS'
]

__version__ = '2.0.0'
