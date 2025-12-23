"""
Конвертеры значений между единицами измерения
"""

import pandas as pd
from typing import Optional
from utils.logger_config import setup_logger

logger = setup_logger('converters')


class ValueConverter:
    """Конвертация значений между единицами измерения"""
    
    @staticmethod
    def mm_to_cm(value: float) -> float:
        """Конвертирует миллиметры в сантиметры"""
        return value / 10
    
    @staticmethod
    def cm_to_mm(value: float) -> float:
        """Конвертирует сантиметры в миллиметры"""
        return value * 10
    
    @staticmethod
    def kg_to_g(value: float) -> float:
        """Конвертирует килограммы в граммы"""
        return value * 1000
    
    @staticmethod
    def g_to_kg(value: float) -> float:
        """Конвертирует граммы в килограммы"""
        return value / 1000
    
    @staticmethod
    def detect_unit(column_name: str) -> Optional[str]:
        """
        Определяет единицу измерения из названия столбца
        
        Args:
            column_name: название столбца
            
        Returns:
            Единица измерения ('kg', 'g', 'mm', 'cm') или None
        """
        if not column_name:
            return None
        
        column_lower = column_name.lower()
        
        # Определяем единицы веса
        if 'кг' in column_lower or 'kg' in column_lower:
            return 'kg'
        if ' г' in column_lower or ',г' in column_lower or 'gram' in column_lower or column_lower.endswith('г'):
            return 'g'
        
        # Определяем единицы длины/размера
        if 'мм' in column_lower or 'mm' in column_lower:
            return 'mm'
        if 'см' in column_lower or 'cm' in column_lower:
            return 'cm'
        
        return None
    
    @classmethod
    def convert_value(
        cls,
        value,
        from_unit: Optional[str],
        to_unit: Optional[str]
    ):
        """
        Конвертирует значение между единицами измерения
        
        Args:
            value: исходное значение
            from_unit: исходная единица измерения
            to_unit: целевая единица измерения
            
        Returns:
            Сконвертированное значение
        """
        # Если единицы измерения не определены или одинаковые - возвращаем как есть
        if not from_unit or not to_unit or from_unit == to_unit:
            return value
        
        # Если значение пустое или не числовое - возвращаем как есть
        if pd.isna(value):
            return value
        
        try:
            numeric_value = float(value)
        except (ValueError, TypeError):
            return value
        
        # Конвертация веса
        if from_unit == 'kg' and to_unit == 'g':
            result = cls.kg_to_g(numeric_value)
            logger.debug(f"[Конвертация] {numeric_value} кг → {result} г")
            return result
        elif from_unit == 'g' and to_unit == 'kg':
            result = cls.g_to_kg(numeric_value)
            logger.debug(f"[Конвертация] {numeric_value} г → {result} кг")
            return result
        
        # Конвертация размеров
        elif from_unit == 'mm' and to_unit == 'cm':
            result = cls.mm_to_cm(numeric_value)
            logger.debug(f"[Конвертация] {numeric_value} мм → {result} см")
            return result
        elif from_unit == 'cm' and to_unit == 'mm':
            result = cls.cm_to_mm(numeric_value)
            logger.debug(f"[Конвертация] {numeric_value} см → {result} мм")
            return result
        
        # Если конвертация не поддерживается - возвращаем исходное значение
        return value
    
    @staticmethod
    def smart_format(val: float) -> str:
        """
        Форматирует число: округляет до целого если близко, иначе до 1 знака
        
        Args:
            val: число для форматирования
            
        Returns:
            Строковое представление
        """
        if abs(val - round(val)) < 0.01:
            return str(int(round(val)))
        return f"{val:.1f}"
