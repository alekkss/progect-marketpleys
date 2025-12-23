"""
Валидация значений через AI и правила маркетплейсов
"""

import re
import pandas as pd
from typing import Optional, List, Dict
from utils.logger_config import setup_logger
from .constants import VALUE_SEPARATORS

logger = setup_logger('validation')


class ValidationChain:
    """Цепочка валидации значений (5 уровней)"""
    
    def __init__(self, ai_comparator=None):
        """
        Args:
            ai_comparator: экземпляр AIComparator для AI-валидации
        """
        self.ai_comparator = ai_comparator
        self.ai_validation_log = []  # Логи AI-сопоставлений
    
    def validate_value(
        self, 
        value, 
        marketplace: str, 
        column_name: str,
        allowed_values: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Проверяет значение через цепочку валидаторов (5 уровней)
        
        Args:
            value: исходное значение
            marketplace: 'wildberries', 'ozon', 'yandex'
            column_name: название столбца
            allowed_values: список допустимых значений
            
        Returns:
            Сопоставленное значение или None
        """
        if not allowed_values:
            return None
        
        value_str = str(value).strip()
        
        # Уровень 1: Точное совпадение
        result = self._exact_match(value_str, allowed_values)
        if result:
            self._log_match(value_str, result, 'Точное совпадение', marketplace, column_name)
            return result
        
        # Уровень 2: Нормализация (регистр + ё/е)
        result = self._normalized_match(value_str, allowed_values)
        if result:
            self._log_match(value_str, result, 'Нормализация (регистр/ё-е)', marketplace, column_name)
            return result
        
        # Уровень 3: Извлечение числа
        result = self._number_match(value_str, allowed_values)
        if result:
            self._log_match(value_str, result, 'Извлечение числа', marketplace, column_name)
            return result
        
        # Уровень 4: Частичное совпадение (по словам)
        result = self._partial_match(value_str, allowed_values)
        if result:
            self._log_match(value_str, result, 'Частичное совпадение (слова)', marketplace, column_name)
            return result
        
        # Уровень 5: AI-запрос
        if self.ai_comparator:
            result = self._ai_match(value_str, allowed_values, column_name)
            if result:
                self._log_match(value_str, result, 'AI запрос', marketplace, column_name)
                return result
        
        logger.warning(f"❌ Не найдено совпадение для '{value_str}' в столбце '{column_name}'")
        return None
    
    def validate_multiple_values(
        self, 
        value, 
        marketplace: str, 
        column_name: str,
        allowed_values: Optional[List[str]] = None
    ) -> Optional[str]:
        """
        Валидирует значения с разделителями (;) и форматирует согласно требованиям маркетплейса
        
        Args:
            value: исходное значение (может содержать ";")
            marketplace: 'wildberries', 'ozon', 'yandex'
            column_name: название столбца
            allowed_values: список допустимых значений
            
        Returns:
            Отформатированная строка или None
        """
        if not value:
            return None
        
        value_str = str(value).strip()
        
        # Проверяем есть ли разделители
        if ';' not in value_str:
            # Одно значение - обычная валидация
            return self.validate_value(value_str, marketplace, column_name, allowed_values)
        
        # Множественные значения - валидируем каждое
        parts = [p.strip() for p in value_str.split(';') if p.strip()]
        validated_parts = []
        
        for part in parts:
            validated = self.validate_value(part, marketplace, column_name, allowed_values)
            if validated:
                validated_parts.append(validated)
            else:
                # Если хотя бы одно значение не прошло валидацию - возвращаем None
                logger.warning(f"⚠️ [{marketplace}] Часть '{part}' не прошла валидацию для '{column_name}'")
                return None
        
        if not validated_parts:
            return None
        
        # Форматируем согласно правилам маркетплейса
        separator = VALUE_SEPARATORS.get(marketplace)
        
        if marketplace == 'wildberries':
            # WB принимает только первое значение
            result = validated_parts[0]
            if len(validated_parts) > 1:
                logger.info(f"ℹ️ [WB] Оставлено только первое значение: '{result}' (было {len(validated_parts)})")
            return result
        elif separator:
            # Ozon: "; ", Yandex: ", "
            return separator.join(validated_parts)
        
        return None
    
    @staticmethod
    def _normalize(text: str) -> str:
        """Нормализует текст: нижний регистр, ё→е"""
        return text.lower().replace('ё', 'е').strip()
    
    @staticmethod
    def _extract_number(text: str) -> Optional[str]:
        """Извлекает первое число из строки типа '1 шт', '2 компрессора'"""
        numbers = re.findall(r'\d+', text)
        return numbers[0] if numbers else None
    
    def _exact_match(self, value: str, allowed_values: List[str]) -> Optional[str]:
        """Уровень 1: Точное совпадение"""
        if value in allowed_values:
            logger.info(f"[Валидация] ТОЧНОЕ совпадение: '{value}'")
            return value
        return None
    
    def _normalized_match(self, value: str, allowed_values: List[str]) -> Optional[str]:
        """Уровень 2: Совпадение с нормализацией"""
        value_normalized = self._normalize(value)
        for allowed in allowed_values:
            if self._normalize(allowed) == value_normalized:
                logger.info(f"[Валидация] Совпадение с нормализацией: '{value}' → '{allowed}'")
                return allowed
        return None
    
    def _number_match(self, value: str, allowed_values: List[str]) -> Optional[str]:
        """Уровень 3: Совпадение по числу"""
        number = self._extract_number(value)
        if not number:
            return None
        
        # Проверяем точное совпадение числа
        if number in allowed_values:
            logger.info(f"[Валидация] Извлечено число: '{value}' → '{number}'")
            return number
        
        # Проверяем с нормализацией
        for allowed in allowed_values:
            if self._extract_number(allowed) == number:
                logger.info(f"[Валидация] Совпадение по числу: '{value}' → '{allowed}'")
                return allowed
        
        return None
    
    def _partial_match(self, value: str, allowed_values: List[str]) -> Optional[str]:
        """Уровень 4: Частичное совпадение (по словам)"""
        value_words = set(self._normalize(value).split())
        for allowed in allowed_values:
            allowed_words = set(self._normalize(allowed).split())
            
            # Если все слова из value есть в allowed
            if value_words and value_words.issubset(allowed_words):
                logger.info(f"[Валидация] Частичное совпадение: '{value}' → '{allowed}'")
                return allowed
        
        return None
    
    def _ai_match(self, value: str, allowed_values: List[str], column_name: str) -> Optional[str]:
        """Уровень 5: AI-запрос"""
        logger.info(f"🤖 [AI] Проверяю '{value}' для столбца '{column_name}'...")
        
        matched_value = self.ai_comparator.match_value_with_list(
            value, 
            allowed_values, 
            column_name=column_name
        )
        
        if matched_value:
            logger.info(f"✅ [AI] Найдено: '{value}' → '{matched_value}'")
            return matched_value
        else:
            logger.warning(f"❌ [AI] Не найдено совпадение для '{value}'")
            return None
    
    def _log_match(
        self, 
        original: str, 
        matched: str, 
        method: str, 
        marketplace: str, 
        column_name: str
    ):
        """Записывает успешное сопоставление в лог"""
        self.ai_validation_log.append({
            'Маркетплейс': marketplace.upper(),
            'Столбец': column_name,
            'Исходное значение': original,
            'Сопоставлено с': matched,
            'Метод': method
        })
