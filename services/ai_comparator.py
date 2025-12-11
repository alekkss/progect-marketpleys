"""
Модуль для сравнения столбцов с помощью AI
"""
from openai import OpenAI
import json
import re
from typing import List, Dict, Set, Optional
from config.config import (
    OPENROUTER_API_KEY, 
    OPENROUTER_BASE_URL, 
    AI_MODEL, 
    AI_TEMPERATURE,
    MANDATORY_MATCHES,
    is_excluded_column
)
from utils.logger_config import setup_logger
from utils.excel_reader import ExcelReader
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))


class AIComparator:
    """Класс для сравнения столбцов с использованием AI"""
    
    def __init__(self):
        """Инициализация клиента OpenAI"""
        self.client = OpenAI(
            base_url=OPENROUTER_BASE_URL,
            api_key=OPENROUTER_API_KEY,
        )
    
    def compare_columns(
        self, 
        columns_1: List[str], 
        columns_2: List[str], 
        columns_3: List[str]
    ) -> Dict:
        """
        Сравнивает столбцы из трех файлов с помощью AI (с двойной проверкой)
        
        Args:
            columns_1: столбцы из первого файла
            columns_2: столбцы из второго файла
            columns_3: столбцы из третьего файла
        
        Returns:
            Словарь с результатами сравнения
        """
        # НОВОЕ: Фильтруем исключенные столбцы
        filtered_1, excluded_1 = self._filter_excluded_columns(columns_1)
        filtered_2, excluded_2 = self._filter_excluded_columns(columns_2)
        filtered_3, excluded_3 = self._filter_excluded_columns(columns_3)
        
        if excluded_1 or excluded_2 or excluded_3:
            print(f"\n[!] Исключены из сравнения:")
            if excluded_1:
                print(f"    WB: {', '.join(excluded_1)}")
            if excluded_2:
                print(f"    Ozon: {', '.join(excluded_2)}")
            if excluded_3:
                print(f"    Яндекс: {', '.join(excluded_3)}")
        
        print("\n[*] Отправляю ПЕРВЫЙ запрос в OpenRouter AI...")
        
        # Первый проход (с отфильтрованными столбцами)
        prompt = self._build_prompt(filtered_1, filtered_2, filtered_3)
        response = self._call_ai(prompt)
        result = self._parse_response(response)
        result = self._add_mandatory_matches(result, filtered_1, filtered_2, filtered_3)
        
        print(f"[+] Первый проход завершен!")
        print(f"    Найдено совпадений (все 3): {len(result.get('matches_all_three', []))}")
        print(f"    Найдено совпадений (1-2): {len(result.get('matches_1_2', []))}")
        print(f"    Найдено совпадений (1-3): {len(result.get('matches_1_3', []))}")
        print(f"    Найдено совпадений (2-3): {len(result.get('matches_2_3', []))}")
        
        # Второй проход - проверяем оставшиеся несовпавшие столбцы
        print("\n[*] Запускаю ВТОРОЙ проход для проверки оставшихся столбцов...")
        remaining_columns = self._get_remaining_columns(result, filtered_1, filtered_2, filtered_3)
        
        if remaining_columns[0] or remaining_columns[1] or remaining_columns[2]:
            print(f"    Осталось проверить: WB={len(remaining_columns[0])}, "
                  f"Ozon={len(remaining_columns[1])}, Яндекс={len(remaining_columns[2])}")
            
            second_result = self._second_pass_comparison(remaining_columns)
            
            # Объединяем результаты
            result = self._merge_results(result, second_result)
            
            print(f"[+] Второй проход завершен!")
            print(f"    Дополнительно найдено совпадений (все 3): {len(second_result.get('matches_all_three', []))}")
            print(f"    Дополнительно найдено совпадений (1-2): {len(second_result.get('matches_1_2', []))}")
            print(f"    Дополнительно найдено совпадений (1-3): {len(second_result.get('matches_1_3', []))}")
            print(f"    Дополнительно найдено совпадений (2-3): {len(second_result.get('matches_2_3', []))}")
        else:
            print("    Все столбцы уже сопоставлены, второй проход не требуется")
        
        # НОВОЕ: Добавляем исключенные столбцы в результат
        result = self._add_excluded_to_result(result, excluded_1, excluded_2, excluded_3)
        
        print("\n[+] Итоговые результаты получены от AI")
        return result
    
    def _filter_excluded_columns(self, columns: List[str]) -> tuple:
        """
        Фильтрует исключенные столбцы
        
        Returns:
            Кортеж (список разрешенных столбцов, список исключенных столбцов)
        """
        allowed = []
        excluded = []
        
        for col in columns:
            if is_excluded_column(col):
                excluded.append(col)
            else:
                allowed.append(col)
        
        return allowed, excluded
    
    def _add_excluded_to_result(
        self, 
        result: Dict, 
        excluded_1: List[str],
        excluded_2: List[str],
        excluded_3: List[str]
    ) -> Dict:
        """
        Добавляет исключенные столбцы в уникальные
        """
        result['only_in_first'].extend(excluded_1)
        result['only_in_second'].extend(excluded_2)
        result['only_in_third'].extend(excluded_3)
        
        return result
    
    def _get_remaining_columns(
        self, 
        result: Dict, 
        columns_1: List[str], 
        columns_2: List[str], 
        columns_3: List[str]
    ) -> tuple:
        """
        Получает списки столбцов, которые НЕ вошли в совпадения всех трех маркетплейсов
        
        Returns:
            Кортеж из трех списков несопоставленных столбцов
        """
        # Собираем только те столбцы, которые УЖЕ вошли в совпадения всех трех
        matched_in_all_three_1 = set()
        matched_in_all_three_2 = set()
        matched_in_all_three_3 = set()
        
        # Только из совпадений всех трех маркетплейсов!
        for match in result.get('matches_all_three', []):
            col_1 = match.get('column_1')
            col_2 = match.get('column_2')
            col_3 = match.get('column_3')
            
            if col_1:
                matched_in_all_three_1.add(col_1)
            if col_2:
                matched_in_all_three_2.add(col_2)
            if col_3:
                matched_in_all_three_3.add(col_3)
        
        # Убираем пустые значения
        matched_in_all_three_1.discard('')
        matched_in_all_three_2.discard('')
        matched_in_all_three_3.discard('')
        
        # Формируем списки оставшихся столбцов (которые НЕ вошли в matches_all_three)
        remaining_1 = [col for col in columns_1 if col and col not in matched_in_all_three_1]
        remaining_2 = [col for col in columns_2 if col and col not in matched_in_all_three_2]
        remaining_3 = [col for col in columns_3 if col and col not in matched_in_all_three_3]
        
        return (remaining_1, remaining_2, remaining_3)
        
    def _second_pass_comparison(self, remaining_columns: tuple) -> Dict:
        """
        Выполняет второй проход сравнения для оставшихся столбцов
        
        Args:
            remaining_columns: кортеж из трех списков оставшихся столбцов
        
        Returns:
            Результаты второго прохода
        """
        remaining_1, remaining_2, remaining_3 = remaining_columns
        
        # Специальный промпт для второго прохода
        prompt = f"""Ты - эксперт по сопоставлению полей товарных каталогов маркетплейсов. 

    ЭТО ВТОРОЙ ПРОХОД ПРОВЕРКИ! 

    Первый AI-анализ уже нашел некоторые совпадения между всеми тремя маркетплейсами.
    Но остались столбцы, которые НЕ вошли в совпадения всех трех маркетплейсов.

    ТВОЯ ГЛАВНАЯ ЗАДАЧА - найти ДОПОЛНИТЕЛЬНЫЕ совпадения именно МЕЖДУ ВСЕМИ ТРЕМЯ маркетплейсами!
    Это очень важно! Приоритет - найти столбцы, которые есть во ВСЕХ ТРЕХ файлах.

    ВАЖНЫЕ ПРАВИЛА СЕМАНТИЧЕСКОГО СРАВНЕНИЯ:

    1. ОПИСАНИЯ И ТЕКСТЫ:
    - "Описание" = "Аннотация" = "Описание товара" = "Rich-контент" = "Описание характеристик"
    - "Комплектация" = "Комплект поставки" = "Что в комплекте" = "Состав комплекта"
    - "Характеристики" = "Технические характеристики" = "Параметры"

    2. РАЗМЕРЫ (ЛЮБЫЕ ЕДИНИЦЫ ИЗМЕРЕНИЯ - ЭТО ОДНО ПОЛЕ!):
    - "Высота" = "Высота, мм" = "Высота, см" = "Высота товара" = "Высота предмета"
    - "Ширина" = "Ширина, мм" = "Ширина, см" = "Ширина товара"
    - "Длина" = "Длина, мм" = "Длина, см" = "Глубина"
    - "Габариты" может включать несколько размеров в одном поле
    - "Вес" = "Вес товара" = "Масса" = "Вес, г" = "Вес, кг"

    3. ИЗОБРАЖЕНИЯ И МЕДИА:
    - "Фото" = "Изображение" = "Картинка" = "Ссылка на фото" = "URL изображения"
    - "Дополнительные фото" = "Доп. фото" = "Галерея" = "Фото 1", "Фото 2" и т.д.
    - "Видео" = "Ссылка на видео" = "Видеоконтент" = "Фото 360"

    4. ТЕХНИЧЕСКИЕ ХАРАКТЕРИСТИКИ:
    - "Цвет" = "Цвет товара" = "Основной цвет" = "Расцветка" = "Цвет для фильтра"
    - "Материал" = "Материал изготовления" = "Состав"
    - "Страна" = "Страна производства" = "Страна-производитель" = "Страна-изготовитель"
    - "Гарантия" = "Гарантийный срок" = "Срок гарантии"
    - "Производитель" = "Изготовитель" = "Бренд" = "Торговая марка"

    5. КАТЕГОРИИ И ТИПЫ:
    - "Тип" = "Вид" = "Модификация" = "Категория" = "Подкатегория"
    - "Назначение" = "Применение" = "Для чего"

    6. ОСОБЫЕ СЛУЧАИ:
    - Звездочка (*) или (!) в конце = обязательное поле, но это ТО ЖЕ поле без звездочки!
    - Если одно название содержит другое полностью - это МОЖЕТ БЫТЬ одно поле
    - Обращай внимание на контекст и смысл, а не только на точное совпадение слов

    ОСТАВШИЕСЯ СТОЛБЦЫ для проверки (НЕ вошедшие в совпадения всех трех):

    Файл 1 (Wildberries) - {len(remaining_1)} столбцов:
    {json.dumps(remaining_1, ensure_ascii=False, indent=2)}

    Файл 2 (Ozon) - {len(remaining_2)} столбцов:
    {json.dumps(remaining_2, ensure_ascii=False, indent=2)}

    Файл 3 (Яндекс Маркет) - {len(remaining_3)} столбцов:
    {json.dumps(remaining_3, ensure_ascii=False, indent=2)}

    Верни результат СТРОГО в формате JSON (без дополнительного текста):
    {{
        "matches_all_three": [
            {{
                "column_1": "точное название из первого набора", 
                "column_2": "точное название из второго набора", 
                "column_3": "точное название из третьего набора",
                "confidence": 0.95
            }}
        ],
        "matches_1_2": [
            {{
                "column_1": "точное название из первого набора", 
                "column_2": "точное название из второго набора",
                "confidence": 0.90
            }}
        ],
        "matches_1_3": [
            {{
                "column_1": "точное название из первого набора", 
                "column_3": "точное название из третьего набора",
                "confidence": 0.90
            }}
        ],
        "matches_2_3": [
            {{
                "column_2": "точное название из второго набора", 
                "column_3": "точное название из третьего набора",
                "confidence": 0.90
            }}
        ],
        "only_in_first": ["названия столбцов только в первом наборе"],
        "only_in_second": ["названия столбцов только во втором наборе"],
        "only_in_third": ["названия столбцов только в третьем наборе"]
    }}

    ВАЖНО: 
    - ПРИОРИТЕТ: ищи совпадения между ВСЕМИ ТРЕМЯ файлами (matches_all_three)!
    - Будь ОЧЕНЬ внимательным! Это второй шанс найти совпадения всех трех маркетплейсов.
    - Используй все правила выше и думай семантически!
    - Confidence должен быть 0.8-1.0 для хороших совпадений
    """
        
        response = self._call_ai(prompt)
        result = self._parse_response(response)
        return result
    
    def _merge_results(self, first_result: Dict, second_result: Dict) -> Dict:
        """
        Объединяет результаты первого и второго прохода
        
        Args:
            first_result: результаты первого прохода
            second_result: результаты второго прохода
        
        Returns:
            Объединенные результаты
        """
        merged = {
            'matches_all_three': first_result.get('matches_all_three', []) + second_result.get('matches_all_three', []),
            'matches_1_2': first_result.get('matches_1_2', []) + second_result.get('matches_1_2', []),
            'matches_1_3': first_result.get('matches_1_3', []) + second_result.get('matches_1_3', []),
            'matches_2_3': first_result.get('matches_2_3', []) + second_result.get('matches_2_3', []),
            'only_in_first': second_result.get('only_in_first', []),  # Берем из второго прохода
            'only_in_second': second_result.get('only_in_second', []),
            'only_in_third': second_result.get('only_in_third', [])
        }
        return merged
    
    def _build_prompt(
        self, 
        columns_1: List[str], 
        columns_2: List[str], 
        columns_3: List[str]
    ) -> str:
        """Формирует промпт для AI (первый проход)"""
        mandatory_text = "\n".join([
            f"- Файл 1: '{m['column_1']}' ↔ Файл 2: '{m['column_2']}' ↔ Файл 3: '{m['column_3']}' ({m['description']})"
            for m in MANDATORY_MATCHES
        ])
        
        return f"""Ты - эксперт по сопоставлению полей товарных каталогов маркетплейсов. 
Твоя задача - найти СЕМАНТИЧЕСКИЕ совпадения между столбцами, даже если названия разные.

ВАЖНО! Следующие столбцы ВСЕГДА являются совпадающими (обязательные совпадения):
{mandatory_text}

Эти совпадения уже известны и не требуют анализа.

ПРАВИЛА СЕМАНТИЧЕСКОГО СРАВНЕНИЯ:

1. ОПИСАНИЯ И ТЕКСТЫ:
   - "Описание" = "Аннотация" = "Описание товара" = "Rich-контент"
   - "Комплектация" = "Комплект поставки" = "Что в комплекте"
   - "Характеристики" = "Описание характеристик" = "Технические характеристики"

2. РАЗМЕРЫ И ГАБАРИТЫ:
   - "Высота упаковки" = "Высота упаковки, мм" = "Высота упаковки, см" (разные единицы - одно поле!)
   - "Ширина упаковки" = "Ширина упаковки, мм" = "Ширина упаковки, см"
   - "Длина упаковки" = "Длина упаковки, мм" = "Глубина упаковки, мм"
   - "Высота предмета" = "Высота товара" = "Высота изделия"
   - "Вес" = "Вес товара" = "Масса"

3. ИЗОБРАЖЕНИЯ И МЕДИА:
   - "Фото" = "Изображение" = "Ссылка на фото" = "URL изображения" = "Картинка"
   - "Дополнительные фото" = "Дополнительные изображения" = "Галерея"
   - "Видео" = "Ссылка на видео" = "Видеоконтент"
   - "Фото 360" = "Панорамное фото" = "3D изображение"

4. КАТЕГОРИИ И КЛАССИФИКАЦИЯ:
   - "Категория" = "Категория товара" = "Тип товара" = "Раздел"
   - "Подкатегория" = "Группа" = "Вид товара"
   - "Тип" = "Вид" = "Модификация"

5. ЦЕНЫ И СКИДКИ:
   - "Цена" = "Стоимость" = "Цена товара" = "Базовая цена"
   - "Цена до скидки" = "Старая цена" = "Зачеркнутая цена"
   - "Скидка" = "Процент скидки" = "Размер скидки"

6. ПРОИЗВОДИТЕЛЬ И СТРАНА:
   - "Страна производства" = "Страна-производитель" = "Производитель (страна)"
   - "Изготовитель" = "Производитель" = "Завод-изготовитель"
   - "Гарантия" = "Гарантийный срок" = "Срок гарантии"

7. УПАКОВКА И ЛОГИСТИКА:
   - "Объем упаковки" = "Объем, л" = "Объём"
   - "Количество в упаковке" = "Кол-во в коробке" = "Штук в упаковке"
   - "Тип упаковки" = "Вид упаковки" = "Упаковка"

8. ТЕХНИЧЕСКИЕ ПАРАМЕТРЫ:
   - "Мощность" = "Мощность, Вт" = "Потребляемая мощность"
   - "Напряжение" = "Напряжение, В" = "Вольтаж"
   - "Цвет" = "Цвет товара" = "Основной цвет" = "Расцветка"
   - "Материал" = "Материал изготовления" = "Состав"

9. ПОЛЯ С ЗВЕЗДОЧКАМИ И БЕЗ:
   - "Название*" = "Название товара" = "Наименование" (звездочка означает "обязательное поле")
   - "Бренд*" = "Бренд" = "Торговая марка"

10. РАЗНЫЕ ФОРМАТЫ ОДНОГО ПОЛЯ:
   - Если видишь похожие поля с разными единицами измерения (мм, см, г, кг) - это ОДНО поле
   - Если поля отличаются только звездочкой (*) или восклицательным знаком (!) - это ОДНО поле
   - Если одно поле длиннее другого, но содержит те же ключевые слова - это ОДНО поле

ТВОЯ ЗАДАЧА:
1. Найти ВСЕ семантические совпадения между тремя файлами (используй правила выше!)
2. Найти частичные совпадения (только между двумя файлами)
3. Определить уникальные столбцы в каждом файле

Первый набор столбцов (Wildberries "Товары", строка 3):
{json.dumps(columns_1, ensure_ascii=False, indent=2)}

Второй набор столбцов (Ozon "Шаблон", строка 2):
{json.dumps(columns_2, ensure_ascii=False, indent=2)}

Третий набор столбцов (Яндекс Маркет "Данные о товарах", строка 4):
{json.dumps(columns_3, ensure_ascii=False, indent=2)}

Верни результат СТРОГО в формате JSON (без дополнительного текста):
{{
    "matches_all_three": [
        {{
            "column_1": "точное название из первого набора", 
            "column_2": "точное название из второго набора", 
            "column_3": "точное название из третьего набора",
            "confidence": 0.95
        }}
    ],
    "matches_1_2": [
        {{
            "column_1": "точное название из первого набора", 
            "column_2": "точное название из второго набора",
            "confidence": 0.90
        }}
    ],
    "matches_1_3": [
        {{
            "column_1": "точное название из первого набора", 
            "column_3": "точное название из третьего набора",
            "confidence": 0.90
        }}
    ],
    "matches_2_3": [
        {{
            "column_2": "точное название из второго набора", 
            "column_3": "точное название из третьего набора",
            "confidence": 0.90
        }}
    ],
    "only_in_first": ["названия столбцов только в первом наборе"],
    "only_in_second": ["названия столбцов только во втором наборе"],
    "only_in_third": ["названия столбцов только в третьем наборе"]
}}

ВАЖНО: 
- Будь внимательным! "Описание" и "Аннотация" - это ОДНО И ТО ЖЕ!
- Confidence должен быть 0.9-1.0 для очевидных совпадений (описание=аннотация)
- Confidence 0.7-0.89 для похожих, но не идентичных полей
- Используй точные названия столбцов из исходных данных!
"""
    
    def _call_ai(self, prompt: str) -> str:
        """Вызывает AI API"""
        response = self.client.chat.completions.create(
            model=AI_MODEL,
            messages=[{"role": "user", "content": prompt}],
            temperature=AI_TEMPERATURE,
        )
        return response.choices[0].message.content
    
    def _parse_response(self, response_text: str) -> Dict:
        """Парсит ответ от AI"""
        try:
            # 1. Сначала пробуем распарсить как есть (если это чистый JSON)
            return json.loads(response_text.strip())
        except json.JSONDecodeError:
            pass

        # 2. Ищем блоки кода `````` или ``````
        json_code_block = re.search(r'``````', response_text, re.DOTALL)
        if json_code_block:
            try:
                return json.loads(json_code_block.group(1))
            except json.JSONDecodeError:
                pass

        # 3. Ищем первую открывающую { и соответствующую ей закрывающую }
        try:
            # Находим первую {
            start_index = response_text.find('{')
            if start_index != -1:
                # Ищем баланс скобок, чтобы найти правильную закрывающую }
                balance = 0
                for i in range(start_index, len(response_text)):
                    char = response_text[i]
                    if char == '{':
                        balance += 1
                    elif char == '}':
                        balance -= 1
                        
                    if balance == 0:
                        json_str = response_text[start_index : i+1]
                        return json.loads(json_str)
        except Exception:
            pass
            
        # 4. Если ничего не помогло - логируем и падаем
        print("[!] Ошибка при парсинге ответа AI")
        print("Raw response start:", response_text[:200])
        print("Raw response end:", response_text[-200:])
        raise ValueError("Не удалось распарсить ответ AI (JSON не найден)")
    
    def match_value_with_list(
        self, 
        value: str, 
        allowed_values: List[str],
        column_name: str = "неизвестный столбец"  # ← ДОБАВИТЬ!
    ) -> Optional[str]:
        """
        Сопоставляет значение со списком допустимых через AI
        
        Args:
            value: значение для проверки
            allowed_values: список validation значений
        
        Returns:
            Сопоставленное значение или None
        """
        if not value or not allowed_values:
            return None
        
        # ДОБАВЬТЕ: Функция нормализации
        def normalize(text: str) -> str:
            """Нормализует текст: нижний регистр, ё→е"""
            return text.lower().replace('ё', 'е').strip()
        
        # Нормализуем входное значение
        value_normalized = normalize(value)
        
        # СНАЧАЛА проверяем точное совпадение с нормализацией
        for allowed in allowed_values:
            if normalize(allowed) == value_normalized:
                print(f"   [normalize] Точное совпадение: '{value}' → '{allowed}'")
                return allowed
        
        # Проверяем частичное совпадение (одно слово содержится в другом)
        value_words = set(value_normalized.split())
        for allowed in allowed_values:
            allowed_words = set(normalize(allowed).split())
            
            # Если все слова из value есть в allowed
            if value_words.issubset(allowed_words):
                print(f"   [normalize] Частичное совпадение: '{value}' → '{allowed}'")
                return allowed
        
        # Если не нашли - спрашиваем AI
        print(f"   [AI] Отправляю запрос для '{value}'...")
        
        prompt = f"""Сопоставь значение с одним из списка для столбца "{column_name}".
        Игнорируй регистр и различия ё/е.

    СТОЛБЕЦ: "{column_name}"    
    ЗНАЧЕНИЕ: "{value}"

    ДОПУСТИМЫЕ ЗНАЧЕНИЯ:
    {chr(10).join(f'- {v}' for v in allowed_values)}

    ПРАВИЛА:
    1. Найди НАИБОЛЕЕ ТОЧНОЕ совпадение
    2. "закалённое стекло" = "Закаленное стекло"
    3. Если значение содержит дополнительные слова, выбери более полный вариант
    4. Если совпадения нет - верни "НЕТ_СОВПАДЕНИЯ"

    Ответь ТОЛЬКО названием из списка или "НЕТ_СОВПАДЕНИЯ", без объяснений."""

        try:
            response = self._call_ai(prompt)
            matched = response.strip()
            
            # Проверяем что AI вернул что-то из списка
            if matched in allowed_values:
                return matched
            
            # Проверяем с нормализацией
            for allowed in allowed_values:
                if normalize(matched) == normalize(allowed):
                    return allowed
            
            # Если AI вернул "НЕТ_СОВПАДЕНИЯ" или что-то не из списка
            if "НЕТ" in matched.upper() or matched not in allowed_values:
                return None
                
            return None
            
        except Exception as e:
            print(f"   [ERROR] Ошибка AI: {e}")
            return None


    
    def _add_mandatory_matches(
        self, 
        result: Dict, 
        columns_1: List[str], 
        columns_2: List[str], 
        columns_3: List[str]
    ) -> Dict:
        """Добавляет обязательные совпадения в результат"""
        matches_all = result.get('matches_all_three', [])
        matches_1_2 = result.get('matches_1_2', [])
        matches_1_3 = result.get('matches_1_3', [])
        matches_2_3 = result.get('matches_2_3', [])
        
        for mandatory in MANDATORY_MATCHES:
            col_1 = ExcelReader.find_column_fuzzy(columns_1, mandatory['column_1'])
            col_2 = ExcelReader.find_column_fuzzy(columns_2, mandatory['column_2']) if mandatory['column_2'] else None
            col_3 = ExcelReader.find_column_fuzzy(columns_3, mandatory['column_3']) if mandatory['column_3'] else None
            
            if col_1 and col_2 and col_3:
                exists = any(
                    m.get('column_1') == col_1 or m.get('column_2') == col_2 or m.get('column_3') == col_3
                    for m in matches_all
                )
                if not exists:
                    matches_all.insert(0, {
                        "column_1": col_1,
                        "column_2": col_2,
                        "column_3": col_3,
                        "confidence": 1.0,
                        "mandatory": True
                    })
                    print(f"[+] Добавлено обязательное совпадение: {mandatory['description']}")
            
            elif col_1 and col_2 and not col_3:
                exists = any(
                    m.get('column_1') == col_1 or m.get('column_2') == col_2
                    for m in matches_1_2
                )
                if not exists:
                    matches_1_2.insert(0, {
                        "column_1": col_1,
                        "column_2": col_2,
                        "confidence": 1.0,
                        "mandatory": True
                    })
            
            elif col_1 and col_3 and not col_2:
                exists = any(
                    m.get('column_1') == col_1 or m.get('column_3') == col_3
                    for m in matches_1_3
                )
                if not exists:
                    matches_1_3.insert(0, {
                        "column_1": col_1,
                        "column_3": col_3,
                        "confidence": 1.0,
                        "mandatory": True
                    })
            
            elif col_2 and col_3 and not col_1:
                exists = any(
                    m.get('column_2') == col_2 or m.get('column_3') == col_3
                    for m in matches_2_3
                )
                if not exists:
                    matches_2_3.insert(0, {
                        "column_2": col_2,
                        "column_3": col_3,
                        "confidence": 1.0,
                        "mandatory": True
                    })
        
        result['matches_all_three'] = matches_all
        result['matches_1_2'] = matches_1_2
        result['matches_1_3'] = matches_1_3
        result['matches_2_3'] = matches_2_3
        
        return result
