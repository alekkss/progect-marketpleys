# Назначение проекта

Telegram-бот для автоматической синхронизации данных товаров между тремя маркетплейсами: Wildberries, Ozon и Яндекс.Маркет. Система использует AI для интеллектуального сопоставления столбцов Excel-файлов и валидации значений с учётом справочников (validation lists).

## Архитектура

- **Bot Framework**: aiogram 3.x (async)
- **База данных**: SQLite (marketplace_sync.db)
- **AI Integration**: OpenRouter API (GPT-модели)
- **File Processing**: pandas + openpyxl
- **State Management**: FSM (Finite State Machine)

## Ключевые директории

/bot/
├── handlers/ # Обработчики команд бота
│ ├── upload.py # Загрузка и обработка файлов
│ ├── schema_create.py # Создание схем сопоставлений
│ ├── schema_edit.py # Редактирование схем
│ ├── schema_update.py # Обновление существующих схем
│ └── stats.py # Статистика пользователя
├── keyboards.py # Клавиатуры бота
├── states.py # FSM состояния
└── storage.py # Хранилище данных

/services/
├── data_synchronizer.py # Основная логика синхронизации данных
├── ai_comparator.py # AI-сопоставление столбцов

/utils/
├── excel_reader.py # Чтение Excel файлов
├── excel_writer.py # Создание отчётов Excel
└── logger_config.py # Конфигурация логирования

/database/
└── database.py # SQLite ORM и миграции

/config/
└── config.py # Конфигурация (API keys, маппинги)


## Стандарты кодирования

### Основные принципы
- **SOLID**: Разделение ответственности (handlers ↔ services ↔ database)
- **Async/Await**: Все IO операции асинхронные
- **Type Hints**: Обязательная типизация функций
- **Logging**: Структурированное логирование через logger

### Конвенции именования
- Классы: `PascalCase` (DataSynchronizer, AIComparator)
- Функции: `snake_case` (sync_three_columns, validate_with_ai)
- Константы: `UPPER_SNAKE_CASE` (FILE_CONFIGS, MANDATORY_MATCHES)
- Private методы: `_leading_underscore` (_validate_with_ai)

### Обработка ошибок
try:
# Основная логика
except SpecificException as e:
logger.error(f"Описание ошибки: {e}", exc_info=True)
# Graceful degradation


## Ключевые компоненты

### 1. Схемы сопоставлений (Schema)
- Сохраняются в БД с полным JSON результата AI
- Содержат тройные (`matches_all_three`) и парные совпадения (`matches_1_2`, `matches_1_3`, `matches_2_3`)
- Confidence threshold: 85%

### 2. Валидация значений (5 уровней)
1. Точное совпадение (case-sensitive)
2. Нормализованное совпадение (lowercase + strip)
3. Извлечение чисел из строк
4. Subset matching (совпадение по словам)
5. AI-сопоставление через OpenRouter

### 3. Множественные значения
- **Разделитель входной**: `;` (точка с запятой)
- **WB**: только первое значение
- **Ozon**: выход через `"; "`
- **Яндекс**: выход через `", "`

### 4. Специальная обработка
- **Габариты**: композитный формат `"длина/ширина/высота"` для Яндекса
- **Единицы измерения**: автоконвертация (мм ↔ см)
- **Артикулы**: выравнивание по всем маркетплейсам

## Формат данных

### Обязательные столбцы (MANDATORY_MATCHES)
{
"column_1": "Артикул продавца", # WB
"column_2": "Артикул*", # Ozon
"column_3": "Ваш SKU *", # Яндекс
"description": "Артикул товара"
}


### Исключённые столбцы
- Цена (все маркетплейсы)
- НДС
- Rich-контент JSON

## База данных

### Таблицы
users # Пользователи бота
schemas # Схемы сопоставлений + full_comparison_json
schema_matches # Тройные совпадения (legacy)
processing_history # История обработок
files # Загруженные файлы


### Миграции
- Автоматические через `Database.migrate()`
- Безопасное добавление столбцов с проверкой существования

## AI Integration

### Используемая модель
- Provider: OpenRouter
- Model: настраивается в `config.py` (по умолчанию GPT-4o)
- Temperature: 0.3 (детерминированность)

### Два прохода AI
1. **Первый проход**: Сопоставление всех столбцов
2. **Второй проход**: Оставшиеся столбцы после первого прохода

### Логирование AI
- Все AI-валидации сохраняются в `ai_validation_log`
- Экспортируются в отдельный лист Excel "AI_Логи"

## Тестирование

### Ручное тестирование
- Загрузка 3 файлов (WB, Ozon, Яндекс)
- Проверка синхронизации по артикулу
- Валидация значений из справочников

### Debug режим
logger.setLevel(logging.DEBUG) # Подробные логи


## Deployment

### Требования
Python 3.9+
aiogram==3.x
pandas
openpyxl
openai (для OpenRouter)


### Запуск
python -m bot.bot # Основной бот


### Переменные окружения
TELEGRAM_BOT_TOKEN=xxx
OPENROUTER_API_KEY=xxx


## Roadmap & Known Issues

### TODO
- [ ] Замена `_validate_with_ai` на `_validate_multiple_values` в `_sync_two_columns()`
- [ ] Миграция старых схем на новый формат JSON
- [ ] Добавление тестов (pytest)

### Ограничения
- Максимум 50 символов в артикуле (фильтрация некорректных данных)
- Файлы только .xlsx формата
- OpenRouter API rate limits

## Важные предостережения

### НЕ изменять без обсуждения
- Формат обязательных столбцов (MANDATORY_MATCHES)
- Логику 5-уровневой валидации
- Разделители множественных значений (`;` → `"; "` / `", "`)

### Критичные зависимости
- Порядок вызовов: схема → синхронизация → валидация
- FSM states должны быть последовательными
- AI-логи обязательны для каждой валидации

## Типичные сценарии работы

### Добавление нового уровня валидации
1. Добавить метод в DataSynchronizer
2. Вставить в цепочку валидации перед AI
3. Обновить AI_Логи формат

### Добавление нового маркетплейса
1. Расширить FILE_CONFIGS
2. Добавить MANDATORY_MATCHES для нового файла
3. Обновить логику sync_three_columns → sync_four_columns