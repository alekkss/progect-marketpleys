"""
Модуль для синхронизации данных между маркетплейсами
"""
import pandas as pd
import re
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter, column_index_from_string
from typing import Dict, List, Tuple, Optional
from utils.excel_reader import ExcelReader
from utils.excel_writer import ExcelWriter
from config.config import FILE_CONFIGS, is_excluded_column
from services.ai_comparator import AIComparator
from utils.logger_config import setup_logger
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# Создаем глобальный логгер
logger = setup_logger('data_sync')  


class DataSynchronizer:
    """Класс для синхронизации данных между тремя маркетплейсами"""
    
    def __init__(self, comparison_result: Dict, ai_comparator=None):
        self.comparison_result = comparison_result
        self.article_columns = {
            'wildberries': 'Артикул продавца',
            'ozon': 'Артикул*',
            'yandex': 'Ваш SKU *'
        }
        self.changes_log = {
            'wildberries': [],
            'ozon': [],
            'yandex': []
        }
        self.original_file_paths = {}
        self.ai_comparator = ai_comparator
        
        
        # ДОБАВЬТЕ: Кэш validation для каждого столбца
        self.column_validations = {}  # {marketplace: {column_name: [allowed_values]}}
        # ДОБАВЬТЕ ЭТУ СТРОКУ:
        self.ai_validation_log = []  # Логи AI-сопоставлений
        logger.info("Инициализация DataSynchronizer")
        logger.debug(f"AI comparator передан: {ai_comparator is not None}")
    
    def _align_articles(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Выравнивает артикулы между маркетплейсами - добавляет отсутствующие строки
        
        Args:
            dfs: словарь с DataFrame для каждого маркетплейса
            
        Returns:
            Обновленные DataFrame с добавленными артикулами
        """
        logger.info("\n" + "="*60)
        logger.info("ВЫРАВНИВАНИЕ АРТИКУЛОВ МЕЖДУ МАРКЕТПЛЕЙСАМИ")
        logger.info("="*60)
        
        # Собираем все уникальные артикулы из всех маркетплейсов
        all_articles = set()
        
        for marketplace in ['wildberries', 'ozon', 'yandex']:
            article_col = self.article_columns[marketplace]
            if article_col in dfs[marketplace].columns:
                articles = dfs[marketplace][article_col].dropna().astype(str).str.strip()
                articles = articles[articles != '']  # Убираем пустые
                
                # Фильтрация: Убираем описания полей и слишком длинные строки
                articles = articles[
                    ~articles.str.contains(
                        'идентифицировать|описание|заполнить|пример|название товара|по которому', 
                        case=False, 
                        na=False
                    )
                ]
                # Убираем строки длиннее 50 символов (скорее всего описание)
                articles = articles[articles.str.len() < 50]
                
                all_articles.update(articles.tolist())
                logger.info(f"📊 {marketplace.upper()}: {len(articles)} артикулов")
        
        logger.info(f"\n🔍 Всего уникальных артикулов: {len(all_articles)}")
        
        # Для каждого маркетплейса проверяем недостающие артикулы
        total_added = 0
        
        for marketplace in ['wildberries', 'ozon', 'yandex']:
            article_col = self.article_columns[marketplace]
            
            if article_col not in dfs[marketplace].columns:
                logger.warning(f"⚠️ {marketplace.upper()}: столбец '{article_col}' не найден, пропускаю")
                continue
            
            # Существующие артикулы
            existing_articles = dfs[marketplace][article_col].dropna().astype(str).str.strip()
            existing_articles = existing_articles[existing_articles != '']
            
            # Фильтрация: та же фильтрация что и выше
            existing_articles = existing_articles[
                ~existing_articles.str.contains(
                    'идентифицировать|описание|заполнить|пример|название товара|по которому', 
                    case=False, 
                    na=False
                )
            ]
            existing_articles = existing_articles[existing_articles.str.len() < 50]
            existing_articles = set(existing_articles.tolist())
            
            # Находим недостающие
            missing_articles = all_articles - existing_articles
            
            if not missing_articles:
                logger.info(f"✅ {marketplace.upper()}: все артикулы присутствуют")
                continue
            
            logger.info(f"\n➕ {marketplace.upper()}: добавляю {len(missing_articles)} артикулов")
            
            # Создаем новые строки для недостающих артикулов
            new_rows = []
            for article in sorted(missing_articles):
                # Создаем пустую строку со всеми столбцами
                new_row = {col: None for col in dfs[marketplace].columns}
                # Заполняем только артикул
                new_row[article_col] = article
                new_rows.append(new_row)
            
            # Добавляем новые строки в DataFrame
            if new_rows:
                new_df = pd.DataFrame(new_rows)
                dfs[marketplace] = pd.concat([dfs[marketplace], new_df], ignore_index=True)
                total_added += len(new_rows)
                
                logger.info(f"   ✓ Добавлено {len(new_rows)} строк")
                logger.info(f"   📊 Было: {len(dfs[marketplace]) - len(new_rows)}, стало: {len(dfs[marketplace])}")
        
        if total_added > 0:
            logger.info(f"\n✅ Итого добавлено {total_added} новых строк во все маркетплейсы")
        else:
            logger.info(f"\n✅ Выравнивание не требуется - все артикулы присутствуют")
        
        return dfs
    
    
    def _detect_unit(self, column_name: str) -> Optional[str]:
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
    
    def _convert_value(
        self, 
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
            result = numeric_value * 1000
            print(f"      [Конвертация] {numeric_value} кг → {result} г")
            return result
        elif from_unit == 'g' and to_unit == 'kg':
            result = numeric_value / 1000
            print(f"      [Конвертация] {numeric_value} г → {result} кг")
            return result
        
        # Конвертация размеров
        elif from_unit == 'mm' and to_unit == 'cm':
            result = numeric_value / 10
            print(f"      [Конвертация] {numeric_value} мм → {result} см")
            return result
        elif from_unit == 'cm' and to_unit == 'mm':
            result = numeric_value * 10
            print(f"      [Конвертация] {numeric_value} см → {result} мм")
            return result
        
        # Если конвертация не поддерживается - возвращаем исходное значение
        return value
    
    def synchronize_data(
        self,
        file_paths: Dict[str, str],
        output_paths: Dict[str, str] = None,
        report_path: str = None  # ← ДОБАВИТЬ
    ) -> Tuple[Dict[str, pd.DataFrame], Dict]:
        logger.info("="*60)
        logger.info("СИНХРОНИЗАЦИЯ ДАННЫХ МЕЖДУ МАРКЕТПЛЕЙСАМИ")
        logger.info("="*60)
        
        # Загружаем данные из всех трех файлов
        dfs = self._load_all_dataframes(file_paths)
        
        # Синхронизируем данные
        synced_dfs = self._sync_all_matches(dfs)
        
        # Сохраняем результаты
        if output_paths:
            self._save_results(synced_dfs, output_paths)
        
        logger.info("✅ Синхронизация завершена!")
        
        
        return synced_dfs, self.changes_log
    
    def _load_all_dataframes(self, file_paths: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """Загружает данные через openpyxl для сохранения форматов"""
        logger.info("📂 Загружаю данные из файлов...")
        dfs = {}
        
        for marketplace, file_path in file_paths.items():
            self.original_file_paths[marketplace] = file_path
            config = FILE_CONFIGS[marketplace]
            
            wb = load_workbook(file_path, data_only=True)
            ws = wb[config['sheet_name']]
            
            self._load_column_validations(ws, marketplace, config)
            
            data = []
            headers = []
            
            # Читаем заголовки
            for cell in ws[config['header_row']]:
                headers.append(cell.value if cell.value else '')
            
            # 🆕 ИСПРАВЛЕНИЕ: Используем data_start_row вместо header_row + 1
            data_start = config.get('data_start_row', config['header_row'] + 1)
            
            # Читаем данные
            for row in ws.iter_rows(min_row=data_start, values_only=True):  # ← ИЗМЕНЕНО!
                data.append(row)
            
            df = pd.DataFrame(data, columns=headers)
            dfs[marketplace] = df
            wb.close()
            
            logger.info(f"✅ {config['display_name']}: загружено {len(df)} товаров")
        
        return dfs

    def _load_column_validations(self, ws, marketplace: str, config: Dict):
        """
        Загружает информацию о validation для каждого столбца
        """
        from openpyxl.utils import range_boundaries
        
        if marketplace not in self.column_validations:
            self.column_validations[marketplace] = {}
        
        header_row = config['header_row']
        
        # Создаем маппинг: номер колонки -> название
        col_idx_to_name = {}
        for col_idx, cell in enumerate(ws[header_row], start=1):
            if cell.value:
                col_name = str(cell.value).strip()
                col_idx_to_name[col_idx] = col_name
        
        logger.info(f"📋 [{marketplace}] Найдено {len(col_idx_to_name)} столбцов")
        logger.debug(f"[{marketplace}] Первые 5 столбцов: {list(col_idx_to_name.values())[:5]}")
        
        # ДОБАВЬТЕ: Получаем именованные диапазоны
        workbook = ws.parent
        named_ranges = {}
        
        try:
            # ПРАВИЛЬНЫЙ способ итерации по именованным диапазонам
            for name_obj in workbook.defined_names.values():
                try:
                    if name_obj.value:
                        named_ranges[name_obj.name] = name_obj.value
                except Exception as e:
                    logger.debug(f"[{marketplace}] Пропущен именованный диапазон: {e}")
            
            logger.info(f"[{marketplace}] Найдено {len(named_ranges)} именованных диапазонов")
            
            # Выводим первые 5 для проверки
            if named_ranges:
                sample = list(named_ranges.items())[:5]
                for name, value in sample:
                    logger.debug(f"[{marketplace}] Именованный диапазон '{name}' = '{value}'")
        except Exception as e:
            logger.error(f"[{marketplace}] Ошибка получения именованных диапазонов: {e}")
        
        # Проходим по всем validation правилам
        validation_count = 0
        dv_index = 0
        
        for dv in ws.data_validations.dataValidation:
            dv_index += 1
            logger.debug(f"[{marketplace}] DV #{dv_index}: type={dv.type}, sqref={dv.sqref}")
            
            if dv.type != "list":
                logger.debug(f"[{marketplace}] DV #{dv_index}: ПРОПУЩЕН (type != 'list')")
                continue
                
            if dv.sqref is None:
                logger.debug(f"[{marketplace}] DV #{dv_index}: ПРОПУЩЕН (sqref is None)")
                continue
            
            # Извлекаем значения из validation
            allowed_values = []
            if dv.formula1:
                formula = dv.formula1
                logger.debug(f"[{marketplace}] DV #{dv_index}: formula1='{formula[:100]}'...")
                
                # Список задан прямо: "Красный,Синий,Зеленый"
                if formula.startswith('"') and formula.endswith('"'):
                    allowed_values = [v.strip() for v in formula.strip('"').split(',')]
                    logger.debug(f"[{marketplace}] DV #{dv_index}: Прямой список, {len(allowed_values)} значений")
                
                # ДОБАВЬТЕ: Проверяем именованный диапазон
                elif formula in named_ranges:
                    logger.debug(f"[{marketplace}] DV #{dv_index}: Именованный диапазон '{formula}'")
                    try:
                        range_formula = named_ranges[formula]
                        logger.debug(f"[{marketplace}] DV #{dv_index}: Формула диапазона: '{range_formula}'")
                        
                        # Парсим формулу вида "Лист!$A$1:$A$10"
                        clean_formula = range_formula.replace('$', '')
                        if '!' in clean_formula:
                            sheet_name, range_ref = clean_formula.split('!', 1)
                            # Убираем кавычки из имени листа если есть
                            sheet_name = sheet_name.strip("'")
                            target_ws = workbook[sheet_name]
                        else:
                            range_ref = clean_formula
                            target_ws = ws
                        
                        # Извлекаем значения
                        for row in target_ws[range_ref]:
                            for cell in row:
                                if cell.value is not None:
                                    allowed_values.append(str(cell.value).strip())
                        
                        logger.info(f"✅ [{marketplace}] DV #{dv_index}: Извлечено {len(allowed_values)} значений из именованного диапазона '{formula}'")
                    except Exception as e:
                        logger.error(f"[{marketplace}] DV #{dv_index}: Ошибка обработки именованного диапазона '{formula}': {e}")
                
                # Список задан через обычный диапазон
                elif ':' in formula:
                    try:
                        clean_formula = formula.replace('$', '')
                        if '!' in clean_formula:
                            sheet_name, range_ref = clean_formula.split('!')
                            target_ws = workbook[sheet_name]
                            logger.debug(f"[{marketplace}] DV #{dv_index}: Диапазон на листе '{sheet_name}': {range_ref}")
                        else:
                            range_ref = clean_formula
                            target_ws = ws
                            logger.debug(f"[{marketplace}] DV #{dv_index}: Диапазон на текущем листе: {range_ref}")
                        
                        for row in target_ws[range_ref]:
                            for cell in row:
                                if cell.value is not None:
                                    allowed_values.append(str(cell.value).strip())
                        
                        logger.debug(f"[{marketplace}] DV #{dv_index}: Извлечено {len(allowed_values)} значений")
                    except Exception as e:
                        logger.error(f"[{marketplace}] DV #{dv_index}: Ошибка извлечения validation: {e}")
                else:
                    logger.warning(f"[{marketplace}] DV #{dv_index}: Неизвестный формат формулы: '{formula}'")
            else:
                logger.debug(f"[{marketplace}] DV #{dv_index}: formula1 отсутствует")
            
            if not allowed_values:
                logger.debug(f"[{marketplace}] DV #{dv_index}: ПРОПУЩЕН (пустой список значений)")
                continue
            
            # Определяем какие столбцы затронуты этим validation
            ranges = str(dv.sqref).split()
            logger.debug(f"[{marketplace}] DV #{dv_index}: sqref содержит {len(ranges)} диапазон(ов): {ranges}")
            
            for range_str in ranges:
                try:
                    if ':' in range_str:
                        min_col, min_row, max_col, max_row = range_boundaries(range_str)
                        logger.debug(f"[{marketplace}] DV #{dv_index}: Диапазон {range_str} -> столбцы {min_col}-{max_col}, строки {min_row}-{max_row}")
                        
                        # Применяем validation ко всем колонкам в диапазоне
                        for col_idx in range(min_col, max_col + 1):
                            if col_idx in col_idx_to_name:
                                col_name = col_idx_to_name[col_idx]
                                self.column_validations[marketplace][col_name] = allowed_values
                                validation_count += 1
                                logger.info(f"✅ [{marketplace}] Validation для '{col_name}': {len(allowed_values)} значений")
                            else:
                                logger.debug(f"[{marketplace}] DV #{dv_index}: Столбец {col_idx} не найден в заголовках")
                except Exception as e:
                    logger.error(f"[{marketplace}] DV #{dv_index}: Ошибка обработки range_str '{range_str}': {e}")
        
        # Итоговая статистика
        logger.info(f"📊 [{marketplace}] Итого загружено validation для {validation_count} столбцов из {len(ws.data_validations.dataValidation)} правил")
        
        if validation_count == 0:
            logger.warning(f"⚠️ [{marketplace}] НЕ НАЙДЕНО ни одного validation!")
        
        for col_name, values in self.column_validations.get(marketplace, {}).items():
            logger.debug(f"  • {col_name}: {len(values)} значений (первые 3: {values[:3]})")

    
    def _sync_all_matches(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Синхронизирует все совпадающие столбцы"""
        
        # Создаем копии для работы
        synced_dfs = {
            'wildberries': dfs['wildberries'].copy(),
            'ozon': dfs['ozon'].copy(),
            'yandex': dfs['yandex'].copy()
        }
        
        # 🆕 НОВОЕ: Выравниваем артикулы ПЕРЕД синхронизацией
        synced_dfs = self._align_articles(synced_dfs)
        
        # Синхронизируем совпадения всех трех маркетплейсов
        print("\n[*] Синхронизирую совпадения всех 3 маркетплейсов...")
        synced_dfs = self._sync_three_way_matches(synced_dfs)
        
        # Синхронизируем совпадения между двумя маркетплейсами
        print("\n[*] Синхронизирую совпадения между парами маркетплейсов...")
        synced_dfs = self._sync_two_way_matches(synced_dfs)
        
        return synced_dfs
    
    def _sync_three_way_matches(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Синхронизирует совпадения всех трех маркетплейсов"""
        
        matches = self.comparison_result.get('matches_all_three', [])
        
        if not matches:
            print("  Нет совпадений для синхронизации")
            return dfs
        
        total_filled = 0
        skipped_count = 0
        
        for match in matches:
            col_wb = match.get('column_1')
            col_ozon = match.get('column_2')
            col_yandex = match.get('column_3')
            
            if not all([col_wb, col_ozon, col_yandex]):
                continue
            
            # Пропускаем исключенные столбцы
            if (is_excluded_column(col_wb) or 
                is_excluded_column(col_ozon) or 
                is_excluded_column(col_yandex)):
                skipped_count += 1
                continue
            
            # Проверяем, что столбцы существуют
            if (col_wb not in dfs['wildberries'].columns or 
                col_ozon not in dfs['ozon'].columns or 
                col_yandex not in dfs['yandex'].columns):
                continue
            
            # Синхронизируем данные между тремя файлами
            filled = self._sync_three_columns(
                dfs, 
                col_wb, col_ozon, col_yandex
            )
            
            if filled > 0:
                confidence = int(match.get('confidence', 0) * 100)
                print(f"  ✓ Заполнено {filled} значений: '{col_wb}' ↔ '{col_ozon}' ↔ '{col_yandex}' ({confidence}%)")
                total_filled += filled
        
        if skipped_count > 0:
            print(f"[!] Пропущено {skipped_count} исключенных столбцов")
        print(f"[+] Всего заполнено {total_filled} пустых ячеек в совпадениях всех 3 маркетплейсов")
        return dfs
    
    def _sync_two_way_matches(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Синхронизирует совпадения между парами маркетплейсов"""
        
        pairs = [
            ('matches_1_2', 'wildberries', 'ozon', 'column_1', 'column_2'),
            ('matches_1_3', 'wildberries', 'yandex', 'column_1', 'column_3'),
            ('matches_2_3', 'ozon', 'yandex', 'column_2', 'column_3')
        ]
        
        total_filled = 0
        skipped_count = 0
        
        for match_key, mp1, mp2, col_key1, col_key2 in pairs:
            matches = self.comparison_result.get(match_key, [])
            
            if not matches:
                continue
            
            for match in matches:
                col1 = match.get(col_key1)
                col2 = match.get(col_key2)
                
                if not all([col1, col2]):
                    continue
                
                # Пропускаем исключенные столбцы
                if is_excluded_column(col1) or is_excluded_column(col2):
                    skipped_count += 1
                    continue
                
                # Проверяем, что столбцы существуют
                if col1 not in dfs[mp1].columns or col2 not in dfs[mp2].columns:
                    continue
                
                # Синхронизируем данные между двумя файлами
                filled = self._sync_two_columns(dfs, mp1, mp2, col1, col2)
                
                if filled > 0:
                    confidence = int(match.get('confidence', 0) * 100)
                    print(f"  ✓ Заполнено {filled} значений: {mp1}:'{col1}' ↔ {mp2}:'{col2}' ({confidence}%)")
                    total_filled += filled
        
        if skipped_count > 0:
            print(f"[!] Пропущено {skipped_count} исключенных столбцов")
        print(f"[+] Всего заполнено {total_filled} пустых ячеек в совпадениях между парами")
        return dfs
    
    def _sync_three_columns(
        self,
        dfs: Dict[str, pd.DataFrame],
        col_wb: str,
        col_ozon: str,
        col_yandex: str
    ) -> int:
        """
        Синхронизирует данные между тремя столбцами на основе артикулов
        Returns:
            Количество заполненных ячеек
        """
        filled_count = 0
        
        # Определяем единицы измерения для каждого столбца
        unit_wb = self._detect_unit(col_wb)
        unit_ozon = self._detect_unit(col_ozon)
        unit_yandex = self._detect_unit(col_yandex)
        
        # Создаем словари для быстрого поиска по артикулу
        wb_data = self._create_article_map(dfs['wildberries'], self.article_columns['wildberries'], col_wb)
        ozon_data = self._create_article_map(dfs['ozon'], self.article_columns['ozon'], col_ozon)
        yandex_data = self._create_article_map(dfs['yandex'], self.article_columns['yandex'], col_yandex)
        
        # Получаем все уникальные артикулы
        all_articles = set(wb_data.keys()) | set(ozon_data.keys()) | set(yandex_data.keys())
        
        for article in all_articles:
            if not article:  # Пропускаем пустые артикулы
                continue
            
            # Получаем значения из всех трех источников
            values = {
                'wildberries': wb_data.get(article, {}).get('value'),
                'ozon': ozon_data.get(article, {}).get('value'),
                'yandex': yandex_data.get(article, {}).get('value')
            }
            
            # Находим непустое значение и его источник
            source_value = None
            source_unit = None
            for marketplace, val in values.items():
                # ИСПРАВЛЕНИЕ: проверяем тип и извлекаем скаляр
                if isinstance(val, pd.Series):
                    if not val.empty:
                        val = val.iloc[0]  # Берем первое значение
                    else:
                        val = None
                
                if pd.notna(val) and str(val).strip():
                    source_value = val
                    if marketplace == 'wildberries':
                        source_unit = unit_wb
                    elif marketplace == 'ozon':
                        source_unit = unit_ozon
                    else:
                        source_unit = unit_yandex
                    break
            
            if source_value is None:
                continue
            
            # WB
            if article in wb_data:
                val_wb = values['wildberries']
                if isinstance(val_wb, pd.Series):
                    val_wb = val_wb.iloc[0] if not val_wb.empty else None
                
                if pd.isna(val_wb) or not str(val_wb).strip():
                    idx = wb_data[article]['index']
                    series = dfs['wildberries'][col_wb]
                    if isinstance(series, pd.DataFrame):
                        series = series.iloc[:, 0]  # Берем первый столбец если это DataFrame
                    col_dtype = series.dtype
                    converted_value = self._convert_value(source_value, source_unit, unit_wb)
                    
                    # Проверка validation через AI
                    final_value = self._validate_with_ai(converted_value, 'wildberries', col_wb)
                    
                    try:
                        # ИСПРАВЛЕНИЕ:
                        if final_value:
                            value_to_set = final_value
                        elif not self.column_validations.get('wildberries', {}).get(col_wb):
                            # Нет validation - записываем как есть
                            value_to_set = converted_value
                        else:
                            # Есть validation но совпадение не найдено - НЕ записываем
                            logger.warning(f"⚠️ [WB] Пропущено '{converted_value}' для '{col_wb}' (не прошло validation)")
                            continue
                        
                        if pd.api.types.is_numeric_dtype(col_dtype):
                            value_to_set = pd.to_numeric(value_to_set, errors='coerce')
                        dfs['wildberries'].at[idx, col_wb] = value_to_set
                        filled_count += 1
                        self._log_change('wildberries', article, col_wb, value_to_set)
                    except Exception:
                        pass
            
            # OZON
            if article in ozon_data:
                val_ozon = values['ozon']
                if isinstance(val_ozon, pd.Series):
                    val_ozon = val_ozon.iloc[0] if not val_ozon.empty else None
                
                if pd.isna(val_ozon) or not str(val_ozon).strip():
                    idx = ozon_data[article]['index']
                    series = dfs['ozon'][col_ozon]
                    if isinstance(series, pd.DataFrame):
                        series = series.iloc[:, 0]
                    col_dtype = series.dtype
                    converted_value = self._convert_value(source_value, source_unit, unit_ozon)
                    
                    final_value = self._validate_with_ai(converted_value, 'ozon', col_ozon)
                    
                    try:
                        # ИСПРАВЛЕНИЕ:
                        if final_value:
                            value_to_set = final_value
                        elif not self.column_validations.get('ozon', {}).get(col_ozon):
                            value_to_set = converted_value
                        else:
                            logger.warning(f"⚠️ [OZON] Пропущено '{converted_value}' для '{col_ozon}' (не прошло validation)")
                            continue
                        
                        if pd.api.types.is_numeric_dtype(col_dtype):
                            value_to_set = pd.to_numeric(value_to_set, errors='coerce')
                        dfs['ozon'].at[idx, col_ozon] = value_to_set
                        filled_count += 1
                        self._log_change('ozon', article, col_ozon, value_to_set)
                    except Exception:
                        pass
            
            # YANDEX
            if article in yandex_data:
                val_yandex = values['yandex']
                if isinstance(val_yandex, pd.Series):
                    val_yandex = val_yandex.iloc[0] if not val_yandex.empty else None
                
                if pd.isna(val_yandex) or not str(val_yandex).strip():
                    idx = yandex_data[article]['index']
                    series = dfs['yandex'][col_yandex]
                    if isinstance(series, pd.DataFrame):
                        series = series.iloc[:, 0]
                    col_dtype = series.dtype
                    converted_value = self._convert_value(source_value, source_unit, unit_yandex)
                    
                    final_value = self._validate_with_ai(converted_value, 'yandex', col_yandex)
                    
                    try:
                        # ИСПРАВЛЕНИЕ:
                        if final_value:
                            value_to_set = final_value
                        elif not self.column_validations.get('yandex', {}).get(col_yandex):
                            value_to_set = converted_value
                        else:
                            logger.warning(f"⚠️ [YANDEX] Пропущено '{converted_value}' для '{col_yandex}' (не прошло validation)")
                            continue
                        
                        if pd.api.types.is_numeric_dtype(col_dtype):
                            value_to_set = pd.to_numeric(value_to_set, errors='coerce')
                        dfs['yandex'].at[idx, col_yandex] = value_to_set
                        filled_count += 1
                        self._log_change('yandex', article, col_yandex, value_to_set)
                    except Exception:
                        pass
        
        return filled_count
    
    def _validate_with_ai(self, value, marketplace: str, column_name: str) -> Optional[str]:
        """
        Проверяет значение через AI если есть validation
        Returns:
            Сопоставленное значение или None если нет validation
        """
        # Проверяем есть ли validation для этого столбца
        allowed_values = self.column_validations.get(marketplace, {}).get(column_name)
        
        if not allowed_values or not self.ai_comparator:
            return None
        
        value_str = str(value).strip()
        
        # Функция нормализации
        def normalize(text: str) -> str:
            """Нормализует текст: нижний регистр, ё→е"""
            return text.lower().replace('ё', 'е').strip()
        
        # Функция извлечения числа
        def extract_number(text: str) -> Optional[str]:
            """Извлекает первое число из строки типа '1 шт', '2 компрессора'"""
            import re
            numbers = re.findall(r'\d+', text)
            return numbers[0] if numbers else None
        
        # Функция логирования
        def log_match(original: str, matched: str, method: str):
            """Записывает успешное сопоставление в лог"""
            self.ai_validation_log.append({
                'Маркетплейс': marketplace.upper(),
                'Столбец': column_name,
                'Исходное значение': original,
                'Сопоставлено с': matched,
                'Метод': method
            })
        
        # 1. Проверяем точное совпадение
        if value_str in allowed_values:
            logger.info(f"[_validate_with_ai] ТОЧНОЕ совпадение: '{value_str}'")
            log_match(value_str, value_str, 'Точное совпадение')
            return value_str
        
        # 2. Проверяем с нормализацией (регистр + ё/е)
        value_normalized = normalize(value_str)
        for allowed in allowed_values:
            if normalize(allowed) == value_normalized:
                logger.info(f"[_validate_with_ai] Совпадение с нормализацией: '{value_str}' → '{allowed}'")
                log_match(value_str, allowed, 'Нормализация (регистр/ё-е)')
                return allowed
        
        # 3. Извлекаем число если это числовое поле
        number = extract_number(value_str)
        if number:
            # Проверяем точное совпадение числа
            if number in allowed_values:
                logger.info(f"[_validate_with_ai] Извлечено число: '{value_str}' → '{number}'")
                log_match(value_str, number, 'Извлечение числа')
                return number
            
            # Проверяем с нормализацией
            for allowed in allowed_values:
                if extract_number(allowed) == number:
                    logger.info(f"[_validate_with_ai] Совпадение по числу: '{value_str}' → '{allowed}'")
                    log_match(value_str, allowed, 'Извлечение числа')
                    return allowed
        
        # 4. Проверяем частичное совпадение (по словам)
        value_words = set(value_normalized.split())
        for allowed in allowed_values:
            allowed_words = set(normalize(allowed).split())
            
            # Если все слова из value есть в allowed
            if value_words and value_words.issubset(allowed_words):
                logger.info(f"[_validate_with_ai] Частичное совпадение: '{value_str}' → '{allowed}'")
                log_match(value_str, allowed, 'Частичное совпадение (слова)')
                return allowed
        
        # 5. Спрашиваем AI (без кэша!)
        logger.info(f"🤖 [AI] Проверяю '{value_str}' для столбца '{column_name}'...")
        matched_value = self.ai_comparator.match_value_with_list(value_str, allowed_values, column_name=column_name)  # ← ДОБАВИТЬ!)
        
        if matched_value:
            logger.info(f"✅ [AI] Найдено: '{value_str}' → '{matched_value}'")
            log_match(value_str, matched_value, 'AI запрос')
            return matched_value
        else:
            logger.warning(f"❌ [AI] Не найдено совпадение для '{value_str}'")
            return None

    
    def _sync_two_columns(
        self,
        dfs: Dict[str, pd.DataFrame],
        mp1: str,
        mp2: str,
        col1: str,
        col2: str
    ) -> int:
        """
        Синхронизирует данные между двумя столбцами на основе артикулов
        Returns:
            Количество заполненных ячеек
        """
        filled_count = 0
        
        # Определяем единицы измерения
        unit1 = self._detect_unit(col1)
        unit2 = self._detect_unit(col2)
        
        # Определяем столбцы артикулов
        article_col1 = self.article_columns[mp1]
        article_col2 = self.article_columns[mp2]
        
        # Создаем словари для быстрого поиска
        data1 = self._create_article_map(dfs[mp1], article_col1, col1)
        data2 = self._create_article_map(dfs[mp2], article_col2, col2)
        
        # Получаем все уникальные артикулы
        all_articles = set(data1.keys()) | set(data2.keys())
        
        for article in all_articles:
            if not article:
                continue
            
            # Получаем значения
            val1 = data1.get(article, {}).get('value')
            val2 = data2.get(article, {}).get('value')
            
            # ИСПРАВЛЕНИЕ: проверяем Series
            if isinstance(val1, pd.Series):
                val1 = val1.iloc[0] if not val1.empty else None
            if isinstance(val2, pd.Series):
                val2 = val2.iloc[0] if not val2.empty else None
            
            # Заполняем пустые ячейки
            if article in data1 and article in data2:
                # Если в первом пусто, а во втором есть
                if (pd.isna(val1) or not str(val1).strip()) and pd.notna(val2) and str(val2).strip():
                    idx = data1[article]['index']
                    series = dfs[mp1][col1]
                    if isinstance(series, pd.DataFrame):
                        series = series.iloc[:, 0]
                    col_dtype = series.dtype  # ✅
                    converted_value = self._convert_value(val2, unit2, unit1)
                    
                    final_value = self._validate_with_ai(converted_value, mp1, col1)
                    
                    try:
                        # ИСПРАВЛЕНИЕ:
                        if final_value:
                            value_to_set = final_value
                        elif not self.column_validations.get(mp1, {}).get(col1):
                            value_to_set = converted_value
                        else:
                            logger.warning(f"⚠️ [{mp1.upper()}] Пропущено '{converted_value}' для '{col1}' (не прошло validation)")
                            continue
                        
                        if pd.api.types.is_numeric_dtype(col_dtype):
                            value_to_set = pd.to_numeric(value_to_set, errors='coerce')
                        dfs[mp1].at[idx, col1] = value_to_set
                        filled_count += 1
                        self._log_change(mp1, article, col1, value_to_set)
                    except Exception:
                        pass
                
                # Если во втором пусто, а в первом есть
                elif (pd.isna(val2) or not str(val2).strip()) and pd.notna(val1) and str(val1).strip():
                    idx = data2[article]['index']
                    series = dfs[mp2][col2]
                    if isinstance(series, pd.DataFrame):
                        series = series.iloc[:, 0]
                    col_dtype = series.dtype  # ✅
                    converted_value = self._convert_value(val1, unit1, unit2)
                    
                    final_value = self._validate_with_ai(converted_value, mp2, col2)
                    
                    try:
                        # ИСПРАВЛЕНИЕ:
                        if final_value:
                            value_to_set = final_value
                        elif not self.column_validations.get(mp2, {}).get(col2):
                            value_to_set = converted_value
                        else:
                            logger.warning(f"⚠️ [{mp2.upper()}] Пропущено '{converted_value}' для '{col2}' (не прошло validation)")
                            continue
                        
                        if pd.api.types.is_numeric_dtype(col_dtype):
                            value_to_set = pd.to_numeric(value_to_set, errors='coerce')
                        dfs[mp2].at[idx, col2] = value_to_set
                        filled_count += 1
                        self._log_change(mp2, article, col2, value_to_set)
                    except Exception:
                        pass
        
        return filled_count
    
    def _create_article_map(self, df: pd.DataFrame, article_col: str, value_col: str) -> Dict:
        """
        Создает словарь для быстрого поиска значений по артикулу
        
        Returns:
            Словарь {артикул: {'value': значение, 'index': индекс строки}}
        """
        article_map = {}
        
        if article_col not in df.columns or value_col not in df.columns:
            return article_map
        
        for idx, row in df.iterrows():
            article = row.get(article_col)
            value = row.get(value_col)
            
            if pd.notna(article):
                article_str = str(article).strip()
                if article_str:
                    article_map[article_str] = {
                        'value': value,
                        'index': idx
                    }
        
        return article_map
    
    def _log_change(self, marketplace: str, article: str, column: str, new_value):
        """Логирует произведенное изменение"""
        self.changes_log[marketplace].append({
            'article': article,
            'column': column,
            'new_value': str(new_value)
        })
    
    def _get_validation_list_values(self, ws, row_idx: int, col_idx: int) -> List[str]:
        """
        Получает список допустимых значений из data validation ячейки
        
        Returns:
            Список допустимых значений или пустой список
        """
        from openpyxl.worksheet.datavalidation import DataValidation
        
        cell_ref = f"{get_column_letter(col_idx)}{row_idx}"
        
        # Проходим по всем data validation правилам
        for dv in ws.data_validations.dataValidation:
            if dv.type != "list":
                continue
            
            # ИСПРАВЛЕНИЕ: проверяем принадлежность ячейки к диапазону validation
            # Используем правильную проверку через sqref (string reference)
            if dv.sqref is None:
                continue
                
            # sqref может содержать несколько диапазонов, разделенных пробелами
            # Например: "B2:B100 D2:D100"
            ranges = str(dv.sqref).split()
            
            cell_in_range = False
            for range_str in ranges:
                # Проверяем входит ли наша ячейка в диапазон
                if ':' in range_str:
                    # Диапазон типа A2:A100
                    try:
                        from openpyxl.utils import range_boundaries
                        min_col, min_row, max_col, max_row = range_boundaries(range_str)
                        
                        if (min_col <= col_idx <= max_col and 
                            min_row <= row_idx <= max_row):
                            cell_in_range = True
                            break
                    except:
                        pass
                else:
                    # Одиночная ячейка типа A2
                    if range_str == cell_ref:
                        cell_in_range = True
                        break
            
            if not cell_in_range:
                continue
            
            # Нашли validation для этой ячейки, извлекаем значения
            if dv.formula1:
                formula = dv.formula1
                
                # Список задан прямо: "Красный,Синий,Зеленый"
                if formula.startswith('"') and formula.endswith('"'):
                    values = formula.strip('"').split(',')
                    return [v.strip() for v in values]
                
                # Список задан через диапазон: $A$1:$A$10 или Sheet1!$A$1:$A$10
                elif ':' in formula:
                    try:
                        # Убираем $ для парсинга
                        clean_formula = formula.replace('$', '')
                        
                        # Проверяем, есть ли ссылка на другой лист
                        if '!' in clean_formula:
                            sheet_name, range_ref = clean_formula.split('!')
                            target_ws = ws.parent[sheet_name]
                        else:
                            range_ref = clean_formula
                            target_ws = ws
                        
                        # Извлекаем значения из диапазона
                        values = []
                        for row in target_ws[range_ref]:
                            for cell in row:
                                if cell.value is not None:
                                    values.append(str(cell.value).strip())
                        
                        return values
                        
                    except Exception as e:
                        print(f"      [!] Не удалось извлечь список из диапазона {formula}: {e}")
                        return []
        
        return []
    
    def _create_ai_log_sheet(self, output_paths: Dict[str, str]):
        """Создаёт лист с логами AI-сопоставлений"""
        import pandas as pd
        from openpyxl import load_workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        
        # Создаём DataFrame из логов
        df_log = pd.DataFrame(self.ai_validation_log)
        
        # Добавляем лист в каждый выходной файл
        for marketplace, output_path in output_paths.items():
            try:
                # Загружаем существующий файл
                wb = load_workbook(output_path)
                
                # Создаём новый лист
                if 'AI_Логи' in wb.sheetnames:
                    del wb['AI_Логи']
                ws = wb.create_sheet('AI_Логи', 0)  # 0 = первый лист
                
                # Заголовки
                headers = ['Маркетплейс', 'Столбец', 'Исходное значение', 'Сопоставлено с', 'Метод']
                for col_idx, header in enumerate(headers, start=1):
                    cell = ws.cell(row=1, column=col_idx, value=header)
                    cell.font = Font(bold=True, size=11)
                    cell.fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                    cell.font = Font(bold=True, color="FFFFFF")
                    cell.alignment = Alignment(horizontal='center', vertical='center')
                
                # Данные
                for row_idx, row_data in enumerate(self.ai_validation_log, start=2):
                    ws.cell(row=row_idx, column=1, value=row_data['Маркетплейс'])
                    ws.cell(row=row_idx, column=2, value=row_data['Столбец'])
                    ws.cell(row=row_idx, column=3, value=row_data['Исходное значение'])
                    ws.cell(row=row_idx, column=4, value=row_data['Сопоставлено с'])
                    ws.cell(row=row_idx, column=5, value=row_data['Метод'])
                
                # Автоширина колонок
                for col in ws.columns:
                    max_length = 0
                    column = col[0].column_letter
                    for cell in col:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    ws.column_dimensions[column].width = adjusted_width
                
                # Сохраняем
                wb.save(output_path)
                logger.info(f"✅ Лист 'AI_Логи' добавлен в {output_path}")
                
            except Exception as e:
                logger.error(f"❌ Ошибка при создании AI-лога для {marketplace}: {e}")
    
    def _create_ai_log_sheet_in_report(self, report_path: str):
        """Создаёт лист с логами AI-сопоставлений в файле результата"""
        import pandas as pd
        from openpyxl import load_workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        
        try:
            # Загружаем файл результата
            wb = load_workbook(report_path)
            
            # Удаляем старый лист если существует
            if 'AI_Логи' in wb.sheetnames:
                del wb['AI_Логи']
            
            # Создаём новый лист (первым после главного)
            ws = wb.create_sheet('AI_Логи', 1)  # Индекс 1 = второй лист
            
            # Заголовки
            headers = ['Маркетплейс', 'Столбец', 'Исходное значение', 'Сопоставлено с', 'Метод']
            
            for col_idx, header in enumerate(headers, start=1):
                cell = ws.cell(row=1, column=col_idx, value=header)
                cell.font = Font(bold=True, color="FFFFFF", size=11)
                cell.fill = PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
                cell.alignment = Alignment(horizontal='center', vertical='center', wrap_text=True)
            
            # Данные
            for row_idx, row_data in enumerate(self.ai_validation_log, start=2):
                ws.cell(row=row_idx, column=1, value=row_data.get('Маркетплейс', ''))
                ws.cell(row=row_idx, column=2, value=row_data.get('Столбец', ''))
                ws.cell(row=row_idx, column=3, value=row_data.get('Исходное значение', ''))
                ws.cell(row=row_idx, column=4, value=row_data.get('Сопоставлено с', ''))
                ws.cell(row=row_idx, column=5, value=row_data.get('Метод', ''))
            
            # Автоширина колонок
            for col in ws.columns:
                max_length = 0
                column_letter = col[0].column_letter
                
                for cell in col:
                    try:
                        cell_length = len(str(cell.value)) if cell.value else 0
                        if cell_length > max_length:
                            max_length = cell_length
                    except:
                        pass
                
                adjusted_width = min(max_length + 3, 50)
                ws.column_dimensions[column_letter].width = adjusted_width
            
            # Морозим шапку
            ws.freeze_panes = 'A2'
            
            # Сохраняем
            wb.save(report_path)
            logger.info(f"✅ Лист 'AI_Логи' добавлен в {report_path}")
            logger.info(f"📊 Всего записей AI-логов: {len(self.ai_validation_log)}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка при создании AI-лога: {e}")

    
    def _save_results(self, dfs: Dict[str, pd.DataFrame], output_paths: Dict[str, str]):
        """Сохраняет синхронизированные данные в файлы С СОХРАНЕНИЕМ ФОРМАТОВ и AI-проверкой"""
        print("\n[*] Сохраняю синхронизированные данные...")
        
        print(f"[DEBUG] AI comparator доступен: {self.ai_comparator is not None}")
        
        stats = {
            'saved': 0,
            'ai_matched': 0,
            'validation_conflicts': 0,
            'skipped': 0
        }
        
        for marketplace, df in dfs.items():
            output_path = output_paths.get(marketplace)
            if not output_path:
                continue
            
            config = FILE_CONFIGS[marketplace]
            original_file = self.original_file_paths[marketplace]
            
            print(f"\n[*] Обработка {config['display_name']}...")
            
            # Сбрасываем индексы ПЕРЕД сохранением!
            df = df.reset_index(drop=True)
            
            # Открываем ОРИГИНАЛЬНЫЙ файл через openpyxl
            wb = load_workbook(original_file)
            ws = wb[config['sheet_name']]
            
            validation_count = len(ws.data_validations.dataValidation)
            print(f"[DEBUG] Найдено data validations на листе: {validation_count}")
            
            # Получаем номер строки заголовка
            header_row = config['header_row']
            data_start_row = config.get('data_start_row', header_row + 1)
            
            # 🆕 ДОБАВЬ ЭТО: Расширяем лист если нужно
            current_rows = ws.max_row
            required_rows = data_start_row + len(df)
            
            if required_rows > current_rows:
                print(f"[INFO] Расширяю лист: текущих строк = {current_rows}, требуется = {required_rows}")
                # Копируем форматы из последней строки данных
                last_data_row = current_rows
                for row_idx in range(current_rows + 1, required_rows + 1):
                    for col_idx in range(1, ws.max_column + 1):
                        # Копируем стиль из строки выше (или из строки data_start_row)
                        source_cell = ws.cell(row=last_data_row, column=col_idx)
                        target_cell = ws.cell(row=row_idx, column=col_idx)
                        
                        # Копируем стиль
                        if source_cell.has_style:
                            target_cell.font = source_cell.font.copy()
                            target_cell.border = source_cell.border.copy()
                            target_cell.fill = source_cell.fill.copy()
                            target_cell.number_format = source_cell.number_format
                            target_cell.protection = source_cell.protection.copy()
                            target_cell.alignment = source_cell.alignment.copy()
            
            # Создаем маппинг: название колонки -> номер колонки в Excel
            column_mapping = {}
            for col_idx, cell in enumerate(ws[header_row], start=1):
                if cell.value:
                    column_mapping[str(cell.value).strip()] = col_idx
            
            # Используем enumerate для правильного подсчёта строк!
            for row_num, (df_row_idx, row) in enumerate(df.iterrows()):
                # Вычисляем правильную строку в Excel
                excel_row_idx = data_start_row + row_num
                
                for col_name, value in row.items():
                    if col_name not in column_mapping or pd.isna(value):
                        continue
                    
                    excel_col_idx = column_mapping[col_name]
                    cell = ws.cell(row=excel_row_idx, column=excel_col_idx)
                    
                    # Получаем список допустимых значений из validation
                    allowed_values = self._get_validation_list_values(ws, excel_row_idx, excel_col_idx)
                    
                    if allowed_values:
                        print(f"[DEBUG] Столбец '{col_name}', строка {excel_row_idx}: найден validation с {len(allowed_values)} значениями")
                        print(f"[DEBUG] Текущее значение: '{value}'")
                        print(f"[DEBUG] Допустимые значения: {allowed_values[:5]}...")
                    
                    if allowed_values and self.ai_comparator:
                        # Есть validation - используем AI для сопоставления
                        matched_value = self.ai_comparator.match_value_with_list(str(value), allowed_values)
                        
                        if matched_value:
                            cell.value = matched_value
                            stats['saved'] += 1
                            stats['ai_matched'] += 1
                        else:
                            stats['validation_conflicts'] += 1
                            print(f"  [!] Конфликт: '{value}' не найдено в списке {allowed_values[:3]}... (строка {excel_row_idx}, {col_name})")
                            stats['skipped'] += 1
                    else:
                        # Нет validation - записываем как есть
                        cell.value = value
                        stats['saved'] += 1
            
            # Сохраняем файл
            wb.save(output_path)
            print(f"[+] {config['display_name']}: сохранено в '{output_path}'")
        
        # Итоговая статистика
        print(f"\n{'='*60}")
        print(f"СТАТИСТИКА СОХРАНЕНИЯ:")
        print(f"  ✓ Записано значений: {stats['saved']}")
        if self.ai_comparator:
            print(f"  🤖 AI-сопоставлений: {stats['ai_matched']}")
            print(f"  ⚠ Конфликтов с validation: {stats['validation_conflicts']}")
            print(f"  ⊘ Пропущено: {stats['skipped']}")
        print(f"{'='*60}")

