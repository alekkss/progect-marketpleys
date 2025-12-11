"""
Модуль для синхронизации данных между маркетплейсами
"""
import pandas as pd
import re
from openpyxl import load_workbook
from typing import Dict, List, Tuple, Optional
from config import FILE_CONFIGS, is_excluded_column


class DataSynchronizer:
    """Класс для синхронизации данных между тремя маркетплейсами"""
    
    def __init__(self, comparison_result: Dict):
        """
        Инициализация синхронизатора
        
        Args:
            comparison_result: результаты сравнения столбцов от AI
        """
        self.comparison_result = comparison_result
        self.article_columns = {
            'wildberries': 'Артикул продавца',
            'ozon': 'Артикул*',
            'yandex': 'Ваш SKU *'
        }
        # Словарь для отслеживания изменений
        self.changes_log = {
            'wildberries': [],
            'ozon': [],
            'yandex': []
        }
    
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
        output_paths: Dict[str, str] = None
    ) -> Tuple[Dict[str, pd.DataFrame], Dict]:
        """
        Синхронизирует данные между тремя файлами
        
        Args:
            file_paths: словарь с путями к файлам {'wildberries': path, 'ozon': path, 'yandex': path}
            output_paths: словарь с путями для сохранения (опционально)
        
        Returns:
            Кортеж (словарь с синхронизированными DataFrame, словарь с логом изменений)
        """
        print("\n" + "="*60)
        print("СИНХРОНИЗАЦИЯ ДАННЫХ МЕЖДУ МАРКЕТПЛЕЙСАМИ")
        print("="*60)
        
        # Загружаем данные из всех трех файлов
        dfs = self._load_all_dataframes(file_paths)
        
        # Синхронизируем данные
        synced_dfs = self._sync_all_matches(dfs)
        
        # Сохраняем результаты
        if output_paths:
            self._save_results(synced_dfs, output_paths)
        
        print("\n✅ Синхронизация завершена!")
        return synced_dfs, self.changes_log
    
    def _load_all_dataframes(self, file_paths: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """Загружает данные из всех трех файлов"""
        print("\n[*] Загружаю данные из файлов...")
        
        dfs = {}
        
        for marketplace, file_path in file_paths.items():
            config = FILE_CONFIGS[marketplace]
            
            # Читаем Excel файл
            df = pd.read_excel(
                file_path,
                sheet_name=config['sheet_name'],
                header=config['header_row'] - 1  # pandas использует 0-индексацию
            )
            
            dfs[marketplace] = df
            print(f"[+] {config['display_name']}: загружено {len(df)} товаров")
        
        return dfs
    
    def _sync_all_matches(self, dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """Синхронизирует все совпадающие столбцы"""
        
        # Создаем копии для работы
        synced_dfs = {
            'wildberries': dfs['wildberries'].copy(),
            'ozon': dfs['ozon'].copy(),
            'yandex': dfs['yandex'].copy()
        }
        
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
            
            # Заполняем пустые ячейки с конвертацией единиц
            if article in wb_data and (pd.isna(values['wildberries']) or not str(values['wildberries']).strip()):
                idx = wb_data[article]['index']
                col_dtype = dfs['wildberries'][col_wb].dtype
                
                # Конвертируем значение
                converted_value = self._convert_value(source_value, source_unit, unit_wb)
                
                try:
                    value_to_set = converted_value
                    if pd.api.types.is_numeric_dtype(col_dtype):
                        value_to_set = pd.to_numeric(converted_value, errors='coerce')
                    dfs['wildberries'].at[idx, col_wb] = value_to_set
                    filled_count += 1
                    
                    # Логируем изменение
                    self._log_change('wildberries', article, col_wb, converted_value)
                except Exception:
                    pass
            
            if article in ozon_data and (pd.isna(values['ozon']) or not str(values['ozon']).strip()):
                idx = ozon_data[article]['index']
                col_dtype = dfs['ozon'][col_ozon].dtype
                
                # Конвертируем значение
                converted_value = self._convert_value(source_value, source_unit, unit_ozon)
                
                try:
                    value_to_set = converted_value
                    if pd.api.types.is_numeric_dtype(col_dtype):
                        value_to_set = pd.to_numeric(converted_value, errors='coerce')
                    dfs['ozon'].at[idx, col_ozon] = value_to_set
                    filled_count += 1
                    
                    # Логируем изменение
                    self._log_change('ozon', article, col_ozon, converted_value)
                except Exception:
                    pass
            
            if article in yandex_data and (pd.isna(values['yandex']) or not str(values['yandex']).strip()):
                idx = yandex_data[article]['index']
                col_dtype = dfs['yandex'][col_yandex].dtype
                
                # Конвертируем значение
                converted_value = self._convert_value(source_value, source_unit, unit_yandex)
                
                try:
                    value_to_set = converted_value
                    if pd.api.types.is_numeric_dtype(col_dtype):
                        value_to_set = pd.to_numeric(converted_value, errors='coerce')
                    dfs['yandex'].at[idx, col_yandex] = value_to_set
                    filled_count += 1
                    
                    # Логируем изменение
                    self._log_change('yandex', article, col_yandex, converted_value)
                except Exception:
                    pass
        
        return filled_count
    
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
            
            # Заполняем пустые ячейки
            if article in data1 and article in data2:
                # Если в первом пусто, а во втором есть
                if (pd.isna(val1) or not str(val1).strip()) and pd.notna(val2) and str(val2).strip():
                    idx = data1[article]['index']
                    col_dtype = dfs[mp1][col1].dtype
                    
                    # Конвертируем значение
                    converted_value = self._convert_value(val2, unit2, unit1)
                    
                    try:
                        value_to_set = converted_value
                        if pd.api.types.is_numeric_dtype(col_dtype):
                            value_to_set = pd.to_numeric(converted_value, errors='coerce')
                        dfs[mp1].at[idx, col1] = value_to_set
                        filled_count += 1
                        
                        # Логируем изменение
                        self._log_change(mp1, article, col1, converted_value)
                    except Exception:
                        pass
                
                # Если во втором пусто, а в первом есть
                elif (pd.isna(val2) or not str(val2).strip()) and pd.notna(val1) and str(val1).strip():
                    idx = data2[article]['index']
                    col_dtype = dfs[mp2][col2].dtype
                    
                    # Конвертируем значение
                    converted_value = self._convert_value(val1, unit1, unit2)
                    
                    try:
                        value_to_set = converted_value
                        if pd.api.types.is_numeric_dtype(col_dtype):
                            value_to_set = pd.to_numeric(converted_value, errors='coerce')
                        dfs[mp2].at[idx, col2] = value_to_set
                        filled_count += 1
                        
                        # Логируем изменение
                        self._log_change(mp2, article, col2, converted_value)
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
    
    def _save_results(self, dfs: Dict[str, pd.DataFrame], output_paths: Dict[str, str]):
        """Сохраняет синхронизированные данные в файлы"""
        print("\n[*] Сохраняю синхронизированные данные...")
        
        for marketplace, df in dfs.items():
            output_path = output_paths.get(marketplace)
            if not output_path:
                continue
            
            config = FILE_CONFIGS[marketplace]
            
            # Сохраняем в Excel
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                df.to_excel(
                    writer,
                    sheet_name=config['sheet_name'],
                    index=False,
                    startrow=config['header_row'] - 1
                )
            
            print(f"[+] {config['display_name']}: сохранено в '{output_path}'")
