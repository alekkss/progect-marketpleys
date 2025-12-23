"""
Синхронизация композитных габаритов между маркетплейсами
"""

import pandas as pd
from typing import Dict, Optional
from utils.logger_config import setup_logger
from .constants import ARTICLE_COLUMNS, DIMENSIONS_MAPPING
from .converters import ValueConverter

logger = setup_logger('dimensions')


class DimensionsSynchronizer:
    """Синхронизация композитных габаритов (Длина/Ширина/Высота)"""
    
    @staticmethod
    def parse_composite_dimensions(value: str) -> Optional[Dict[str, float]]:
        """
        Парсит строку "71/68/197" в словарь {length, width, height}
        
        Args:
            value: строка формата "Длина/Ширина/Высота"
            
        Returns:
            {'length': 71.0, 'width': 68.0, 'height': 197.0} или None
        """
        if pd.isna(value) or not str(value).strip():
            return None
        
        try:
            parts = str(value).strip().split('/')
            if len(parts) != 3:
                return None
            
            # Удаляем пробелы и конвертируем в float
            dimensions = {
                'length': float(parts[0].strip()),
                'width': float(parts[1].strip()),
                'height': float(parts[2].strip())
            }
            
            # Проверяем что все значения положительные
            if all(v > 0 for v in dimensions.values()):
                return dimensions
        except (ValueError, AttributeError):
            pass
        
        return None
    
    @staticmethod
    def format_composite_dimensions(length: float, width: float, height: float) -> str:
        """
        Форматирует габариты в строку "Длина/Ширина/Высота"
        
        Args:
            length: длина в см
            width: ширина в см
            height: высота в см
            
        Returns:
            Строка формата "71/68/197"
        """
        return f"{ValueConverter.smart_format(length)}/{ValueConverter.smart_format(width)}/{ValueConverter.smart_format(height)}"
    
    @classmethod
    def sync_dimensions(cls, dfs: Dict[str, pd.DataFrame]) -> int:
        """
        Синхронизирует габариты между всеми маркетплейсами
        
        Args:
            dfs: словарь DataFrame'ов по маркетплейсам
            
        Returns:
            Количество синхронизированных значений
        """
        synced_count = 0
        
        # Создаём маппинг артикул → данные
        yandex_dimensions = {}  # {article: {'length': 71, 'width': 68, 'height': 197}}
        wb_dimensions = {}
        ozon_dimensions = {}
        
        # 1. Читаем данные из Яндекс (композитный формат)
        if 'yandex' in dfs and DIMENSIONS_MAPPING['yandex']['composite'] in dfs['yandex'].columns:
            for idx, row in dfs['yandex'].iterrows():
                article = row.get(ARTICLE_COLUMNS['yandex'])
                if pd.notna(article) and str(article).strip():
                    composite = row.get(DIMENSIONS_MAPPING['yandex']['composite'])
                    dimensions = cls.parse_composite_dimensions(composite)
                    if dimensions:
                        yandex_dimensions[str(article).strip()] = dimensions
        
        # 2. Читаем данные из WB (раздельные столбцы, см)
        if 'wildberries' in dfs:
            wb_map = DIMENSIONS_MAPPING['wildberries']
            df_wb = dfs['wildberries']
            
            for col in [wb_map['length'], wb_map['width'], wb_map['height']]:
                if col not in df_wb.columns:
                    logger.warning(f"[WB] Столбец '{col}' не найден!")
                    return synced_count
            
            for idx, row in df_wb.iterrows():
                article = row.get(ARTICLE_COLUMNS['wildberries'])
                if pd.notna(article) and str(article).strip():
                    article_str = str(article).strip()
                    length = row.get(wb_map['length'])
                    width = row.get(wb_map['width'])
                    height = row.get(wb_map['height'])
                    
                    if all(pd.notna(v) and str(v).strip() for v in [length, width, height]):
                        try:
                            wb_dimensions[article_str] = {
                                'length': float(length),
                                'width': float(width),
                                'height': float(height)
                            }
                        except ValueError:
                            pass
        
        # 3. Читаем данные из Ozon (раздельные столбцы, мм)
        if 'ozon' in dfs:
            ozon_map = DIMENSIONS_MAPPING['ozon']
            df_ozon = dfs['ozon']
            
            for col in [ozon_map['length'], ozon_map['width'], ozon_map['height']]:
                if col not in df_ozon.columns:
                    logger.warning(f"[OZON] Столбец '{col}' не найден!")
                    return synced_count
            
            for idx, row in df_ozon.iterrows():
                article = row.get(ARTICLE_COLUMNS['ozon'])
                if pd.notna(article) and str(article).strip():
                    article_str = str(article).strip()
                    length_mm = row.get(ozon_map['length'])
                    width_mm = row.get(ozon_map['width'])
                    height_mm = row.get(ozon_map['height'])
                    
                    if all(pd.notna(v) and str(v).strip() for v in [length_mm, width_mm, height_mm]):
                        try:
                            # Конвертируем мм → см
                            ozon_dimensions[article_str] = {
                                'length': ValueConverter.mm_to_cm(float(length_mm)),
                                'width': ValueConverter.mm_to_cm(float(width_mm)),
                                'height': ValueConverter.mm_to_cm(float(height_mm))
                            }
                        except ValueError:
                            pass
        
        # 4. СИНХРОНИЗАЦИЯ: Яндекс → WB/Ozon
        synced_count += cls._sync_yandex_to_others(dfs, yandex_dimensions)
        
        # 5. СИНХРОНИЗАЦИЯ: WB → Яндекс
        synced_count += cls._sync_wb_to_yandex(dfs, wb_dimensions, yandex_dimensions)
        
        # 6. СИНХРОНИЗАЦИЯ: Ozon → Яндекс/WB
        synced_count += cls._sync_ozon_to_others(dfs, ozon_dimensions, yandex_dimensions)
        
        logger.info(f"✅ Габариты: синхронизировано {synced_count} значений")
        return synced_count
    
    @classmethod
    def _sync_yandex_to_others(cls, dfs: Dict[str, pd.DataFrame], yandex_dimensions: Dict) -> int:
        """Синхронизирует габариты из Яндекса в WB и Ozon"""
        count = 0
        
        for article, dimensions in yandex_dimensions.items():
            # Синхронизация в WB
            if 'wildberries' in dfs:
                df_wb = dfs['wildberries']
                wb_map = DIMENSIONS_MAPPING['wildberries']
                mask = df_wb[ARTICLE_COLUMNS['wildberries']].astype(str).str.strip() == article
                
                if mask.any():
                    idx = df_wb[mask].index[0]
                    
                    if pd.isna(df_wb.at[idx, wb_map['length']]) or not str(df_wb.at[idx, wb_map['length']]).strip():
                        df_wb.at[idx, wb_map['length']] = dimensions['length']
                        count += 1
                    if pd.isna(df_wb.at[idx, wb_map['width']]) or not str(df_wb.at[idx, wb_map['width']]).strip():
                        df_wb.at[idx, wb_map['width']] = dimensions['width']
                        count += 1
                    if pd.isna(df_wb.at[idx, wb_map['height']]) or not str(df_wb.at[idx, wb_map['height']]).strip():
                        df_wb.at[idx, wb_map['height']] = dimensions['height']
                        count += 1
            
            # Синхронизация в Ozon
            if 'ozon' in dfs:
                df_ozon = dfs['ozon']
                ozon_map = DIMENSIONS_MAPPING['ozon']
                mask = df_ozon[ARTICLE_COLUMNS['ozon']].astype(str).str.strip() == article
                
                if mask.any():
                    idx = df_ozon[mask].index[0]
                    
                    if pd.isna(df_ozon.at[idx, ozon_map['length']]) or not str(df_ozon.at[idx, ozon_map['length']]).strip():
                        df_ozon.at[idx, ozon_map['length']] = int(ValueConverter.cm_to_mm(dimensions['length']))
                        count += 1
                    if pd.isna(df_ozon.at[idx, ozon_map['width']]) or not str(df_ozon.at[idx, ozon_map['width']]).strip():
                        df_ozon.at[idx, ozon_map['width']] = int(ValueConverter.cm_to_mm(dimensions['width']))
                        count += 1
                    if pd.isna(df_ozon.at[idx, ozon_map['height']]) or not str(df_ozon.at[idx, ozon_map['height']]).strip():
                        df_ozon.at[idx, ozon_map['height']] = int(ValueConverter.cm_to_mm(dimensions['height']))
                        count += 1
        
        return count
    
    @classmethod
    def _sync_wb_to_yandex(cls, dfs: Dict[str, pd.DataFrame], wb_dimensions: Dict, yandex_dimensions: Dict) -> int:
        """Синхронизирует габариты из WB в Яндекс"""
        count = 0
        
        for article, dimensions in wb_dimensions.items():
            if article in yandex_dimensions:
                continue  # Уже есть данные из Яндекса
            
            if 'yandex' in dfs:
                df_yandex = dfs['yandex']
                yandex_col = DIMENSIONS_MAPPING['yandex']['composite']
                mask = df_yandex[ARTICLE_COLUMNS['yandex']].astype(str).str.strip() == article
                
                if mask.any():
                    idx = df_yandex[mask].index[0]
                    if pd.isna(df_yandex.at[idx, yandex_col]) or not str(df_yandex.at[idx, yandex_col]).strip():
                        composite = cls.format_composite_dimensions(
                            dimensions['length'],
                            dimensions['width'],
                            dimensions['height']
                        )
                        df_yandex.at[idx, yandex_col] = composite
                        count += 1
                        logger.info(f"[WB→Яндекс] {article}: {composite}")
        
        return count
    
    @classmethod
    def _sync_ozon_to_others(cls, dfs: Dict[str, pd.DataFrame], ozon_dimensions: Dict, yandex_dimensions: Dict) -> int:
        """Синхронизирует габариты из Ozon в Яндекс и WB"""
        count = 0
        
        for article, dimensions in ozon_dimensions.items():
            # В Яндекс
            if 'yandex' in dfs and article not in yandex_dimensions:
                df_yandex = dfs['yandex']
                yandex_col = DIMENSIONS_MAPPING['yandex']['composite']
                mask = df_yandex[ARTICLE_COLUMNS['yandex']].astype(str).str.strip() == article
                
                if mask.any():
                    idx = df_yandex[mask].index[0]
                    if pd.isna(df_yandex.at[idx, yandex_col]) or not str(df_yandex.at[idx, yandex_col]).strip():
                        composite = cls.format_composite_dimensions(
                            dimensions['length'],
                            dimensions['width'],
                            dimensions['height']
                        )
                        df_yandex.at[idx, yandex_col] = composite
                        count += 1
                        logger.info(f"[Ozon→Яндекс] {article}: {composite}")
            
            # В WB
            if 'wildberries' in dfs:
                df_wb = dfs['wildberries']
                wb_map = DIMENSIONS_MAPPING['wildberries']
                mask = df_wb[ARTICLE_COLUMNS['wildberries']].astype(str).str.strip() == article
                
                if mask.any():
                    idx = df_wb[mask].index[0]
                    
                    if pd.isna(df_wb.at[idx, wb_map['length']]) or not str(df_wb.at[idx, wb_map['length']]).strip():
                        df_wb.at[idx, wb_map['length']] = dimensions['length']
                        count += 1
                        logger.info(f"[Ozon→WB] {article}: length={dimensions['length']}")
                    if pd.isna(df_wb.at[idx, wb_map['width']]) or not str(df_wb.at[idx, wb_map['width']]).strip():
                        df_wb.at[idx, wb_map['width']] = dimensions['width']
                        count += 1
                        logger.info(f"[Ozon→WB] {article}: width={dimensions['width']}")
                    if pd.isna(df_wb.at[idx, wb_map['height']]) or not str(df_wb.at[idx, wb_map['height']]).strip():
                        df_wb.at[idx, wb_map['height']] = dimensions['height']
                        count += 1
                        logger.info(f"[Ozon→WB] {article}: height={dimensions['height']}")
        
        return count
