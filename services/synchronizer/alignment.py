"""
Выравнивание артикулов между маркетплейсами
"""

import pandas as pd
from typing import Dict
from utils.logger_config import setup_logger
from .constants import ARTICLE_COLUMNS

logger = setup_logger('alignment')


class ArticleAligner:
    """Выравнивание артикулов - добавление отсутствующих строк в DataFrame"""
    
    @staticmethod
    def align_articles(dfs: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
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
        all_articles = ArticleAligner._collect_all_articles(dfs)
        logger.info(f"\n🔍 Всего уникальных артикулов: {len(all_articles)}")
        
        # Для каждого маркетплейса проверяем недостающие артикулы
        total_added = 0
        for marketplace in ['wildberries', 'ozon', 'yandex']:
            article_col = ARTICLE_COLUMNS[marketplace]
            
            if article_col not in dfs[marketplace].columns:
                logger.warning(f"⚠️ {marketplace.upper()}: столбец '{article_col}' не найден, пропускаю")
                continue
            
            added = ArticleAligner._add_missing_articles(
                dfs[marketplace], 
                marketplace, 
                article_col, 
                all_articles
            )
            
            if added > 0:
                dfs[marketplace] = dfs[marketplace]
                total_added += added
                logger.info(f" 📊 {marketplace.upper()}: добавлено {added} артикулов")
        
        if total_added > 0:
            logger.info(f"\n✅ Итого добавлено {total_added} новых строк во все маркетплейсы")
        else:
            logger.info(f"\n✅ Выравнивание не требуется - все артикулы присутствуют")
        
        return dfs
    
    @staticmethod
    def _collect_all_articles(dfs: Dict[str, pd.DataFrame]) -> set:
        """Собирает все уникальные артикулы из всех маркетплейсов"""
        all_articles = set()
        
        for marketplace in ['wildberries', 'ozon', 'yandex']:
            article_col = ARTICLE_COLUMNS[marketplace]
            
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
        
        return all_articles
    
    @staticmethod
    def _add_missing_articles(
        df: pd.DataFrame, 
        marketplace: str, 
        article_col: str, 
        all_articles: set
    ) -> int:
        """Добавляет недостающие артикулы в DataFrame"""
        
        # Сбрасываем индексы ПЕРЕД обработкой
        df_reset = df.reset_index(drop=True)
        
        # Находим строки с заполненными артикулами
        article_series = df_reset[article_col].dropna().astype(str).str.strip()
        article_series = article_series[article_series != '']
        
        # Фильтрация
        valid_mask = (
            ~article_series.str.contains(
                'идентифицировать|описание|заполнить|пример|название товара|по которому',
                case=False,
                na=False
            ) &
            (article_series.str.len() < 50)
        )
        
        article_series = article_series[valid_mask]
        
        # Получаем позиционный индекс последней заполненной строки
        if len(article_series) > 0:
            last_label_idx = article_series.index[-1]
            last_filled_position = df_reset.index.get_loc(last_label_idx)
        else:
            last_filled_position = -1
        
        existing_articles_set = set(article_series.tolist())
        
        # Находим недостающие
        missing_articles = all_articles - existing_articles_set
        
        if not missing_articles:
            logger.info(f"✅ {marketplace.upper()}: все артикулы присутствуют")
            return 0
        
        logger.info(f"\n➕ {marketplace.upper()}: добавляю {len(missing_articles)} артикулов")
        
        # Создаем новые строки для недостающих артикулов
        new_rows = []
        for article in sorted(missing_articles):
            # Создаем пустую строку со всеми столбцами
            new_row = {col: None for col in df_reset.columns}
            # Заполняем только артикул
            new_row[article_col] = article
            new_rows.append(new_row)
        
        # Вставляем новые строки СРАЗУ ПОСЛЕ последней заполненной
        if new_rows:
            new_df = pd.DataFrame(new_rows)
            
            if last_filled_position >= 0:
                # Есть заполненные строки - вставляем после них
                before = df_reset.iloc[:last_filled_position + 1].copy()
                after = df_reset.iloc[last_filled_position + 1:].copy()
                
                # Склеиваем: заполненные + новые + пустые
                result_df = pd.concat([before, new_df, after], ignore_index=True)
                logger.info(f" ✓ Добавлено {len(new_rows)} строк после позиции {last_filled_position}")
            else:
                # Нет заполненных строк - добавляем в начало
                result_df = pd.concat([new_df, df_reset], ignore_index=True)
                logger.info(f" ✓ Добавлено {len(new_rows)} строк в начало")
            
            # Обновляем оригинальный DataFrame
            df.drop(df.index, inplace=True)
            for col in result_df.columns:
                df[col] = result_df[col].values
            
            logger.info(f" 📊 Было: {len(df_reset)}, стало: {len(result_df)}")
            return len(new_rows)
        
        return 0
