"""
Главный модуль приложения
"""
from pathlib import Path
from config import FILE_CONFIGS
from excel_reader import ExcelReader
from ai_comparator import AIComparator
from excel_writer import ExcelWriter
from data_synchronizer import DataSynchronizer


def main():
    """Главная функция программы"""
    
    print("=" * 60)
    print("Сравнение столбцов Excel с помощью AI (3 маркетплейса)")
    print("=" * 60)
    
    # Пути к файлам - ИЗМЕНИ НА СВОИ!
    file_paths = {
        "wildberries": '/Users/aleksander/Documents/Коды/my-poisk/progect-marketplays/Стиральные машины шаблон с товарами WB.xlsx',
        "ozon": '/Users/aleksander/Documents/Коды/my-poisk/progect-marketplays/Стиральная машина_шаблон с товарами Ozon.xlsx',
        "yandex": '/Users/aleksander/Documents/Коды/my-poisk/progect-marketplays/Стиральные машины шаблон с товарами Яндекс Маркет.xlsx'
    }

    # Пути для сохранения синхронизированных файлов
    output_sync_paths = {
        "wildberries": '/Users/aleksander/Documents/Коды/my-poisk/progect-marketplays/WB_синхронизировано.xlsx',
        "ozon": '/Users/aleksander/Documents/Коды/my-poisk/progect-marketplays/Ozon_синхронизировано.xlsx',
        "yandex": '/Users/aleksander/Documents/Коды/my-poisk/progect-marketplays/Яндекс_синхронизировано.xlsx'
    }
    
    output_file = "результат_сравнения_маркетплейсов.xlsx"
    
    try:
        # Проверяем существование файлов
        for marketplace, file_path in file_paths.items():
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Файл '{file_path}' не найден")
        
        # Создаем экземпляры классов
        reader = ExcelReader()
        comparator = AIComparator()
        writer = ExcelWriter()
        
        # Загружаем столбцы из файлов
        columns = {}
        for marketplace, file_path in file_paths.items():
            config = FILE_CONFIGS[marketplace]
            print(f"\n[*] Загружаю столбцы из {config['display_name']} "
                  f"(лист '{config['sheet_name']}', строка {config['header_row']})...")
            
            columns[marketplace] = reader.get_column_names(
                file_path, 
                config['sheet_name'], 
                config['header_row']
            )
            print(f"[+] Найдено {len(columns[marketplace])} столбцов")
        
        # Сравниваем столбцы с помощью AI
        print(f"\n[*] Сравниваю столбцы с помощью AI...")
        comparison_result = comparator.compare_columns(
            columns['wildberries'],
            columns['ozon'],
            columns['yandex']
        )
        
        # Выводим результаты в консоль
        print_results(comparison_result)
        
        # Запрашиваем синхронизацию
        print(f"\n{'='*60}")
        print("Хотите синхронизировать данные между маркетплейсами? (y/n)")
        user_input = input("Ввод: ").strip().lower()
        
        if user_input == 'y':
            synchronizer = DataSynchronizer(comparison_result)
            synced_dfs, changes_log = synchronizer.synchronize_data(file_paths, output_sync_paths)
            
            # Создаем отчет с логом изменений
            writer.create_report_with_changes(comparison_result, changes_log, output_file)
        else:
            # Создаем обычный отчет без логов изменений
            writer.create_report(comparison_result, output_file)
        
        print(f"\n{'='*60}")
        print(f"✅ УСПЕШНО! Результаты в файле '{output_file}'")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n[!] ОШИБКА: {e}")
        import traceback
        traceback.print_exc()


def print_results(comparison_result: dict):
    """Выводит результаты сравнения в консоль"""
    print(f"\n{'='*60}")
    print("РЕЗУЛЬТАТЫ СРАВНЕНИЯ")
    print(f"{'='*60}")
    
    print(f"\n🔗 Совпадения во всех 3 маркетплейсах ({len(comparison_result.get('matches_all_three', []))} шт):")
    for match in comparison_result.get('matches_all_three', []):
        confidence = int(match.get('confidence', 0) * 100)
        marker = "🔒" if match.get('mandatory') or confidence == 100 else "✓"
        print(f"  {marker} WB: '{match.get('column_1', '')}' ↔ "
              f"Ozon: '{match.get('column_2', '')}' ↔ "
              f"Яндекс: '{match.get('column_3', '')}' ({confidence}%)")
    
    print(f"\n🔗 Совпадения WB ↔ Ozon ({len(comparison_result.get('matches_1_2', []))} шт):")
    for match in comparison_result.get('matches_1_2', [])[:5]:
        confidence = int(match.get('confidence', 0) * 100)
        marker = "🔒" if match.get('mandatory') or confidence == 100 else "✓"
        print(f"  {marker} '{match.get('column_1', '')}' ↔ '{match.get('column_2', '')}' ({confidence}%)")
    if len(comparison_result.get('matches_1_2', [])) > 5:
        print(f"  ... и еще {len(comparison_result.get('matches_1_2', [])) - 5} совпадений")
    
    print(f"\n📊 Статистика:")
    print(f"  • Только в WB: {len(comparison_result.get('only_in_first', []))} столбцов")
    print(f"  • Только в Ozon: {len(comparison_result.get('only_in_second', []))} столбцов")
    print(f"  • Только в Яндекс: {len(comparison_result.get('only_in_third', []))} столбцов")


if __name__ == "__main__":
    main()
