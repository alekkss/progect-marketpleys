"""
Конфигурация логирования
"""
import logging
import sys
from datetime import datetime
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

def setup_logger(name: str = 'data_sync', log_dir: str = 'logs'):
    """
    Настраивает логгер с выводом в консоль и файл
    
    Args:
        name: имя логгера
        log_dir: директория для логов
        
    Returns:
        Настроенный логгер
    """
    # Создаем директорию для логов
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)
    
    # Имя файла с текущей датой
    log_file = log_path / f"sync_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    
    # Создаем логгер
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    
    # Убираем существующие handlers
    logger.handlers.clear()
    
    # Формат логов
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Handler для файла
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Handler для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    logger.info(f"📝 Логи сохраняются в: {log_file}")
    
    return logger
