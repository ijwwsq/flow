import logging
import sys
from datetime import datetime
from pathlib import Path


class TaskFlowLogger:
    def __init__(self, log_file="taskflow.log"):
        self.log_file = Path(log_file)
        self.setup_logging()
    
    def setup_logging(self):
        # Настройка логгера для файла
        file_handler = logging.FileHandler(self.log_file)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        
        # Настройка логгера для консоли
        console_handler = logging.StreamHandler(sys.stdout)
        console_formatter = logging.Formatter('%(message)s')
        console_handler.setFormatter(console_formatter)
        
        # Основной логгер
        self.logger = logging.getLogger('taskflow')
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        # Предотвращаем дублирование логов
        self.logger.propagate = False
    
    def info(self, msg):
        self.logger.info(msg)
    
    def error(self, msg):
        self.logger.error(msg)
    
    def warning(self, msg):
        self.logger.warning(msg)