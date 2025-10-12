from loguru import logger
import sys
from pathlib import Path
from typing import Optional
from threading import Lock


class Logger:
    _instance = None
    _lock = Lock()

    def __new__(cls, log_file: Optional[str] = None):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._initialized = False
        return cls._instance

    def __init__(self, log_file: Optional[str] = None):
        if self._initialized:
            return
        self._initialized = True

        # Remove default handlers
        logger.remove()

        # Add console handler (colored)
        logger.add(
            sys.stdout,
            colorize=True,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                   "<level>{level: <8}</level> | "
                   "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                   "<level>{message}</level>"
        )

        # Add file handler if path is given
        if log_file:
            log_path = Path(log_file)
            log_path.parent.mkdir(parents=True, exist_ok=True)
            logger.add(
                log_path,
                rotation="10 MB",
                retention="7 days",
                compression="zip",
                enqueue=True,
                level="INFO",
                format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {name}:{function}:{line} - {message}"
            )

        self._logger = logger

    def get_logger(self):
        """Return the configured singleton logger."""
        return self._logger


# Global accessor
def get_logger(log_file: Optional[str] = None):
    """Retrieve the global singleton logger instance."""
    return Logger(log_file).get_logger()
