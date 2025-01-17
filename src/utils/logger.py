import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

def setup_logger(
    name: str = 'analytics',
    level: int = logging.INFO,
    log_dir: Optional[str] = 'logs',
    console: bool = True
) -> logging.Logger:
    """Setup logger with file and console handlers.
    
    Args:
        name: Logger name
        level: Logging level
        log_dir: Log directory path
        console: Whether to add console handler
    
    Returns:
        logging.Logger: Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    # Setup file handler
    if log_dir:
        log_dir = Path(log_dir)
        log_dir.mkdir(exist_ok=True)
        
        log_file = log_dir / f"{name}_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(detailed_formatter)
        logger.addHandler(file_handler)

    # Setup console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(console_formatter)
        logger.addHandler(console_handler)

    return logger
