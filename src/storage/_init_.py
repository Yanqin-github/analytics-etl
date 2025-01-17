"""
Storage components for raw and processed data.
"""

from .base_storage import BaseStorage
from .raw_storage import RawStorage
from .processed_storage import ProcessedStorage

__all__ = ['BaseStorage', 'RawStorage', 'ProcessedStorage']
