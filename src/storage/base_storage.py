from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import pandas as pd
import logging
from datetime import datetime
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from ..utils.iceberg_utils import IcebergTableManager

logger = logging.getLogger(__name__)

class BaseStorage(ABC):
    """Base class for all storage implementations using Iceberg."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize base storage.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.iceberg = IcebergTableManager(config)
        self.metrics = {
            'last_write_time': None,
            'records_written': 0,
            'write_errors': 0,
            'average_write_time': 0,
            'table_snapshots': 0
        }
        self._write_count = 0

    @abstractmethod
    def get_schema(self) -> Schema:
        """Get Iceberg schema for the table.
        
        Returns:
            Schema: Iceberg schema
        """
        raise NotImplementedError("Storage classes must implement get_schema method")

    @abstractmethod
    def store(self, data: pd.DataFrame, **kwargs) -> bool:
        """Store data to Iceberg table.
        
        Args:
            data: DataFrame to store
            **kwargs: Additional storage options
        
        Returns:
            bool: True if storage was successful
        """
        raise NotImplementedError("Storage classes must implement store method")

    def update_metrics(self, start_time: datetime, records: int, error: bool = False) -> None:
        """Update storage metrics.
        
        Args:
            start_time: Operation start time
            records: Number of records written
            error: Whether an error occurred
        """
        end_time = datetime.now()
        write_time = (end_time - start_time).total_seconds()

        self.metrics['last_write_time'] = end_time
        self.metrics['records_written'] += records
        
        if error:
            self.metrics['write_errors'] += 1
        else:
            self.metrics['table_snapshots'] += 1

        # Update average write time
        self._write_count += 1
        self.metrics['average_write_time'] = (
            (self.metrics['average_write_time'] * (self._write_count - 1) + write_time)
            / self._write_count
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get storage metrics.
        
        Returns:
            Dict[str, Any]: Storage metrics
        """
        return self.metrics

    def reset_metrics(self) -> None:
        """Reset storage metrics."""
        self.metrics = {
            'last_write_time': None,
            'records_written': 0,
            'write_errors': 0,
            'average_write_time': 0,
            'table_snapshots': 0
        }
        self._write_count = 0

    @abstractmethod
    def health_check(self) -> bool:
        """Check storage health.
        
        Returns:
            bool: True if storage is healthy
        """
        raise NotImplementedError("Storage classes must implement health_check method")

    def __str__(self) -> str:
        """String representation of the storage.
        
        Returns:
            str: Storage description
        """
        return (
            f"{self.__class__.__name__}("
            f"records={self.metrics['records_written']}, "
            f"errors={self.metrics['write_errors']}, "
            f"snapshots={self.metrics['table_snapshots']})"
        )
