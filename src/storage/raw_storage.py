import logging
from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    StringType,
    DoubleType,
    MapType
)
from .base_storage import BaseStorage

logger = logging.getLogger(__name__)

class RawStorage(BaseStorage):
    """Storage implementation for raw metrics data using Iceberg."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize raw storage."""
        super().__init__(config)
        self.warehouse_namespace = config['storage'].get('raw_namespace', 'raw')

    def get_schema(self) -> Schema:
        """Get Iceberg schema for raw metrics.
        
        Returns:
            Schema: Iceberg schema
        """
        return Schema(
            TimestampType(),
            StringType(),
            DoubleType(),
            MapType(StringType(), StringType()),
            names=["timestamp", "metric_name", "value", "metadata"]
        )

    def store(self, data: pd.DataFrame, source: str) -> bool:
        """Store raw metrics data in Iceberg table.
        
        Args:
            data: DataFrame to store
            source: Data source identifier
        
        Returns:
            bool: True if storage was successful
        """
        start_time = datetime.now()
        try:
            if data.empty:
                logger.warning("Empty DataFrame provided, skipping storage")
                return False

            # Validate source
            if not source or not isinstance(source, str):
                raise ValueError("Invalid source identifier")

            # Table name in the format: raw.metrics_source
            table_name = f"{self.warehouse_namespace}.metrics_{source}"

            # Get or create table
            try:
                table = self.iceberg.load_table(table_name)
                logger.info(f"Using existing table: {table_name}")
            except Exception:
                logger.info(f"Creating new table: {table_name}")
                table = self.iceberg.create_time_partitioned_table(
                    table_name=table_name,
                    schema=self.get_schema(),
                    timestamp_column='timestamp',
                    granularity='day'
                )

            # Write data
            success = self.iceberg.write_dataframe(table, data)
            
            if success:
                self.update_metrics(start_time, len(data))
                logger.info(f"Successfully wrote {len(data)} records to {table_name}")
                return True
            else:
                self.update_metrics(start_time, 0, error=True)
                return False

        except Exception as e:
            self.update_metrics(start_time, 0, error=True)
            logger.error(f"Error storing raw data: {str(e)}")
            raise

    def health_check(self) -> bool:
        """Check raw storage health.
        
        Returns:
            bool: True if storage is healthy
        """
        try:
            # Create test data
            test_data = pd.DataFrame({
                'timestamp': [datetime.now()],
                'metric_name': ['test_metric'],
                'value': [1.0],
                'metadata': [{'test': 'value'}]
            })
            
            # Try to write test data
            return self.store(test_data, source='health_check')
        except Exception as e:
            logger.error(f"Raw storage health check failed: {str(e)}")
            return False
