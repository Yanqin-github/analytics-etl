import logging
from typing import Dict, Any, Optional
import pandas as pd
from datetime import datetime
from pyiceberg.schema import Schema
from pyiceberg.types import (
    TimestampType,
    StringType,
    DoubleType,
    MapType,
    LongType
)
from .base_storage import BaseStorage

logger = logging.getLogger(__name__)

class ProcessedStorage(BaseStorage):
    """Storage implementation for processed metrics data using Iceberg."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize processed storage."""
        super().__init__(config)
        self.warehouse_namespace = config['storage'].get('processed_namespace', 'processed')
        self.schema_version = config['storage'].get('schema_version', 'v1')

    def get_schema(self) -> Schema:
        """Get Iceberg schema for processed metrics.
        
        Returns:
            Schema: Iceberg schema
        """
        return Schema(
            TimestampType(),
            StringType(),
            DoubleType(),
            StringType(),
            MapType(StringType(), StringType()),
            DoubleType(),
            DoubleType(),
            LongType(),
            names=[
                "timestamp",
                "metric_name",
                "value",
                "source",
                "dimensions",
                "value_min",
                "value_max",
                "value_count"
            ]
        )

    def store(
        self,
        data: pd.DataFrame,
        table_name: Optional[str] = None
    ) -> bool:
        """Store processed metrics data in Iceberg table.
        
        Args:
            data: DataFrame to store
            table_name: Optional custom table name
        
        Returns:
            bool: True if storage was successful
        """
        start_time = datetime.now()
        try:
            if data.empty:
                logger.warning("Empty DataFrame provided, skipping storage")
                return False

            # Default table name: processed.metrics_v1
            if table_name is None:
                table_name = f"{self.warehouse_namespace}.metrics_{self.schema_version}"

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
                
                # Optimize table if needed
                if len(data) > 1000000:  # Arbitrary threshold
                    logger.info(f"Optimizing table {table_name}")
                    self.iceberg.optimize_table(table)
                
                return True
            else:
                self.update_metrics(start_time, 0, error=True)
                return False

        except Exception as e:
            self.update_metrics(start_time, 0, error=True)
            logger.error(f"Error storing processed data: {str(e)}")
            raise

    def health_check(self) -> bool:
        """Check processed storage health.
        
        Returns:
            bool: True if storage is healthy
        """
        try:
            # Create test data
            test_data = pd.DataFrame({
                'timestamp': [datetime.now()],
                'metric_name': ['test_metric'],
                'value': [1.0],
                'source': ['test'],
                'dimensions': [{'test': 'value'}],
                'value_min': [1.0],
                'value_max': [1.0],
                'value_count': [1]
            })
            
            # Try to write test data
            return self.store(test_data, table_name=f"{self.warehouse_namespace}.health_check")
        except Exception as e:
            logger.error(f"Processed storage health check failed: {str(e)}")
            return False
