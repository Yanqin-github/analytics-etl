import logging
from typing import Dict, Any, List, Optional
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.transforms import (
    IdentityTransform,
    TimeTransform,
    BucketTransform
)
import pandas as pd
from datetime import datetime

logger = logging.getLogger(__name__)

class IcebergTableManager:
    """Manage Iceberg tables in S3."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Iceberg table manager.
        
        Args:
            config: Configuration dictionary containing Iceberg and AWS settings
        """
        self.config = config
        self.catalog_name = config['storage'].get('catalog_name', 'analytics')
        
        # Iceberg catalog configuration
        self.catalog_config = {
            'type': 'rest',
            'uri': config['storage'].get('catalog_uri', 'http://localhost:8181'),
            'warehouse': config['storage'].get('warehouse_path'),
            's3.endpoint': config['storage'].get('s3_endpoint', 'https://s3.amazonaws.com'),
            's3.access-key-id': config['storage'].get('aws_access_key'),
            's3.secret-access-key': config['storage'].get('aws_secret_key'),
            's3.region': config['storage'].get('aws_region', 'us-east-1')
        }
        
        self.catalog = load_catalog(self.catalog_name, **self.catalog_config)

    def create_table(
        self,
        table_name: str,
        schema: Schema,
        partition_spec: Optional[PartitionSpec] = None,
        properties: Optional[Dict[str, str]] = None
    ) -> Table:
        """Create new Iceberg table.
        
        Args:
            table_name: Name of the table
            schema: Iceberg schema
            partition_spec: Partition specification
            properties: Table properties
        
        Returns:
            Table: Created Iceberg table
        """
        try:
            default_properties = {
                'write.format.default': 'parquet',
                'write.metadata.compression-codec': 'gzip',
                'write.parquet.compression-codec': 'snappy',
                'format-version': '2'
            }
            
            if properties:
                default_properties.update(properties)

            return self.catalog.create_table(
                identifier=table_name,
                schema=schema,
                partition_spec=partition_spec,
                properties=default_properties
            )
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {str(e)}")
            raise

    def load_table(self, table_name: str) -> Table:
        """Load existing Iceberg table.
        
        Args:
            table_name: Name of the table
        
        Returns:
            Table: Loaded Iceberg table
        """
        try:
            return self.catalog.load_table(table_name)
        except Exception as e:
            logger.error(f"Error loading table {table_name}: {str(e)}")
            raise

    def write_dataframe(
        self,
        table: Table,
        df: pd.DataFrame,
        overwrite: bool = False
    ) -> bool:
        """Write DataFrame to Iceberg table.
        
        Args:
            table: Target Iceberg table
            df: DataFrame to write
            overwrite: Whether to overwrite existing data
        
        Returns:
            bool: True if write successful
        """
        try:
            # Convert DataFrame to PyArrow table
            arrow_table = df.to_arrow_table()
            
            if overwrite:
                # Overwrite entire table
                with table.new_overwrite() as overwrite:
                    overwrite.overwrite_table(arrow_table)
            else:
                # Append to table
                with table.new_append() as append:
                    append.append_table(arrow_table)
            
            return True
        except Exception as e:
            logger.error(f"Error writing to table {table.name}: {str(e)}")
            return False

    def read_table(
        self,
        table: Table,
        columns: Optional[List[str]] = None,
        snapshot_id: Optional[int] = None,
        filters: Optional[List[Any]] = None
    ) -> pd.DataFrame:
        """Read Iceberg table into DataFrame.
        
        Args:
            table: Source Iceberg table
            columns: Columns to read
            snapshot_id: Specific snapshot to read
            filters: Row filters to apply
        
        Returns:
            pd.DataFrame: Table data
        """
        try:
            # Build scan
            scan = table.scan()
            
            if columns:
                scan = scan.select(*columns)
            
            if snapshot_id:
                scan = scan.use_snapshot(snapshot_id)
            
            if filters:
                scan = scan.filter(*filters)
            
            # Convert to DataFrame
            return scan.to_pandas()
        except Exception as e:
            logger.error(f"Error reading table {table.name}: {str(e)}")
            raise

    def get_table_metadata(self, table: Table) -> Dict[str, Any]:
        """Get table metadata.
        
        Args:
            table: Iceberg table
        
        Returns:
            Dict[str, Any]: Table metadata
        """
        try:
            current_snapshot = table.current_snapshot()
            return {
                'name': table.name,
                'location': table.location,
                'format_version': table.format_version,
                'schema': table.schema(),
                'partition_spec': table.partition_spec(),
                'properties': table.properties,
                'current_snapshot_id': current_snapshot.snapshot_id if current_snapshot else None,
                'timestamp': datetime.fromtimestamp(
                    current_snapshot.timestamp_ms / 1000
                ) if current_snapshot else None
            }
        except Exception as e:
            logger.error(f"Error getting metadata for table {table.name}: {str(e)}")
            raise

    def optimize_table(
        self,
        table: Table,
        target_file_size_bytes: Optional[int] = None
    ) -> bool:
        """Optimize table by compacting small files.
        
        Args:
            table: Table to optimize
            target_file_size_bytes: Target file size for compaction
        
        Returns:
            bool: True if optimization successful
        """
        try:
            # Rewrite small files
            table.rewrite_data_files(
                target_file_size_bytes=target_file_size_bytes
            )
            
            # Remove old snapshots
            table.expire_snapshots()
            
            return True
        except Exception as e:
            logger.error(f"Error optimizing table {table.name}: {str(e)}")
            return False

    def create_time_partitioned_table(
        self,
        table_name: str,
        schema: Schema,
        timestamp_column: str = 'timestamp',
        granularity: str = 'day'
    ) -> Table:
        """Create time-partitioned Iceberg table.
        
        Args:
            table_name: Name of the table
            schema: Table schema
            timestamp_column: Timestamp column name
            granularity: Time partition granularity (hour, day, month, year)
        
        Returns:
            Table: Created table
        """
        try:
            partition_spec = PartitionSpec.builder()
            
            # Add time partitioning
            if granularity == 'hour':
                partition_spec.add_transform(
                    TimeTransform('hour'),
                    timestamp_column,
                    'hour'
                )
            elif granularity == 'day':
                partition_spec.add_transform(
                    TimeTransform('day'),
                    timestamp_column,
                    'day'
                )
            elif granularity == 'month':
                partition_spec.add_transform(
                    TimeTransform('month'),
                    timestamp_column,
                    'month'
                )
            elif granularity == 'year':
                partition_spec.add_transform(
                    TimeTransform('year'),
                    timestamp_column,
                    'year'
                )
            
            return self.create_table(
                table_name=table_name,
                schema=schema,
                partition_spec=partition_spec.build()
            )
        except Exception as e:
            logger.error(f"Error creating partitioned table {table_name}: {str(e)}")
            raise
