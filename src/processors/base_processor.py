from abc import ABC, abstractmethod
from typing import Dict, Any
import pandas as pd
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseProcessor(ABC):
    """Base class for all processors."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize base processor.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config
        self.metrics = {
            'last_processing_time': None,
            'records_processed': 0,
            'processing_errors': 0,
            'average_processing_time': 0,
            'data_quality_score': 0.0
        }
        self._processing_count = 0

    @abstractmethod
    def process(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process the collected data.
        
        Args:
            data: Dictionary of DataFrames from different collectors
        
        Returns:
            pd.DataFrame: Processed and unified data
        
        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Processors must implement process method")

    def validate_schema(self, df: pd.DataFrame) -> bool:
        """Validate processed data schema.
        
        Args:
            df: DataFrame to validate
        
        Returns:
            bool: True if validation passes
        
        Raises:
            ValueError: If validation fails
        """
        required_columns = {
            'timestamp',
            'metric_name',
            'value',
            'source',
            'dimensions'
        }
        
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Validate data types
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            raise ValueError("timestamp column must be datetime type")
        
        if not pd.api.types.is_numeric_dtype(df['value']):
            raise ValueError("value column must be numeric type")
        
        if not pd.api.types.is_string_dtype(df['metric_name']):
            raise ValueError("metric_name column must be string type")
        
        if not pd.api.types.is_string_dtype(df['source']):
            raise ValueError("source column must be string type")

        return True

    def calculate_data_quality(self, df: pd.DataFrame) -> float:
        """Calculate data quality score.
        
        Args:
            df: Processed DataFrame
        
        Returns:
            float: Data quality score (0-1)
        """
        quality_score = 1.0
        total_checks = 4

        # Check for missing values
        missing_pct = df.isnull().mean().mean()
        quality_score -= missing_pct * 0.25

        # Check for duplicate records
        duplicate_pct = df.duplicated().mean()
        quality_score -= duplicate_pct * 0.25

        # Check for out-of-range values (assuming values should be positive)
        invalid_values_pct = (df['value'] < 0).mean()
        quality_score -= invalid_values_pct * 0.25

        # Check for future timestamps
        future_timestamps_pct = (df['timestamp'] > datetime.now()).mean()
        quality_score -= future_timestamps_pct * 0.25

        return max(0.0, min(1.0, quality_score))

    def update_metrics(self, start_time: datetime, records: int, quality_score: float, error: bool = False) -> None:
        """Update processor metrics.
        
        Args:
            start_time: Processing start time
            records: Number of records processed
            quality_score: Data quality score
            error: Whether an error occurred
        """
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        self.metrics['last_processing_time'] = end_time
        self.metrics['records_processed'] += records
        self.metrics['data_quality_score'] = quality_score
        
        if error:
            self.metrics['processing_errors'] += 1

        # Update average processing time
        self._processing_count += 1
        self.metrics['average_processing_time'] = (
            (self.metrics['average_processing_time'] * (self._processing_count - 1) + processing_time)
            / self._processing_count
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get processor metrics.
        
        Returns:
            Dict[str, Any]: Processor metrics
        """
        return self.metrics

    def reset_metrics(self) -> None:
        """Reset processor metrics."""
        self.metrics = {
            'last_processing_time': None,
            'records_processed': 0,
            'processing_errors': 0,
            'average_processing_time': 0,
            'data_quality_score': 0.0
        }
        self._processing_count = 0

    @abstractmethod
    def health_check(self) -> bool:
        """Check processor health.
        
        Returns:
            bool: True if processor is healthy
        """
        raise NotImplementedError("Processors must implement health_check method")

    def __str__(self) -> str:
        """String representation of the processor.
        
        Returns:
            str: Processor description
        """
        return (
            f"{self.__class__.__name__}("
            f"records={self.metrics['records_processed']}, "
            f"errors={self.metrics['processing_errors']}, "
            f"quality={self.metrics['data_quality_score']:.2f})"
        )
