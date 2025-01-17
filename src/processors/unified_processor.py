import logging
from typing import Dict, Any, List
import pandas as pd
from datetime import datetime
import numpy as np
from .base_processor import BaseProcessor

logger = logging.getLogger(__name__)

class UnifiedProcessor(BaseProcessor):
    """Processes and unifies metrics from different sources."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize unified processor.
        
        Args:
            config: Configuration dictionary
        """
        super().__init__(config)
        self.aggregation_window = config['processors']['unified'].get('aggregation_window', '1H')
        self.outlier_threshold = config['processors']['unified'].get('outlier_threshold', 3.0)

    def detect_outliers(self, group: pd.DataFrame) -> pd.Series:
        """Detect outliers using Z-score method.
        
        Args:
            group: DataFrame group to check for outliers
        
        Returns:
            pd.Series: Boolean mask of outliers
        """
        z_scores = np.abs((group['value'] - group['value'].mean()) / group['value'].std())
        return z_scores > self.outlier_threshold

    def process(self, data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """Process and unify metrics from different sources.
        
        Args:
            data: Dictionary of DataFrames from different collectors
        
        Returns:
            pd.DataFrame: Processed and unified data
        
        Raises:
            ValueError: If data validation fails
        """
        start_time = datetime.now()
        try:
            if not data:
                raise ValueError("No data to process")

            processed_dfs = []

            # Process each source
            for source, df in data.items():
                if df.empty:
                    continue

                # Standardize column names
                df = df.rename(columns={
                    'metric_id': 'metric_name',
                    'name': 'metric_name',
                    'metadata': 'dimensions',
                    'attributes': 'dimensions'
                })

                # Ensure timestamp is datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'])

                # Add source column
                df['source'] = source

                # Handle missing dimensions
                if 'dimensions' not in df.columns:
                    df['dimensions'] = None

                # Select required columns
                df = df[['timestamp', 'metric_name', 'value', 'source', 'dimensions']]

                processed_dfs.append(df)

            if not processed_dfs:
                return pd.DataFrame(columns=['timestamp', 'metric_name', 'value', 'source', 'dimensions'])

            # Combine all sources
            unified_df = pd.concat(processed_dfs, ignore_index=True)

            # Remove duplicates
            unified_df = unified_df.drop_duplicates()

            # Sort by timestamp
            unified_df = unified_df.sort_values('timestamp')

            # Detect and handle outliers
            grouped = unified_df.groupby(['metric_name', 'source'])
            outliers = grouped.apply(self.detect_outliers).reset_index(level=[0, 1], drop=True)
            unified_df.loc[outliers, 'value'] = np.nan

            # Aggregate by window
            unified_df = unified_df.set_index('timestamp').groupby([
                pd.Grouper(freq=self.aggregation_window),
                'metric_name',
                'source',
                'dimensions'
            ]).agg({
                'value': ['mean', 'min', 'max', 'count']
            }).reset_index()

            # Flatten column names
            unified_df.columns = [
                'timestamp', 'metric_name', 'source', 'dimensions',
                'value_mean', 'value_min', 'value_max', 'value_count'
            ]

            # Use mean as the main value
            unified_df = unified_df.rename(columns={'value_mean': 'value'})

            # Validate processed data
            self.validate_schema(unified_df)

            # Calculate data quality score
            quality_score = self.calculate_data_quality(unified_df)

            # Update metrics
            self.update_metrics(start_time, len(unified_df), quality_score)

            return unified_df

        except Exception as e:
            self.update_metrics(start_time, 0, 0.0, error=True)
            logger.error(f"Error processing data: {str(e)}")
            raise

    def health_check(self) -> bool:
        """Check processor health.
        
        Returns:
            bool: True if processor is healthy
        """
        try:
            # Create a small test DataFrame
            test_data = {
                'test_source': pd.DataFrame({
                    'timestamp': [datetime.now()],
                    'metric_name': ['test_metric'],
                    'value': [1.0],
                    'dimensions': [{'test': 'value'}]
                })
            }
            
            # Try to process it
            result = self.process(test_data)
            return len(result) > 0
        except Exception as e:
            logger.error(f"Processor health check failed: {str(e)}")
            return False
