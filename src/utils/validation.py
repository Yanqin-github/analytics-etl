import pandas as pd
from typing import List, Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Validate data structures and content."""

    @staticmethod
    def validate_dataframe(
        df: pd.DataFrame,
        required_columns: List[str],
        dtypes: Optional[Dict[str, str]] = None
    ) -> bool:
        """Validate DataFrame structure and types.
        
        Args:
            df: DataFrame to validate
            required_columns: List of required column names
            dtypes: Expected data types for columns
        
        Returns:
            bool: True if validation passes
        
        Raises:
            ValueError: If validation fails
        """
        # Check for required columns
        missing_columns = set(required_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Check data types if specified
        if dtypes:
            for column, expected_type in dtypes.items():
                if column in df.columns:
                    current_type = df[column].dtype
                    if not pd.api.types.is_dtype_equal(current_type, expected_type):
                        raise ValueError(
                            f"Column {column} has type {current_type}, "
                            f"expected {expected_type}"
                        )

        return True

    @staticmethod
    def validate_metrics(metrics: Dict[str, Any]) -> bool:
        """Validate metrics dictionary structure.
        
        Args:
            metrics: Metrics dictionary to validate
        
        Returns:
            bool: True if validation passes
        
        Raises:
            ValueError: If validation fails
        """
        required_keys = [
            'start_time',
            'end_time',
            'duration',
            'success_count',
            'error_count'
        ]

        missing_keys = set(required_keys) - set(metrics.keys())
        if missing_keys:
            raise ValueError(f"Missing required metrics keys: {missing_keys}")

        return True
