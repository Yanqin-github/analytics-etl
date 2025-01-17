from typing import Dict, Any, Optional
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)

class MetricsTracker:
    """Track and aggregate metrics for components."""

    def __init__(self, component_name: str):
        """Initialize metrics tracker.
        
        Args:
            component_name: Name of the component being tracked
        """
        self.component_name = component_name
        self.reset()

    def reset(self) -> None:
        """Reset all metrics."""
        self.metrics = {
            'start_time': None,
            'end_time': None,
            'duration': 0,
            'success_count': 0,
            'error_count': 0,
            'total_bytes': 0,
            'total_records': 0,
            'custom_metrics': {}
        }

    def start_operation(self) -> None:
        """Mark the start of an operation."""
        self.metrics['start_time'] = datetime.now()

    def end_operation(self) -> None:
        """Mark the end of an operation."""
        self.metrics['end_time'] = datetime.now()
        if self.metrics['start_time']:
            self.metrics['duration'] = (
                self.metrics['end_time'] - self.metrics['start_time']
            ).total_seconds()

    def track_success(self, records: int = 0, bytes: int = 0) -> None:
        """Track successful operation.
        
        Args:
            records: Number of records processed
            bytes: Number of bytes processed
        """
        self.metrics['success_count'] += 1
        self.metrics['total_records'] += records
        self.metrics['total_bytes'] += bytes

    def track_error(self) -> None:
        """Track operation error."""
        self.metrics['error_count'] += 1

    def add_metric(self, name: str, value: Any) -> None:
        """Add custom metric.
        
        Args:
            name: Metric name
            value: Metric value
        """
        self.metrics['custom_metrics'][name] = value

    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics.
        
        Returns:
            Dict[str, Any]: Current metrics
        """
        return self.metrics

    def to_json(self) -> str:
        """Convert metrics to JSON string.
        
        Returns:
            str: JSON representation of metrics
        """
        metrics_copy = self.metrics.copy()
        
        # Convert datetime objects to strings
        if metrics_copy['start_time']:
            metrics_copy['start_time'] = metrics_copy['start_time'].isoformat()
        if metrics_copy['end_time']:
            metrics_copy['end_time'] = metrics_copy['end_time'].isoformat()
            
        return json.dumps(metrics_copy)
