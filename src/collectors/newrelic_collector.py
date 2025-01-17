import logging
from typing import Dict, Any, List
import pandas as pd
import requests
from datetime import datetime, timedelta
import backoff
from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

class NewRelicCollector(BaseCollector):
    """Collects metrics from NewRelic."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize NewRelic collector.
        
        Args:
            config: Configuration dictionary containing NewRelic API details
        """
        super().__init__(config)
        self.api_key = config['collectors']['newrelic']['api_key']
        self.account_id = config['collectors']['newrelic']['account_id']
        self.base_url = "https://api.newrelic.com/v2"
        self.headers = {
            "Api-Key": self.api_key,
            "Content-Type": "application/json"
        }

    @backoff.on_exception(
        backoff.expo,
        requests.exceptions.RequestException,
        max_tries=3
    )
    def _make_request(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """Make HTTP request to NewRelic API.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
        
        Returns:
            Dict[str, Any]: API response
        
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        url = f"{self.base_url}/{endpoint}"
        response = requests.get(url, headers=self.headers, params=params)
        
        if response.status_code == 429:
            logger.warning("Rate limit exceeded, backing off...")
            raise requests.exceptions.RequestException("Rate limit exceeded")
            
        response.raise_for_status()
        return response.json()

    def collect(self) -> pd.DataFrame:
        """Collect metrics from NewRelic.
        
        Returns:
            pd.DataFrame: Collected metrics data
        
        Raises:
            Exception: If collection fails
        """
        start_time = datetime.now()
        try:
            metrics = []
            next_page = 1

            while next_page:
                # Fetch metrics with pagination
                response = self._make_request(
                    f"accounts/{self.account_id}/metrics",
                    params={"page": next_page}
                )

                for metric in response['metric_data']['metrics']:
                    for timeslice in metric['timeslices']:
                        metrics.append({
                            'timestamp': pd.to_datetime(timeslice['from']),
                            'name': metric['name'],
                            'value': next(iter(timeslice['values'].values())),
                            'attributes': {
                                'metric_type': metric.get('metric_type', 'unknown'),
                                'unit': metric.get('unit', 'unknown')
                            }
                        })

                next_page = response.get('next_page')

            if not metrics:
                df = pd.DataFrame(columns=['timestamp', 'name', 'value', 'attributes'])
                self.update_metrics(start_time, 0)
                return df

            df = pd.DataFrame(metrics)
            
            # Validate the collected data
            self.validate_data(df)
            
            # Update metrics
            self.update_metrics(start_time, len(df))
            
            return df

        except requests.exceptions.RequestException as e:
            self.update_metrics(start_time, 0, error=True)
            logger.error(f"NewRelic API request failed: {str(e)}")
            raise
        except Exception as e:
            self.update_metrics(start_time, 0, error=True)
            logger.error(f"Unexpected error in NewRelic collector: {str(e)}")
            raise

    def health_check(self) -> bool:
        """Check NewRelic API health.
        
        Returns:
            bool: True if API is accessible
        """
        try:
            # Try to fetch a simple health endpoint
            self._make_request(f"accounts/{self.account_id}/applications")
            return True
        except Exception as e:
            logger.error(f"NewRelic health check failed: {str(e)}")
            return False
