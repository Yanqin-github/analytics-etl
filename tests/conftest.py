import pytest
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, Any

@pytest.fixture
def test_config() -> Dict[str, Any]:
    return {
        'environment': 'test',
        'storage': {
            'warehouse_path': 's3://analytics-warehouse-test',
            'format': 'iceberg',
            'catalog': {
                'type': 'glue',
                'warehouse': 's3://analytics-warehouse-test'
            }
        },
        'collectors': {
            'batch_size': 1000,
            'timeout_seconds': 30,
            'mongodb': {
                'uri': 'mongodb://test:test@localhost:27017',
                'database': 'test_db',
                'collection': 'test_metrics'
            },
            'newrelic': {
                'api_key': 'test_key',
                'account_id': 'test_account'
            },
            'postgres': {
                'host': 'localhost',
                'port': 5432,
                'database': 'test_db',
                'user': 'test_user',
                'password': 'test_pass'
            }
        },
        'processors': {
            'metric_mappings': {
                'mongodb': {
                    'response_time_ms': 'latency',
                    'error_count': 'errors',
                    'request_count': 'requests'
                },
                'newrelic': {
                    'duration': 'latency',
                    'error.count': 'errors',
                    'request.count': 'requests'
                }
            }
        }
    }

@pytest.fixture
def sample_mongodb_data() -> pd.DataFrame:
    return pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01', periods=3, freq='H'),
        'metric_id': ['response_time_ms', 'error_count', 'request_count'],
        'value': [100, 5, 1000],
        'metadata': [{'service': 'api'}, {'service': 'api'}, {'service': 'api'}]
    })

@pytest.fixture
def sample_newrelic_data() -> pd.DataFrame:
    return pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01', periods=3, freq='H'),
        'name': ['duration', 'error.count', 'request.count'],
        'value': [150, 3, 1200],
        'attributes': [{'app': 'web'}, {'app': 'web'}, {'app': 'web'}]
    })

@pytest.fixture
def sample_processed_data() -> pd.DataFrame:
    return pd.DataFrame({
        'timestamp': pd.date_range('2023-01-01', periods=3, freq='H'),
        'metric_name': ['latency', 'errors', 'requests'],
        'metric_value': [125, 4, 1100],
        'source': ['unified'] * 3,
        'dimensions': [{'service': 'api', 'app': 'web'}] * 3
    })
