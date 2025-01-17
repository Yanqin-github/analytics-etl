import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
import pandas as pd
from src.collectors.mongodb_collector import MongoDBCollector

@pytest.fixture
def mock_mongodb_data():
    return [
        {
            'timestamp': datetime(2024, 1, 1, 0, 0),
            'metric_id': 'response_time_ms',
            'value': 100,
            'metadata': {'service': 'api'}
        },
        {
            'timestamp': datetime(2024, 1, 1, 0, 0),
            'metric_id': 'error_count',
            'value': 5,
            'metadata': {'service': 'api'}
        }
    ]

def test_mongodb_collector_init(test_config):
    """Test MongoDB collector initialization."""
    collector = MongoDBCollector(test_config)
    assert collector.config == test_config
    assert collector.uri == test_config['collectors']['mongodb']['uri']
    assert collector.database == test_config['collectors']['mongodb']['database']

@patch('pymongo.MongoClient')
def test_mongodb_collector_collect(mock_client, test_config, mock_mongodb_data):
    """Test MongoDB metrics collection."""
    # Setup mock
    mock_collection = Mock()
    mock_collection.find.return_value = mock_mongodb_data
    mock_client.return_value[test_config['collectors']['mongodb']['database']]\
        [test_config['collectors']['mongodb']['collection']] = mock_collection

    # Test collection
    collector = MongoDBCollector(test_config)
    result = collector.collect()

    # Verify results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'timestamp' in result.columns
    assert 'metric_id' in result.columns
    assert 'value' in result.columns
    assert 'metadata' in result.columns

@patch('pymongo.MongoClient')
def test_mongodb_collector_collect_empty(mock_client, test_config):
    """Test MongoDB collector with empty results."""
    # Setup mock
    mock_collection = Mock()
    mock_collection.find.return_value = []
    mock_client.return_value[test_config['collectors']['mongodb']['database']]\
        [test_config['collectors']['mongodb']['collection']] = mock_collection

    # Test collection
    collector = MongoDBCollector(test_config)
    result = collector.collect()

    # Verify results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0

@pytest.mark.parametrize("error_type", [
    ConnectionError,
    TimeoutError,
    Exception
])
@patch('pymongo.MongoClient')
def test_mongodb_collector_error_handling(mock_client, test_config, error_type):
    """Test MongoDB collector error handling."""
    mock_client.side_effect = error_type("Test error")
    
    with pytest.raises(error_type):
        collector = MongoDBCollector(test_config)
        collector.collect()

@patch('pymongo.MongoClient')
def test_mongodb_collector_batch_processing(mock_client, test_config):
    """Test MongoDB collector batch processing."""
    # Generate large dataset
    large_dataset = [
        {
            'timestamp': datetime(2024, 1, 1, 0, 0),
            'metric_id': f'metric_{i}',
            'value': i,
            'metadata': {'service': 'api'}
        }
        for i in range(1000)
    ]

    # Setup mock
    mock_collection = Mock()
    mock_collection.find.return_value = large_dataset
    mock_client.return_value[test_config['collectors']['mongodb']['database']]\
        [test_config['collectors']['mongodb']['collection']] = mock_collection

    # Test collection
    collector = MongoDBCollector(test_config)
    result = collector.collect()

    # Verify results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1000

@patch('pymongo.MongoClient')
def test_mongodb_collector_data_validation(mock_client, test_config):
    """Test MongoDB collector data validation."""
    # Setup invalid data
    invalid_data = [
        {
            'timestamp': 'invalid_timestamp',
            'metric_id': 123,  # should be string
            'value': 'invalid_value',  # should be number
            'metadata': 'invalid_metadata'  # should be dict
        }
    ]

    # Setup mock
    mock_collection = Mock()
    mock_collection.find.return_value = invalid_data
    mock_client.return_value[test_config['collectors']['mongodb']['database']]\
        [test_config['collectors']['mongodb']['collection']] = mock_collection

    # Test collection
    collector = MongoDBCollector(test_config)
    with pytest.raises(ValueError):
        collector.collect()
