import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime
from src.storage.raw_storage import RawStorage

@pytest.fixture
def sample_raw_data():
    return pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=3, freq='H'),
        'metric_id': ['response_time', 'error_count', 'request_count'],
        'value': [100, 5, 1000],
        'metadata': [{'service': 'api'}, {'service': 'api'}, {'service': 'api'}]
    })

@pytest.fixture
def mock_catalog():
    catalog = Mock()
    catalog.load_table.return_value = Mock()
    return catalog

def test_raw_storage_init(test_config):
    """Test RawStorage initialization."""
    storage = RawStorage(test_config)
    assert storage.config == test_config
    assert storage.warehouse_path == test_config['storage']['warehouse_path']
    assert storage.format == test_config['storage']['format']

@patch('pyiceberg.catalog.load_catalog')
def test_raw_storage_store(mock_load_catalog, test_config, sample_raw_data, mock_catalog):
    """Test storing raw data."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage
    storage = RawStorage(test_config)
    result = storage.store(sample_raw_data, 'mongodb')
    
    # Verify results
    assert result is True
    mock_catalog.load_table.assert_called_once()
    mock_catalog.load_table.return_value.append.assert_called_once()

@patch('pyiceberg.catalog.load_catalog')
def test_raw_storage_store_empty_data(mock_load_catalog, test_config, mock_catalog):
    """Test storing empty data."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    empty_data = pd.DataFrame()
    
    # Test storage
    storage = RawStorage(test_config)
    with pytest.raises(ValueError) as exc_info:
        storage.store(empty_data, 'mongodb')
    assert "Empty data cannot be stored" in str(exc_info.value)

@patch('pyiceberg.catalog.load_catalog')
def test_raw_storage_invalid_source(mock_load_catalog, test_config, sample_raw_data, mock_catalog):
    """Test storing data with invalid source."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage
    storage = RawStorage(test_config)
    with pytest.raises(ValueError) as exc_info:
        storage.store(sample_raw_data, 'invalid_source')
    assert "Invalid source" in str(exc_info.value)

@patch('pyiceberg.catalog.load_catalog')
def test_raw_storage_schema_validation(mock_load_catalog, test_config, mock_catalog):
    """Test data schema validation."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Create data with invalid schema
    invalid_data = pd.DataFrame({
        'invalid_column': [1, 2, 3]
    })
    
    # Test storage
    storage = RawStorage(test_config)
    with pytest.raises(ValueError) as exc_info:
        storage.store(invalid_data, 'mongodb')
    assert "Schema validation failed" in str(exc_info.value)

@patch('pyiceberg.catalog.load_catalog')
def test_raw_storage_partitioning(mock_load_catalog, test_config, sample_raw_data, mock_catalog):
    """Test data partitioning."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage
    storage = RawStorage(test_config)
    result = storage.store(sample_raw_data, 'mongodb')
    
    # Verify partitioning
    assert result is True
    mock_catalog.load_table.return_value.append.assert_called_once()
    # Verify partition columns were added
    call_args = mock_catalog.load_table.return_value.append.call_args
    assert 'partition_by' in call_args.kwargs

@patch('pyiceberg.catalog.load_catalog')
def test_raw_storage_error_handling(mock_load_catalog, test_config, sample_raw_data):
    """Test error handling during storage."""
    # Setup mock to raise exception
    mock_load_catalog.side_effect = Exception("Storage error")
    
    # Test storage
    storage = RawStorage(test_config)
    with pytest.raises(Exception) as exc_info:
        storage.store(sample_raw_data, 'mongodb')
    assert "Storage error" in str(exc_info.value)

@patch('pyiceberg.catalog.load_catalog')
def test_raw_storage_compression(mock_load_catalog, test_config, sample_raw_data, mock_catalog):
    """Test data compression settings."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage with compression
    storage = RawStorage(test_config)
    result = storage.store(sample_raw_data, 'mongodb', compression='snappy')
    
    # Verify compression settings
    assert result is True
    call_args = mock_catalog.load_table.return_value.append.call_args
    assert 'compression' in call_args.kwargs
    assert call_args.kwargs['compression'] == 'snappy'
