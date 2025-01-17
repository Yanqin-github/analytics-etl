import pytest
from unittest.mock import Mock, patch
import pandas as pd
from datetime import datetime
from src.storage.processed_storage import ProcessedStorage

@pytest.fixture
def sample_processed_data():
    return pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=3, freq='H'),
        'metric_name': ['latency', 'errors', 'requests'],
        'metric_value': [100, 5, 1000],
        'source': ['mongodb'] * 3,
        'dimensions': [{'service': 'api'}] * 3
    })

@pytest.fixture
def mock_catalog():
    catalog = Mock()
    catalog.load_table.return_value = Mock()
    return catalog

def test_processed_storage_init(test_config):
    """Test ProcessedStorage initialization."""
    storage = ProcessedStorage(test_config)
    assert storage.config == test_config
    assert storage.warehouse_path == test_config['storage']['warehouse_path']
    assert storage.format == test_config['storage']['format']

@patch('pyiceberg.catalog.load_catalog')
def test_processed_storage_store(mock_load_catalog, test_config, sample_processed_data, mock_catalog):
    """Test storing processed data."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage
    storage = ProcessedStorage(test_config)
    result = storage.store(sample_processed_data)
    
    # Verify results
    assert result is True
    mock_catalog.load_table.assert_called_once()
    mock_catalog.load_table.return_value.append.assert_called_once()

@patch('pyiceberg.catalog.load_catalog')
def test_processed_storage_schema_evolution(mock_load_catalog, test_config, mock_catalog):
    """Test schema evolution handling."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Create data with new column
    data_with_new_column = pd.DataFrame({
        'timestamp': pd.date_range('2024-01-01', periods=1),
        'metric_name': ['latency'],
        'metric_value': [100],
        'source': ['mongodb'],
        'dimensions': [{'service': 'api'}],
        'new_column': ['value']  # New column
    })
    
    # Test storage
    storage = ProcessedStorage(test_config)
    result = storage.store(data_with_new_column)
    
    # Verify schema evolution
    assert result is True
    mock_catalog.load_table.return_value.update_schema.assert_called_once()

@patch('pyiceberg.catalog.load_catalog')
def test_processed_storage_data_validation(mock_load_catalog, test_config, mock_catalog):
    """Test data validation."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Create invalid data
    invalid_data = pd.DataFrame({
        'timestamp': ['invalid_date'],
        'metric_name': [123],  # Should be string
        'metric_value': ['invalid_value'],  # Should be number
    })
    
    # Test storage
    storage = ProcessedStorage(test_config)
    with pytest.raises(ValueError) as exc_info:
        storage.store(invalid_data)
    assert "Data validation failed" in str(exc_info.value)

@patch('pyiceberg.catalog.load_catalog')
def test_processed_storage_partitioning(mock_load_catalog, test_config, sample_processed_data, mock_catalog):
    """Test data partitioning strategy."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage
    storage = ProcessedStorage(test_config)
    result = storage.store(sample_processed_data)
    
    # Verify partitioning
    assert result is True
    call_args = mock_catalog.load_table.return_value.append.call_args
    assert 'partition_by' in call_args.kwargs
    assert 'date' in call_args.kwargs['partition_by']
    assert 'source' in call_args.kwargs['partition_by']

@patch('pyiceberg.catalog.load_catalog')
def test_processed_storage_metrics(mock_load_catalog, test_config, sample_processed_data, mock_catalog):
    """Test storage metrics collection."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage
    storage = ProcessedStorage(test_config)
    result = storage.store(sample_processed_data)
    
    # Verify metrics
    assert hasattr(storage, 'metrics')
    assert 'rows_written' in storage.metrics
    assert 'bytes_written' in storage.metrics
    assert storage.metrics['rows_written'] == len(sample_processed_data)

@patch('pyiceberg.catalog.load_catalog')
def test_processed_storage_compression(mock_load_catalog, test_config, sample_processed_data, mock_catalog):
    """Test compression settings."""
    # Setup mock
    mock_load_catalog.return_value = mock_catalog
    
    # Test storage with compression
    storage = ProcessedStorage(test_config)
    result = storage.store(sample_processed_data, compression='zstd')
    
    # Verify compression settings
    assert result is True
    call_args = mock_catalog.load_table.return_value.append.call_args
    assert 'compression' in call_args.kwargs
    assert call_args.kwargs['compression'] == 'zstd'
