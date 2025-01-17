import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
import pandas as pd
from src.collectors.postgres_collector import PostgresCollector

@pytest.fixture
def mock_postgres_data():
    return [
        (datetime(2024, 1, 1, 0, 0), 'query_time', 100.0, {'query_type': 'SELECT'}),
        (datetime(2024, 1, 1, 0, 0), 'error_count', 5.0, {'severity': 'ERROR'})
    ]

def test_postgres_collector_init(test_config):
    """Test PostgreSQL collector initialization."""
    collector = PostgresCollector(test_config)
    assert collector.config == test_config
    assert collector.host == test_config['collectors']['postgres']['host']
    assert collector.database == test_config['collectors']['postgres']['database']

@patch('psycopg2.connect')
def test_postgres_collector_collect(mock_connect, test_config, mock_postgres_data):
    """Test PostgreSQL metrics collection."""
    # Setup mock
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = mock_postgres_data
    mock_connect.return_value.cursor.return_value = mock_cursor

    # Test collection
    collector = PostgresCollector(test_config)
    result = collector.collect()

    # Verify results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'timestamp' in result.columns
    assert 'metric_name' in result.columns
    assert 'value' in result.columns
    assert 'metadata' in result.columns

@patch('psycopg2.connect')
def test_postgres_collector_connection_error(mock_connect, test_config):
    """Test PostgreSQL collector connection error handling."""
    mock_connect.side_effect = Exception("Connection failed")

    with pytest.raises(Exception) as exc_info:
        collector = PostgresCollector(test_config)
        collector.collect()
    assert "Connection failed" in str(exc_info.value)

@patch('psycopg2.connect')
def test_postgres_collector_query_error(mock_connect, test_config):
    """Test PostgreSQL collector query error handling."""
    # Setup mock
    mock_cursor = Mock()
    mock_cursor.execute.side_effect = Exception("Query failed")
    mock_connect.return_value.cursor.return_value = mock_cursor

    # Test collection
    collector = PostgresCollector(test_config)
    with pytest.raises(Exception) as exc_info:
        collector.collect()
    assert "Query failed" in str(exc_info.value)

@patch('psycopg2.connect')
def test_postgres_collector_empty_results(mock_connect, test_config):
    """Test PostgreSQL collector with empty results."""
    # Setup mock
    mock_cursor = Mock()
    mock_cursor.fetchall.return_value = []
    mock_connect.return_value.cursor.return_value = mock_cursor

    # Test collection
    collector = PostgresCollector(test_config)
    result = collector.collect()

    # Verify results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0

@patch('psycopg2.connect')
def test_postgres_collector_connection_cleanup(mock_connect, test_config):
    """Test PostgreSQL collector connection cleanup."""
    # Setup mocks
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_connect.return_value = mock_connection
    mock_connection.cursor.return_value = mock_cursor
    mock_cursor.fetchall.return_value = []

    # Test collection
    collector = PostgresCollector(test_config)
    collector.collect()

    # Verify cleanup
    mock_cursor.close.assert_called_once()
    mock_connection.close.assert_called_once()
