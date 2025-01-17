import pytest
from unittest.mock import Mock, patch
from datetime import datetime, timedelta
import pandas as pd
from src.collectors.newrelic_collector import NewRelicCollector

@pytest.fixture
def mock_newrelic_data():
    return {
        'metric_data': {
            'metrics': [
                {
                    'name': 'response_time',
                    'timeslices': [
                        {
                            'from': '2024-01-01T00:00:00Z',
                            'values': {'average_response_time': 100}
                        }
                    ]
                },
                {
                    'name': 'error_rate',
                    'timeslices': [
                        {
                            'from': '2024-01-01T00:00:00Z',
                            'values': {'error_rate': 0.5}
                        }
                    ]
                }
            ]
        }
    }

def test_newrelic_collector_init(test_config):
    """Test NewRelic collector initialization."""
    collector = NewRelicCollector(test_config)
    assert collector.config == test_config
    assert collector.api_key == test_config['collectors']['newrelic']['api_key']
    assert collector.account_id == test_config['collectors']['newrelic']['account_id']

@patch('requests.get')
def test_newrelic_collector_collect(mock_get, test_config, mock_newrelic_data):
    """Test NewRelic metrics collection."""
    # Setup mock
    mock_response = Mock()
    mock_response.json.return_value = mock_newrelic_data
    mock_response.status_code = 200
    mock_get.return_value = mock_response

    # Test collection
    collector = NewRelicCollector(test_config)
    result = collector.collect()

    # Verify results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert 'timestamp' in result.columns
    assert 'name' in result.columns
    assert 'value' in result.columns

@patch('requests.get')
def test_newrelic_collector_rate_limiting(mock_get, test_config):
    """Test NewRelic collector rate limiting handling."""
    # Setup mock for rate limit response
    mock_response = Mock()
    mock_response.status_code = 429
    mock_get.return_value = mock_response

    # Test collection
    collector = NewRelicCollector(test_config)
    with pytest.raises(Exception) as exc_info:
        collector.collect()
    assert "Rate limit exceeded" in str(exc_info.value)

@pytest.mark.parametrize("error_code,error_message", [
    (401, "Unauthorized"),
    (403, "Forbidden"),
    (500, "Internal Server Error"),
    (503, "Service Unavailable")
])
@patch('requests.get')
def test_newrelic_collector_error_handling(mock_get, test_config, error_code, error_message):
    """Test NewRelic collector error handling."""
    # Setup mock for error response
    mock_response = Mock()
    mock_response.status_code = error_code
    mock_response.text = error_message
    mock_get.return_value = mock_response

    # Test collection
    collector = NewRelicCollector(test_config)
    with pytest.raises(Exception) as exc_info:
        collector.collect()
    assert error_message in str(exc_info.value)

@patch('requests.get')
def test_newrelic_collector_pagination(mock_get, test_config):
    """Test NewRelic collector pagination handling."""
    # Setup mock responses for pagination
    first_page = {'metric_data': {'metrics': [{'name': 'metric_1'}]}, 'next_page': 2}
    second_page = {'metric_data': {'metrics': [{'name': 'metric_2'}]}, 'next_page': None}
    
    mock_get.side_effect = [
        Mock(status_code=200, json=lambda: first_page),
        Mock(status_code=200, json=lambda: second_page)
    ]

    # Test collection
    collector = NewRelicCollector(test_config)
    result = collector.collect()

    # Verify results
    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert mock_get.call_count == 2
