import pytest
import pandas as pd
from src.processors.unified_processor import UnifiedProcessor

def test_unified_processor_init(test_config):
    processor = UnifiedProcessor(test_config)
    assert processor.config == test_config
    assert processor.metric_mappings == test_config['processors']['metric_mappings']

def test_unified_processor_process(test_config, sample_mongodb_data, sample_newrelic_data):
    processor = UnifiedProcessor(test_config)
    
    result = processor.process({
        'mongodb': sample_mongodb_data,
        'newrelic': sample_newrelic_data
    })

    assert isinstance(result, pd.DataFrame)
    assert not result.empty
    assert 'metric_name' in result.columns
    assert 'metric_value' in result.columns
    assert 'source' in result.columns

def test_unified_processor_empty_data(test_config):
    processor = UnifiedProcessor(test_config)
    result = processor.process({})
    
    assert isinstance(result, pd.DataFrame)
    assert result.empty

@pytest.mark.parametrize("input_data,expected_metrics", [
    ({'mongodb': pd.DataFrame()}, 0),
    ({'newrelic': pd.DataFrame()}, 0),
    ({'invalid_source': pd.DataFrame()}, 0)
])
def test_unified_processor_edge_cases(test_config, input_data, expected_metrics):
    processor = UnifiedProcessor(test_config)
    result = processor.process(input_data)
    assert len(result) == expected_metrics
