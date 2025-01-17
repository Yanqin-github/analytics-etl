import pytest
from src.collectors import MongoDBCollector, NewRelicCollector
from src.processors import UnifiedProcessor
from src.storage import RawStorage, ProcessedStorage

@pytest.mark.integration
def test_full_pipeline(test_config, sample_mongodb_data, sample_newrelic_data):
    # Setup collectors
    mongodb_collector = MongoDBCollector(test_config)
    newrelic_collector = NewRelicCollector(test_config)
    
    # Setup processor
    processor = UnifiedProcessor(test_config)
    
    # Setup storage
    raw_storage = RawStorage(test_config)
    processed_storage = ProcessedStorage(test_config)
    
    # Run pipeline
    try:
        # Collect data
        mongodb_data = mongodb_collector.collect()
        newrelic_data = newrelic_collector.collect()
        
        # Store raw data
        raw_storage.store(mongodb_data, 'mongodb')
        raw_storage.store(newrelic_data, 'newrelic')
        
        # Process data
        processed_data = processor.process({
            'mongodb': mongodb_data,
            'newrelic': newrelic_data
        })
        
        # Store processed data
        processed_storage.store(processed_data)
        
        assert True  # Pipeline completed successfully
    except Exception as e:
        pytest.fail(f"Pipeline failed: {str(e)}")
