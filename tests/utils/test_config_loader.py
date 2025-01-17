import pytest
import os
from unittest.mock import mock_open, patch
from src.utils.config_loader import load_config

def test_load_config():
    mock_config = '''
    environment: test
    storage:
        warehouse_path: s3://test-warehouse
    '''
    
    with patch('builtins.open', mock_open(read_data=mock_config)):
        with patch.dict(os.environ, {'ENVIRONMENT': 'test'}):
            config = load_config()
            
            assert config['environment'] == 'test'
            assert config['storage']['warehouse_path'] == 's3://test-warehouse'

def test_load_config_missing_file():
    with patch('builtins.open', side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            load_config()

def test_load_config_invalid_yaml():
    mock_config = '''
    invalid:
        - yaml
        content
    '''
    
    with patch('builtins.open', mock_open(read_data=mock_config)):
        with pytest.raises(Exception):
            load_config()
