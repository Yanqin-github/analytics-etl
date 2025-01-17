import yaml
import os
from typing import Dict, Any, Optional
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class ConfigLoader:
    """Configuration loader with environment variable support."""

    def __init__(self, config_path: str = 'configs/config.yml'):
        """Initialize config loader.
        
        Args:
            config_path: Path to configuration file
        """
        self.config_path = config_path
        self._config = None

    def load(self) -> Dict[str, Any]:
        """Load configuration from file and environment variables.
        
        Returns:
            Dict[str, Any]: Configuration dictionary
        
        Raises:
            FileNotFoundError: If config file not found
            yaml.YAMLError: If config file is invalid
        """
        if self._config is None:
            self._config = self._load_config()
        return self._config

    def _load_config(self) -> Dict[str, Any]:
        """Internal method to load configuration."""
        try:
            # Load base configuration
            config = self._load_yaml()
            
            # Override with environment variables
            self._override_from_env(config)
            
            # Validate configuration
            self._validate_config(config)
            
            return config

        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            raise

    def _load_yaml(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        if not os.path.exists(self.config_path):
            raise FileNotFoundError(f"Config file not found: {self.config_path}")

        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    def _override_from_env(self, config: Dict[str, Any], prefix: str = 'ANALYTICS_') -> None:
        """Override configuration with environment variables.
        
        Args:
            config: Configuration dictionary to update
            prefix: Environment variable prefix
        """
        for key, value in os.environ.items():
            if key.startswith(prefix):
                # Remove prefix and split into parts
                config_key = key[len(prefix):].lower()
                parts = config_key.split('_')
                
                # Navigate the config dictionary
                current = config
                for part in parts[:-1]:
                    if part not in current:
                        current[part] = {}
                    current = current[part]
                
                # Set the value
                current[parts[-1]] = value

    def _validate_config(self, config: Dict[str, Any]) -> None:
        """Validate configuration structure.
        
        Args:
            config: Configuration dictionary to validate
        
        Raises:
            ValueError: If configuration is invalid
        """
        required_sections = ['collectors', 'processors', 'storage']
        for section in required_sections:
            if section not in config:
                raise ValueError(f"Missing required config section: {section}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key.
        
        Args:
            key: Configuration key (dot-separated)
            default: Default value if key not found
        
        Returns:
            Any: Configuration value
        """
        config = self.load()
        parts = key.split('.')
        
        current = config
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return default
                
        return current

    def reload(self) -> Dict[str, Any]:
        """Reload configuration from file.
        
        Returns:
            Dict[str, Any]: Updated configuration
        """
        self._config = None
        return self.load()
