import logging
from typing import Dict, Any, List
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from pymongo.errors import ConnectionError, OperationFailure
from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

class MongoDBCollector(BaseCollector):
    """Collects metrics from MongoDB."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize MongoDB collector.
        
        Args:
            config: Configuration dictionary containing MongoDB connection details
        """
        super().__init__(config)
        self.uri = config['collectors']['mongodb']['uri']
        self.database = config['collectors']['mongodb']['database']
        self.collection = config['collectors']['mongodb']['collection']
        self.batch_size = config['collectors'].get('batch_size', 1000)

    def connect(self) -> MongoClient:
        """Create MongoDB connection.
        
        Returns:
            MongoClient: MongoDB client instance
        
        Raises:
            ConnectionError: If connection fails
        """
        try:
            return MongoClient(self.uri)
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise ConnectionError(f"MongoDB connection failed: {str(e)}")

    def collect(self) -> pd.DataFrame:
        """Collect metrics from MongoDB.
        
        Returns:
            pd.DataFrame: Collected metrics data
        
        Raises:
            ConnectionError: If connection fails
            OperationFailure: If query fails
            ValueError: If data validation fails
        """
        start_time = datetime.now()
        client = None
        try:
            client = self.connect()
            db = client[self.database]
            collection = db[self.collection]

            # Query metrics with batch processing
            metrics = []
            cursor = collection.find(
                {},
                batch_size=self.batch_size
            )

            for document in cursor:
                metrics.append({
                    'timestamp': document['timestamp'],
                    'metric_id': document['metric_id'],
                    'value': document['value'],
                    'metadata': document['metadata']
                })

            # Convert to DataFrame
            if not metrics:
                df = pd.DataFrame(columns=['timestamp', 'metric_id', 'value', 'metadata'])
                self.update_metrics(start_time, 0)
                return df

            df = pd.DataFrame(metrics)
            
            # Validate data types
            if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
                df['timestamp'] = pd.to_datetime(df['timestamp'])
            
            # Validate the collected data
            self.validate_data(df)
            
            # Update metrics
            self.update_metrics(start_time, len(df))
            
            return df

        except (ConnectionError, OperationFailure) as e:
            self.update_metrics(start_time, 0, error=True)
            logger.error(f"MongoDB collection error: {str(e)}")
            raise
        except Exception as e:
            self.update_metrics(start_time, 0, error=True)
            logger.error(f"Unexpected error in MongoDB collector: {str(e)}")
            raise
        finally:
            if client:
                client.close()

    def health_check(self) -> bool:
        """Check MongoDB connection health.
        
        Returns:
            bool: True if connection is healthy
        """
        client = None
        try:
            client = self.connect()
            # Try to execute a simple command
            client.admin.command('ping')
            return True
        except Exception as e:
            logger.error(f"MongoDB health check failed: {str(e)}")
            return False
        finally:
            if client:
                client.close()
