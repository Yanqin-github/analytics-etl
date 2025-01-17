import logging
from typing import Dict, Any, List, Tuple
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
from datetime import datetime
from .base_collector import BaseCollector

logger = logging.getLogger(__name__)

class PostgresCollector(BaseCollector):
    """Collects metrics from PostgreSQL."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize PostgreSQL collector.
        
        Args:
            config: Configuration dictionary containing PostgreSQL connection details
        """
        super().__init__(config)
        self.host = config['collectors']['postgres']['host']
        self.port = config['collectors']['postgres']['port']
        self.database = config['collectors']['postgres']['database']
        self.user = config['collectors']['postgres']['user']
        self.password = config['collectors']['postgres']['password']

    @contextmanager
    def get_connection(self):
        """Create PostgreSQL connection context manager.
        
        Yields:
            psycopg2.extensions.connection: Database connection
        
        Raises:
            psycopg2.Error: If connection fails
        """
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            yield conn
        except psycopg2.Error as e:
            logger.error(f"PostgreSQL connection error: {str(e)}")
            raise
        finally:
            if conn is not None:
                conn.close()

    def collect(self) -> pd.DataFrame:
        """Collect metrics from PostgreSQL.
        
        Returns:
            pd.DataFrame: Collected metrics data
        
        Raises:
            Exception: If collection fails
        """
        start_time = datetime.now()
        metrics_query = """
            SELECT
                current_timestamp as timestamp,
                metric_name,
                metric_value as value,
                metric_metadata as metadata
            FROM (
                -- Query Performance
                SELECT
                    'query_time' as metric_name,
                    EXTRACT(EPOCH FROM mean_exec_time) as metric_value,
                    jsonb_build_object(
                        'query_type', queryid::text,
                        'calls', calls
                    ) as metric_metadata
                FROM pg_stat_statements
                
                UNION ALL
                
                -- Connection Stats
                SELECT
                    'connections' as metric_name,
                    count(*) as metric_value,
                    jsonb_build_object(
                        'state', state,
                        'database', datname
                    ) as metric_metadata
                FROM pg_stat_activity
                GROUP BY state, datname
                
                UNION ALL
                
                -- Table Statistics
                SELECT
                    'table_stats' as metric_name,
                    n_live_tup as metric_value,
                    jsonb_build_object(
                        'table', relname,
                        'schema', schemaname
                    ) as metric_metadata
                FROM pg_stat_user_tables
            ) metrics
        """

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(metrics_query)
                    results = cur.fetchall()

                    if not results:
                        df = pd.DataFrame(columns=['timestamp', 'metric_name', 'value', 'metadata'])
                        self.update_metrics(start_time, 0)
                        return df

                    # Convert results to DataFrame
                    df = pd.DataFrame(results, columns=['timestamp', 'metric_name', 'value', 'metadata'])
                    
                    # Ensure proper data types
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    df['value'] = pd.to_numeric(df['value'])
                    
                    # Validate the collected data
                    self.validate_data(df)
                    
                    # Update metrics
                    self.update_metrics(start_time, len(df))
                    
                    return df

        except Exception as e:
            self.update_metrics(start_time, 0, error=True)
            logger.error(f"Error collecting PostgreSQL metrics: {str(e)}")
            raise

    def health_check(self) -> bool:
        """Check PostgreSQL connection health.
        
        Returns:
            bool: True if connection is healthy
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    return cur.fetchone()[0] == 1
        except Exception as e:
            logger.error(f"PostgreSQL health check failed: {str(e)}")
            return False
