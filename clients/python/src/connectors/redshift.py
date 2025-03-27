import psycopg2
import psycopg2.extras
from typing import Optional, Dict, List, Any
from contextlib import contextmanager

class RedshiftConnector:
    def __init__(self, config: Dict[str, str]):
        """
        Initialize Redshift connector with configuration parameters.
        
        Args:
            config (Dict[str, str]): Configuration dictionary containing:
                - host: Redshift cluster endpoint
                - port: Port number (usually 5439)
                - database: Database name
                - user: Username
                - password: Password
        """
        self.config = config
        self._validate_config()
        self._conn = None
        self._cursor = None

    def _validate_config(self) -> None:
        """Validate that required configuration parameters are present."""
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing_params = [param for param in required_params if param not in self.config]
        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {missing_params}")

    @contextmanager
    def connect(self):
        """Context manager for database connections."""
        self._conn = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            dbname=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
        self._cursor = self._conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        self._cursor.execute("SET enable_result_cache_for_session TO off;")

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict[str, Any]]): Query parameters for parameterized queries
            
        Returns:
            List[Dict]: Query results as a list of dictionaries
        """
        if not self._conn or not self._cursor:
            self.connect() 
        try:
            self._cursor.execute(query)
            if self._cursor.description:
                return self._cursor.fetchall()
            return []
        except Exception as e:
            raise Exception(f"Error executing redshift query: {str(e)}")
            
    def close(self) -> None:
        """Close the Snowflake connection if it exists."""
        if self._conn:
            self._conn.close()
            self._conn = None
            self._cursor = None