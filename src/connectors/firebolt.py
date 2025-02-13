from typing import Dict, Any, Optional, List
from firebolt.db import connect
from firebolt.client.auth import ClientCredentials

class FireboltConnector:
    def __init__(self, config: Dict[str, str]):
        """
        Initialize Firebolt connector with configuration parameters.
        
        Args:
            config (Dict[str, str]): Configuration dictionary containing:
                - engine_name: Firebolt engine name
                - database: Database name
                - account_name: Account name
                - client_id: OAuth client ID
                - client_secret: OAuth client secret
        """
        self.config = config
        self._validate_config()
        self._conn = None
        self.cursor = None

    def _validate_config(self) -> None:
        """Validate that required configuration parameters are present."""
        required_params = ['engine_name', 'database', 'account_name', 'auth']
        missing_params = [param for param in required_params if param not in self.config]
        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {missing_params}")

    def connect(self) -> None:
        """Connect to Firebolt using stored configuration."""
        if not self._conn:
            self._conn = connect(
                engine_name=self.config['engine_name'],
                database=self.config['database'],
                account_name=self.config['account_name'],
                auth=ClientCredentials(
                    self.config['auth']['id'],
                    self.config['auth']['secret']
                )
            )
            self.cursor = self._conn.cursor()
            self.cursor.execute("SET enable_result_cache=false")
            self.cursor.execute("SELECT hash_agg(*) FROM agents")
            self.cursor.execute("SELECT hash_agg(*) FROM ipaddresses")
            self.cursor.execute("SELECT hash_agg(*) FROM rankings")
            self.cursor.execute("SELECT hash_agg(*) FROM searchwords")

    def execute_query(self, query: str, parameters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a query on Firebolt.
        
        Args:
            query (str): SQL query to execute
            parameters (Optional[Dict[str, Any]]): Query parameters
            
        Returns:
            List[Dict[str, Any]]: Query results
        """
        if not self._conn or not self.cursor:
            self.connect()

        try:
            if parameters:
                self.cursor.execute(query, parameters)
            else:
                self.cursor.execute(query)
            
            if self.cursor.description:  # If the query returns results
                return self.cursor.fetchall()
            return []
            
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")

    def close(self) -> None:
        """Close the Firebolt connection if it exists."""
        if self._conn:
            self._conn.close()
            self._conn = None
            self.cursor = None
