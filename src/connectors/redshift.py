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

    def _validate_config(self) -> None:
        """Validate that required configuration parameters are present."""
        required_params = ['host', 'port', 'database', 'user', 'password']
        missing_params = [param for param in required_params if param not in self.config]
        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {missing_params}")

    @contextmanager
    def connect(self):
        """Context manager for database connections."""
        try:
            if not self._conn:
                self._conn = psycopg2.connect(
                    host=self.config['host'],
                    port=self.config['port'],
                    dbname=self.config['database'],
                    user=self.config['user'],
                    password=self.config['password']
                )
            yield self._conn
        finally:
            if self._conn:
                self._conn.close()
                self._conn = None

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict[str, Any]]): Query parameters for parameterized queries
            
        Returns:
            List[Dict]: Query results as a list of dictionaries
        """
        with self.connect() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cursor.execute(query, params or {})
                return cursor.fetchall()
            finally:
                cursor.close()

    def execute_batch(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """
        Execute a batch of parameterized queries.
        
        Args:
            query (str): SQL query template
            params_list (List[Dict[str, Any]]): List of parameter dictionaries
        """
        with self.connect() as conn:
            cursor = conn.cursor()
            try:
                psycopg2.extras.execute_batch(cursor, query, params_list)
                conn.commit()
            finally:
                cursor.close()

    def copy_from_s3(self, table: str, s3_path: str, iam_role: str, options: Optional[Dict[str, str]] = None) -> None:
        """
        Copy data from S3 into a Redshift table.
        
        Args:
            table (str): Target table name
            s3_path (str): S3 path to source data
            iam_role (str): IAM role ARN with necessary permissions
            options (Optional[Dict[str, str]]): Additional COPY command options
        """
        copy_options = options or {}
        options_str = ' '.join([f"{k} {v}" for k, v in copy_options.items()])
        
        copy_query = f"""
            COPY {table}
            FROM '{s3_path}'
            IAM_ROLE '{iam_role}'
            {options_str}
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(copy_query)
                conn.commit()
            finally:
                cursor.close()

    def unload_to_s3(self, query: str, s3_path: str, iam_role: str, options: Optional[Dict[str, str]] = None) -> None:
        """
        Unload query results to S3.
        
        Args:
            query (str): Query to unload
            s3_path (str): S3 path for output
            iam_role (str): IAM role ARN with necessary permissions
            options (Optional[Dict[str, str]]): Additional UNLOAD command options
        """
        unload_options = options or {}
        options_str = ' '.join([f"{k} {v}" for k, v in unload_options.items()])
        
        unload_query = f"""
            UNLOAD ('{query}')
            TO '{s3_path}'
            IAM_ROLE '{iam_role}'
            {options_str}
        """
        
        with self.connect() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(unload_query)
                conn.commit()
            finally:
                cursor.close()