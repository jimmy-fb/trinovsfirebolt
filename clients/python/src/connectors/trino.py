import trino
from typing import Optional, Dict, List, Any

class TrinoConnector:
    def __init__(self, config: Dict[str, str]):
        """
        Initialize Trino connector with configuration parameters.
        
        Args:
            config (Dict[str, str]): Configuration dictionary containing:
                - host: Trino/Starburst coordinator host
                - port: Trino/Starburst coordinator port (default: 443 for https, 8080 for http)
                - catalog: Default catalog to use
                - schema: Default schema to use
                - user: Username for authentication
                - password: (Optional) Password for authentication
                - auth: (Optional) Authentication method ('basic', 'oauth2', 'jwt', 'kerberos')
                - use_https: (Optional) Whether to use HTTPS (default: True)
                - verify_ssl: (Optional) Whether to verify SSL certificates (default: True)
        """
        self.config = config
        self._validate_config()
        self._conn = None

    def _validate_config(self) -> None:
        """Validate that required configuration parameters are present."""
        required_params = ['host', 'catalog', 'schema', 'user']
        missing_params = [param for param in required_params if param not in self.config]
        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {missing_params}")

    def connect(self):
        """Establish connection to the Trino/Starburst warehouse."""
        if not self._conn:
            # Determine connection parameters
            host = self.config['host']
            port = int(self.config.get('port', 443 if self.config.get('use_https', True) else 8080))
            catalog = self.config['catalog']
            schema = self.config['schema']
            user = self.config['user']
            use_https = self.config.get('use_https', True)
            verify_ssl = self.config.get('verify_ssl', True)
            
            # Setup authentication
            auth = None
            # Only use authentication if using HTTPS or if explicitly configured
            if use_https or self.config.get('force_auth', False):
                if 'password' in self.config and self.config['password']:
                    # Use getattr to avoid linter issues with dynamic imports
                    BasicAuth = getattr(trino.auth, 'BasicAuthentication', None)
                    if BasicAuth:
                        auth = BasicAuth(user, self.config['password'])
                elif self.config.get('auth') == 'oauth2':
                    # OAuth2 authentication for Starburst
                    OAuth2Auth = getattr(trino.auth, 'OAuth2Authentication', None)
                    if OAuth2Auth:
                        auth = OAuth2Auth()
                elif self.config.get('auth') == 'jwt':
                    # JWT authentication
                    if 'jwt_token' in self.config:
                        JWTAuth = getattr(trino.auth, 'JWTAuthentication', None)
                        if JWTAuth:
                            auth = JWTAuth(self.config['jwt_token'])
            
            # Create connection
            connection_params = {
                'host': host,
                'port': port,
                'user': user,
                'catalog': catalog,
                'schema': schema,
                'http_scheme': 'https' if use_https else 'http',
                'verify': verify_ssl
            }
            
            if auth:
                connection_params['auth'] = auth
                
            # Use getattr to avoid linter issues with dynamic imports
            dbapi = getattr(trino, 'dbapi', None)
            if dbapi:
                self._conn = dbapi.connect(**connection_params)
            else:
                raise ImportError("trino.dbapi not available")

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict[str, Any]]): Query parameters for parameterized queries
            
        Returns:
            List[Dict]: Query results as a list of dictionaries
        """
        if not self._conn:
            self.connect()

        try:
            cursor = self._conn.cursor()
            cursor.execute(query, params or {})
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all results and convert to list of dictionaries
            rows = cursor.fetchall()
            results = []
            for row in rows:
                result_dict = dict(zip(columns, row))
                results.append(result_dict)
                
            cursor.close()
            return results
            
        except Exception as e:
            raise Exception(f"Error executing query: {str(e)}")

    def close(self) -> None:
        """Close the Trino connection if it exists."""
        if self._conn:
            self._conn.close()
            self._conn = None 