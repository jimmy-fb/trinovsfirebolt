from google.cloud import bigquery
from google.oauth2 import service_account
from typing import Optional, Dict, List, Any, Union
from contextlib import contextmanager

class BigQueryConnector:
    def __init__(self, config: Dict[str, str]):
        """
        Initialize BigQuery connector with configuration parameters.
        
        Args:
            config (Dict[str, str]): Configuration dictionary containing:
                - project_id: Google Cloud project ID
                - credentials_path: Path to service account JSON file
                - dataset: (Optional) Default dataset to use
                - location: (Optional) Default location for jobs
        """
        self.config = config
        self._validate_config()
        self._client = None
        self._init_client()

    def _validate_config(self) -> None:
        """Validate that required configuration parameters are present."""
        required_params = ['project_id', 'key']
        missing_params = [param for param in required_params if param not in self.config]
        if missing_params:
            raise ValueError(f"Missing required configuration parameters: {missing_params}")

    def _init_client(self) -> None:
        """Initialize the BigQuery client."""
        project_id = self.config['project_id']
        dataset_id = self.config.get('dataset')
        default_config = bigquery.QueryJobConfig(default_dataset=f"{project_id}.{dataset_id}")
        credentials = service_account.Credentials.from_service_account_info(self.config['key'])
        self._client = bigquery.Client(
            project=project_id,
            credentials=credentials,
            default_query_job_config=default_config,
            location=self.config.get('location')
        )

    def connect(self) -> None:
        """Establish a connection to BigQuery."""
        self._init_client()  # Initialize the BigQuery client

    def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        dry_run: bool = False
    ) -> List[Dict]:
        """
        Execute a SQL query and return results as a list of dictionaries.
        
        Args:
            query (str): SQL query to execute
            params (Optional[Dict[str, Any]]): Query parameters
            dry_run (bool): If True, only estimate bytes processed
            
        Returns:
            List[Dict]: Query results as a list of dictionaries
        """
        job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            dry_run=dry_run
        )

        if params:
            job_config.query_parameters = [
                bigquery.ScalarQueryParameter(k, self._get_param_type(v), v)
                for k, v in params.items()
            ]

        query_job = self._client.query(query, job_config=job_config)
        
        if dry_run:
            return [{'bytes_processed': query_job.total_bytes_processed}]

        return [dict(row.items()) for row in query_job]

    def _get_param_type(self, value: Any) -> str:
        """Determine BigQuery parameter type from Python value."""
        type_map = {
            str: 'STRING',
            int: 'INT64',
            float: 'FLOAT64',
            bool: 'BOOL',
            dict: 'RECORD',
            list: 'ARRAY'
        }
        return type_map.get(type(value), 'STRING')
    
    def close(self) -> None:
        """Close the BigQuery connection if it exists."""
        if self._client:
            self._client.close()
            self._client = None