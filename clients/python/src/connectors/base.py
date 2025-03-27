from abc import ABC, abstractmethod
from typing import Any, Dict

class WarehouseConnector(ABC):
    @abstractmethod
    def connect(self, credentials: Dict[str, Any]) -> Any:
        """Establish connection to the warehouse"""
        pass

    @abstractmethod
    def execute_query(self, query: str) -> tuple[bool, float, str]:
        """Execute query and return (success, execution_time, error_message)"""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the connection"""
        pass
