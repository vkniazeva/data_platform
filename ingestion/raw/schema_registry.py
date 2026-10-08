import requests
import logging

logger = logging.getLogger(__name__)


class SchemaRegistryClient:
    def __init__(self, url: str):
        self._url = url.rstrip("/")
        self._cache: set[int] = set()

    def schema_exists(self, schema_id: int) -> bool:
        if schema_id in self._cache:
            return True
        try:
            response = requests.get(f"{self._url}/schemas/ids/{schema_id}", timeout=2)
            if response.status_code == 200:
                self._cache.add(schema_id)
                return True
            logger.warning(f"Schema ID {schema_id} not found in registry: {response.status_code}")
            return False
        except Exception as e:
            logger.error(f"Schema Registry unreachable: {e}")
            return False
