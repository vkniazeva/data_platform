import json
import requests
import logging

logger = logging.getLogger(__name__)


class SchemaRegistryClient:
    def __init__(self, url: str):
        self._url = url.rstrip("/")
        self._cache: dict[int, dict] = {}

    def schema_exists(self, schema_id: int) -> bool:
        """Return True if a schema with this ID exists in the registry."""
        return self.get_schema(schema_id) is not None

    def get_schema(self, schema_id: int) -> dict | None:
        """Fetch schema by ID from cache or registry API; returns None if unknown or unreachable."""
        if schema_id in self._cache:
            return self._cache[schema_id]
        try:
            response = requests.get(f"{self._url}/schemas/ids/{schema_id}", timeout=2)
            if response.status_code == 200:
                schema = json.loads(response.json()["schema"])
                self._cache[schema_id] = schema
                return schema
            logger.warning(f"Schema ID {schema_id} not found in registry: {response.status_code}")
            return None
        except Exception as e:
            logger.error(f"Schema Registry unreachable: {e}")
            return None
