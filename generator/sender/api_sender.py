import requests

class ApiSender:
    def __init__(self, base_url: str):
        self.base_url = base_url


    def send_inventory_snapshot(self, request: dict) -> None:
        response = requests.post(
            url=f"{self.base_url}/v1/inventory-snapshots",
            headers=request["headers"],
            json=request["body"],
            timeout=5
        )

        response.raise_for_status()