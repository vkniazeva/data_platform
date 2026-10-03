# price_min/price_max - in USD, lot - in tons

METALS = {
    "CA": {"name": "copper",    "price_min": 9000,  "price_max": 10500, "lot": 25},
    "AH": {"name": "aluminium", "price_min": 2400,  "price_max": 2900,  "lot": 25},
    "ZS": {"name": "zinc", "price_min": 2600,  "price_max": 3200,  "lot": 25},
    "NI": {"name": "nickel", "price_min": 15000,  "price_max": 18000,  "lot": 6},
    "PB": {"name": "lead", "price_min": 1900,  "price_max": 2200,  "lot": 25},
    "SN": {"name": "tin", "price_min": 28000,  "price_max": 35000,  "lot": 5}
}

WAREHOUSES = {
    "WH_RTM": {"city": "Rotterdam", "region": "EU", "lat": 51.92, "lon": 4.48},
    "WH_ANR": {"city": "Antwerp", "region": "EU",  "lat": 51.22, "lon": 4.40},
    "WH_SNG": {"city": "Singapore", "region": "AS", "lat": 1.26,  "lon": 103.82},
    "WH_DBX": {"city": "Dubai", "region": "ME", "lat": 25.01, "lon": 55.06},
    "WH_PUS": {"city": "Busan", "region": "AS", "lat": 35.10, "lon": 129.04},
}

REGIONS = {
    "ME": "Middle East",
    "EU": "Europe",
    "AS": "Asia"
}

BARGES = {
    "BRG_001": {"name": "barge_001", "hosting_port": "WH_RTM", "target_port": "WH_SNG", "estimated_travel_days": 29.5},
    "BRG_002": {"name": "barge_002", "hosting_port": "WH_PUS", "target_port": "WH_ANR", "estimated_travel_days": 39.0},
    "BRG_003": {"name": "barge_003", "hosting_port": "WH_SNG", "target_port": "WH_RTM", "estimated_travel_days": 28.5},
    "BRG_004": {"name": "barge_004", "hosting_port": "WH_DBX", "target_port": "WH_ANR", "estimated_travel_days": 20.0},
    "BRG_005": {"name": "barge_005", "hosting_port": "WH_DBX", "target_port": "WH_PUS", "estimated_travel_days": 21.5},
    "BRG_006": {"name": "barge_006", "hosting_port": "WH_ANR", "target_port": "WH_PUS", "estimated_travel_days": 39.0},
    "BRG_007": {"name": "barge_007", "hosting_port": "WH_SNG", "target_port": "WH_DBX", "estimated_travel_days": 14.0},
    "BRG_008": {"name": "barge_008", "hosting_port": "WH_SNG", "target_port": "WH_RTM", "estimated_travel_days": 29.5},
}

