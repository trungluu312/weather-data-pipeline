"""
Shared BrightSky API client.
"""
import logging
import requests
from typing import Dict, List
from common.config import settings

logger = logging.getLogger(__name__)

def _make_request(endpoint: str, params: Dict) -> Dict:
    url = f"{settings.brightsky_api_url}{endpoint}"
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def get_sources(lat: float, lon: float, max_dist: int = 50000) -> List[Dict]:
    logger.info(f"Fetching sources near ({lat}, {lon})")
    data = _make_request('/sources', {
        'lat': lat, 
        'lon': lon, 
        'max_dist': max_dist
    })
    return data.get('sources', [])

def get_weather(
    wmo_station_id: str = None,
    lat: float = None,
    lon: float = None,
    date: str = None,
    last_date: str = None
) -> Dict:
    params = {}
    if wmo_station_id:
        params['wmo_station_id'] = wmo_station_id
    elif lat is not None and lon is not None:
        params['lat'] = lat
        params['lon'] = lon
    else:
        raise ValueError("Must provide wmo_station_id or lat/lon")

    if date: params['date'] = date
    if last_date: params['last_date'] = last_date
    
    return _make_request('/weather', params)