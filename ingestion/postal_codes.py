"""
Postal code ingestion pipeline.

Downloads German postal codes, filters for Berlin, and loads into DuckDB.
"""
import logging
import json
import duckdb
import brotli
import requests
import geopandas as gpd
from shapely.geometry import shape

from common.config import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GEOJSON_URL = "https://github.com/yetzt/postleitzahlen/releases/download/2025.12/postleitzahlen.geojson.br"
BERLIN_PREFIXES = ("10", "12", "13")

def main():
    logger.info("Starting Postal Code Ingestion...")
    
    logger.info(f"Downloading from {GEOJSON_URL}")
    resp = requests.get(GEOJSON_URL, stream=True)
    resp.raise_for_status()
    
    # Decompress on the fly
    decompressed = brotli.decompress(resp.content)
    data = json.loads(decompressed)
    del decompressed # Free memory immediately

    # Filter for Berlin
    features = []
    for f in data.get('features', []):
        plz = f.get('properties', {}).get('postcode', '')
        if plz.startswith(BERLIN_PREFIXES) and f.get('geometry'):
            features.append({
                'plz': plz,
                'name': plz,
                'geometry': shape(f['geometry'])
            })
    
    if not features:
        logger.warning("No Berlin postal codes found.")
        return

    gdf = gpd.GeoDataFrame(features, crs='EPSG:4326')
    
    # Simple geometry fix (buffer 0 to fix self-intersections)
    gdf['geometry'] = gdf.geometry.buffer(0)
    
    # Prepare for DuckDB (WKT)
    gdf['geometry_wkt'] = gdf.geometry.apply(lambda g: g.wkt)
    
    logger.info(f"Filtered to {len(gdf)} Berlin postal codes")

    logger.info("Loading into DuckDB...")
    conn = duckdb.connect(settings.duckdb_path)
    try:
        conn.execute("INSTALL spatial; LOAD spatial;")
        
        conn.execute("DELETE FROM raw.postal_codes")
        
        conn.register('df_source', gdf[['plz', 'name', 'geometry_wkt']])
        conn.execute("""
            INSERT INTO raw.postal_codes (plz, name, geometry)
            SELECT plz, name, ST_GeomFromText(geometry_wkt) FROM df_source
        """)
        
        logger.info(f"✅ Loaded {len(gdf)} postal codes.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()