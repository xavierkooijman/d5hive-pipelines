from ingestion.sources.api import fetch_data_from_api
from utils.common import detect_environment
from utils.destinations_executer import run_destinations
from utils.email import send_email
import clts_pcp as clts
import logging
from datetime import datetime
import geopandas as gpd
from shapely.geometry import Point


def run(config):

    try:
        logger = logging.getLogger(__name__)

        tstart = clts.getts()
        logger.info("Pipeline Started")
        clts.elapt["Pipeline Started"] = clts.deltat(tstart)

        env = detect_environment()

        clts.elapt[f"Environment Detected: {env}"] = clts.deltat(tstart)

        clts.setcontext(
            f'Postos de Abastecimento DGEG Data Retrieval - Environment: {env}')

        clts.elapt[f"Fetching data from API URL: {config["source"]["url"]}"] = clts.deltat(
            tstart)
        raw_data = fetch_data_from_api(config["source"]["url"])

        clts.elapt["Data fetched from API"] = clts.deltat(tstart)

        clts.elapt["Normalizing and transforming data"] = clts.deltat(tstart)

        clts.elapt["Filter Coordinates within Maia"] = clts.deltat(tstart)

        maia_gdf = gpd.read_file("maia_polygon.geojson").to_crs(epsg=4326)
        maia_polygon = maia_gdf.geometry.iloc[0]
        minx, miny, maxx, maxy = maia_polygon.bounds

        gdf_points = gpd.GeoDataFrame([
            {
                "globalid": feature["properties"]["globalid"].strip("{}"),
                "marca": feature["properties"]["Marca"],
                "geometry": Point(feature["geometry"]["coordinates"])

            }
            for feature in raw_data["features"]
        ], geometry="geometry", crs="EPSG:4326")

        gdf_points = gdf_points.cx[minx:maxx, miny:maxy]
        clts.elapt["Coordinates Filtered by Bounding Box"] = clts.deltat(
            tstart)

        gdf_filtered = gdf_points[gdf_points.geometry.within(maia_polygon)]
        clts.elapt["Coordinates Filtered within Maia Polygon"] = clts.deltat(
            tstart)

        clts.elapt["Normalizing filtered data"] = clts.deltat(tstart)

        current_timestamp = datetime.now().isoformat()

        data = []

        for idx, row in gdf_filtered.iterrows():
            data.append({
                "globalId": row["globalid"],
                "hostfeed": "hostfeed",
                "source": config["source"]["name"],
                "brand": row["marca"],
                "latitude": row.geometry.y,
                "longitude": row.geometry.x,
                "tstamp": current_timestamp
            })

        clts.elapt["Data normalized and transformed"] = clts.deltat(tstart)

        clts.elapt[f"Inserting data into destinations: {', '.join([dest['name'] for dest in config['destinations']])}"] = clts.deltat(
            tstart)

        run_destinations(config, data)
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        toemail = clts.listtimes()
        if config["email"]["send"]:
            send_email(env, config["email"], toemail)
