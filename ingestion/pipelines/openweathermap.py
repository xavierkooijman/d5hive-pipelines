from ingestion.sources.api import fetch_data_from_api
from ingestion.transformations.common import normalize_timestamp, ms_to_kmh
from utils.common import detect_environment
from utils.destinations_executer import run_destinations
from utils.email import send_email
import clts_pcp as clts
import logging
from utils.common import resolve_secret


def run(config):
    try:
        logger = logging.getLogger(__name__)

        tstart = clts.getts()
        logger.info("Pipeline Started")
        clts.elapt["Pipeline Started"] = clts.deltat(tstart)

        env = detect_environment()

        clts.elapt[f"Environment Detected: {env}"] = clts.deltat(tstart)

        clts.setcontext(
            f'Openweathermap Weather Data Retrieval - Environment: {env}')

        clts.elapt[f"Fetching data from API URL: {config["source"]["url"]}"] = clts.deltat(
            tstart)

        params = config["source"].get("parameters", {})
        if "appid" in params:
            params["appid"] = resolve_secret(params["appid"])

        raw_data = fetch_data_from_api(
            config["source"]["url"], params=params)

        clts.elapt["Data fetched from API"] = clts.deltat(tstart)

        clts.elapt["Normalizing and transforming data"] = clts.deltat(tstart)

        coordinates = raw_data.get("coord", {})
        current_weather = raw_data.get("main", {})
        wind = raw_data.get("wind", {})

        data = [{
            "hostfeed": "hostfeed",
            "source": config["source"]["name"],
            "tstamp": raw_data.get("dt"),
            "latitude": coordinates.get("lat"),
            "longitude": coordinates.get("lon"),
            "temperature_celsius": current_weather.get("temp"),
            "wind_speed_kmh": ms_to_kmh(wind.get("speed")),
            "wind_direction_degrees": wind.get("deg"),
            "wind_gust_kmh": ms_to_kmh(wind.get("gust")),
            "sea_level_pressure_hpa": current_weather.get("sea_level"),
            "ground_level_pressure_hpa": current_weather.get("grnd_level"),
            "humidity_percentage": current_weather.get("humidity"),
            "cloudiness_percentage": raw_data.get("clouds", {}).get("all"),
            "visibility_meters": raw_data.get("visibility"),
            "precipitation_mm": raw_data.get("rain", {}).get("1h", 0)
        }]

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
