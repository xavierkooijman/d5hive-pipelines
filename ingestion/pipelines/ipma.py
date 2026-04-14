from ingestion.sources.api import fetch_data_from_api
from ingestion.transformations.common import normalize_timestamp, wind_direction_to_degrees
from utils.common import detect_environment
from utils.logging import get_logger
from utils.destinations_executer import run_destinations
from utils.email import send_email
import clts_pcp as clts


def run(config):

    tstart = clts.getts()
    clts.elapt["Pipeline Started"] = clts.deltat(tstart)

    env = detect_environment()

    clts.elapt[f"Environment Detected: {env}"] = clts.deltat(tstart)

    clts.setcontext(
        f'IPMA Weather Station Data Retrieval - Environment: {env}')

    clts.elapt[f"Fetching data from API Endpoint: {config["source"]["endpoint"]}"] = clts.deltat(
        tstart)
    raw_data = fetch_data_from_api(config["source"]["endpoint"])

    clts.elapt["Data fetched from API"] = clts.deltat(tstart)

    clts.elapt["Normalizing and transforming data"] = clts.deltat(tstart)

    features = []
    for feature in raw_data.get("features", []):
        if feature['properties'].get('idEstacao') == config["source"]["station_id"]:
            features.append(feature)

    data = []

    for feature in features:
        props = feature.get("properties", {})
        coords = feature.get("geometry", {}).get("coordinates", [None, None])

        data.append({
            "hostfeed": "hostfeed",
            "source": config["source"]["name"],
            "timestamp": normalize_timestamp(props.get("time")),
            "latitude": coords[1],
            "longitude": coords[0],
            "temperature_celsius": props.get("temperatura"),
            "wind_speed_kmh": props.get("intensidadeVentoKM"),
            "wind_direction_degrees": wind_direction_to_degrees(props.get("descDirVento")),
            "humidity_percentage": None if props.get("humidade") == -99.0 else props.get("humidade"),
            "pressure_hpa": props.get("pressao"),
            "precipitation_mm": props.get("precAcumulada"),
            "radiation_kjm2": props.get("radiacao")
        })

    clts.elapt["Data normalized and transformed"] = clts.deltat(tstart)

    clts.elapt["Inserting data into destinations"] = clts.deltat(tstart)

    run_destinations(config, data)

    clts.elapt["Data inserted into destinations"] = clts.deltat(tstart)

    toemail = clts.listtimes()

    if config["email"]["send"]:
        send_email(env, config["email"], toemail)
