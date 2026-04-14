from utils.destinations_loader import load_destination
from utils.destinations_registry import get_destination
from utils.common import resolve_secret


def run_destinations(config, data):

    for dest in config["destinations"]:

        load_destination(dest["type"])

        insert_fn = get_destination(dest["type"])

        dest_config = dict(dest)

        dest_config["password"] = resolve_secret(dest_config["password"])

        insert_fn(dest_config, data)
