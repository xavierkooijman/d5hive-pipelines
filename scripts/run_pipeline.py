import sys
import yaml
from dotenv import load_dotenv

from ingestion.pipelines.ipma import run as ipma_run
from utils.logging import get_logger

load_dotenv()


PIPELINES = {
    "ipma_ingestion": ipma_run,
}


def load_config(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    config_path = sys.argv[1]

    config = load_config(config_path)

    pipeline_name = config["pipeline_name"]

    logger = get_logger(pipeline_name)

    pipeline = PIPELINES[pipeline_name]

    pipeline(config)
