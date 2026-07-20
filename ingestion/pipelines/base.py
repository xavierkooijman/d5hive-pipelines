from ingestion.sources.api import APIClient
from utils.connectors import run_inserts
from utils.tracer import get_tracer
from utils.common import save_raw_payload
import logging
from abc import ABC, abstractmethod
from opentelemetry import trace
from pydantic import ValidationError


class BaseETLPipeline(ABC):
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(self.__class__.__name__)
        self.tracer = get_tracer()

    def extract_data(self):
        api_client = APIClient(self.config["source"]["base_url"])
        return api_client.get(self.config["source"]["endpoint"], params=self.config["source"].get(
            "parameters", {}), headers=self.config["source"].get("headers", {}))

    @abstractmethod
    def validate_raw_schema(self, data):
        pass

    @abstractmethod
    def transform_data(self, data):
        pass

    def load_data(self, transformed_data):
        run_inserts(self.config, transformed_data)

    def run(self):
        pipeline_name = self.config.get("pipeline_name")
        self.logger.info(f"Pipeline {pipeline_name} Started")

        with self.tracer.start_as_current_span(f"{pipeline_name}") as root_span:
            root_span.set_attribute("pipeline.name", pipeline_name)
            root_span.set_attribute(
                "pipeline.source", self.config["source"]["name"])

            try:
                with self.tracer.start_as_current_span("extract"):
                    data = self.extract_data()
                    save_raw_payload(self.config.get("pipeline_name"), data)

                with self.tracer.start_as_current_span("validate raw schema"):
                    self.logger.info("Validating raw data schema")
                    try:
                        validated_raw_schema = self.validate_raw_schema(data)
                        self.logger.info(
                            "Raw data schema validated")
                    except ValidationError as e:
                        self.logger.error(
                            f"Data validation failed for pipeline {pipeline_name}: {e}")
                        raise

                with self.tracer.start_as_current_span("transform") as span:
                    self.logger.info("Normalizing and transforming data")
                    transformed_data = self.transform_data(
                        validated_raw_schema)
                    span.set_attribute("data.records_transformed",
                                       len(transformed_data))
                    self.logger.info("Data normalized and transformed")

                with self.tracer.start_as_current_span("load"):
                    self.load_data(transformed_data)

            except Exception as e:
                root_span.record_exception(e)
                root_span.set_status(trace.status.Status(
                    trace.status.StatusCode.ERROR, str(e)))
                raise

        self.logger.info(f"Pipeline {pipeline_name} completed successfully")
