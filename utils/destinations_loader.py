import importlib


def load_destination(dest_type):
    module_path = f"ingestion.destinations.{dest_type}"
    importlib.import_module(module_path)
