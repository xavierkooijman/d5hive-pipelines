import os
import json
from datetime import datetime, timezone


class DeadLetterStore:
    def __init__(self, pipeline_name: str, b2_client, bucket: str):
        self.pipeline_name = pipeline_name
        self.b2 = b2_client
        self.bucket = bucket

        self.base_path = "/opt/airflow/dead_letter_tmp"
        self.local_path = f"{self.base_path}/{pipeline_name}.json"

    def save_raw_payload(self, raw_data: dict):
        os.makedirs(self.base_path, exist_ok=True)
        with open(self.local_path, "w") as f:
            json.dump(raw_data, f)

    def upload_and_clear(self):
        if not os.path.exists(self.local_path):
            return

        key = (
            f"dead_letter/{self.pipeline_name}/"
            f"{datetime.now(timezone.utc).isoformat()}.json"
        )

        self.b2.upload_file(
            bucket=self.bucket,
            key=key,
            file_path=self.local_path,
        )

        os.remove(self.local_path)

    def clear_local(self):
        if os.path.exists(self.local_path):
            os.remove(self.local_path)
