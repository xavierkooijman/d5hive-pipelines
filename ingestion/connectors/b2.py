import boto3


class B2Client:
    def __init__(self, endpoint_url, key_id, app_key):
        self.client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=key_id,
            aws_secret_access_key=app_key,
        )

    def upload_file(self, bucket: str, key: str, file_path: str):
        self.client.upload_file(file_path, bucket, key)
