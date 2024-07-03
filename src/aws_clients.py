import boto3
from botocore.config import Config
import logging
import yaml

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
import os

# retry_config = Config(
#     region_name="us-east-1", retries={"max_attempts": 10, "mode": "standard"}
# )


class AWSClients:
    s3_client = None
    athena_client = None
    glue_client = None

    @classmethod
    def get_s3_client(cls):
        if cls.s3_client is None:
            cls.s3_client = boto3.client("s3")
        return cls.s3_client

    @classmethod
    def get_athena_client(cls):
        if cls.athena_client is None:
            cls.athena_client = boto3.client("athena")
        return cls.athena_client

    @classmethod
    def get_glue_client(cls):
        if cls.glue_client is None:
            cls.glue_client = boto3.client("glue")
        return cls.glue_client


def load_config(config_file="config.yaml"):
    # Construct the path to the config file
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "config", config_file
    )
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


config = load_config()

aws_client = AWSClients()
