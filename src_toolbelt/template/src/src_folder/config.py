import logging
import os

from dotenv import load_dotenv

from $PROJECT_NAME$ import get_lib_path

load_dotenv(get_lib_path(".env"))


class Config:

    _instance = None
    _is_initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not self._is_initialized:
            self._load_env_vars()
            self._configure_logger()
            self._load_sensitive_env_variables()
            self.logger.info("Env variables configured")
            self._is_initialized = True

    def _load_env_vars(self):
        self.ENV = os.getenv("ENV", "DEV")

        # Google
        self.GCP_PROJECT = os.getenv("GCP_PROJECT", "insider-data-lake")
        self.GCP_REGION = os.getenv("REGION", "us-central1")

        os.environ.setdefault("GCLOUD_PROJECT", self.GCP_PROJECT)

        self.TRACKING_URI = os.getenv(
            "TRACKING_URI", "https://mlflow.insiderstore.com.br"
        )
        self.MLFLOW_TRACKING_USERNAME = os.getenv("MLFLOW_TRACKING_USERNAME")
        self.MLFLOW_TRACKING_PASSWORD = os.getenv("MLFLOW_TRACKING_PASSWORD")

    def _configure_logger(self):
        self.logger = logging.getLogger("Lib")
        self.logger.setLevel("DEBUG")

        # Create formatter and add it to the handlers
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

        # Create a console handler and add it to the logger
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)
