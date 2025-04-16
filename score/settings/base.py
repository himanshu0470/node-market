import os

from decouple import config as env_config

API_VERSION="1.0.0"
APP_TITLE="Score Prediction App"
APP_DESCRIPTION="Score Prediction App APIs"

RESOURCE_BASE_PATH = "resources"
APP_PROPS_FILE = env_config("APP_PROPS_FILE", default='application-dev.yml')
APP_PROPS_PATH = os.path.join(RESOURCE_BASE_PATH, APP_PROPS_FILE)

DATA_TEMP_BASE_PATH = os.path.join("resources", "data")