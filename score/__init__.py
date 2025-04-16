import logging
from logging import config as logging_config

from score.settings import LOGGING_CONFIG

logging_config.dictConfig(LOGGING_CONFIG)

LOG = logging.getLogger('app')