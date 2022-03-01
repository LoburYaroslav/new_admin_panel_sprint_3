import logging

LEVEL = logging.INFO
FORMAT = '%(levelname)s: %(message)s'

logging.basicConfig(
    level=logging.INFO,
    format=FORMAT
)

logger = logging.getLogger('default_logger')
