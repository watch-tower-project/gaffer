import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("[%(levelname)s]	%(asctime)s.%(msecs)dZ %(message)s"))
    logger.addHandler(handler)