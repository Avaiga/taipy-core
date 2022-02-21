import logging.config
import os


class TaipyLogger:
    ENVIRONMENT_VARIABLE_NAME_WITH_LOGGER_CONFIG_PATH = "TAIPY_LOGGER_CONFIG_PATH"
    if config_filename := os.environ.get(ENVIRONMENT_VARIABLE_NAME_WITH_LOGGER_CONFIG_PATH):
        logging.config.fileConfig(config_filename)
    logger = logging.getLogger("Taipy")

    if not config_filename:
        logger.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter("[%(asctime)s][%(name)s][%(levelname)s] %(message)s")
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if config_filename:
        logger.info(f"Logger configuration '{config_filename}' successfully loaded.")
