'Logging defaults.'
import logging
import os

import coloredlogs

LOG_DIR_NAME = os.environ.get('ROLLER_LOG_DIR', './')
LOG_FILE_NAME = os.environ.get('ROLLER_LOG_FILE', 'roller.log')
FORMAT = os.environ.get('ROLLER_LOG_FMT', '%(asctime)s %(levelname).3s: %(message)s - %(name)s +%(lineno)03d')
DATE_FORMAT = os.environ.get('ROLLER_LOG_DATE_FMT', '%Y-%m-%d %H:%M:%S')
LEVEL = os.environ.get('ROLLER_LOG_LEVEL', logging.DEBUG)


def setup(suppress_loggers=None):
    """Setup the root logger."""
    # Colored logs for terminal. Do this first, because it messes with the logger's level.
    stream_formatter = coloredlogs.ColoredFormatter(
        fmt=FORMAT, datefmt=DATE_FORMAT, level_styles={
            'info': {'color': 'green'}, 'warning': {'color': 'yellow', 'bold': True},
            'error': {'color': 'red', 'bold': True}, 'critical': {'color': 'red', 'bold': True},
            'mail': {'color': 'cyan', 'bold': True}, 'watch': {'color': 'magenta', 'bold': True}
        }, field_styles={'name': {'color': 'cyan'}, 'lineno': {'color': 'cyan'}})
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(stream_formatter)

    # Logging to file.
    file_formatter = logging.Formatter(FORMAT, DATE_FORMAT)
    file_handler = logging.FileHandler(os.path.join(LOG_DIR_NAME, LOG_FILE_NAME))
    file_handler.setFormatter(file_formatter)

    # Suppress handlers of unwanted loggers.
    if suppress_loggers is not None:
        for logger_name in suppress_loggers:
            logging.getLogger(logger_name).setLevel(logging.WARNING)

    logger = logging.getLogger()
    logger.handlers = []
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.setLevel(LEVEL)
