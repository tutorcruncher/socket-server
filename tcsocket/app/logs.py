import logging
import logging.config

from arq.logs import default_log_config as arq_log_config

logger = logging.getLogger('socket.main')


def setup_logging(verbose: bool=False):
    """
    setup logging config for socket by updating the arq logging config
    """
    log_level = 'DEBUG' if verbose else 'INFO'
    config = arq_log_config(verbose)
    update_config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'socket.default': {
                'format': '%(message)s',
            },
        },
        'handlers': {
            'socket.default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'socket.default'
            },
        },
        'loggers': {
            logger.name: {
                'handlers': ['socket.default'],
                'level': log_level,
            },
        },
    }
    for k, v in update_config.items():
        if isinstance(config.get(k), dict):
            config[k].update(v)
        else:
            config[k] = v
    logging.config.dictConfig(config)
