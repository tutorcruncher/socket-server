import logging
import logging.config

logger = logging.getLogger('socket.main')


def setup_logging(verbose: bool=False):
    """
    setup logging config for socket by updating the arq logging config
    """
    log_level = 'DEBUG' if verbose else 'INFO'
    config = {
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
            'arq.main': {
                'handlers': ['socket.default'],
                'level': log_level,
            },
            'arq.work': {
                'handlers': ['socket.default'],
                'level': log_level,
            },
            'arq.jobs': {
                'handlers': ['socket.default'],
                'level': log_level,
            },
        },
    }
    logging.config.dictConfig(config)
