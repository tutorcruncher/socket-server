import logging
import logging.config
import os

logger = logging.getLogger('socket.main')


def setup_logging(verbose: bool=False):
    """
    setup logging config for socket by updating the arq logging config
    """
    log_level = 'DEBUG' if verbose else 'INFO'
    raven_dsn = os.getenv('RAVEN_DSN', None)
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'socket.default': {
                'format': '%(levelname)s %(message)s',
            },
        },
        'handlers': {
            'socket.default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'socket.default'
            },
            'sentry': {
                'level': 'WARNING',
                'class': 'raven.handlers.logging.SentryHandler',
                'dsn': raven_dsn,
            },
        },
        'loggers': {
            'socket': {
                'handlers': ['socket.default', 'sentry'],
                'level': log_level,
            },
            'gunicorn.error': {
                'handlers': ['sentry'],
                'level': 'ERROR',
            },
            'arq.main': {
                'handlers': ['socket.default', 'sentry'],
                'level': log_level,
            },
            'arq.work': {
                'handlers': ['socket.default', 'sentry'],
                'level': log_level,
            },
            'arq.jobs': {
                'handlers': ['socket.default', 'sentry'],
                'level': log_level,
            },
        },
    }
    logging.config.dictConfig(config)
