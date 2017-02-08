import logging
import logging.config
import os

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
                'format': '%(levelname)s %(name)s %(message)s',
            },
        },
        'handlers': {
            'socket.default': {
                'level': log_level,
                'class': 'logging.StreamHandler',
                'formatter': 'socket.default',
            },
            'sentry': {
                'level': 'WARNING',
                'class': 'raven.handlers.logging.SentryHandler',
                'dsn': os.getenv('RAVEN_DSN', None),
                'release': os.getenv('COMMIT', None),
                'name': os.getenv('SERVER_NAME', '-')
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
            'arq': {
                'handlers': ['socket.default', 'sentry'],
                'level': log_level,
            },
        },
    }
    logging.config.dictConfig(config)
