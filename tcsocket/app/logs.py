import logging
import logging.config
import os


def setup_logging(verbose: bool = False):
    """
    setup logging config for socket by updating the arq logging config
    """
    log_level = 'DEBUG' if verbose else 'INFO'
    raven_dsn = os.getenv('RAVEN_DSN', None)
    if raven_dsn in ('', '-'):
        # this means setting an environment variable of "-" means no raven
        raven_dsn = None
    config = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {'socket': {'format': '%(levelname)s %(name)s %(message)s'}},
        'handlers': {
            'socket': {'level': log_level, 'class': 'logging.StreamHandler', 'formatter': 'socket'},
            'sentry': {
                'level': 'WARNING',
                'class': 'raven.handlers.logging.SentryHandler',
                'dsn': raven_dsn,
                'release': os.getenv('COMMIT', None),
                'name': os.getenv('SERVER_NAME', '-'),
            },
            'logfire': {'class': 'logfire.integrations.logging.LogfireLoggingHandler'},
        },
        'loggers': {
            'socket': {'handlers': ['socket', 'sentry', 'logfire'], 'level': log_level},
            'gunicorn.error': {'handlers': ['sentry', 'logfire'], 'level': 'ERROR'},
            'arq': {'handlers': ['socket', 'sentry', 'logfire'], 'level': log_level},
            'aiohttp': {'handlers': ['logfire'], 'level': log_level},
        },
    }
    logging.config.dictConfig(config)
