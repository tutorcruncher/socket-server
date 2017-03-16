import os
from pathlib import Path

import trafaret as t
from sqlalchemy.engine.url import URL
from trafaret_config import ConfigError, read_and_validate

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent
SETTINGS_FILE = BASE_DIR / 'settings.yml'  # type: Path

DEV_DICT = t.Dict()
DEV_DICT.allow_extra('*')
ENV_PREFIX = 'APP_'


def check_media_dir(p):
    path = Path(p).resolve()
    path.mkdir(parents=True, exist_ok=True)
    assert path.is_dir(), f'"{path}" is not a directory'
    return str(path)


SETTINGS_STRUCTURE = t.Dict({
    # the "dev" dictionary contains information used by aiohttp-devtools to serve your app locally
    # you may wish to use it yourself,
    # eg. you might use dev.static_path in a management script to deploy static assets
    t.Key('dev', default={}): DEV_DICT,
    'database': t.Dict({
        'name': t.String,
        'password': t.Or(t.String | t.Null),
        'user': t.String,
        'host': t.String,
        'port': t.Int(gte=0) >> str,
    }),
    'redis': t.Dict({
        'host': t.String,
        'port': t.Int,
        'password': t.Or(t.String | t.Null),
        'database': t.Int,
    }),
    'master_key': t.String >> (lambda s: s if isinstance(s, bytes) else s.encode()),
    'root_url': t.URL,
    'media_dir': t.String >> check_media_dir,
    'media_url': t.URL,
    'tc_api_root': t.URL,
    'grecaptcha_secret': t.String(min_length=30, max_length=50),
    'grecaptcha_url': t.URL,
})


def substitute_environ(s_dict: dict, prefix: str) -> dict:
    """
    Substitute environment variables into a settings dict.

    Names are searched hierarchically with underscores representing levels, environment variables must be
    capitalised.

    For sample lets say we have ` {'foo': 'bar', 'subdict': {'value': 123}}` with prefix 'APP_',
    the environment variable "APP_FOO = spam" would replace "bar" and "APP_SUBDICT_VALUE = 3"
    would be converted to int and replace 123 in the dict.

    :param: s_dict: dict to replace values in
    :param: prefix: required prefix for environment variables to
    :return: modified dict
    """
    for key, value in s_dict.items():
        if isinstance(value, dict):
            s_dict[key] = substitute_environ(value, prefix + key + '_')
        elif isinstance(value, list):
            # doesn't make sense, we can't do anything here
            pass
        else:
            env_var = os.getenv((prefix + key).upper(), None)
            if env_var:
                # basic attempt to convert the new value to match the original type
                if isinstance(value, int):
                    s_dict[key] = int(env_var)
                else:
                    # are there any other types we might need to cope with here?
                    s_dict[key] = env_var
    return s_dict


def pg_dsn(db_settings: dict) -> str:
    """
    :param db_settings: dict of connection settings, see SETTINGS_STRUCTURE for definition
    :return: DSN url suitable for sqlalchemy and aiopg.
    """
    return str(URL(
        database=db_settings['name'],
        password=db_settings['password'],
        host=db_settings['host'],
        port=db_settings['port'],
        username=db_settings['user'],
        drivername='postgres',
    ))


def load_settings(settings_file: Path=None, *, env_prefix: str=ENV_PREFIX) -> dict:
    """
    Read settings.yml, overwrite with environment variables, validate.
    :return: settings dict
    """
    settings_file = settings_file or SETTINGS_FILE.resolve()
    try:
        settings = read_and_validate(str(settings_file), SETTINGS_STRUCTURE)
        settings = substitute_environ(settings, env_prefix)
        settings = SETTINGS_STRUCTURE.check(settings)
    except AssertionError as e:
        raise ConfigError([str(e)]) from e
    return settings