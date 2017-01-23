import os
from pathlib import Path

import trafaret as t
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
    'dev': DEV_DICT,
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
    'shared_secret': t.String >> (lambda s: s.encode() if isinstance(s, str) else s),
    'root_url': t.URL,
    'media_dir': t.String >> check_media_dir,
    'media_url': t.URL,
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
            if env_var is not None:
                # basic attempt to convert the new value to match the original type
                if isinstance(value, int):
                    s_dict[key] = int(env_var)
                else:
                    # are there any other types we might need to cope with here?
                    s_dict[key] = env_var
    return s_dict


def load_settings() -> dict:
    """
    Read settings.yml, overwrite with environment variables, validate.
    :return: settings dict
    """
    settings_file = SETTINGS_FILE.resolve()
    try:
        settings = read_and_validate(str(settings_file), SETTINGS_STRUCTURE)
        settings = substitute_environ(settings, ENV_PREFIX)
        settings = SETTINGS_STRUCTURE.check(settings)
    except AssertionError as e:
        raise ConfigError([str(e)]) from e
    return settings
