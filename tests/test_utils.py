from datetime import datetime

import pytest

from tcsocket.app.logs import logger, setup_logging
from tcsocket.app.settings import load_settings
from tcsocket.app.utils import to_pretty_json


def test_load_settings():
    s = load_settings()
    assert isinstance(s, dict)
    assert s['database']['host'] == 'localhost'


def test_universal_encoder():
    d = {
        'dt': datetime(2032, 1, 1),
        'bytes': b'hello'
    }
    assert (
        '{\n'
        '  "bytes": "hello",\n'
        '  "dt": "2032-01-01T00:00:00"\n'
        '}\n'
    ) == to_pretty_json(d)


def test_universal_encoder_error():

    class Foo:
        pass

    d = {'dt': Foo()}
    with pytest.raises(TypeError):
        to_pretty_json(d)


def test_no_logging(capsys):
    logger.info('foobar')
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ''


def test_setup_logging(capsys):
    setup_logging()
    logger.info('foobar')
    out, err = capsys.readouterr()
    assert out == ''
    assert err == 'foobar\n'
