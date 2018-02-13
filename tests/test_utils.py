import logging
from datetime import datetime

import pytest

from tcsocket.app.logs import setup_logging
from tcsocket.app.utils import pretty_lenient_json


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
    ) == pretty_lenient_json(d)


def test_universal_encoder_error():

    class Foo:
        pass

    d = {'dt': Foo()}
    with pytest.raises(TypeError):
        pretty_lenient_json(d)


def test_no_logging(capsys):
    logger = logging.getLogger('socket.main')
    logger.info('foobar')
    out, err = capsys.readouterr()
    assert out == ''
    assert err == ''


def test_setup_logging(capsys):
    logger = logging.getLogger('socket.main')
    setup_logging()
    logger.info('foobar')
    out, err = capsys.readouterr()
    assert out == ''
    assert err == 'INFO socket.main foobar\n'
