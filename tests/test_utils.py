import logging
from asyncio import Future
from datetime import datetime

import pytest
from psycopg2 import OperationalError

from tcsocket.app.logs import setup_logging
from tcsocket.app.utils import pretty_lenient_json
from tcsocket.app.worker import MainActor


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


async def test_setup_worker_fails(settings, mocker, caplog):
    actor = MainActor(settings=settings)
    actor.retry_sleep = 0.01
    m = mocker.patch('tcsocket.app.worker.create_engine')
    m.side_effect = OperationalError
    with pytest.raises(OperationalError):
        await actor.startup()
    assert m.call_count == 6
    assert actor.session is None
    assert 'socket.worker INFO: create_engine failed, 5 retries remaining, retrying...' in caplog
    assert 'socket.worker INFO: create_engine failed, 3 retries remaining, retrying...' in caplog
    assert 'socket.worker INFO: create_engine failed, 1 retries remaining, retrying...' in caplog


async def test_setup_worker_fails_then_works(settings, mocker, caplog):
    actor = MainActor(settings=settings)
    actor.retry_sleep = 0.01
    m = mocker.patch('tcsocket.app.worker.create_engine')
    f = Future()
    f.set_result(1)
    m.side_effect = [OperationalError, OperationalError, f]
    await actor.startup()
    assert m.call_count == 3
    await actor.session.close()
    assert 'socket.worker INFO: create_engine failed, 5 retries remaining, retrying...' in caplog
    assert 'socket.worker INFO: create_engine failed, 3 retries remaining, retrying...' not in caplog
    assert 'socket.worker INFO: create_engine failed, 1 retries remaining, retrying...' not in caplog
