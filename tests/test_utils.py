import logging
from asyncio import Future
from datetime import datetime

import pytest
from aiohttp.web import Application, Response
from psycopg2 import OperationalError

from tcsocket.app import middleware
from tcsocket.app.logs import setup_logging
from tcsocket.app.utils import HTTPBadRequestJson, pretty_lenient_json
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
    caplog.set_level(logging.INFO)
    actor = MainActor(settings=settings)
    actor.retry_sleep = 0.01
    m = mocker.patch('tcsocket.app.worker.create_engine')
    m.side_effect = OperationalError
    with pytest.raises(OperationalError):
        await actor.startup()
    assert m.call_count == 6
    assert actor.session is None
    assert 'create_engine failed, 5 retries remaining, retrying...' in caplog.text
    assert 'create_engine failed, 3 retries remaining, retrying...' in caplog.text
    assert 'create_engine failed, 1 retries remaining, retrying...' in caplog.text


async def test_setup_worker_fails_then_works(settings, mocker, caplog):
    caplog.set_level(logging.INFO)
    actor = MainActor(settings=settings)
    actor.retry_sleep = 0.01
    m = mocker.patch('tcsocket.app.worker.create_engine')
    f = Future()
    f.set_result(1)
    m.side_effect = [OperationalError, OperationalError, f]
    await actor.startup()
    assert m.call_count == 3
    await actor.session.close()
    assert 'create_engine failed, 5 retries remaining, retrying...' in caplog.text
    assert 'create_engine failed, 3 retries remaining, retrying...' not in caplog.text
    assert 'create_engine failed, 1 retries remaining, retrying...' not in caplog.text


async def snap(request):
    raise RuntimeError('snap')


async def test_500_error(test_client, caplog):
    app = Application(middlewares=[middleware.error_middleware])
    app.router.add_get('/', snap)
    client = await test_client(app)
    r = await client.get('/')
    assert r.status == 500
    assert '500: Internal Server Error' == await r.text()
    assert 'ERROR    RuntimeError: snap' in caplog.text


async def test_401_return_error(test_client, mocker):
    mocker.spy(middleware.request_logger, 'warning')
    app = Application(middlewares=[middleware.error_middleware])
    app.router.add_get('/', lambda request: Response(text='foobar', status=401))
    client = await test_client(app)
    r = await client.get('/')
    assert r.status == 401
    assert middleware.request_logger.warning.call_count == 1
    call_data = middleware.request_logger.warning.call_args[1]['extra']['data']['data']
    assert call_data['response_status'] == 401
    assert call_data['response_text'] == 'foobar'


async def raise_400(request):
    raise HTTPBadRequestJson(status='foobar')


async def test_400_raise_error(test_client, mocker):
    mocker.spy(middleware.request_logger, 'warning')
    app = Application(middlewares=[middleware.error_middleware])
    app.router.add_route('*', '/', raise_400)
    client = await test_client(app)
    r = await client.post('/', data='foobar')
    assert r.status == 400
    assert middleware.request_logger.warning.call_count == 1
    call_data = middleware.request_logger.warning.call_args[1]['extra']['data']['data']
    assert call_data['request_text'] == 'foobar'
    assert call_data['response_status'] == 400
    assert call_data['response_text'] == '{\n  "status": "foobar"\n}\n'
    assert call_data['response_headers']['Access-Control-Allow-Origin'] == '*'
