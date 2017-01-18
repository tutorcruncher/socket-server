from datetime import datetime

import pytest

from app.settings import load_settings
from app.utils import json_response, to_pretty_json


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


class MockRequest:
    def __init__(self, debug):
        self.app = {'debug': debug}


def test_json_response_debug():
    r = json_response({'x': 'y'}, request=MockRequest(True))
    assert '{\n  "x": "y"\n}\n' == r.text


def test_json_response_not_debug():
    r = json_response({'x': 'y'}, request=MockRequest(False))
    assert '{"x": "y"}' == r.text
