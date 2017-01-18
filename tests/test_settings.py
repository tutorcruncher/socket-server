from app.settings import load_settings


def test_load_settings():
    s = load_settings()
    assert isinstance(s, dict)
    assert s['database']['host'] == 'localhost'
