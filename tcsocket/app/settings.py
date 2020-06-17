from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from arq.connections import RedisSettings
from pydantic import BaseSettings, validator

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent


class Settings(BaseSettings):
    pg_dsn: Optional[str] = 'postgres://postgres@localhost:5432/socket'
    redis_settings: RedisSettings = 'redis://localhost:6379'
    redis_database: int = 0

    master_key = b'this is a secret'

    media_dir = Path('./media')
    media_url = '/media'
    tc_api_root = 'https://secure.tutorcruncher.com/api'
    grecaptcha_secret = 'required secret for google recaptcha'
    grecaptcha_url = 'https://www.google.com/recaptcha/api/siteverify'
    geocoding_url = 'https://maps.googleapis.com/maps/api/geocode/json'
    geocoding_key = 'required secret for google geocoding'

    tc_contractors_endpoint = '/public_contractors/'
    tc_enquiry_endpoint = '/enquiry/'
    tc_book_apt_endpoint = '/recipient_appointments/'

    @validator('media_dir')
    def check_media_dir(cls, p):
        path = p.resolve()
        path.mkdir(parents=True, exist_ok=True)
        if not path.is_dir():
            raise ValueError(f'"{path}" is not a directory')
        return str(path)

    @validator('redis_settings', always=True, pre=True)
    def parse_redis_settings(cls, v):
        conf = urlparse(v)
        return RedisSettings(
            host=conf.hostname, port=conf.port, password=conf.password, database=int((conf.path or '0').strip('/')),
        )

    @property
    def _pg_dsn_parsed(self):
        return urlparse(self.pg_dsn)

    @property
    def pg_name(self):
        return self._pg_dsn_parsed.path.lstrip('/')

    @property
    def pg_password(self):
        return self._pg_dsn_parsed.password or None

    @property
    def pg_host(self):
        return self._pg_dsn_parsed.hostname

    @property
    def pg_port(self):
        return self._pg_dsn_parsed.port

    class Config:
        fields = {
            'port': {'env': 'PORT'},
            'pg_dsn': {'env': 'DATABASE_URL'},
            'redis_settings': {'env': 'REDISCLOUD_URL'},
            'tc_api_root': {'env': 'TC_API_ROOT'},
        }
