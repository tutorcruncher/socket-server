from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from arq.connections import RedisSettings
from pydantic import BaseSettings, validator

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent


class Settings(BaseSettings):
    pg_dsn: Optional[str] = 'postgresql://postgres@localhost:5432/socket'

    redis_host = 'localhost'
    redis_port = 6379
    redis_database = 0
    redis_password: str = None

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

    @property
    def redis_settings(self) -> RedisSettings:
        return RedisSettings(
            host=self.redis_host,
            port=self.redis_port,
            database=self.redis_database,
            password=self.redis_password,
        )

    @property
    def _pg_dsn_parsed(self):
        return urlparse(self.pg_dsn)

    @property
    def pg_name(self):
        return self._pg_dsn_parsed.path.lstrip('/')

    @property
    def pg_host(self):
        return self._pg_dsn_parsed.hostname

    @property
    def pg_port(self):
        return self._pg_dsn_parsed.port

    @property
    def pg_password(self):
        return self._pg_dsn_parsed.password

    @property
    def pg_user(self):
        return self._pg_dsn_parsed.username

    class Config:
        fields = {'pg_dsn': {'env': 'DATABASE_URL'}}
