from pathlib import Path

from arq import RedisSettings
from pydantic import BaseSettings, validator
from pydantic.utils import make_dsn

THIS_DIR = Path(__file__).parent
BASE_DIR = THIS_DIR.parent


class Settings(BaseSettings):
    pg_name = 'socket'
    pg_user = 'postgres'
    pg_password: str = None
    pg_host = 'localhost'
    pg_port = '5432'
    pg_driver = 'postgresql'

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
    def pg_dsn(self) -> str:
        return make_dsn(
            driver=self.pg_driver,
            user=self.pg_user,
            password=self.pg_password,
            host=self.pg_host,
            port=self.pg_port,
            name=self.pg_name,
            query=None,
        )
