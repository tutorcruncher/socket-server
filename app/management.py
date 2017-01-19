from contextlib import contextmanager
from typing import Union

import psycopg2
from sqlalchemy import create_engine

from .main import pg_dsn
from .models import Base
from .settings import load_settings


@contextmanager
def psycopg2_cursor(**db_settings):
    conn = psycopg2.connect(
        password=db_settings['password'],
        host=db_settings['host'],
        port=db_settings['port'],
        user=db_settings['user'],
    )
    conn.autocommit = True
    cur = conn.cursor()

    yield cur

    cur.close()
    conn.close()


def prepare_database(delete_existing: Union[bool, callable], print_func=print) -> bool:
    """
    (Re)create a fresh database and run migrations.

    :param delete_existing: whether or not to drop an existing database if it exists
    :param print_func: function to use for printing, eg. could be set to `logger.info`
    :return: whether or not a database as (re)created
    """
    db = load_settings()['database']

    with psycopg2_cursor(**db) as cur:
        cur.execute('SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname=%s)', (db['name'],))
        already_exists = bool(cur.fetchone()[0])
        if already_exists:
            if callable(delete_existing):
                _delete_existing = delete_existing()
            else:
                _delete_existing = bool(delete_existing)
            if not _delete_existing:
                print_func('database "{name}" already exists, not recreating it'.format(**db))
                return False
            else:
                print_func('dropping database "{name}" as it already exists...'.format(**db))
                cur.execute('DROP DATABASE {name}'.format(**db))
        else:
            print_func('database "{name}" does not yet exist'.format(**db))

        print_func('creating database "{name}"...'.format(**db))
        cur.execute('CREATE DATABASE {name}'.format(**db))

    engine = create_engine(pg_dsn(db))
    print_func('creating tables from model definition...')
    Base.metadata.create_all(engine)
    engine.dispose()
    return True
