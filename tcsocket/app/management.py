import logging
from contextlib import contextmanager
from io import BytesIO
from time import sleep
from typing import Union

import boto3
import psycopg2
import requests
from sqlalchemy import create_engine, select, update
from sqlalchemy.sql import functions

from .models import Base, sa_companies, sa_contractors
from .settings import Settings

logger = logging.getLogger('socket')


SQL_PREPARE = """
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;

CREATE OR REPLACE FUNCTION delete_services() RETURNS trigger AS $$
  BEGIN
    -- if there are no appointments on the old appointments service delete it
    PERFORM * FROM appointments WHERE service=OLD.service;
    IF NOT FOUND THEN
        DELETE FROM services WHERE id=OLD.service;
    END IF;
    RETURN NULL;
  END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS service_delete on appointments;
CREATE TRIGGER service_delete AFTER DELETE ON appointments FOR EACH ROW EXECUTE PROCEDURE delete_services();
"""


def lenient_connection(settings: Settings, retries=5):
    try:
        return psycopg2.connect(password=settings.pg_password, dsn=settings.pg_dsn)
    except psycopg2.Error as e:
        if retries <= 0:
            raise
        else:
            logger.warning('%s: %s (%d retries remaining)', e.__class__.__name__, e, retries)
            sleep(1)
            return lenient_connection(settings, retries=retries - 1)


@contextmanager
def psycopg2_cursor(settings):
    conn = lenient_connection(settings)
    conn.autocommit = True
    cur = conn.cursor()

    yield cur

    cur.close()
    conn.close()


def populate_db(engine):
    Base.metadata.create_all(engine)
    engine.execute(SQL_PREPARE)


DROP_CONNECTIONS = """
SELECT pg_terminate_backend(pg_stat_activity.pid)
FROM pg_stat_activity
WHERE pg_stat_activity.datname = %s AND pid <> pg_backend_pid();
"""


def prepare_database(delete_existing: Union[bool, callable], settings: Settings = None) -> bool:
    """
    (Re)create a fresh database and run migrations.

    :param delete_existing: whether or not to drop an existing database if it exists
    :return: whether or not a database as (re)created
    """
    settings = settings or Settings()

    with psycopg2_cursor(settings) as cur:
        cur.execute('SELECT EXISTS (SELECT datname FROM pg_catalog.pg_database WHERE datname=%s)', (settings.pg_name,))
        already_exists = bool(cur.fetchone()[0])
        if already_exists:
            if callable(delete_existing):
                _delete_existing = delete_existing()
            else:
                _delete_existing = bool(delete_existing)
            if not _delete_existing:
                print(f'database "{settings.pg_name}" already exists, not recreating it')
                return False
            else:
                print(f'dropping existing connections to "{settings.pg_name}"...')
                cur.execute(DROP_CONNECTIONS, (settings.pg_name,))

                logger.debug('dropping and re-creating the schema...')
                cur.execute('drop schema public cascade;\ncreate schema public;')
        else:
            print(f'database "{settings.pg_name}" does not yet exist, creating')
            cur.execute(f'CREATE DATABASE {settings.pg_name}')

    engine = create_engine(settings.pg_dsn)
    print('creating tables from model definition...')
    populate_db(engine)
    engine.dispose()
    print('db and tables creation finished.')
    return True


patches = []


def patch(func):
    patches.append(func)
    return func


def run_patch(live, patch_name):
    if patch_name is None:
        print(
            'available patches:\n{}'.format(
                '\n'.join('  {}: {}'.format(p.__name__, p.__doc__.strip('\n ')) for p in patches)
            )
        )
        return
    patch_lookup = {p.__name__: p for p in patches}
    try:
        patch_func = patch_lookup[patch_name]
    except KeyError:
        raise RuntimeError(f'patch {patch_name} not found in patches: {[p.__name__ for p in patches]}')

    print(f'running patch {patch_name} live {live}')
    settings = Settings()
    engine = create_engine(settings.pg_dsn)
    conn = engine.connect()
    trans = conn.begin()
    print('=' * 40)
    try:
        patch_func(conn)
    except BaseException as e:
        print('=' * 40)
        trans.rollback()
        raise RuntimeError('error running patch, rolling back') from e
    else:
        print('=' * 40)
        if live:
            trans.commit()
            print('live, committed patch')
        else:
            print('not live, rolling back')
            trans.rollback()
    finally:
        engine.dispose()


@patch
def print_tables(conn):
    """
    print names of all tables
    """
    # TODO unique, indexes, references
    result = conn.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname='public'")
    type_lookup = {
        'int4': 'INT',
        'float8': 'FLOAT',
    }
    for table_name, *_ in result:
        r = conn.execute(
            "SELECT column_name, udt_name, character_maximum_length, is_nullable, column_default "
            "FROM information_schema.columns WHERE table_name=%s",
            table_name,
        )
        fields = []
        for name, col_type, max_chars, nullable, dft in r:
            col_type = type_lookup.get(col_type, col_type.upper())
            field = [name]
            if col_type == 'VARCHAR':
                field.append(f'{col_type}({max_chars})')
            else:
                field.append(col_type)
            if nullable == 'NO':
                field.append('NOT NULL')
            if dft:
                field.append(f'DEFAULT {dft}')
            fields.append(' '.join(field))
        print('{} (\n  {}\n)\n'.format(table_name, '\n  '.join(fields)))


@patch
def add_missing_tables(conn):
    """
    adding tables to the database that are defined in models but not the db.
    """
    c = next(v[0] for v in conn.execute("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'"))
    print(f'tables: {c}, running create_all...')
    Base.metadata.create_all(conn)
    c = next(v[0] for v in conn.execute("SELECT COUNT(*) FROM pg_catalog.pg_tables WHERE schemaname='public'"))
    print(f'tables: {c}, done')


@patch
def add_labels(conn):
    """
    add labels field to contractors
    """
    conn.execute('ALTER TABLE contractors ADD labels VARCHAR(255)[]')
    conn.execute(
        """
    CREATE INDEX ix_contractors_labels
      ON contractors
      USING btree (labels);
    """
    )


@patch
def add_domains_options(conn):
    """
    add domains and options fields to companies, move domain values to domains, delete domain field
    """
    conn.execute('ALTER TABLE companies ADD domains VARCHAR(255)[]')
    conn.execute('ALTER TABLE companies ADD options JSONB')
    updated = 0
    for id, domain in conn.execute('SELECT id, domain FROM companies WHERE domain IS NOT NULL'):
        conn.execute(
            (update(sa_companies).values({'domains': [domain, 'www.' + domain]}).where(sa_companies.c.id == id))
        )
        updated += 1
    print(f'domains updated for {updated} companies')
    conn.execute('ALTER TABLE companies DROP COLUMN domain')


@patch
def add_review_fields(conn):
    """
    add review_rating and review_duration to contractors
    """
    conn.execute('ALTER TABLE contractors ADD review_rating DOUBLE PRECISION')
    conn.execute('ALTER TABLE contractors ADD review_duration INTEGER NOT NULL DEFAULT 0')


@patch
def resize_tag_line(conn):
    """
    resize the tag_line field on contractors to 255 chars
    """
    conn.execute('ALTER TABLE contractors ALTER COLUMN tag_line TYPE VARCHAR(255)')


@patch
def add_photo_hash(conn):
    """
    add photo_hash to contractors
    """
    conn.execute("ALTER TABLE contractors ADD photo_hash VARCHAR(6) DEFAULT '-'")


@patch
def run_sql_prepare(conn):
    """
    run SQL_PREPARE code to (re)create procedures and triggers
    """
    conn.execute(SQL_PREPARE)


@patch
def update_socket_images(conn):
    """
    Downloading images from server on EC2 and uploading them to S3
    """
    con_c = sa_contractors.c
    company_c = sa_companies.c
    base_url = 'https://socket.tutorcruncher.com/media'
    q_iter = select([con_c.id, company_c.public_key]).select_from(sa_contractors.join(sa_companies))
    session = requests.Session()
    settings = Settings()
    s3_client = boto3.client(
        's3', aws_access_key_id=settings.aws_access_key, aws_secret_access_key=settings.aws_secret_key
    )
    count = conn.execute(select([functions.count(con_c.id)]).select_from(sa_contractors))
    print(f'Processing images for {count.first()[0]} contractors')
    for row in conn.execute(q_iter):
        img_key = f'{row.public_key}/{row.id}.jpg'
        r = session.get(f'{base_url}/{img_key}')
        if r.status_code == 200:
            with BytesIO() as temp_file:
                temp_file.write(r.content)
                temp_file.seek(0)
                s3_client.upload_fileobj(Fileobj=temp_file, Bucket=settings.aws_bucket_name, Key=img_key)
            print(f'Uploading image {img_key}')
        elif r.status_code == 404:
            print(f'Unable to find {img_key}, returned 404')
        else:
            r.raise_for_status()

        img_thumb_key = f'{row.public_key}/{row.id}.thumb.jpg'
        r = session.get(f'{base_url}/{img_thumb_key}')
        if r.status_code == 200:
            with BytesIO() as temp_file:
                temp_file.write(r.content)
                temp_file.seek(0)
                s3_client.upload_fileobj(Fileobj=temp_file, Bucket=settings.aws_bucket_name, Key=img_thumb_key)
            print(f'Uploading image {img_thumb_key}')
        elif r.status_code == 404:
            print(f'Unable to find {img_thumb_key}, returned 404')
        else:
            r.raise_for_status()
