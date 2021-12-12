'roller-balance database access.'
import contextlib
import importlib
import logging
import os
import re
import subprocess

import pymysql

LOGGER = logging.getLogger('roller.db')
DB_HOST = os.environ.get('ROLLER_DB_HOST', 'localhost')
DB_USER = os.environ.get('ROLLER_DB_USER', 'root')
DB_PASS = os.environ.get('ROLLER_DB_PASS', 'pass')
DB_NAME = os.environ.get('ROLLER_DB_NAME', 'roller')
MIGRATIONS_DIRECTORY = './migrations'


class DuplicateMigrationNumber(Exception):
    'Found more than one migration file with the same number.'


class FailedMigration(Exception):
    'A migration failed.'


@contextlib.contextmanager
def sql_connection(db_name=False):
    'Context manager for querying the database.'
    # Default to DB_NAME dynamically (not at def time).
    if db_name is False:
        db_name = DB_NAME
    try:
        connection = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASS, database=db_name)
        yield connection.cursor(pymysql.cursors.DictCursor)
        connection.commit()
    except pymysql.MySQLError:
        LOGGER.exception('database error')
        if 'connection' in locals():
            connection.rollback()
        raise
    finally:
        if 'connection' in locals():
            connection.close()


def collect_migrations():
    'Collect all valid migration sorted by number.'
    migrations = {}
    for file_name in os.listdir(MIGRATIONS_DIRECTORY):
        file_path = os.path.join(MIGRATIONS_DIRECTORY, file_name)
        if not os.path.isfile(file_path):
            LOGGER.warning(f"skipping unsupported migration file type {file_name} (is not a file)")
            continue
        if not os.path.splitext(file_name)[-1].lower() in ['.sql', '.py']:
            LOGGER.warning(f"skipping unsupported migration file type {file_name} (is not sql or py)")
            continue
        try:
            migration_number = int(re.match('[0-9]+', file_name).group(0), 10)
        except AttributeError:
            LOGGER.warning(f"skipping unsupported migration file type {file_name} (does not start with a digit)")
            continue
        if migration_number in migrations:
            raise DuplicateMigrationNumber(
                f"duplicate migration numbers detected - {migrations[migration_number]} and {file_path}")
        migrations[migration_number] = file_path
    return sorted(migrations.values())


def nuke_database_and_create_new_please_think_twice():
    'Remove and recreate the database completely - only for debug environment.'
    with sql_connection(db_name=None) as sql:
        LOGGER.warning(f"dropping database {DB_NAME}")
        sql.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        LOGGER.info(f"creating database {DB_NAME}")
        sql.execute(f"CREATE DATABASE {DB_NAME}")
    for migration in collect_migrations():
        if migration.lower().endswith('.sql'):
            with open(migration, 'r', encoding='utf-8') as sql_file:
                if subprocess.call(
                    ['mysql', '-h', DB_HOST, '-u', DB_USER, f"-p{DB_PASS}", DB_NAME], stdin=sql_file
                ) != 0:
                    raise FailedMigration(f"migration file in {migration} failed")
        elif migration.lower().endswith('.py'):
            try:
                spec = importlib.util.spec_from_file_location('migration', migration)
                migration_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(migration_module)
                migration_module.apply()
            except Exception:
                raise FailedMigration(
                    f"failed running apply method in migration file in {migration}") from None
