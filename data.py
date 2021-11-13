'roller-balance data handling.'
import contextlib
import logging
import os

import pymysql

LOGGER = logging.getLogger('roller.data')
SAFE = os.environ.get('ROLLER_SAFE_ADDRESS', 40*'F')
DEBUG = os.environ.get('ROLLER_DEBUG', 'false').lower() in ['true', 'yes', 'y', '1']
DB_HOST = os.environ.get('ROLLER_DB_HOST', 'localhost')
DB_USER = os.environ.get('ROLLER_DB_USER', 'root')
DB_PASS = os.environ.get('ROLLER_DB_PASS', 'pass')
DB_NAME = os.environ.get('ROLLER_DB_NAME', 'roller')


class InsufficientFunds(Exception):
    'Not enough funds to perform operation.'


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


def nuke_database_and_create_new_please_think_twice():
    'Remove and recreate the database completely - only for debug environment.'
    with sql_connection(db_name=None) as sql:
        LOGGER.warning(f"dropping database {DB_NAME}")
        sql.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        LOGGER.info(f"creating database {DB_NAME}")
        sql.execute(f"CREATE DATABASE {DB_NAME}")
    with sql_connection() as sql:
        sql.execute('''
CREATE TABLE transactions(
    idx SERIAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    source CHAR(40) NOT NULL,
    target CHAR(40) NOT NULL,
    amount BIGINT UNSIGNED NOT NULL,
    INDEX(source),
    INDEX(target))''')
        sql.execute('''
CREATE TABLE payments(
    remote_transaction CHAR(64) NOT NULL,
    local_transaction BIGINT UNSIGNED NOT NULL,
    INDEX(remote_transaction),
    FOREIGN KEY(local_transaction) REFERENCES transactions(idx))''')


def get_balance(address):
    'Get the roller balance of an address.'
    with sql_connection() as sql:
        sql.execute("""
            SELECT COALESCE(SUM(amount), 0) AS sum FROM transactions WHERE target = %(address)s
            UNION ALL
            SELECT COALESCE(SUM(amount), 0) AS sum FROM transactions WHERE source = %(address)s
        """, dict(address=address))
        credit, debit = (row['sum'] for row in sql.fetchall())
        return int(credit - debit)


def transfer(source, target, amount):
    'Transfer rollers from source to target.'
    if amount > get_balance(source) and source != SAFE:
        raise InsufficientFunds(f"address {source} has less than {amount} rollers")
    with sql_connection() as sql:
        sql.execute(
            "INSERT INTO transactions(source, target, amount) VALUES(%(source)s, %(target)s, %(amount)s)",
            dict(source=source, target=target, amount=amount))
        return sql.lastrowid


def deposit(address, amount, remote_transaction):
    'Fund an address from the safe.'
    local_transaction = transfer(SAFE, address, amount)
    with sql_connection() as sql:
        sql.execute("""
            INSERT INTO payments(remote_transaction, local_transaction)
            VALUES(%(remote_transaction)s, %(local_transaction)s)
        """, dict(remote_transaction=remote_transaction, local_transaction=local_transaction))


def withdraw(address, amount):
    'Request a withdraw.'
    transfer(address, SAFE, amount)


def get_unsettled_withdrawals():
    'Get a list of all unsettled_withdrawals.'
    with sql_connection() as sql:
        sql.execute("""
            SELECT idx, source AS address, amount FROM transactions
            LEFT JOIN payments ON transactions.idx = payments.local_transaction
            WHERE target = %(safe)s AND remote_transaction IS NULL
            ORDER BY idx
        """, dict(safe=SAFE))
        return sql.fetchall()


def get_unsettled_withdrawals_aggregated_csv():
    'Get a list of all unsettled_withdrawals.'
    with sql_connection() as sql:
        sql.execute("""
            SELECT source AS address, SUM(amount) AS amount FROM transactions
            LEFT JOIN payments ON transactions.idx = payments.local_transaction
            WHERE target = %(safe)s AND remote_transaction IS NULL
            GROUP BY address ORDER BY MIN(idx)
        """, dict(safe=SAFE))
        return "\n".join([f"{withdrawal['address']}, {withdrawal['amount']}" for withdrawal in sql.fetchall()])


def settle(remote_transaction):
    'Mark withdrawals that were settled by remote_transaction as - currently just marks all withdrawals.'
    with sql_connection() as sql:
        settled_transactions_count = sql.executemany("""
            INSERT INTO payments(remote_transaction, local_transaction)
            VALUES(%(remote_transaction)s, %(local_transaction)s)""", [
                dict(remote_transaction=remote_transaction, local_transaction=unsettled_withdrawl['idx'])
                for unsettled_withdrawl in get_unsettled_withdrawals()])
    return dict(
            settled_transactions_count=settled_transactions_count,
            unsettled_transaction_count=len(get_unsettled_withdrawals()))
