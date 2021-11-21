'roller-balance data handling.'
import contextlib
import json
import logging
import os

import pymysql

import etherscan

LOGGER = logging.getLogger('roller.data')
SAFE = os.environ.get('ROLLER_SAFE_ADDRESS', 40*'F')
REQUIRED_BLOCK_DEPTH = 10
DEBUG = os.environ.get('ROLLER_DEBUG', 'false').lower() in ['true', 'yes', 'y', '1']
DB_HOST = os.environ.get('ROLLER_DB_HOST', 'localhost')
DB_USER = os.environ.get('ROLLER_DB_USER', 'root')
DB_PASS = os.environ.get('ROLLER_DB_PASS', 'pass')
DB_NAME = os.environ.get('ROLLER_DB_NAME', 'roller')


class InsufficientFunds(Exception):
    'Not enough funds to perform operation.'


class ScanError(Exception):
    'An error when scanning for transactions.'
    def __init__(self, message, data):
        super().__init__(message)
        self.data = data


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
    amount DECIMAL(65) UNSIGNED NOT NULL,
    INDEX(source),
    INDEX(target))''')
        sql.execute('''
CREATE TABLE payments(
    remote_transaction CHAR(64) NOT NULL,
    local_transaction BIGINT UNSIGNED NOT NULL,
    INDEX(remote_transaction),
    FOREIGN KEY(local_transaction) REFERENCES transactions(idx))''')
        sql.execute('''
CREATE TABLE deposit_scans(
    idx SERIAL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    start_block BIGINT UNSIGNED NOT NULL,
    end_block BIGINT UNSIGNED NOT NULL,
    transactions JSON,
    INDEX(end_block))''')


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


def transfer_in_session(source, target, amount, sql):
    'Transfer rollers from source to target within a running session - no validaiton!'
    sql.execute(
        "INSERT INTO transactions(source, target, amount) VALUES(%(source)s, %(target)s, %(amount)s)",
        dict(source=source, target=target, amount=int(amount)))
    return sql.lastrowid


def transfer(source, target, amount):
    'Transfer rollers from source to target.'
    if amount > get_balance(source):
        raise InsufficientFunds(f"address {source} has less than {amount} rollers")
    with sql_connection() as sql:
        return transfer_in_session(source, target, amount, sql)


def deposit_in_session(address, amount, remote_transaction, sql):
    'Fund an address from the safe within a running session - no validation!.'
    local_transaction = transfer_in_session(SAFE, address, amount, sql)
    sql.execute("""
        INSERT INTO payments(remote_transaction, local_transaction)
        VALUES(%(remote_transaction)s, %(local_transaction)s)
    """, dict(remote_transaction=remote_transaction, local_transaction=local_transaction))


def deposit(address, amount, remote_transaction):
    'Fund an address from the safe.'
    with sql_connection() as sql:
        return deposit_in_session(address, amount, remote_transaction, sql)


def scan_for_deposits():
    'Scan transactions sending ether to the safe, and update deposits accordingly.'
    with sql_connection() as sql:
        sql.execute('SELECT COALESCE(MAX(end_block) + 1, 0) AS start_block FROM deposit_scans')
        start_block = sql.fetchone()['start_block']
    end_block = etherscan.get_latest_block_number() - REQUIRED_BLOCK_DEPTH
    if end_block < start_block:
        return

    payments = etherscan.get_ether_payments(SAFE, start_block, end_block)
    with sql_connection() as sql:
        if payments:
            # Check for duplicate transactions.
            sql.execute(f"""SELECT 1 FROM payments WHERE remote_transaction IN (
                {','.join(['%s' for i in range(len(payments))])}
            ) LIMIT 1""", [payment['transaction'] for payment in payments])
            if sql.fetchone():
                LOGGER.error([payment['transaction'] for payment in payments])
                raise ScanError('invalid payments detected', data=dict(payments=payments))

            for payment in payments:
                deposit_in_session(payment['source'], payment['amount'], payment['transaction'], sql)

        payments = {}
        sql.execute("""INSERT INTO deposit_scans(start_block, end_block, transactions) VALUES(
            %(start_block)s, %(end_block)s, %(transactions)s
        )""", dict(start_block=start_block, end_block=end_block, transactions=json.dumps(payments)))


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
