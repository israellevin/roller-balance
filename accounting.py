'roller-balance accounting.'
import collections
import json
import logging
import os

import eth_utils

import etherscan
import db

LOGGER = logging.getLogger('roller.accounting')
WEI_DEPOSIT_FOR_ONE_ROLLER = 1*10**14  # 1/1000 ether, so a hundred will cost 0.01 eth.
WEI_WITHDRAW_FOR_ONE_ROLLER = 7*10**13  # 7/10000 ether, so a hundred will withdraw 0.007 eth.
REQUIRED_BLOCK_DEPTH = 10  # This is the required depth for accepting deposits and settling withdrawals.
BOT_INITIAL_FUND = 1000
BOT_TRANSFER_MAX = 50
BOT_USAGE_MIN = 'INTERVAL 10 SECOND'
BOT_USAGE_MAX = 'INTERVAL 10 MINUTE'
BOTS = {
    'dD2FD4581271e230360230F9337D5c0430Bf44C0': BOT_INITIAL_FUND,
    '9873b417A5276ac533B51238C8E314BCCced2a1F': BOT_INITIAL_FUND,
    '53993b1a1c6FB4714e9D02FaF9c72a0118e2F9FE': BOT_INITIAL_FUND}
SAFE = os.environ.get('ROLLER_SAFE_ADDRESS')
DEBUG = os.environ.get('ROLLER_DEBUG', 'false').lower() in ['true', 'yes', 'y', '1']


class BotNotFound(Exception):
    'No bot found to play with.'


class InsufficientFunds(Exception):
    'Not enough funds to perform operation.'


class ScanError(Exception):
    'An error when scanning for transactions.'


class SettleError(Exception):
    'An error when settling payments.'


def get_balance(address):
    'Get the roller balance of an address.'
    with db.sql_connection() as sql:
        sql.execute("""
            SELECT COALESCE(SUM(amount), 0) AS sum FROM transactions WHERE target = %(address)s
            UNION ALL
            SELECT COALESCE(SUM(amount), 0) AS sum FROM transactions WHERE source = %(address)s
        """, dict(address=address))
        credit, debit = (row['sum'] for row in sql.fetchall())
        return int(credit - debit)


def update_bots():
    'Update the balances of the BOTS global - a bot that exhausted its balance is removed from the list.'
    # Using a global here because I think a singleton class in an overkill.
    # pylint: disable=global-variable-not-assigned,global-statement
    global BOTS
    # pylint: enable=global-variable-not-assigned,global-statement
    if not BOTS:
        raise BotNotFound
    new_bots = {}
    for bot in BOTS:
        balance = get_balance(bot)
        if balance == 0:
            LOGGER.warning(f"bot {bot} is out of funds")
            continue
        new_bots[bot] = balance
    if new_bots != BOTS:
        BOTS = new_bots
    if not BOTS:
        raise BotNotFound


def get_bot(player_address):
    'Get a bot to play with.'
    update_bots()
    with db.sql_connection() as sql:
        # First make sure the player is not already holding a bot.
        sql.execute(f"""SELECT bot_address FROM bots WHERE idx IN (
            SELECT MAX(idx) FROM bots WHERE player_address = %(player_address)s
        ) AND busy = 1 AND timestamp > (NOW() - {BOT_USAGE_MAX})""", dict(player_address=player_address))
        try:
            bot_address = sql.fetchone()['bot_address']
            raise BotNotFound(f"bot not avaiable for {player_address} because he is using bot {bot_address}")
        except (TypeError, KeyError):
            pass

        # Then find an avaiable bot.
        if sql.execute(f"""INSERT INTO bots(bot_address, player_address, busy)
            SELECT bot_address, %(player_address)s AS player_address, 1 as busy FROM
                ({' UNION ALL '.join([f"SELECT %(bot_{bot})s AS bot_address" for bot in BOTS])}) AS bots
            WHERE bot_address NOT IN(
                SELECT bot_address FROM bots WHERE idx IN (SELECT MAX(idx) FROM bots GROUP BY bot_address)
                AND busy = 1 AND timestamp > (NOW() - {BOT_USAGE_MAX})
            ) ORDER BY bot_address LIMIT 1
        """, dict({f"bot_{bot}": bot for bot in BOTS}, player_address=player_address)) != 1:
            raise BotNotFound
        sql.execute("SELECT bot_address FROM bots WHERE idx = %(row_id)s", dict(row_id=sql.lastrowid))
        address = sql.fetchone()['bot_address']
        return dict(address=address, balance=BOTS[address])


def free_bot(source, target, amount, sql):
    'Check if one of the transfer addresses belongs to a recognized bot, and if so if it was requested by the other.'
    if source in BOTS:
        if amount > BOT_TRANSFER_MAX:
            raise InsufficientFunds(f"bot transfer of {amount} from {source} to {target} too big")
        bot_address = source
        player_address = target
    elif target not in BOTS:
        return
    else:
        bot_address = target
        player_address = source
    sql.execute(f"""
        SELECT 1 FROM bots
        WHERE timestamp > (NOW() - {BOT_USAGE_MAX}) AND timestamp < (NOW() - {BOT_USAGE_MIN})
        AND bot_address = %(bot_address)s AND player_address = %(player_address)s
    """, dict(bot_address=bot_address, player_address=player_address))
    if sql.fetchone() is None:
        raise BotNotFound(f"bot {bot_address} not avaiable for {player_address}")
    sql.execute(
        "INSERT INTO bots(bot_address, player_address, busy) VALUES(%(bot_address)s, %(player_address)s, 0)",
        dict(bot_address=bot_address, player_address=player_address))


def transfer_in_session(source, target, amount, sql):
    'Transfer rollers from source to target within a running session - no validaiton!'
    LOGGER.info(f"transfer {amount} from {source} to {target}")
    sql.execute(
        "INSERT INTO transactions(source, target, amount) VALUES(%(source)s, %(target)s, %(amount)s)",
        dict(source=source, target=target, amount=int(amount)))
    return sql.lastrowid


def transfer(source, target, amount):
    'Transfer rollers from source to target.'
    if amount > get_balance(source):
        raise InsufficientFunds(f"address {source} has less than {amount} rollers")
    with db.sql_connection() as sql:
        free_bot(source, target, amount, sql)
        return transfer_in_session(source, target, amount, sql)


def deposit_in_session(address, amount, remote_transaction, sql):
    'Fund an address from the safe within a running session - no validation!.'
    local_transaction = transfer_in_session(SAFE, address, amount, sql)
    sql.execute("""
        INSERT INTO ether_transactions(remote_transaction, local_transaction)
        VALUES(%(remote_transaction)s, %(local_transaction)s)
    """, dict(remote_transaction=remote_transaction, local_transaction=local_transaction))


def debug_deposit(address, amount, remote_transaction):
    'Fund an address from the safe.'
    with db.sql_connection() as sql:
        return deposit_in_session(address, amount, remote_transaction, sql)


def scan_for_deposits(start_block=None, end_block=None):
    'Scan transactions sending ether to the safe, and update deposits accordingly.'
    if start_block is None:
        with db.sql_connection() as sql:
            sql.execute('SELECT COALESCE(MAX(end_block) + 1, 0) AS start_block FROM deposit_scans')
            start_block = sql.fetchone()['start_block']
    if end_block is None:
        end_block = etherscan.get_latest_block_number() - REQUIRED_BLOCK_DEPTH
    if end_block < start_block:
        return

    deposits = etherscan.get_deposits(SAFE, start_block, end_block)
    with db.sql_connection() as sql:
        if deposits:
            # Check for duplicate transactions.
            sql.execute(f"""SELECT 1 FROM ether_transactions WHERE remote_transaction IN (
                {', '.join(['%s' for i in range(len(deposits))])}
            ) LIMIT 1""", [deposit['transaction'] for deposit in deposits])
            if sql.fetchone():
                LOGGER.error(f"duplicate deposits reported: {[deposit['transaction'] for deposit in deposits]}")
                raise ScanError('invalid deposits detected')

            for deposit in deposits:
                if deposit['amount'] % WEI_DEPOSIT_FOR_ONE_ROLLER != 0:
                    LOGGER.error(f"non integer deposit - {deposit}")
                roller_amount = deposit['amount'] // WEI_DEPOSIT_FOR_ONE_ROLLER
                deposit_in_session(deposit['source'], roller_amount, deposit['transaction'], sql)

        sql.execute("""INSERT INTO deposit_scans(start_block, end_block, transactions) VALUES(
            %(start_block)s, %(end_block)s, %(transactions)s
        )""", dict(start_block=start_block, end_block=end_block, transactions=json.dumps(deposits)))


def withdraw(address, amount):
    'Request a withdraw.'
    transfer(address, SAFE, amount)


def get_unsettled_withdrawals():
    'Get an aggregated list of all unsettled_withdrawals done at least five days before the end of the month.'
    withdrawals = collections.defaultdict(list)
    with db.sql_connection() as sql:
        sql.execute("""
            SELECT idx, source AS address, amount FROM transactions
            LEFT JOIN ether_transactions ON transactions.idx = ether_transactions.local_transaction
            WHERE target = %(safe)s AND remote_transaction IS NULL AND timestamp <= LAST_DAY(NOW()) - INTERVAL 5 DAY
            ORDER BY idx
        """, dict(safe=SAFE))
        for withdrawal in sql.fetchall():
            withdrawals[withdrawal.pop('address')].append(withdrawal)
    return withdrawals


def roller_to_eth(roller_amount):
    'Convert an amount of rollers to a sell price of eth.'
    return eth_utils.from_wei(roller_amount * WEI_WITHDRAW_FOR_ONE_ROLLER, 'ether')


def match_settlable_withdrawals(remote_transaction):
    'Match unsettled withdrawals with ether payments made in a multisender call.'
    candidates = get_unsettled_withdrawals()
    matches = set()
    for payment in etherscan.get_payments(SAFE, remote_transaction):
        if payment['amount'] % WEI_WITHDRAW_FOR_ONE_ROLLER != 0:
            LOGGER.error(f"non integer payment - {payment}")
        for withdrawal in candidates[payment['address']]:
            wei_amount = withdrawal['amount'] * WEI_WITHDRAW_FOR_ONE_ROLLER
            if payment['amount'] < wei_amount:
                LOGGER.error(f"unmatched withdrawal - {withdrawal} not in {payment}")
                continue
            payment['amount'] -= wei_amount
            matches.add(withdrawal['idx'])
        if payment['amount'] != 0:
            raise SettleError(f"unmatched payment - {payment}")
    return matches


def settle(remote_transaction):
    'Mark withdrawals that were settled by remote_transaction as - currently just marks all withdrawals.'
    settlable = match_settlable_withdrawals(remote_transaction)
    settled_transactions_count = 0
    if settlable:
        with db.sql_connection() as sql:
            settled_transactions_count = sql.executemany("""
                INSERT INTO ether_transactions(remote_transaction, local_transaction)
                VALUES(%(remote_transaction)s, %(local_transaction)s)""", [
                    dict(remote_transaction=remote_transaction, local_transaction=withdrawal_idx)
                    for withdrawal_idx in settlable])
    return dict(
        settled_transactions_count=settled_transactions_count,
        unsettled_transaction_count=len(get_unsettled_withdrawals()))
