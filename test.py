'Tests for roller-balance server.'
import collections
import decimal
import os.path
import uuid

# pylint: disable=unused-import
import pytest
# pylint: enable=unused-import

import accounting
import db
import etherscan
import logs
import web

LOGGER = logs.logging.getLogger('roller.test')
ADDRESSES = [40*str(digit) for digit in range(10)]

# Details of deposits already made on ropsten.
SAFE = '5afe51A3E2f4bfDb689DDf89681fe09116b6894A'
DEPOSITS = [dict(
    source='7f6041155c0db03eb2b49abf2d61b370b4253ef7', amount=500000000000000000, block_number=11527328,
    transaction='ca3f6c423a4f66dd53e59498af42562d66c0a32d83faf6eb44d41102433c28d1'
), dict(
    source='44569aa35ff6d97e6531880712a41d2af72a007c', amount=500000000000000000, block_number=11527333,
    transaction='e055965dc4e848cfa188bf623dc62ad46142669809f132a591155543b15b035b')]
DEPOSIT_BLOCK_RANGE = (11527328, 11527333)
ADDRESSES = [DEPOSITS[idx]['source'] for idx in range(2)] + ADDRESSES

# Details of payments already made on ropsten.
PAYMENTS_ADDRESS = '5afe51A3E2f4bfDb689DDf89681fe09116b6894A'
PAYMENT_TRANSACTION = 'b90bab2330e838922a78ddad28f8c0fe0749b9e6a4434c2dc367a88e71197c7b'
PAYMENTS = [
    dict(address='7f6041155c0db03eb2b49abf2d61b370b4253ef7', amount=350000000000000000),
    dict(address='44569aa35ff6d97e6531880712a41d2af72a007c', amount=350000000000000000)]

# Details of a non multisend ether transaction.
PAYMENTS_ADDRESS_INVALID = 'd59af98e9b8885829aa5924e482549e2c24a50b9'
PAYMENT_TRANSACTION_INVALID = '17e9cdbec1030c129d8bf8d64b9a5fc54fce60d2b84ddf6f14a4b68384d197f2'


def initialize_test_database():
    'Initialize the database for testing.'
    assert db.DB_NAME[-5:] == '_test', f"will not run accounting tests on non test database {db.DB_NAME}"
    db.nuke_database_and_create_new_please_think_twice()


def get_last_transaction_idx():
    'Get the latest transaction idx.'
    with db.sql_connection() as sql:
        sql.execute('SELECT idx FROM transactions ORDER BY idx DESC LIMIT 1')
    return sql.fetchone()['idx']


def fake_transaction_hash():
    'Create a fake transaction hash.'
    return f"fake-{uuid.uuid4().hex}{uuid.uuid4().hex}"[:64]


def test_accounting_basic():
    'Test accounting.'
    initialize_test_database()

    # Test deposits.
    assert accounting.get_balance(ADDRESSES[0]) == 0
    accounting.debug_deposit(ADDRESSES[0], 1, fake_transaction_hash())
    assert accounting.get_balance(ADDRESSES[0]) == 1
    accounting.debug_deposit(ADDRESSES[0], 10, fake_transaction_hash())
    assert accounting.get_balance(ADDRESSES[0]) == 11

    # Test transfers.
    assert accounting.get_balance(ADDRESSES[1]) == 0
    accounting.transfer(ADDRESSES[0], ADDRESSES[1], 2)
    assert accounting.get_balance(ADDRESSES[0]) == 9
    assert accounting.get_balance(ADDRESSES[1]) == 2

    # Test withdraw.
    withdrawals = collections.defaultdict(list)
    transaction_idx = get_last_transaction_idx()
    assert accounting.get_unsettled_withdrawals() == withdrawals

    accounting.withdraw(ADDRESSES[0], 3)
    assert accounting.get_balance(ADDRESSES[0]) == 6
    transaction_idx += 1
    withdrawals[ADDRESSES[0]].append(dict(idx=transaction_idx, amount=decimal.Decimal(3)))
    assert accounting.get_unsettled_withdrawals() == withdrawals

    accounting.withdraw(ADDRESSES[0], 4)
    assert accounting.get_balance(ADDRESSES[0]) == 2
    transaction_idx += 1
    withdrawals[ADDRESSES[0]].append(dict(idx=transaction_idx, amount=decimal.Decimal(4)))
    assert accounting.get_unsettled_withdrawals() == withdrawals

    accounting.withdraw(ADDRESSES[1], 2)
    assert accounting.get_balance(ADDRESSES[1]) == 0
    transaction_idx += 1
    withdrawals[ADDRESSES[1]].append(dict(idx=transaction_idx, amount=decimal.Decimal(2)))
    assert accounting.get_unsettled_withdrawals() == withdrawals


def test_accounting_with_etherscan():
    'Test integration of accounting with etherscan module.'
    initialize_test_database()
    bot_fund = len(accounting.BOTS) * accounting.BOT_INITIAL_FUND
    assert accounting.get_balance(accounting.SAFE) == -bot_fund
    accounting.scan_for_deposits(*DEPOSIT_BLOCK_RANGE)
    assert accounting.get_balance(accounting.SAFE) == (
        -1 * sum([deposit['amount'] for deposit in DEPOSITS]) // accounting.WEI_DEPOSIT_FOR_ONE_ROLLER) - bot_fund
    assert not accounting.get_unsettled_withdrawals()

    withdrawals = collections.defaultdict(list)
    transaction_idx = get_last_transaction_idx()
    for deposit in DEPOSITS:
        balance = accounting.get_balance(deposit['source'])
        assert balance * accounting.WEI_DEPOSIT_FOR_ONE_ROLLER == deposit['amount']
        accounting.withdraw(deposit['source'], balance)
        transaction_idx += 1
        withdrawals[deposit['source']].append(dict(idx=transaction_idx, amount=decimal.Decimal(balance)))
    assert accounting.get_unsettled_withdrawals() == withdrawals
    assert accounting.settle(PAYMENT_TRANSACTION) == dict(settled_transactions_count=2, unsettled_transaction_count=0)
    assert not accounting.get_unsettled_withdrawals()

    # Test full scan and make sure we hit the same block twice.
    accounting.scan_for_deposits()
    accounting.scan_for_deposits()

    # Make sure we hit old data.
    with db.sql_connection() as sql:
        sql.execute('UPDATE deposit_scans SET end_block = %s', (DEPOSIT_BLOCK_RANGE[0],))
    with pytest.raises(accounting.ScanError):
        accounting.scan_for_deposits()


def test_accounting_bots(monkeypatch):
    'Test accounting bots.'
    initialize_test_database()

    # Save bots for later.
    monkeypatch.setattr(accounting, 'BOTS', dict(accounting.BOTS))

    # Get all bots.
    bots = []
    for idx in range(len(accounting.BOTS)):
        bots.append(dict(accounting.get_bot(ADDRESSES[idx]), player_address=ADDRESSES[idx]))
        assert bots[-1]['balance'] == accounting.BOTS[bots[-1]['address']]

    # No bots remaining.
    with pytest.raises(accounting.BotNotFound):
        accounting.get_bot(ADDRESSES[len(accounting.BOTS)])

    # Try to transfer too soon from a bot.
    with pytest.raises(accounting.BotNotFound):
        accounting.transfer(list(accounting.BOTS)[0], ADDRESSES[0], 1)
    monkeypatch.setattr(accounting, 'BOT_USAGE_MIN', 'INTERVAL -1 SECOND')

    # Try to transfer too much from a bot.
    with pytest.raises(accounting.InsufficientFunds):
        accounting.transfer(list(accounting.BOTS)[0], ADDRESSES[0], accounting.BOT_INITIAL_FUND)
    monkeypatch.setattr(accounting, 'BOT_TRANSFER_MAX', accounting.BOT_INITIAL_FUND)

    # Try to transfer with wrong player address.
    with pytest.raises(accounting.BotNotFound):
        accounting.transfer(list(accounting.BOTS)[0], ADDRESSES[1], 1)

    # Free a bot, try to grab it by a player that already has a bot, then transfer to and fro.
    accounting.transfer(bots[0]['address'], bots[0]['player_address'], 1)
    with pytest.raises(accounting.BotNotFound):
        accounting.get_bot(bots[1]['player_address'])
    accounting.get_bot(bots[0]['player_address'])
    accounting.transfer(bots[0]['player_address'], bots[0]['address'], 1)
    accounting.get_bot(bots[0]['player_address'])

    # Free a bot, make sure it is free by requesting it, then free it and deplete it to make sure it is removed.
    for bot in bots:
        accounting.transfer(bot['address'], bot['player_address'], accounting.BOT_INITIAL_FUND - 1)
        assert accounting.get_bot(bot['player_address'])['address'] == bot['address']
        accounting.transfer(bot['address'], bot['player_address'], 1)
        with pytest.raises(accounting.BotNotFound):
            accounting.get_bot(ADDRESSES[idx])

    # Find that there are no bots avaiable.
    with pytest.raises(accounting.BotNotFound):
        accounting.update_bots()


def test_accounting_errors(monkeypatch):
    'Test accounting errors.'
    initialize_test_database()

    accounting.debug_deposit(ADDRESSES[0], 1, fake_transaction_hash())
    with pytest.raises(accounting.InsufficientFunds):
        accounting.transfer(ADDRESSES[0], ADDRESSES[1], 2)

    with pytest.raises(accounting.InsufficientFunds):
        accounting.withdraw(ADDRESSES[0], 2)

    bad_deposits = [deposit.copy() for deposit in DEPOSITS]
    bad_deposits[0]['amount'] -= 1
    monkeypatch.setattr(etherscan, 'get_deposits', lambda *args, **kwargs: bad_deposits)
    accounting.scan_for_deposits(*DEPOSIT_BLOCK_RANGE)

    bad_payments = [payment.copy() for payment in PAYMENTS]
    bad_payments[0]['amount'] -= 1
    monkeypatch.setattr(etherscan, 'get_payments', lambda *args, **kwargs: bad_payments)
    with pytest.raises(accounting.SettleError):
        accounting.settle(PAYMENT_TRANSACTION)

    for deposit in DEPOSITS:
        balance = accounting.get_balance(deposit['source'])
        accounting.withdraw(deposit['source'], balance)

    bad_withdrawals = accounting.get_unsettled_withdrawals()
    bad_withdrawals[ADDRESSES[0]].append(bad_withdrawals[ADDRESSES[0]][0])
    monkeypatch.setattr(accounting, 'get_unsettled_withdrawals', lambda *args, **kwargs: bad_withdrawals)
    with pytest.raises(accounting.SettleError):
        accounting.settle(PAYMENT_TRANSACTION)


def test_webserver_errors():
    'General webserver errors.'
    initialize_test_database()
    web.DEBUG = False
    with web.APP.test_client() as client:
        error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=10))
        assert error_response.status == '403 FORBIDDEN'

        error_response = client.post('/get_balance')
        assert error_response.status == '400 BAD REQUEST'
        assert error_response.json == dict(
            status=400, error_name='ArgumentMismatch',
            error_message='request does not contain arguments(s): address')

        error_response = client.post('/get_balance', data=dict(bad_argument='stam', address=ADDRESSES[0]))
        assert error_response.status == '400 BAD REQUEST'
        assert error_response.json == dict(
            status=400, error_name='ArgumentMismatch',
            error_message='request contain unexpected arguments(s): bad_argument')

        for bad_amount in ['string', 1.1]:
            error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=bad_amount))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message='argument amount has to be an integer')

        for bad_amount in [0, -1]:
            error_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=bad_amount))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message='argument amount must be larger than zero')

        for bad_address, error_message in [
            (f"{ADDRESSES[0][:-1]}", 'argument address must be 40 characters long'),
            (f"{ADDRESSES[0][:-1]}g", 'argument address is not a hex string')
        ]:
            error_response = client.post('/get_balance', data=dict(address=bad_address))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message=error_message)

        for bad_tx_hash, error_message in [
            (63*'0', 'argument transaction_hash must be 64 characters long'),
            (64*'g', 'argument transaction_hash is not a hex string')
        ]:
            error_response = client.post('/settle', data=dict(transaction_hash=bad_tx_hash))
            assert error_response.status == '400 BAD REQUEST'
            assert error_response.json == dict(
                status=400, error_name='ArgumentMismatch',
                error_message=error_message)

        error_response = client.get('/five_hundred')
        for reason in ['response', 'exception']:
            error_response = client.post('/five_hundred', data=dict(reason=reason))
            assert error_response.status == '500 INTERNAL SERVER ERROR'

        assert client.get('/no_such_endpoint').status == '403 FORBIDDEN'


def test_webserver_debug():
    'Test an almost full flow in debug mode.'
    initialize_test_database()
    web.DEBUG = True
    with web.APP.test_client() as client:
        prices_repsonse = client.get('/get_prices')
        assert prices_repsonse.status == '200 OK'
        assert prices_repsonse.json == dict(
            status=200, safe=accounting.SAFE,
            wei_deposit_for_one_roller=accounting.WEI_DEPOSIT_FOR_ONE_ROLLER,
            wei_withdraw_for_one_roller=accounting.WEI_WITHDRAW_FOR_ONE_ROLLER)

        bot_response = client.post('/get_bot', data=dict(player_address=ADDRESSES[0]))
        assert bot_response.status == '200 OK'
        assert bot_response.json == dict(status=200, bot=dict(
            address=sorted(accounting.BOTS)[0], balance=accounting.BOT_INITIAL_FUND))

        balance_response = client.post('/get_balance', data=dict(address=ADDRESSES[0]))
        assert balance_response.status == '200 OK'
        assert balance_response.json == dict(status=200, balance=0)

        deposit_response = client.post('/deposit', data=dict(address=ADDRESSES[0], amount=100))
        assert deposit_response.status == '201 CREATED'
        assert client.post('/get_balance', data=dict(address=ADDRESSES[0])).json['balance'] == 100

        transfer_response = client.post('/transfer', data=dict(
            source=ADDRESSES[0], target=ADDRESSES[1],  amount=101))
        assert transfer_response.status == '400 BAD REQUEST'
        assert transfer_response.json == dict(
                status=400, error_name='InsufficientFunds',
                error_message=f"address {ADDRESSES[0]} has less than 101 rollers")
        transfer_response = client.post('/transfer', data=dict(
            source=ADDRESSES[0], target=ADDRESSES[1],  amount=10))
        assert transfer_response.status == '201 CREATED'
        assert client.post('/get_balance', data=dict(address=ADDRESSES[0])).json['balance'] == 90
        assert client.post('/get_balance', data=dict(address=ADDRESSES[1])).json['balance'] == 10

        assert client.get('/get_unsettled_withdrawals').status == '200 OK'
        assert client.get('/get_unsettled_withdrawals').json['unsettled_withdrawals'] == ''
        withdraw_response = client.post('/withdraw', data=dict(address=ADDRESSES[0], amount=91))
        assert withdraw_response.status == '400 BAD REQUEST'
        assert withdraw_response.json == dict(
                status=400, error_name='InsufficientFunds',
                error_message=f"address {ADDRESSES[0]} has less than 91 rollers")
        withdraw_response = client.post('/withdraw', data=dict(address=ADDRESSES[0], amount=5))
        assert withdraw_response.status == '201 CREATED'
        assert client.post('/get_balance', data=dict(address=ADDRESSES[0])).json['balance'] == 85
        assert client.get('/get_unsettled_withdrawals').json['unsettled_withdrawals'] != ''
        withdraw_response = client.post('/withdraw', data=dict(address=ADDRESSES[1], amount=5))
        assert withdraw_response.status == '201 CREATED'
        assert client.post('/get_balance', data=dict(address=ADDRESSES[1])).json['balance'] == 5
        assert client.get('/get_unsettled_withdrawals').json['unsettled_withdrawals'] != ''


def test_webserver_payment_flow():
    'To test the full flow we run a production webserver.'
    initialize_test_database()
    accounting.scan_for_deposits(*DEPOSIT_BLOCK_RANGE)
    web.DEBUG = False
    with web.APP.test_client() as client:
        for deposit in DEPOSITS:
            roller_balance = deposit['amount'] // accounting.WEI_DEPOSIT_FOR_ONE_ROLLER
            balance_response = client.post('/get_balance', data=dict(address=deposit['source']))
            assert balance_response.json == dict(status=200, balance=roller_balance)
            client.post('/withdraw', data=dict(address=deposit['source'], amount=roller_balance))
        assert client.get('/get_unsettled_withdrawals').json['unsettled_withdrawals'] != ''
        assert client.post('/settle', data=dict(transaction_hash=PAYMENT_TRANSACTION)).status == '201 CREATED'
        assert client.get('/get_unsettled_withdrawals').json['unsettled_withdrawals'] == ''


def test_etherscan(monkeypatch):
    'Test etherscan module.'
    assert etherscan.get_latest_block_number() > 0
    assert etherscan.get_deposits(SAFE, *DEPOSIT_BLOCK_RANGE) == DEPOSITS
    assert etherscan.get_payments(PAYMENTS_ADDRESS, PAYMENT_TRANSACTION) == PAYMENTS

    # Test a non matching address.
    assert etherscan.get_payments(PAYMENTS_ADDRESS_INVALID, PAYMENT_TRANSACTION) == []

    # Test a non multisend transaction.
    assert etherscan.get_payments(PAYMENTS_ADDRESS_INVALID, PAYMENT_TRANSACTION_INVALID) == []

    original_headers = etherscan.ETHERSCAN_HEADERS
    etherscan.ETHERSCAN_HEADERS = {}
    with pytest.raises(etherscan.EtherscanError):
        etherscan.get_latest_block_number()
    with pytest.raises(etherscan.EtherscanError):
        etherscan.get_deposits(SAFE, *DEPOSIT_BLOCK_RANGE)
    etherscan.ETHERSCAN_HEADERS = original_headers

    monkeypatch.setattr(etherscan, 'call', lambda *args, **kwargs: 'not a hex string')
    with pytest.raises(etherscan.EtherscanError):
        etherscan.get_latest_block_number()


def test_database(monkeypatch, tmp_path):
    'Test database access.'
    initialize_test_database()
    with db.sql_connection() as sql:
        sql.execute('SELECT 1 FROM transactions')

    with pytest.raises(db.pymysql.MySQLError):
        with db.sql_connection() as sql:
            sql.execute('bad sql')

    # Try bad migrations.
    monkeypatch.setattr(db, 'MIGRATIONS_DIRECTORY', tmp_path)
    for migration, migration_file_name in (
        ('Bad SQL;', '0.bad.sql'),
        ('# No apply function.', '0.bad.py'),
        ('Bad python', '0.bad.py')
    ):
        with open(os.path.join(tmp_path, migration_file_name), 'w', encoding='utf-8') as migration_file:
            migration_file.write(migration)
        # It's okay, really.
        # pylint: disable=cell-var-from-loop
        monkeypatch.setattr(db.os, 'listdir', lambda *args, **kwargs: [migration_file_name])
        # pylint: enable=cell-var-from-loop
        with pytest.raises(db.FailedMigration):
            initialize_test_database()
    # monkeypatch.undo()

    # Invalid migration file names.
    monkeypatch.setattr(db.os.path, 'isfile', lambda *args, **kwargs: True)
    monkeypatch.setattr(db.os, 'listdir', lambda *args, **kwargs: [
        '0.schema.sqnot', 'schema.sql', '/tmp', '0.schema.sql', '0.duplicate.sql'])
    with pytest.raises(db.DuplicateMigrationNumber):
        initialize_test_database()
    monkeypatch.undo()


def test_logs():
    'Just for coverage.'
    web.logs.setup(suppress_loggers=['foo'])
