'Tests for roller-balance server.'
import uuid

# pylint: disable=unused-import
import pytest
# pylint: enable=unused-import
import _pytest.monkeypatch

import data
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
    assert data.DB_NAME[-5:] == '_test', f"will not run data tests on non test database {data.DB_NAME}"
    data.nuke_database_and_create_new_please_think_twice()


def test_data_etherscan():
    'Test integration of data with etherscan module.'
    initialize_test_database()
    assert data.get_balance(data.SAFE) == 0
    data.scan_for_deposits()
    assert data.get_balance(data.SAFE) == (
        -1 * sum([deposit['amount'] for deposit in DEPOSITS]) // data.WEI_DEPOSIT_FOR_ONE_ROLLER)
    data.withdraw(ADDRESSES[0], 5)
    data.withdraw(ADDRESSES[1], 5)
    assert data.get_unsettled_withdrawals() == {
        3: dict(address=ADDRESSES[0], amount=5), 4: dict(address=ADDRESSES[1], amount=5)}
    assert data.settle(PAYMENT_TRANSACTION) == dict(settled_transactions_count=2, unsettled_transaction_count=0)
    assert data.get_unsettled_withdrawals() == {}

    # Make sure we hit the same block.
    data.scan_for_deposits()
    data.scan_for_deposits()

    # Make sure we hit old data.
    with data.sql_connection() as sql:
        sql.execute('UPDATE deposit_scans SET end_block = %s', (DEPOSIT_BLOCK_RANGE[0],))
    with pytest.raises(data.ScanError):
        data.scan_for_deposits()


def fake_transaction_hash():
    'Create a fake transaction hash.'
    return f"fake-{uuid.uuid4().hex}{uuid.uuid4().hex}"[:64]


def test_data():
    'Test data access.'
    initialize_test_database()

    # Check error handling.
    with pytest.raises(data.pymysql.MySQLError):
        with data.sql_connection() as sql:
            sql.execute('bad sql')

    # Test deposits.
    assert data.get_balance(ADDRESSES[0]) == 0
    data.debug_deposit(ADDRESSES[0], 1, fake_transaction_hash())
    assert data.get_balance(ADDRESSES[0]) == 1
    data.debug_deposit(ADDRESSES[0], 10, fake_transaction_hash())
    assert data.get_balance(ADDRESSES[0]) == 11

    # Test transfers.
    assert data.get_balance(ADDRESSES[1]) == 0
    data.transfer(ADDRESSES[0], ADDRESSES[1], 2)
    assert data.get_balance(ADDRESSES[0]) == 9
    assert data.get_balance(ADDRESSES[1]) == 2
    with pytest.raises(data.InsufficientFunds):
        data.transfer(ADDRESSES[0], ADDRESSES[1], 10)

    # Test withdraw.
    with pytest.raises(data.InsufficientFunds):
        data.withdraw(ADDRESSES[0], 100)
    assert data.get_unsettled_withdrawals() == {}
    data.withdraw(ADDRESSES[0], 3)
    assert data.get_balance(ADDRESSES[0]) == 6
    assert data.get_unsettled_withdrawals() == {
        4: dict(address=ADDRESSES[0], amount=3)}
    LOGGER.info(data.get_unsettled_withdrawals_aggregated_csv())
    assert data.get_unsettled_withdrawals_aggregated_csv() == (
        f"0x{ADDRESSES[0]}, {data.roller_to_eth(3)}")
    data.withdraw(ADDRESSES[0], 4)
    assert data.get_balance(ADDRESSES[0]) == 2
    assert data.get_unsettled_withdrawals() == {
        4: dict(address=ADDRESSES[0], amount=3),
        5: dict(address=ADDRESSES[0], amount=4)}
    assert data.get_unsettled_withdrawals_aggregated_csv() == (
        f"0x{ADDRESSES[0]}, {data.roller_to_eth(7)}")
    data.withdraw(ADDRESSES[1], 2)
    assert data.get_balance(ADDRESSES[1]) == 0
    assert data.get_unsettled_withdrawals() == {
        4: dict(address=ADDRESSES[0], amount=3),
        5: dict(address=ADDRESSES[0], amount=4),
        6: dict(address=ADDRESSES[1], amount=2)}
    assert data.get_unsettled_withdrawals_aggregated_csv() == (
        f"0x{ADDRESSES[0]}, {data.roller_to_eth(7)}\n"
        f"0x{ADDRESSES[1]}, {data.roller_to_eth(2)}")

    # False settle.
    assert data.settle(PAYMENT_TRANSACTION) == dict(settled_transactions_count=0, unsettled_transaction_count=3)
    assert data.get_unsettled_withdrawals() == {
        4: dict(address=ADDRESSES[0], amount=3),
        5: dict(address=ADDRESSES[0], amount=4),
        6: dict(address=ADDRESSES[1], amount=2)}

    # Add a true settle.
    # assert data.settle() == dict(settled_transactions_count=3, unsettled_transaction_count=0)
    # assert data.get_unsettled_withdrawals() == {}
    # assert data.get_unsettled_withdrawals_aggregated_csv() == ''


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


def test_webserver_flow():
    'To test the full flow we run a debug webserver.'
    initialize_test_database()
    web.DEBUG = True
    with web.APP.test_client() as client:
        prices_repsonse = client.get('/get_prices')
        assert prices_repsonse.status == '200 OK'
        assert prices_repsonse.json == dict(
            status=200, safe=data.SAFE,
            wei_deposit_for_one_roller=data.WEI_DEPOSIT_FOR_ONE_ROLLER,
            wei_withdraw_for_one_roller=data.WEI_WITHDRAW_FOR_ONE_ROLLER)

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

        assert client.post('/settle', data=dict(transaction_hash=PAYMENT_TRANSACTION)).status == '201 CREATED'
        LOGGER.info(client.get('/get_unsettled_withdrawals').json['unsettled_withdrawals'])
        assert client.get('/get_unsettled_withdrawals').json['unsettled_withdrawals'] == ''


def test_etherscan():
    'Test etherscan module.'
    assert etherscan.get_latest_block_number() > 0
    assert etherscan.get_deposits(SAFE, *DEPOSIT_BLOCK_RANGE) == DEPOSITS
    LOGGER.info(etherscan.get_payments(PAYMENTS_ADDRESS, PAYMENT_TRANSACTION))
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

    monkeypatch = _pytest.monkeypatch.MonkeyPatch()
    monkeypatch.setattr(etherscan, 'call', lambda *args, **kwargs: 'not a hex string')
    with pytest.raises(etherscan.EtherscanError):
        etherscan.get_latest_block_number()


def test_logs():
    'Just for coverage.'
    web.logs.setup(suppress_loggers=['foo'])
