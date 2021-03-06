'Roller Balance Web server.'
import functools
import os
import traceback

import flasgger
import flask
import flask_cors

import accounting
import api_spec
import logs

logs.setup()
LOGGER = logs.logging.getLogger('roller.web')
DEBUG = accounting.DEBUG


class ArgumentMismatch(Exception):
    'Wrong arguments supplied to call.'


class Unauthorized(Exception):
    'An unauthorized request.'


APP = flask.Flask('roller')
APP.config['SECRET_KEY'] = os.environ.get('ROLLER_SESSIONS_KEY', os.urandom(24))
APP.config['SWAGGER'] = api_spec.CONFIG
flasgger.Swagger(APP)
flask_cors.CORS(APP, resources={'*': {'origins': '*'}})


def make_response(status=None, error_name=None, error_message=None, **kwargs):
    'Make a dict for a basic server response.'
    if error_name is not None:
        if isinstance(error_name, Exception):
            kwargs['error_name'] = type(error_name).__name__
            kwargs['error_message'] = str(error_name)
        else:
            kwargs['error_name'] = error_name
    if error_message is not None:
        kwargs['error_message'] = error_message
    kwargs['status'] = status or 200
    return flask.jsonify(dict(kwargs)), kwargs['status']


def check_arguments(required_arguments, given_arguments):
    'Raise exception if request arguments do not match requirements.'
    if required_arguments is None:
        required_arguments = set()
    elif not isinstance(required_arguments, set):
        required_arguments = set(required_arguments)
    if not isinstance(given_arguments, set):
        given_arguments = set(given_arguments)
    missing_arguments = required_arguments - given_arguments
    if missing_arguments:
        raise ArgumentMismatch(f"request does not contain arguments(s): {', '.join(missing_arguments)}")
    extra_arguments = given_arguments - required_arguments
    if extra_arguments:
        raise ArgumentMismatch(f"request contain unexpected arguments(s): {', '.join(extra_arguments)}")


def parse_argument(key, value):
    'Parse a single argument in a request.'
    if key == 'amount':
        try:
            # Convert to string first, so that floats fail.
            value = int(str(value))
        except ValueError:
            raise ArgumentMismatch(f"argument {key} has to be an integer") from None
        if value <= 0:
            raise ArgumentMismatch(f"argument {key} must be larger than zero") from None
    elif key in ['address', 'source', 'target', 'transaction_hash']:
        required_length = 64 if key == 'transaction_hash' else 40
        if len(value) != required_length:
            raise ArgumentMismatch(f"argument {key} must be {required_length} characters long") from None
        value = value.lower()
        try:
            int(value, 16)
        except ValueError:
            raise ArgumentMismatch(f"argument {key} is not a hex string") from None
    return value


def parse_request(request, required_arguments):
    'Validate and parse a request.'
    given_arguments = request.values.to_dict()
    check_arguments(required_arguments, given_arguments.keys())
    return {key: parse_argument(key, value) for key, value in given_arguments.items()}


def optional_arg_decorator(decorator):
    'A decorator for decorators than can accept optional arguments.'
    @functools.wraps(decorator)
    def wrapped_decorator(*args, **kwargs):
        'A wrapper to return a filled up function in case arguments are given.'
        if len(args) == 1 and not kwargs and callable(args[0]):
            return decorator(args[0])
        return lambda decoratee: decorator(decoratee, *args, **kwargs)
    return wrapped_decorator


@optional_arg_decorator
# Since this is a decorator the handler argument will never be None, it is
# defined as such only to comply with python's syntactic sugar.
def call(handler=None, required_arguments=None):
    'A decorator for API calls.'
    @functools.wraps(handler)
    def _call(*_, **__):
        request = None
        # If anything fails, we want to catch it here.
        # pylint: disable=broad-except
        try:
            request = parse_request(flask.request, required_arguments)
            response = handler(**request)
        except (ArgumentMismatch, accounting.InsufficientFunds, accounting.SettleError) as exception:
            response = dict(status=400, error_name=exception)
        except Unauthorized as exception:
            response = dict(status=403, error_name=exception)
        except Exception as exception:
            LOGGER.exception(f"unexpected server exception on {flask.request.url}: {request}")
            response = dict(status=500, error_name=exception, stacktrace=traceback.format_exc().split('\n'))
        # pylint: enable=broad-except
        try:
            return make_response(**(response))
        except TypeError:
            return make_response(500, 'InternalError', f"handler {handler.__name__} returned an unparsable response")
    return _call


@APP.route("/get_prices", methods=['GET'])
@flasgger.swag_from(api_spec.GET_PRICES)
@call()
def get_prices_handler():
    'Get current prices and safe address.'
    return dict(
        status=200, safe=accounting.SAFE,
        wei_deposit_for_one_roller=accounting.WEI_DEPOSIT_FOR_ONE_ROLLER,
        wei_withdraw_for_one_roller=accounting.WEI_WITHDRAW_FOR_ONE_ROLLER)


@APP.route("/get_balance", methods=['POST'])
@flasgger.swag_from(api_spec.GET_BALANCE)
@call(['address'])
def get_balance_handler(address):
    'Get the balance of an address.'
    return dict(status=200, balance=accounting.get_balance(address))


@APP.route("/transfer", methods=['POST'])
@flasgger.swag_from(api_spec.TRANSFER)
@call(['source', 'target', 'amount'])
def transfer_handler(source, target, amount):
    'Transfer amount from source to target.'
    accounting.transfer(source, target, amount)
    return dict(status=201)


@APP.route("/withdraw", methods=['POST'])
@flasgger.swag_from(api_spec.WITHDRAW)
@call(['address', 'amount'])
def withdraw_handler(address, amount):
    'Withdraw amount from system.'
    accounting.withdraw(address, amount)
    return dict(status=201)


@APP.route("/get_unsettled_withdrawals", methods=['GET'])
@flasgger.swag_from(api_spec.GET_UNSETTLED_WITHDRAWALS)
@call
def get_unsettled_withdrawals_handler():
    'Get a CSV list of unsettled withdrawals.'
    return dict(status=200, unsettled_withdrawals="\n".join([
        f"0x{address}, {accounting.roller_to_eth(sum([withdrawal['amount'] for withdrawal in withdrawals]))}"
        for address, withdrawals in accounting.get_unsettled_withdrawals().items()]))


@APP.route("/settle", methods=['POST'])
@flasgger.swag_from(api_spec.SETTLE)
@call(['transaction_hash'])
def settle_handler(transaction_hash):
    'Settle transactions that were paid by ethereum transaction_hash.'
    return dict(status=201, **accounting.settle(transaction_hash))


@APP.route("/deposit", methods=['POST'])
@flasgger.swag_from(api_spec.DEPOSIT)
@call(['address', 'amount'])
def deposit_handler(address, amount):
    'Fake a deposit by an address.'
    if not DEBUG:
        raise Unauthorized('deposit endpoint is only available in debug mode')
    accounting.debug_deposit(address, amount, 'debug deposit')
    return dict(status=201)


@APP.route("/five_hundred", methods=['POST'])
@call(['reason'])
def five_hundred_handler(reason):
    'Test our 500 reporting - only for testing, but also available in production.'
    if reason == 'response':
        return None
    raise Exception('five hundred response was requested')


@APP.route('/')
@APP.route('/<path:path>', methods=['GET', 'POST'])
def catch_all_handler(path='index.html'):
    'All undefined endpoints try to serve from the static directories.'
    return make_response(403, Unauthorized(f"Forbidden path: {path}"))
