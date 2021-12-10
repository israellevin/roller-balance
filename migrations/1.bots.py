'Adding bot data.'
import logging

import accounting
import db

LOGGER = logging.getLogger('donilla.db.game_data')


def apply():
    'Insert base data which is always true.'
    with db.sql_connection() as sql:
        sql.executemany("INSERT INTO bots(bot_address, player_address, busy) VALUES(%s, %s, %s)", zip(
            accounting.BOTS, len(accounting.BOTS) * [accounting.SAFE], len(accounting.BOTS) * [0]))
        for bot in accounting.BOTS:
            accounting.deposit_in_session(bot, accounting.BOT_INITIAL_FUND, 'bot fund transaction', sql)
