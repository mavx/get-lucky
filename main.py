import datetime as dt
import logging
import os
import sqlite3

import requests

import keys
from util.logger import log_setup

ENDPOINT = 'https://blockchain.info/balance'
SLACK_WEBHOOK = 'https://hooks.slack.com/services/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'


def main():
    # Initialize address generator
    k = keys.Keys()
    conn = sqlite3.connect('address.db')
    batch_size = 60
    n = 1

    while 1:
        logger.info('\nAttempt #{0} (Checked: {1})'.format(n, batch_size * n))
        try:
            address_search(k, conn, batch_size)
        except Exception as e:
            logger.info(e)
        n += 1


def address_search(key_generator_instance, db_connection, batch_size):
    logger.info('Generating {} keys in a batch...'.format(batch_size))
    address_dict = generate_address_dict(key_generator_instance, batch_size)
    balances = get_balances(address_dict.keys())

    logger.info('Checking queried addresses:')
    for address in balances:
        if balances[address] > 0:
            logger.info('Found something:', address)
            save_address(
                connection=db_connection,
                public_key=address,
                private_key=address_dict[address],
                balance=balances[address]
            )
            send_address_details(
                public_key=address,
                private_key=address_dict[address_dict],
                balance=balances[address]
            )


def generate_address_dict(key_generator_instance, batch_size):
    address_dict = {}
    for _ in range(batch_size):
        keypair = key_generator_instance.generate()
        address_dict[keypair['public_key']] = keypair['private_key']

    return address_dict


def get_balances(address_list):
    logger.info('Querying addresses for balance...')
    r = requests.get(ENDPOINT, params={'active': '|'.join(address_list)})
    balances = {}
    if r.ok:
        j = r.json()
        balances = {address: j[address]['final_balance'] for address in j}
        logger.info('Retrieved {} results.'.format(len(balances)))
        return balances
    else:
        logger.info(r)
        logger.info(r.content)
        return balances


def save_address(connection, public_key, private_key, balance):
    c = connection.cursor()
    insert_sql = """
        INSERT INTO address
        VALUES (?, ?, ?, ?)
    """
    c.execute(
        insert_sql,
        (str(dt.datetime.utcnow()), public_key, private_key, balance)
    )
    connection.commit()
    c.close()


def send_address_details(public_key, private_key, balance):
    message = (
        "Public Key: {0}\n"
        "Private Key: {1}\n"
        "Balance: {2}"
    ).format(public_key, private_key, balance)
    payload = {
        'attachments': [{
            'fallback': message,
            'fields': [{
                'title': 'BTC Address',
                'value': message
            }]
        }]
    }
    return requests.post(SLACK_WEBHOOK, json=payload)


if __name__ == '__main__':
    # Global variables
    LOGGER_LEVEL = logging.INFO  # TODO: Set logging level | Options: 'INFO', 'DEBUG', 'ERROR', etc.

    PARENT_DIR = os.path.dirname(os.path.abspath(__file__))
    SCRIPT_NAME = os.path.basename(__file__)[:-3]
    LOG_FILENAME = SCRIPT_NAME
    logger = log_setup(PARENT_DIR, LOG_FILENAME, LOGGER_LEVEL)

    main()

