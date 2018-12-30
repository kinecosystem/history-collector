#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
History collector/importer for Kin history files.

Script to download XDR files from an S3 bucket, unpack them, filter the transactions
and write the relevant ones to a database.
"""


import argparse
import os
import logging
import re
import sys
from functools import partial
from multiprocessing.pool import ThreadPool
from time import sleep

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
import psycopg2
from psycopg2.extras import execute_values

from xdrparser import parser as xdrparser
from utils import OperationType, verify_file_sequence, get_new_file_sequence, get_s3_bucket_subdir


argparser = argparse.ArgumentParser(description='history-collector - Stellar history parser/importer',
                                    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
argparser.add_argument('--host', default=os.getenv('POSTGRES_HOST', 'localhost'), help='postgres host')
argparser.add_argument('--superuser', default=os.getenv('POSTGRES_USER', 'postgres'), help='postgres superuser')
argparser.add_argument('--superpass', default=os.getenv('POSTGRES_PASSWORD', 'postgres'), help='postgres superuser password')
argparser.add_argument('--db', default=os.getenv('POSTGRES_DB', 'kin'), help='default database')
argparser.add_argument('--user', default=os.getenv('DB_USER', 'python'), help='database user')
argparser.add_argument('--password', default=os.getenv('DB_USER_PASSWORD', '1234'), help='user password')
argparser.add_argument('--issuer', default=os.getenv('KIN_ISSUER',
                                                     'GDF42M3IPERQCBLWFEZKQRK77JQ65SCKTU3CW36HZVCX7XX5A5QXZIVK'),
                       help='KIN issuer address')
argparser.add_argument('--passphrase', default=os.getenv('NETWORK_PASSPHRASE',
                                                         'Public Global Kin Ecosystem Network ; June 2018'),
                       help='Stellar network passphrase')
argparser.add_argument('--bucket', default=os.getenv('BUCKET_NAME', 'stellar-core-ecosystem-6145'), help='s3 bucket name')
argparser.add_argument('--first', default=os.getenv('FIRST_FILE'), help='ledger file to scan from')
argparser.add_argument('--app', default=os.getenv('APP_ID'), help='application id to filter by')
argparser.add_argument('--retries', type=int, default=os.getenv('MAX_RETRIES', 5), help='how many times to retry downloading')
argparser.add_argument('--coredir', default=os.getenv('CORE_DIRECTORY', ''), help='core root directory on s3')
argparser.add_argument('--loglevel', default=os.getenv('LOG_LEVEL', 'INFO'), help='app log level (ERROR/WARNING/INFO/DEBUG)')
args = argparser.parse_args()

POSTGRES_HOST = args.host
POSTGRES_USER = args.superuser
POSTGRES_PASSWORD = args.superpass
DB_NAME = args.db
DB_USER = args.user
DB_USER_PASSWORD = args.password
KIN_ISSUER = args.issuer
NETWORK_PASSPHRASE = args.passphrase
BUCKET_NAME = args.bucket
APP_ID = args.app
CORE_DIRECTORY = args.coredir
MAX_RETRIES = args.retries
RETRY_DELAY = 180  # TODO: check if fair and can be a const
FIRST_FILE = args.first
LOG_LEVEL = args.loglevel

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s | %(levelname)s | %(message)s')


if FIRST_FILE and not verify_file_sequence(FIRST_FILE):
    logging.error('Invalid first file')
    sys.exit(1)

# Add trailing / to core directory if needed
CORE_DIRECTORY = os.path.join(CORE_DIRECTORY, '')

# 1-<uppercase|lowercase|digits>*4-anything
APP_ID_REGEX = re.compile('^1-[A-z0-9]{4}-.*')

if APP_ID and not re.match('^[A-z0-9]{4}$', APP_ID):
    logging.error('APP ID is invalid')
    sys.exit(1)


def setup_s3():
    """Set up the s3 client with anonymous connection."""
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    logging.info('Successfully initialized S3 client')
    return s3


def setup_db_connection():
    """Set up a connection to the postgres database."""
    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(POSTGRES_HOST, DB_NAME, DB_USER, DB_USER_PASSWORD)
    conn = psycopg2.connect(conn_string)
    logging.info('Successfully connected to the database {} on host {}'.format(DB_NAME, POSTGRES_HOST))
    return conn


def get_last_file_sequence(cur):
    """Get the sequence of the last file scanned."""
    cur.execute('SELECT file_sequence FROM last_state;')
    return cur.fetchone()[0]


def download_file(s3, file_name):
    """Download the files from the s3 bucket."""

    subdir = get_s3_bucket_subdir(file_name)

    for attempt in range(MAX_RETRIES + 1):
        try:
            logging.debug('Trying to download file {}.xdr.gz'.format(file_name))
            s3.download_file(BUCKET_NAME, CORE_DIRECTORY + subdir + file_name + '.xdr.gz', file_name + '.xdr.gz')
            logging.debug('File {} downloaded'.format(file_name))
            break
        except ClientError as e:
            if attempt == MAX_RETRIES:
                logging.error('Reached retry limit when downloading file {}, quitting.'.format(file_name))
                raise

            # If I get a 404, it might mean that the file does not exist yet, so I will try again after a delay
            # TODO: this is how new files are ingested, have to make it more explicit.
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logging.warning('404, file not found {}, retrying in {} seconds'.format(file_name, RETRY_DELAY))
                sleep(RETRY_DELAY)


def get_ledgers_dictionary(ledgers):
    """Get a dictionary of ledgers."""
    return {ledger['header']['ledgerSeq']: ledger['header'] for ledger in ledgers}


def get_result_dictionary(results):
    """Get a dictionary of transaction results."""
    results_dict = {}
    for result in results:
        for tx_result in result['txResultSet']['results']:
            results_dict[tx_result['transactionHash']] = tx_result['result']
    return results_dict


def get_app_id(tx_memo):
    """Get app_id from transaction memo."""
    if APP_ID_REGEX.match(str(tx_memo)):
        return tx_memo.split('-')[1]
    return None


def is_asset_kin(asset):
    return asset is not None \
           and asset['type'] == 1 \
           and asset['alphaNum4']['assetCode'] == 'KIN' \
           and asset['alphaNum4']['issuer']['ed25519'] == KIN_ISSUER


def filter_data(history_transactions, ledgers_dictionary, results_dictionary):
    """Filter transactions and extract only relevant data."""
    aggregator = []

    for transaction_history_entry in history_transactions:
        ledger_sequence = transaction_history_entry['ledgerSeq']
        ledger = ledgers_dictionary.get(ledger_sequence)
        ledger_timestamp = ledger['scpValue']['closeTime']

        for tx_order, transaction in enumerate(transaction_history_entry['txSet']['txs']):
            tx_hash = transaction['hash']
            tx_memo = transaction['tx']['memo']['text']

            # If app filter is defined and the transaction is not from our app, skip it
            if APP_ID and get_app_id(tx_memo) != APP_ID:
                continue

            tx_result = results_dictionary.get(tx_hash)
            tx_status = tx_result['result']['code']
            tx_account = transaction['tx']['sourceAccount']['ed25519']
            tx_account_sequence = transaction['tx']['seqNum']

            for op_order, (tx_operation, op_result) in \
                    enumerate(zip(transaction['tx']['operations'], tx_result['result']['results'])):

                # Override the tx source with the operation source if available
                if len(tx_operation['sourceAccount']) > 0 and 'ed25519' in tx_operation['sourceAccount'][0]:
                    source = tx_operation['sourceAccount'][0]['ed25519']
                else:
                    source = tx_account

                op_type = OperationType(tx_operation['body']['type'])

                if op_type == OperationType.PAYMENT:
                    # Handle only KIN payments
                    op_asset = tx_operation['body']['paymentOp']['asset']
                    if not is_asset_kin(op_asset):
                        continue
                    op_status = op_result['tr']['paymentResult']['code']
                    amount = tx_operation['body']['paymentOp']['amount']
                    destination = tx_operation['body']['paymentOp']['destination']['ed25519']
                elif op_type == OperationType.CREATE_ACCOUNT:
                    op_status = op_result['tr']['createAccountResult']['code']
                    amount = tx_operation['body']['createAccountOp']['startingBalance']
                    destination = tx_operation['body']['createAccountOp']['destination']['ed25519']
                elif op_type == OperationType.CHANGE_TRUST:
                    op_asset = tx_operation['body']['changeTrustOp']['line']
                    if not is_asset_kin(op_asset):
                        continue
                    op_status = op_result['tr']['changeTrustResult']['code']
                    amount = 0
                    destination = source
                elif op_type == OperationType.ACCOUNT_MERGE:
                    op_status = op_result['tr']['accountMergeResult']['code']
                    if op_status == 'ACCOUNT_MERGE_SUCCESS':
                        amount = op_result['tr']['accountMergeResult']['sourceAccountBalance']
                    else:
                        amount = 0
                    destination = tx_operation['body']['destination']
                    continue  # TODO: remove when Kin is a base currency!
                else:
                    continue

                is_signed_by_app = False  # TODO

                # Append to our aggregator
                aggregator.append((ledger_sequence,
                                   tx_hash, tx_order, tx_status,
                                   tx_account, tx_account_sequence,
                                   op_order, str(op_type), op_status,
                                   source, destination, amount,
                                   tx_memo, is_signed_by_app, ledger_timestamp))
    return aggregator


def save_data(cur, file_sequence, data):
    """Write data to database."""
    logging.debug('Writing contents of file {} to database'.format(file_sequence))

    # Insert aggregated values
    execute_values(cur, 'INSERT INTO transactions (ledger_sequence, '
                        'tx_hash, tx_order, tx_status, '
                        'account, account_sequence, '
                        'operation_order, operation_type, operation_status, '
                        'source, destination, amount, '
                        'memo_text, is_signed_by_app, timestamp) VALUES %s ON CONFLICT DO NOTHING',
                   data, '(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))')

    # Update the last state
    cur.execute("UPDATE last_state SET file_sequence = %s", (file_sequence,))

    cur.connection.commit()
    logging.info('Saved file {}. Filtered operations: {}'.format(file_sequence, len(data)))


def main():
    """Main entry point."""
    # Initialize everything
    conn = setup_db_connection()
    cur = conn.cursor()
    s3 = setup_s3()
    file_sequence = FIRST_FILE if FIRST_FILE else get_last_file_sequence(cur)

    # Receive/Process/Store loop
    while True:
        # Download the files from S3
        files = ['ledger-' + file_sequence, 'transactions-' + file_sequence, 'results-' + file_sequence]
        pool = ThreadPool(3)
        pool.map_async(partial(download_file, s3), files)
        pool.close()
        try:
            pool.join()
        except KeyboardInterrupt:
            logging.info('Stopped by user, exiting')
            exit(0)

        results_file = 'results-{}.xdr.gz'.format(file_sequence)
        ledger_file = 'ledger-{}.xdr.gz'.format(file_sequence)
        transaction_file = 'transactions-{}.xdr.gz'.format(file_sequence)

        # Unpack the files
        results = xdrparser.parse(results_file)
        ledgers = xdrparser.parse(ledger_file)
        transactions = xdrparser.parse(transaction_file, with_hash=True, network_id=NETWORK_PASSPHRASE)

        # Build dictionaries
        ledgers_dictionary = get_ledgers_dictionary(ledgers)
        results_dictionary = get_result_dictionary(results)

        # Filter data
        filtered_data = filter_data(transactions, ledgers_dictionary, results_dictionary)

        # Store data
        save_data(cur, file_sequence, filtered_data)

        # Remove the files from storage
        logging.debug('Removing downloaded files.')
        os.remove(ledger_file)
        os.remove(transaction_file)
        os.remove(results_file)

        # Get the name of the next file I should work on
        file_sequence = get_new_file_sequence(file_sequence)


if __name__ == '__main__':
    main()
