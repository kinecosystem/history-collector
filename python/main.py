"""
ETL for stellar history files.

Script to download xdr files from an s3 bucket,
unpack them, filter the transactions in them,
and write the relevant transactions to a database.
"""
#!/usr/bin/python
# -*- coding: utf-8 -*-

import argparse
import os
import time
import logging
import re
import sys
from functools import partial
from multiprocessing.pool import ThreadPool

import boto3
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
import psycopg2
from psycopg2.extras import execute_values
from xdrparser import parser as xdrparser

logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')

argparser = argparse.ArgumentParser(description='history-collector - Stellar history importer')
argparser.add_argument('--host', default=os.getenv('POSTGRES_HOST', 'localhost'), help='host')
argparser.add_argument('--password', default=os.getenv('POSTGRES_PASSWORD', 'postgres'), help='password')
argparser.add_argument('--db', default=os.getenv('DB_NAME', 'kin'), help='database')
argparser.add_argument('--user', default=os.getenv('DB_USER', 'python'), help='database user')
argparser.add_argument('--userpass', default=os.getenv('DB_USER_PASSWORD', '1234'), help='user password')
argparser.add_argument('--issuer', default=os.getenv('KIN_ISSUER',
                                                     'GDF42M3IPERQCBLWFEZKQRK77JQ65SCKTU3CW36HZVCX7XX5A5QXZIVK'),
                       help='KIN issuer address')
argparser.add_argument('--passphrase', default=os.getenv('NETWORK_PASSPHRASE',
                                                         'Public Global Kin Ecosystem Network ; June 2018'),
                       help='Stellar network passphrase')
argparser.add_argument('--bucket', default=os.getenv('BUCKET_NAME', 'stellar-core-ecosystem-6145'), help='s3 bucket name')
argparser.add_argument('--app', default=os.getenv('APP_ID'), help='database')
argparser.add_argument('--retries', default=os.getenv('MAX_RETRIES', 5), help='max retries')
argparser.add_argument('--dir', default=os.getenv('CORE_DIRECTORY', ''), help='working directory')
args = argparser.parse_args()

POSTGRES_HOST = args.host
POSTGRES_PASSWORD = args.password
DB_NAME = args.db
DB_USER = args.user
DB_USER_PASSWORD = args.userpass
KIN_ISSUER = args.issuer
NETWORK_PASSPHRASE = args.passphrase
BUCKET_NAME = args.bucket
APP_ID = args.app
MAX_RETRIES = int(args.retries)
CORE_DIRECTORY = args.dir

# Add trailing / to core directory
CORE_DIRECTORY = os.path.join(CORE_DIRECTORY, '')

# 1-<uppercase|lowercase|digits>*4-anything
APP_ID_REGEX = re.compile('^1-[A-z0-9]{4}-.*')

if APP_ID is not None and re.match('^[A-z0-9]{4}$', APP_ID) is None:
    logging.error('APP ID is invalid')
    sys.exit(1)

def setup_s3():
    """Set up the s3 client with anonymous connection."""
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    logging.info('Successfully initialized S3 client')
    return s3


def setup_postgres():
    """Set up a connection to the postgres database using the user 'python'."""
    conn_string = "host='{}' dbname='{}' user='{}' password='{}'".format(POSTGRES_HOST, DB_NAME, DB_USER, DB_USER_PASSWORD)
    conn = psycopg2.connect(conn_string)
    logging.info('Successfully connected to the database')
    return conn


def get_last_file_sequence(conn, cur):
    """Get the sequence of the last file scanned."""
    cur.execute('SELECT * FROM lastfile;')
    return cur.fetchone()[0]


def download_file(s3, file_name):
    """Download the files from the s3 bucket."""
    # File transactions-004c93bf.xdr.gz will be in:
    # BUCKET_NAME/CORE_DIRECTORY/transactions/00/4c/93/

    # "ledger-004c93bf" > "00/4c/93/"
    file_number = file_name.split('-')[-1]
    sub_directory = '/'.join(file_number[i:i + 2] for i in range(0, len(file_number), 2))
    sub_directory = sub_directory[:9]
    sub_directory = file_name.split('-')[0] + '/' + sub_directory

    for attempt in range(MAX_RETRIES + 1):
        try:
            logging.info('Trying to download file {}.xdr.gz'.format(file_name))
            s3.download_file(BUCKET_NAME, CORE_DIRECTORY + sub_directory + file_name + '.xdr.gz', file_name + '.xdr.gz')
            logging.info('File {} downloaded'.format(file_name))
            break
        except ClientError as e:

            # If you failed to get the file more than MAX_RETRIES times: raise the exception
            if attempt == MAX_RETRIES:
                logging.error('Reached retry limit when downloading file {}, quitting.'.format(file_name))
                raise

            # If I get a 404, it might mean that the file does not exist yet, so I will try again in 3 minutes
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logging.warning('404, could not get file {}, retrying in 3 minutes'.format(file_name))
                time.sleep(180)


def get_ledgers_dictionary(ledgers):
    """Get a dictionary of a ledgerSequence and closing time."""
    return {ledger['header']['ledgerSeq']: ledger['header'] for ledger in ledgers}


def get_result_dictionary(results):
    """Get a dictionary of a transaction hash and its result"""
    results_dict = {}
    for result in results:
        for tx_result in result['txResultSet']['results']:
            results_dict[tx_result['transactionHash']] = tx_result['result']

    return results_dict


def get_app_id(tx_memo):
    """Get app_id from transaction memo"""
    if APP_ID_REGEX.match(str(tx_memo)):
        return tx_memo.split('-')[1]
    return None


def is_asset_kin(asset):
    return asset is not None \
           and asset['type'] == 1 \
           and asset['alphaNum4']['assetCode'] == 'KIN' \
           and asset['alphaNum4']['issuer']['ed25519'] == KIN_ISSUER


def write_to_postgres(conn, cur, history_transactions, ledgers_dictionary, results_dictionary, file_name):
    """Filter payment/creation operations and write them to the database."""
    logging.info('Writing contents of file: {} to database'.format(file_name))

    # aggregator
    payments = []

    for transaction_history_entry in history_transactions:
        ledger = ledgers_dictionary.get(transaction_history_entry['ledgerSeq'])
        timestamp = ledger['scpValue']['closeTime']

        for transaction in transaction_history_entry['txSet']['txs']:
            tx_hash = transaction['hash']

            # Find the results of this tx based on its hash
            results = results_dictionary.get(tx_hash)
            
            # Handle only successful history_transactions
            tx_status = results['result']['code']  # txSUCCESS/FAILED/BAD_AUTH etc
            if tx_status != 'txSUCCESS':
                logging.warning('skipping failed transaction {}'.format(tx_hash))
                continue

            tx_memo = transaction['tx']['memo']['text']

            # If the transaction is not from our app, skip it
            if APP_ID and get_app_id(tx_memo) != APP_ID:
                continue

            tx_account = transaction['tx']['sourceAccount']['ed25519']
            tx_account_sequence = transaction['tx']['seqNum']
            tx_ledger_sequence = transaction_history_entry['ledgerSeq']

            for op_index, (tx_operation, result_operation) in enumerate(zip(transaction['tx']['operations'], results['result']['results'])):
                # Handle only payments
                if tx_operation['body']['type'] != 1:
                    continue

                # Handle only KIN payments
                if not is_asset_kin(tx_operation['body']['paymentOp']['asset']):
                    continue

                source = transaction['tx']['sourceAccount']['ed25519']
                destination = tx_operation['body']['paymentOp']['destination']['ed25519']
                amount = tx_operation['body']['paymentOp']['amount']

                # Override the tx source with the operation source if it exists
                if len(tx_operation['sourceAccount']) > 0 and 'ed25519' in tx_operation['sourceAccount'][0]:
                    source = tx_operation['sourceAccount'][0]['ed25519']

                # append to our aggregator
                payments.append((tx_hash,
                                 tx_account,
                                 tx_account_sequence,
                                 source, destination,
                                 amount,
                                 tx_memo,
                                 op_index,
                                 tx_ledger_sequence,
                                 timestamp))

        # Insert aggregated values
        execute_values(cur, 'INSERT INTO payments (tx_hash, account, account_sequence, source, destination, '
                            'amount, memo_text, op_index, ledger_sequence, date) VALUES %s ON CONFLICT DO NOTHING',
                       payments, '(%s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))')

    # Update the 'lastfile' entry in the database
    cur.execute("UPDATE lastfile SET name = %s", (file_name,))

    conn.commit()
    logging.info('Successfully imported file {} to database: {} records written'.format(file_name, len(payments)))


def get_new_file_sequence(old_file_name):
    """
    Return the name of the next file to scan.

    Transaction files are stored with an ascending hexadecimal name, for example:
    └── transactions
    └── 00
        └── 72
            ├── 6a
            │   ├── transactions-00726a3f.xdr.gz
            │   ├── transactions-00726a7f.xdr.gz
            │   ├── transactions-00726abf.xdr.gz
            │   └── transactions-00726aff.xdr.gz

    So get the sequence of the last file scanned > convert to decimal > add 64 > convert back to hex >
    remove the '0x' prefix > and add '0' until the file name is 8 letters long
    """
    new_file_name = int(old_file_name, 16)
    new_file_name = new_file_name + 64
    new_file_name = hex(new_file_name)
    new_file_name = new_file_name.replace('0x', '')
    new_file_name = '0' * (8 - len(new_file_name)) + new_file_name

    return new_file_name


def main():
    """Main entry point."""
    # Initialize everything
    conn = setup_postgres()
    cur = conn.cursor()
    file_sequence = get_last_file_sequence(conn, cur)
    s3 = setup_s3()

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

        # Write the data to the postgres database
        write_to_postgres(conn, cur, transactions, ledgers_dictionary, results_dictionary, file_sequence)

        # Remove the files from storage
        logging.info('Removing downloaded files.')
        os.remove(ledger_file)
        os.remove(transaction_file)
        os.remove(results_file)

        # Get the name of the next file I should work on
        file_sequence = get_new_file_sequence(file_sequence)


if __name__ == '__main__':
    main()
