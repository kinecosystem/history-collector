"""
ETL for stellar history files.

Script to download xdr files from an s3 bucket,
unpack them, filter the transactions in them,
and write the relevant transactions to a database.
"""

import os
import time
import logging
import re
import sys
import traceback
import smtplib
import ssl
import boto3
import psycopg2
import json
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
from xdrparser import parser
from adapters import *
from utils import *

# Get constants from env variables
FIRST_FILE = os.environ['FIRST_FILE']
PYTHON_PASSWORD = os.environ['PYTHON_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
S3_STORAGE_AWS_ACCESS_KEY = os.environ['S3_STORAGE_AWS_ACCESS_KEY']
S3_STORAGE_AWS_SECRET_KEY = os.environ['S3_STORAGE_AWS_SECRET_KEY']
S3_STORAGE_BUCKET = os.environ['S3_STORAGE_BUCKET']
S3_STORAGE_KEY_PREFIX = os.environ['S3_STORAGE_KEY_PREFIX']
S3_STORAGE_REGION = os.environ['S3_STORAGE_REGION']
NETWORK_PASSPHARSE = os.environ['NETWORK_PASSPHRASE']
MAX_RETRIES = int(os.environ['MAX_RETRIES'])
BUCKET_NAME = os.environ['BUCKET_NAME']  # TODO: Support list of buckets, so if one suffers a delay, use a different one
LOG_LEVEL = os.environ['LOG_LEVEL']

APP_ID = os.environ.get('APP_ID', None)
CORE_DIRECTORY = os.environ.get('CORE_DIRECTORY', '')

EMAIL_SMTP = os.environ.get('EMAIL_SMTP')
EMAIL_ACCOUNT = os.environ.get('EMAIL_ACCOUNT')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
EMAIL_RECIPIENTS = os.environ.get('EMAIL_RECIPIENTS')

LAMBDA_NAME = os.environ.get('LAMBDA_NAME')
LAMBDA_REGION = os.environ.get('LAMBDA_REGION', 'us-east-1')

RETRY_DELAY = os.environ.get('RETRY_DELAY', 180)


if FIRST_FILE and not verify_file_sequence(FIRST_FILE):
    logging.error('Invalid first file')
    sys.exit(1)

# Add trailing / to core directory
if CORE_DIRECTORY != '' and CORE_DIRECTORY[-1] != '/':
    CORE_DIRECTORY += '/'

# 1-<uppercase|lowercase|digits>*3,4-anything
APP_ID_REGEX = re.compile('^1-[A-z0-9]{3,4}-.*')
SSL_PORT = 465


def setup_s3():
    """Set up the s3 client with anonymous connection."""
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    logging.debug('Successfully initialized S3 client')
    return s3


def setup_postgres():
    """Set up a connection to the postgres database using the user 'python'."""
    conn = psycopg2.connect("postgresql://python:{}@{}:5432/kin".format(PYTHON_PASSWORD, POSTGRES_HOST))
    logging.debug('Successfully connected to the database')
    return conn


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
                logging.error('Reached retry limit when downloading file {}, raising exception.'.format(file_name))
                raise

            # If got a 404, it might mean that the file does not exist yet, so I will try again after a delay
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logging.warning('404, file not found {}, retrying in {} seconds'.format(file_name, RETRY_DELAY))
                time.sleep(RETRY_DELAY)


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


def write_data(storage_adapter, transactions, ledgers_dictionary, results_dictionary, file_name):
    """Filter payment/creation operations and write them to the storage."""
    logging.debug('Writing contents of file: {} to storage'.format(file_name))

    operations_list = []

    for transaction_history_entry in transactions:
        ledger_sequence = transaction_history_entry['ledgerSeq']  # TODO: do we want to change it to BigInt?
        ledger = ledgers_dictionary.get(ledger_sequence)
        ledger_timestamp = ledger['scpValue']['closeTime']

        for tx_order, transaction in enumerate(transaction_history_entry['txSet']['txs']):
            # Find the results of this tx based on its hash
            tx_hash = transaction['hash']
            tx_result = results_dictionary.get(tx_hash)
            tx_memo = transaction['tx']['memo']['text']

            # If app filter is defined and the transaction is not from our app, skip it
            if APP_ID and get_app_id(tx_memo) != APP_ID:
                continue

            tx_account = transaction['tx']['sourceAccount']['ed25519']
            tx_account_sequence = transaction['tx']['seqNum']
            tx_fee = transaction['tx']['fee']
            tx_charged_fee = tx_result['feeCharged']
            tx_status = tx_result['result']['code']  # txSUCCESS/FAILED/BAD_AUTH etc

            for op_order, (tx_operation, op_result) in enumerate(zip(transaction['tx']['operations'],
                                                                     tx_result['result'].get('results', []))):

                operation_obj = get_operation_object(tx_operation, op_result)
                if not operation_obj:
                    logging.warning('Unhandled operations ({type})- {operation}'.format(
                        type=OperationType(tx_operation['body']['type']), operation=tx_operation))
                    continue

                # If no operation source available, use the tx source
                source = operation_obj.get_source()
                if not source:
                    source = tx_account

                is_signed_by_app = None  # TODO: next version

                operations_list.append(storage_adapter.convert_operation(
                    source, operation_obj.get_destination(), operation_obj.get_amount(), tx_order, tx_memo, tx_account,
                    tx_account_sequence, tx_fee, tx_charged_fee, tx_status, tx_hash, op_order,
                    operation_obj.get_status(), operation_obj.get_type(), ledger_timestamp, is_signed_by_app, file_name,
                    ledger_sequence)
                )

    # Try saving data into storage as a single 'transaction'
    storage_adapter.save(operations_list, file_name)


def main():
    """Main entry point."""
    # Initialize everything
    logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s | %(levelname)s | %(message)s')
    if APP_ID is not None:
        if re.match('^[A-z0-9]{4}$', APP_ID) is None:
            logging.error('APP ID is invalid')
            sys.exit(1)

    # Validating email alert if necessary
    if EMAIL_SMTP:
        __email_validation()

    storage_adapter = get_storage_adapter()

    file_sequence = storage_adapter.get_last_file_sequence()
    if file_sequence != FIRST_FILE:
        # If restarted, getting next file in sequence as the last one was ingested
        file_sequence = get_new_file_sequence(file_sequence)
    s3 = setup_s3()

    consecutive_failed_attempts = 0

    while True:

        try:
            # Download the files from S3
            download_file(s3, 'ledger-' + file_sequence)
            download_file(s3, 'transactions-' + file_sequence)
            download_file(s3, 'results-' + file_sequence)

            # Unpack the files
            results = parser.parse('results-{}.xdr.gz'.format(file_sequence))
            ledgers = parser.parse('ledger-{}.xdr.gz'.format(file_sequence))
            transactions = parser.parse('transactions-{}.xdr.gz'.format(file_sequence),
                                        with_hash=True, network_id=NETWORK_PASSPHARSE, raw_amount=True)

            # Get a ledger:closeTime dictionary
            ledgers_dictionary = get_ledgers_dictionary(ledgers)
            # Get a txHash:txResult dictionary
            results_dictionary = get_result_dictionary(results)

            # Remove the files from storage
            logging.debug('Removing downloaded files.')
            os.remove('ledger-{}.xdr.gz'.format(file_sequence))
            os.remove('transactions-{}.xdr.gz'.format(file_sequence))
            os.remove('results-{}.xdr.gz'.format(file_sequence))

            # Write the data to storage
            write_data(storage_adapter, transactions, ledgers_dictionary, results_dictionary, file_sequence)

            # Get the name of the next file I should work on
            file_sequence = get_new_file_sequence(file_sequence)
            consecutive_failed_attempts = 0

        except ClientError:
            # Avoiding failing the process, only sending notification on 1st occurrence
            if consecutive_failed_attempts == 0:
                # Sending notification only if there is a new delay
                send_notification("Reached retry limit when downloading the next ledger: {}\n".format(file_sequence) +
                                  "There might be a delay in the blockchain archiving bucket.")
                consecutive_failed_attempts += 1
            continue

        except Exception:
            # Retries until we exceed the max retries. Logging the exception either way

            logging.warning('Exception occurred')
            logging.warning(traceback.format_exc())

            if consecutive_failed_attempts > MAX_RETRIES:

                logging.error('Reached retry limit. Quitting.')
                send_notification(traceback.format_exc())
                raise

            logging.info('Retrying in {} seconds'.format(RETRY_DELAY))
            time.sleep(RETRY_DELAY)

            # Refresh the storage adapter just in case the exception was related to it
            storage_adapter = get_storage_adapter()
            consecutive_failed_attempts += 1


def send_email_alert(error_msg):

    context = ssl.create_default_context()
    recipients = EMAIL_RECIPIENTS if isinstance(EMAIL_RECIPIENTS, list) else __convert_recipients_to_list(EMAIL_RECIPIENTS)
    with smtplib.SMTP_SSL(EMAIL_SMTP, SSL_PORT, context=context) as server:

        server.login(user=EMAIL_ACCOUNT, password=EMAIL_PASSWORD)

        # Preparing mail message
        body = "Exception occurred while trying to parse blockchain history from {bucket} S3 bucket:\n\n" \
               "{error_msg}".format(bucket=BUCKET_NAME, error_msg=error_msg)

        message = 'From: {sender}\nTo: {recipients}\nSubject: {subject}\n\n{body}'.\
            format(sender=EMAIL_ACCOUNT, recipients=', '.join(recipients),
                   subject='Alert! History Collector exception', body=body)

        server.sendmail(EMAIL_ACCOUNT, recipients, message)


def __convert_recipients_to_list(recipients_str):
    # Removing from the strings any character that might be included in a string representation of a python list
    remove_chars_trans = str.maketrans('', '', '\'"[] ')
    return recipients_str.translate(remove_chars_trans).split(',')


def invoke_lambda(args):

    lambda_client = boto3.client('lambda', LAMBDA_REGION)
    lambda_client.invoke(FunctionName=LAMBDA_NAME, Payload=json.dumps(args))


def __email_validation():
    logging.debug('SMTP host given, authenticates login to the SMTP server')
    if not (EMAIL_ACCOUNT and EMAIL_PASSWORD and EMAIL_RECIPIENTS):
        logging.error('Missing at least one of the EMAIL environment variables')
        sys.exit(1)

    context = ssl.create_default_context()

    try:
        with smtplib.SMTP_SSL(EMAIL_SMTP, SSL_PORT, context=context) as server:
            server.login(user=EMAIL_ACCOUNT, password=EMAIL_PASSWORD)
    except smtplib.SMTPAuthenticationError:
        logging.error('Could not login to the SMTP server with the given email account and password')
        sys.exit(1)


def send_notification(notification_message):
    if EMAIL_SMTP:
        send_email_alert(notification_message)
        logging.error('Error occurred, alert email sent')

    if LAMBDA_NAME:
        invoke_lambda({"message": notification_message})
        logging.error('Error occurred, lambda invoked')


def get_storage_adapter():
    """
    This function generates an instance of the right storage adapter according the docker-compose file.
    It will also raise exception if configuration for both/none of the storage adapters were supplied.
    Only one supported.
    :return: An instance of the relevant storage adapter
    """
    # Validating supplied configuration
    if POSTGRES_HOST and S3_STORAGE_BUCKET:
        raise ValueError('Only one storage method is supported')

    storage_adapter = None

    if S3_STORAGE_BUCKET:
        storage_adapter = S3StorageAdapter(S3_STORAGE_BUCKET, S3_STORAGE_KEY_PREFIX,
                                           S3_STORAGE_AWS_ACCESS_KEY, S3_STORAGE_AWS_SECRET_KEY, S3_STORAGE_REGION)
    elif POSTGRES_HOST:
        storage_adapter = PostgresStorageAdapter(POSTGRES_HOST, PYTHON_PASSWORD)
    else:
        raise Exception('No storage method supplied')

    return storage_adapter


if __name__ == '__main__':
    main()
