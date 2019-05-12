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
import json
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import ClientError
import psycopg2
from xdrparser import parser

# Get constants from env variables
FIRST_FILE = os.environ['FIRST_FILE']
PYTHON_PASSWORD = os.environ['PYTHON_PASSWORD']
POSTGRES_HOST = os.environ['POSTGRES_HOST']
KIN_ISSUER = os.environ['KIN_ISSUER']
NETWORK_PASSPHARSE = os.environ['NETWORK_PASSPHRASE']
MAX_RETRIES = int(os.environ['MAX_RETRIES'])
BUCKET_NAME = os.environ['BUCKET_NAME']
LOG_LEVEL = os.environ['LOG_LEVEL']

APP_ID = os.environ.get('APP_ID', None)
CORE_DIRECTORY = os.environ.get('CORE_DIRECTORY', '')

EMAIL_SMTP = os.environ.get('EMAIL_SMTP')
EMAIL_ACCOUNT = os.environ.get('EMAIL_ACCOUNT')
EMAIL_PASSWORD = os.environ.get('EMAIL_PASSWORD')
EMAIL_RECIPIENTS = os.environ.get('EMAIL_RECIPIENTS')

LAMBDA_NAME = os.environ.get('LAMBDA_NAME')
LAMBDA_REGION = os.environ.get('LAMBDA_REGION', 'us-east-1')


# Add trailing / to core directory
if CORE_DIRECTORY != '' and CORE_DIRECTORY[-1] != '/':
    CORE_DIRECTORY += '/'

# 1-<uppercase|lowercase|digits>*4-anything
APP_ID_REGEX = re.compile('^1-[A-z0-9]{4}-.*')
SSL_PORT = 465


def setup_s3():
    """Set up the s3 client with anonymous connection."""
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    logging.info('Successfully initialized S3 client')
    return s3


def setup_postgres():
    """Set up a connection to the postgres database using the user 'python'."""
    conn = psycopg2.connect("postgresql://python:{}@{}:5432/kin".format(PYTHON_PASSWORD, POSTGRES_HOST))
    logging.info('Successfully connected to the database')
    return conn


def get_last_file_sequence(conn, cur):
    """Get the sequence of the last file scanned."""
    cur.execute('select * from lastfile;')
    conn.commit()
    last_file = cur.fetchone()[0]

    return last_file


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
                logging.error('Reached retry limit when downloading file {}, raising exception.'.format(file_name))
                raise

            # If I get a 404, it might mean that the file does not exist yet, so I will try again in 3 minutes
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                logging.warning('404, could not get file {}, retrying in 3 minutes'.format(file_name))
                time.sleep(180)


def get_ledgers_dictionary(ledgers):
    """Get a dictionary of a ledgerSequence and closing time."""
    return {ledger['header']['ledgerSeq']: ledger['header']['scpValue']['closeTime'] for ledger in ledgers}


def get_result_dictionary(results):
    """Get a dictionary of a transaction hash and its result"""
    #return {result['txResultSet']['results'][0]['transactionHash']:
    #        result['txResultSet']['results'][0]['result'] for result in results}

    results_dict = {}
    for result in results:
        for tx_result in result['txResultSet']['results']:
            results_dict[tx_result['transactionHash']] = tx_result['result']

    return results_dict


def write_to_postgres(conn, cur, transactions, ledgers_dictionary, results_dictionary, file_name):
    """Filter payment/creation operations and write them to the database."""
    logging.info('Writing contents of file: {} to database'.format(file_name))
    for transaction_history_entry in transactions:
        timestamp = ledgers_dictionary.get(transaction_history_entry['ledgerSeq'])

        for transaction in transaction_history_entry['txSet']['txs']:
            # Find the results of this tx based on its hash
            results = results_dictionary.get(transaction['hash'])
            memo = transaction['tx']['memo']['text']

            # If the transaction is not from our app, skip it
            if APP_ID is not None:
                if APP_ID_REGEX.match(str(memo)) is not None:
                    app = memo.split('-')[1]
                    if app != APP_ID:
                        continue
                else:
                    continue

            tx_hash = transaction['hash']
            tx_fee = transaction['tx']['fee']
            tx_charged_fee = results['feeCharged']
            tx_status = results['result']['code']  # txSUCCESS/FAILED/BAD_AUTH etc

            for op_index, (tx_operation, result_operation) in enumerate(zip(transaction['tx']['operations'], results['result'].get('results', []))):

                op_status = None

                # Operation type 1 = Payment
                if tx_operation['body']['type'] == 1:
                    # Check if this is a payment for our asset
                    if tx_operation['body']['paymentOp']['asset']['alphaNum4'] is not None and \
                                    tx_operation['body']['paymentOp']['asset']['alphaNum4']['assetCode'] == 'KIN' and \
                                    tx_operation['body']['paymentOp']['asset']['alphaNum4']['issuer']['ed25519'] == KIN_ISSUER:

                        source = transaction['tx']['sourceAccount']['ed25519']
                        destination = tx_operation['body']['paymentOp']['destination']['ed25519']
                        amount = tx_operation['body']['paymentOp']['amount']
                        if result_operation:
                            op_status = result_operation['tr']['paymentResult']['code']

                        # Override the tx source with the operation source if it exists
                        try:
                            source = tx_operation['sourceAccount'][0]['ed25519']
                        except (KeyError, IndexError):
                            pass

                        cur.execute("INSERT INTO payments VALUES (%s ,%s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s))",
                                    (source,
                                     destination,
                                     amount,
                                     memo,
                                     tx_fee,
                                     tx_charged_fee,
                                     op_index,
                                     tx_status,
                                     op_status,
                                     tx_hash,
                                     timestamp))

                # Operation type 0 = Create account
                elif tx_operation['body']['type'] == 0:
                    source = transaction['tx']['sourceAccount']['ed25519']
                    destination = tx_operation['body']['createAccountOp']['destination']['ed25519']
                    balance = tx_operation['body']['createAccountOp']['startingBalance']
                    if result_operation:
                        op_status = result_operation['tr']['createAccountResult']['code']

                    # Override the tx source with the operation source if it exists
                    try:
                        source = tx_operation['sourceAccount'][0]['ed25519']
                    except (KeyError, IndexError):
                        pass

                    cur.execute("INSERT INTO creations VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, to_timestamp(%s));",
                                (source,
                                 destination,
                                 balance,
                                 memo,
                                 tx_fee,
                                 tx_charged_fee,
                                 op_index,
                                 tx_status,
                                 op_status,
                                 tx_hash,
                                 timestamp))

    # Update the 'lastfile' entry in the database
    cur.execute("UPDATE lastfile SET name = %s", (file_name,))
    conn.commit()
    logging.info('Successfully wrote contents of file: {} to database'.format(file_name))


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
    logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s | %(levelname)s | %(message)s')
    if APP_ID is not None:
        if re.match('^[A-z0-9]{4}$', APP_ID) is None:
            logging.error('APP ID is invalid')
            sys.exit(1)

    # Validating email alert if necessary
    if EMAIL_SMTP:
        __email_validation()

    conn = setup_postgres()
    cur = conn.cursor()
    file_sequence = get_last_file_sequence(conn, cur)
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
                                        with_hash=True, network_id=NETWORK_PASSPHARSE)

            # Get a ledger:closeTime dictionary
            ledgers_dictionary = get_ledgers_dictionary(ledgers)
            # Get a txHash:txResult dictionary
            results_dictionary = get_result_dictionary(results)

            # Remove the files from storage
            logging.info('Removing downloaded files.')
            os.remove('ledger-{}.xdr.gz'.format(file_sequence))
            os.remove('transactions-{}.xdr.gz'.format(file_sequence))
            os.remove('results-{}.xdr.gz'.format(file_sequence))

            # Write the data to the postgres database
            write_to_postgres(conn, cur, transactions, ledgers_dictionary, results_dictionary, file_sequence)

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

            logging.info('Retrying in 3 minutes')
            time.sleep(180)
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
    logging.info('SMTP host given, authenticates login to the SMTP server')
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

    if LAMBDA_REGION:
        invoke_lambda({"message": notification_message})
        logging.error('Error occurred, lambda invoked')


if __name__ == '__main__':
    main()
