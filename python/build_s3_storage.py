"""Script to build a storage infrastructure for main.py to write data to."""

import os
import sys
import logging
import boto3
from adapters.s3_storage_adapter import S3StorageAdapter, LAST_FILE_NAME, DEFAULT_REGION, HC_ROOT_FOLDER
from adapters.hc_storage_adapter import HistoryCollectorStorageError

# Get constants from env variables
FIRST_FILE = os.environ['FIRST_FILE']
S3_STORAGE_AWS_ACCESS_KEY = os.environ['S3_STORAGE_AWS_ACCESS_KEY']
S3_STORAGE_AWS_SECRET_KEY = os.environ['S3_STORAGE_AWS_SECRET_KEY']
S3_STORAGE_BUCKET = os.environ['S3_STORAGE_BUCKET']
S3_STORAGE_KEY_PREFIX = os.environ['S3_STORAGE_KEY_PREFIX']
S3_STORAGE_REGION = os.environ.get('S3_STORAGE_REGION', 'us-east-1')


def verify_file_sequence():
    """Verifies that the file sequence is valid"""
    file_sequence = int(FIRST_FILE, 16) + 1
    return file_sequence % 64


def main():
    """Main entry point."""
    logging.basicConfig(level='INFO', format='%(asctime)s | %(levelname)s | %(message)s')
    if not S3_STORAGE_BUCKET:
        logging.info('S3 is not being used as a storage for this run, skipping this script')
        sys.exit(0)

    # Check if the database already exists
    try:
        storage_adapter = S3StorageAdapter(S3_STORAGE_BUCKET, S3_STORAGE_KEY_PREFIX, S3_STORAGE_AWS_ACCESS_KEY,
                                           S3_STORAGE_AWS_SECRET_KEY, S3_STORAGE_REGION)

        logging.info('Using existing S3 storage')
        sys.exit(0)

    except HistoryCollectorStorageError:

        if verify_file_sequence() != 0:
            logging.error('First file selected is invalid')
            sys.exit(1)

        setup_s3_storage()


def setup_s3_storage():

    # Connect to the S3 client
    try:
        aws_access_key = S3_STORAGE_AWS_ACCESS_KEY if S3_STORAGE_AWS_ACCESS_KEY != '' else None
        aws_secret_key = S3_STORAGE_AWS_SECRET_KEY if S3_STORAGE_AWS_SECRET_KEY != '' else None
        aws_region = S3_STORAGE_REGION if S3_STORAGE_REGION != '' else DEFAULT_REGION

        s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                 region_name=aws_region)

        # Creating last_file object and saving the first file into it
        full_key_prefix = S3_STORAGE_KEY_PREFIX + HC_ROOT_FOLDER
        last_file_location = '{}{}'.format(full_key_prefix, LAST_FILE_NAME)
        s3_client.put_object(Body=FIRST_FILE, Bucket=S3_STORAGE_BUCKET, Key=last_file_location)

        logging.debug('Storage created successfully.')

    except Exception:
        logging.error('Could not fully create storage, please delete all data before retrying.')
        raise


if __name__ == '__main__':
    main()
