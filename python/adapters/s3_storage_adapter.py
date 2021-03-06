import boto3
import logging
import time
import io
import pandas
from datetime import datetime
from adapters.hc_storage_adapter import HistoryCollectorStorageAdapter, HistoryCollectorStorageError

LAST_FILE_NAME = 'last_file'
HC_ROOT_FOLDER = 'kin_history_collector/'
COMPLETED_LEDGERS_DIR_NAME = 'completed_ledgers'
DEFAULT_REGION = 'us-east-1'
MAX_RETRIES = 3


class S3StorageAdapter(HistoryCollectorStorageAdapter):

    def __init__(self, bucket, key_prefix, aws_access_key=None, aws_secret_key=None,
                 region='us-east-1', test_connection=True):
        super().__init__()
        self.bucket = bucket
        self.full_key_prefix = key_prefix + HC_ROOT_FOLDER
        self.aws_access_key = aws_access_key if aws_access_key != '' else None
        self.aws_secret_key = aws_secret_key if aws_secret_key != '' else None
        self.aws_region = region if region != '' else DEFAULT_REGION
        self.last_file_location = '{}{}'.format(self.full_key_prefix, LAST_FILE_NAME)
        self.completion_indication_path ='{}{}/'.format(self.full_key_prefix, COMPLETED_LEDGERS_DIR_NAME)
        self.ledgers_prefix = '{}{}ledger='.format(self.full_key_prefix, 'ledgers/')
        self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                      region_name=region)
        self.__init_operations_to_save()

        if test_connection:
            self.__test_connection()

        logging.info('Successfully connected to the storage')

    def get_last_file_sequence(self):
        """Get the sequence of the last file scanned. Using the last file object"""

        try:
            last_file_object = self.s3_client.get_object(Bucket=self.bucket, Key=self.last_file_location)
            last_file_seq = last_file_object['Body'].read().decode('utf-8')

        except Exception as e:
            if 'NoSuchKey' in str(e):
                last_file_seq = None
            else:
                logging.error('Error while getting the last file sequence {}'.format(e))
                raise

        if not last_file_seq:
            raise HistoryCollectorStorageError('Could not obtain last file from S3')

        return last_file_seq

    def _save_payments(self, payments: list):
        # Preparing
        self.operations_to_save += payments

    def _save_creations(self, creations: list):
        self.operations_to_save += creations

    def _commit(self):
        """
        Mark the ledger directory with a completed flag (using empty file) and also update the last file
        """

        # Saving all operations of ledger
        self.__save_to_s3()

        # Marking completion of ledger
        self.s3_client.put_object(Body='', Bucket=self.bucket,
                                  Key='{}{}'.format(self.completion_indication_path, self.file_name))
        self.s3_client.put_object(Body=self.file_name, Bucket=self.bucket, Key=self.last_file_location)

        # Empty operations to save
        self.__init_operations_to_save()

    def _rollback(self):
        """
        Deletes all objects created for the specific ledger
        """

        try:
            self.__init_operations_to_save()
            is_all_deleted = False
            while not is_all_deleted:
                # list_objects_v2 and delete_objects can handle up to 1000 objects at a time
                res = self.s3_client.list_objects_v2(Bucket=self.bucket,
                                                     Prefix='{}{}/'.format(self.ledgers_prefix, self.file_name))
                if res['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise RuntimeError(
                        'Could not complete rollback for ledger {}. S3 list_objects response error.'.format(
                            self.file_name))
                if res.get('Contents'):
                    s3_objects = [s3_object.get('Key') for s3_object in res.get('Contents')]
                    self.__delete_objects(s3_objects)

                # IsTruncated is True when there were more objects in the directory than list_objects_v2 could fetch
                is_all_deleted = not res['IsTruncated']

            self.__delete_objects(['{}{}'.format(self.completion_indication_path, self.file_name)])

        except Exception:
            logging.error('Error while rollback ledger {}. Failed clearing the whole ledger history'.format(
                self.file_name))
            raise

    def convert_payment(self, source, destination, amount, memo, tx_fee, tx_charged_fee, op_index, tx_status, op_status,
                        tx_hash, timestamp):
        # Converting timestamp from int to utc time
        payment = dict.fromkeys(self.payments_output_schema())
        payment['source'] = source
        payment['destination'] = destination
        payment['amount'] = amount
        payment['memo'] = memo
        payment['tx_fee'] = tx_fee
        payment['tx_charged_fee'] = tx_charged_fee
        payment['op_index'] = op_index
        payment['tx_status'] = tx_status
        payment['op_status'] = op_status
        payment['tx_hash'] = tx_hash
        payment['timestamp'] = datetime.utcfromtimestamp(timestamp)
        payment['type'] = 'payment'
        return payment

    def convert_creation(self, source, destination, balance, memo, tx_fee, tx_charged_fee, op_index, tx_status,
                         op_status, tx_hash, timestamp):
        # Converting timestamp from int to utc time
        creation = dict.fromkeys(self.creations_output_schema())
        creation['source'] = source
        creation['destination'] = destination
        creation['starting_balance'] = balance
        creation['memo'] = memo
        creation['tx_fee'] = tx_fee
        creation['tx_charged_fee'] = tx_charged_fee
        creation['op_index'] = op_index
        creation['tx_status'] = tx_status
        creation['op_status'] = op_status
        creation['tx_hash'] = tx_hash
        creation['timestamp'] = datetime.utcfromtimestamp(timestamp)
        creation['type'] = 'creation'
        return creation

    @staticmethod
    def payments_output_schema():
        """
        :return: A dictionary of columns saved by the History collector. Key - name, Value - type
        """

        schema = HistoryCollectorStorageAdapter.payments_output_schema()
        schema.update({'type': str})
        return schema

    @staticmethod
    def creations_output_schema():
        """
        :return: A dictionary of columns saved by the History collector. Key - name, Value - type
        """

        schema = HistoryCollectorStorageAdapter.creations_output_schema()
        schema.update({'type': str})
        return schema

    def __init_operations_to_save(self):
        self.operations_to_save = []

    def __test_connection(self):
        """
        Uses the given credential and simulates all the different actions, to verify permissions for them.
        Raises error if one of the actions fails.
        :return:
        """
        try:
            # Trying to read last_file
            self.get_last_file_sequence()
            # Trying to write 'test' ledger, then delete it. Not using the 'save' method, as it will rewrite last_file
            self.file_name = 'test'
            self._save_creations([{'source': 'GCQTAWULBNFLBAEQLEN6FDGGCPYTVZ3Y55AB4F7HSTMQKNX3HZINMQJM',
                                   'destination': 'GDDFYG3OSTSHADS7SP6TZ4XM62EQ522CI7UYJSNAETGJJCGOX66TP5Q5',
                                   'starting_balance': 10.0, 'memo': None, 'tx_fee': 100, 'tx_charged_fee': 100,
                                   'op_index': 0, 'tx_status': 'txFAILED', 'op_status': 'CREATE_ACCOUNT_LOW_RESERVE',
                                   'tx_hash': 'a17aa64d4f0ae434dceb16501dd1d2217a59e42d555e24fdf7e17fffa13a1331',
                                   'timestamp': datetime(2018, 6, 20, 12, 47, 21)}])
            self.__save_to_s3()
            self._rollback()
            self.file_name = None
        except Exception:
            logging.error('Failed while trying to verify permissions on S3 storage bucket. '
                          'Requires Read/Write/Delete permissions')
            raise

    def __save_to_s3(self):
        """
        Stores all the ledger's operations in the right partition and hierarchy on S3.
        If data is empty, we don't save empty file
        :return:
        """

        # Skipping saving empty files.
        if not self.operations_to_save:
            return

        # Converting the data into a dataframe. The columns will be sorted alphabetically
        # TODO: Use a different method to stream csv dataframe other than pandas and remove pandas from pipfile and
        #  Dockerfile, as it really pumps the size of the docker image (from 150MB to 1GB)
        pd_dataframe = pandas.DataFrame(self.operations_to_save)

        # Arranging columns order according the schema if there's data and converting it to csv with no header or index
        try:
            pd_dataframe = pd_dataframe[self.payments_output_schema().keys()]
        except KeyError:
            pass

        bytes_stream_csv = io.BytesIO(pd_dataframe.to_csv(header=False, index=False).encode('utf-8'))

        # Uploading the stream to S3 to the right hierarchy and right partition
        self.s3_client.upload_fileobj(bytes_stream_csv, self.bucket,
                                      '{prefix}{ledger}/{ledger}.csv'.format(
                                          prefix=self.ledgers_prefix, ledger=self.file_name))

    def __delete_objects(self, list_of_objects):
        if list_of_objects:

            retry_count = 0
            is_deleted = False
            while not is_deleted:
                try:
                    res = self.s3_client.delete_objects(Bucket=self.bucket,
                                                        Delete={'Objects': [{'Key': key_name} for key_name in list_of_objects],
                                                                'Quiet': True})

                    # Making sure there were no errors on deletion
                    if res['ResponseMetadata']['HTTPStatusCode'] != 200:
                        raise RuntimeError(
                            'Could not complete rollback. S3 delete_objects response error.')
                    elif res.get('Errors'):
                        raise Exception('Error deleting {} objects: {}'.format(len(res.get('Errors')),
                                                                               res.get('Errors')))
                    else:
                        # Deletion completed successfully
                        is_deleted = True

                except Exception as e:
                    if not retry_count < MAX_RETRIES:
                        raise

                    logging.error('Error while trying to delete objects from storage: {}.\n Retry'.format(e))
                    time.sleep(10)
                    retry_count += 1
