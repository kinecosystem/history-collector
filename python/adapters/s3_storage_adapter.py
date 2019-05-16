import boto3
import logging
import time
import io
import pandas
from python.adapters.hc_storage_adapter import HistoryCollectorStorageAdapter

LAST_FILE_NAME = 'last_file'
COMPLETE_INDICATION = '__COMPLETED__'
MAX_RETRIES = 3


class S3StorageAdapter(HistoryCollectorStorageAdapter):

    def __init__(self, bucket, key_prefix='kin_history_collector/', aws_access_key=None, aws_secret_key=None,
                 region='us-east-1'):
        super().__init__()
        self.bucket = bucket
        self.key_prefix = key_prefix
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key
        self.aws_region = region
        self.last_file_location = '{}{}'.format(key_prefix, LAST_FILE_NAME)
        self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key,
                                      region_name=region)

    def get_last_file_sequence(self):
        """Get the sequence of the last file scanned. Using the last file object"""
        last_file_object = self.s3_client.get_object(Bucket=self.bucket, Key=self.last_file_location)
        last_file_seq = last_file_object['Body'].read().decode('utf-8')
        return last_file_seq

    def __save_payments(self, payments: list):
        self.__save_to_s3(payments, 'payment')

    def __save_creations(self, creations: list):
        self.__save_to_s3(creations, 'creation')

    def __commit(self):
        """
        Mark the ledger directory with a completed flag (using empty file) and also update the last file
        """
        self.s3_client.put_object(Body='', Bucket=self.bucket,
                                  Key='{}{}/{}'.format(self.key_prefix, self.file_name, COMPLETE_INDICATION))
        self.s3_client.put_object(Body=self.file_name, Bucket=self.bucket, Key=self.last_file_location)

    def __rollback(self):
        """
        Deletes all objects created for the specific ledger
        """

        try:
            is_all_deleted = False
            while not is_all_deleted:
                # list_objects_v2 and delete_objects can handle up to 1000 objects at a time
                res = self.s3_client.list_objects_v2(Bucket=self.bucket,
                                                     Prefix='{}{}/'.format(self.key_prefix, self.file_name))
                if res['ResponseMetadata']['HTTPStatusCode'] != 200:
                    raise RuntimeError(
                        'Could not complete rollback for ledger {}. S3 list_objects response error.'.format(
                            self.file_name))

                s3_objects = [s3_object['Key'] for s3_object in res.get('Contents')]
                self.__delete_objects(s3_objects)

                # IsTruncated is True when there were more objects in the directory than list_objects_v2 could fetch
                is_all_deleted = not res['IsTruncated']

        except Exception:
            logging.error('Error while rollback ledger {}. Failed clearing the whole ledger history'.format(
                self.file_name))
            raise

        logging.info('Rollback finished successfully')

    def __save_to_s3(self, data, data_type):
        """
        Gets the data and the type of it, and stores it in the right partition and hierarchy on S3.
        :param data: list of dictionaries, each dictionary is a records to be saved in csv format
        :param data_type: Helps partitioning the data. type could be payment/creation
        :return:
        """

        # Converting the data into a dataframe and loading converting it to csv with no header or index
        pd_dataframe = pandas.DataFrame(data)
        bytes_stream_csv = io.BytesIO(pd_dataframe.to_csv(header=False, index=False).encode('utf-8'))

        # Uploading the stream to S3 to the right hierarchy and right partition
        self.s3_client.upload_fileobj(bytes_stream_csv, self.bucket,
                                      '{prefix}/ledger={ledger}/type={data_type}/{ledger}_{data_type}.csv'.format(
                                          prefix=self.key_prefix, ledger=self.file_name, data_type=data_type))

    def __delete_objects(self, list_of_objects):
        if list_of_objects:

            retry_count = 0
            is_deleted = False
            while not is_deleted:
                try:
                    res = self.s3_client.delete_objects(Bucket='dev-kin-history-collector',
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
