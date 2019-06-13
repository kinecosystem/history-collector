import pytest
import boto3
import botocore
from unittest.mock import patch
from python.adapters.s3_storage_adapter import S3StorageAdapter, COMPLETED_LEDGERS_DIR_NAME
from datetime import datetime


@pytest.fixture('session')
def s3_storage_adapter_instance(test_bucket, test_prefix, aws_access_key_id, aws_secret_access_key, test_region):

    return S3StorageAdapter(test_bucket, test_prefix, aws_access_key_id, aws_secret_access_key, test_region)


def test_constructor_with_wrong_credentials(test_bucket, test_prefix):
    from botocore.errorfactory import ClientError

    try:
        S3StorageAdapter(test_bucket, test_prefix, 'foo', 'goo')
    except ClientError:
        assert True
    else:
        assert False


def test_constructor_with_access(s3_storage_adapter_instance):
    # On failure - make sure to run build_s3_storage.py on the relevant bucket and that your aws access have permissions
    assert True


def test_get_last_file_sequence(s3_storage_adapter_instance: S3StorageAdapter):
    last_file_sequence_name = 'test'
    __put_file_on_s3(s3_storage_adapter_instance, s3_storage_adapter_instance.last_file_location, last_file_sequence_name)

    assert s3_storage_adapter_instance.get_last_file_sequence() == last_file_sequence_name


def test_get_last_file_sequence_file_not_found(test_bucket, aws_access_key_id, aws_secret_access_key, test_region):

    try:
        S3StorageAdapter(test_bucket, 'key_that_should_not_exist', aws_access_key_id, aws_secret_access_key, test_region)
    except Exception as e:
        assert 'NoSuchKey' in str(e)
    else:
        assert False


def test_save_payments(s3_storage_adapter_instance: S3StorageAdapter):
    s3_storage_adapter_instance.operations_to_save = []
    test_list = ['test']
    s3_storage_adapter_instance._save_payments(test_list)
    assert s3_storage_adapter_instance.operations_to_save == test_list


def test_save_payments_empty(s3_storage_adapter_instance: S3StorageAdapter):
    s3_storage_adapter_instance.operations_to_save = []
    test_list = []
    s3_storage_adapter_instance._save_payments(test_list)
    assert s3_storage_adapter_instance.operations_to_save == test_list
    pass


def test_save_creations(s3_storage_adapter_instance: S3StorageAdapter):
    s3_storage_adapter_instance.operations_to_save = []
    test_list = ['test']
    s3_storage_adapter_instance._save_creations(test_list)
    assert s3_storage_adapter_instance.operations_to_save == test_list


def test_save_creations_empty(s3_storage_adapter_instance: S3StorageAdapter):
    s3_storage_adapter_instance.operations_to_save = []
    test_list = []
    s3_storage_adapter_instance._save_creations(test_list)
    assert s3_storage_adapter_instance.operations_to_save == test_list
    pass


def test_save(s3_storage_adapter_instance : S3StorageAdapter):
    # Test Setup
    s3_storage_adapter_instance.operations_to_save = []
    ledger_name = 'test_commit'
    s3_storage_adapter_instance.file_name = ledger_name
    s3_storage_adapter_instance._rollback()

    # Test
    s3_storage_adapter_instance.save([{'type': 'payment'}], [{'type': 'creation'}], ledger_name)

    assert ledger_name == s3_storage_adapter_instance.get_last_file_sequence()
    list_of_files = __get_files_in_key(s3_storage_adapter_instance,
                                       '{}{}'.format(s3_storage_adapter_instance.ledgers_prefix, ledger_name))
    assert len(list_of_files) == 1
    # Making sure COMPLETE_INDICATION exists
    completion_file = __get_files_in_key(s3_storage_adapter_instance,
                                         '{}{}'.format(s3_storage_adapter_instance.completion_indication_path,
                                                       ledger_name))
    assert len(completion_file) == 1

    # Test Cleanup
    s3_storage_adapter_instance.operations_to_save = []
    s3_storage_adapter_instance._rollback()


def test_save_empty_file(s3_storage_adapter_instance : S3StorageAdapter):
    """
    When file is empty, only the flag for completion should be created. We don't want empty ledger file
    :param s3_storage_adapter_instance:
    :return:
    """
    # Test Setup
    s3_storage_adapter_instance.operations_to_save = []
    ledger_name = 'test_commit'
    s3_storage_adapter_instance.file_name = ledger_name
    s3_storage_adapter_instance._rollback()

    # Test
    s3_storage_adapter_instance.save([], [], ledger_name)

    assert ledger_name == s3_storage_adapter_instance.get_last_file_sequence()
    list_of_files = __get_files_in_key(s3_storage_adapter_instance,
                                       '{}{}'.format(s3_storage_adapter_instance.ledgers_prefix, ledger_name))
    assert len(list_of_files) == 0

    # Making sure COMPLETE_INDICATION exists
    completion_file = __get_files_in_key(s3_storage_adapter_instance,
                                         '{}{}'.format(s3_storage_adapter_instance.completion_indication_path,
                                                       ledger_name))
    assert len(completion_file) == 1

    # Test Cleanup
    s3_storage_adapter_instance.operations_to_save = []
    s3_storage_adapter_instance._rollback()


def test_rollback(s3_storage_adapter_instance: S3StorageAdapter):
    # Test Setup
    s3_storage_adapter_instance.operations_to_save = []
    ledger_name = 'test_commit'
    s3_storage_adapter_instance.file_name = ledger_name
    ledger_key_location_on_s3 = '{}{}/{}'.format(s3_storage_adapter_instance.ledgers_prefix, ledger_name, 'test.csv')
    # Putting another manual file in the designated directory, to be sure that there is a file that should be deleted
    __put_file_on_s3(s3_storage_adapter_instance, ledger_key_location_on_s3, 'test')

    # Test
    assert len(__get_files_in_key(s3_storage_adapter_instance, ledger_key_location_on_s3)) == 1

    with patch('botocore.client.BaseClient._make_api_call', new=__mock_make_api_call_fail_when_posting_complete):
        with pytest.raises(Exception):
            s3_storage_adapter_instance.save([{'type': 'payment'}], [{'type': 'creation'}], ledger_name)

    # At this point a roll back should be invoked, so we need to make sure there are no files in the folders
    assert len(__get_files_in_key(s3_storage_adapter_instance, ledger_key_location_on_s3)) == 0

    # Test Cleanup
    s3_storage_adapter_instance.operations_to_save = []


def test_convert_payment(s3_storage_adapter_instance : S3StorageAdapter):

    returned_dict = s3_storage_adapter_instance.convert_payment({'timestamp': 1535594286})
    assert returned_dict == {'timestamp': datetime.strptime('2018-08-30 01:58:06', '%Y-%m-%d %H:%M:%S'), 'type': 'payment'}


def test_convert_creations(s3_storage_adapter_instance : S3StorageAdapter):

    returned_dict = s3_storage_adapter_instance.convert_payment({'timestamp': 1535594286})
    assert returned_dict == {'timestamp': datetime.strptime('2018-08-30 01:58:06', '%Y-%m-%d %H:%M:%S'), 'type': 'payment'}


def __put_file_on_s3(s3_storage_adapter_instance: S3StorageAdapter, key, string):
    client = boto3.client('s3', aws_access_key_id=s3_storage_adapter_instance.aws_access_key,
                          aws_secret_access_key=s3_storage_adapter_instance.aws_secret_key,
                          region_name=s3_storage_adapter_instance.aws_region)
    client.put_object(Body=string, Bucket=s3_storage_adapter_instance.bucket,
                      Key=key)


def __get_files_in_key(s3_storage_adapter_instance: S3StorageAdapter, key):
    client = boto3.client('s3', aws_access_key_id=s3_storage_adapter_instance.aws_access_key,
                          aws_secret_access_key=s3_storage_adapter_instance.aws_secret_key,
                          region_name=s3_storage_adapter_instance.aws_region)
    return client.list_objects_v2(Bucket=s3_storage_adapter_instance.bucket, Prefix=key).get('Contents', [])


# Using a reference to the function, otherwise there will be a loop because we call again to _make_api_call
reference_function = botocore.client.BaseClient._make_api_call


def __mock_make_api_call_fail_when_posting_complete(self, operation_name, kwarg):
    # The addition of the complete ledgers dir name, is because other fucntions using PutObject operation name
    if operation_name == 'PutObject' and COMPLETED_LEDGERS_DIR_NAME in kwarg['Key']:
        raise Exception('test')

    return reference_function(self, operation_name, kwarg)
