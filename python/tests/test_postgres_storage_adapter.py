import pytest
import re
import random
import string
from adapters.postgres_storage_adapter import PostgresStorageAdapter
from datetime import datetime
from psycopg2 import IntegrityError


@pytest.fixture('session')
def postgres_storage_adapter_instance(postgres_host, postgres_password, postgres_database_name):

    return PostgresStorageAdapter(postgres_host, postgres_password, postgres_database_name)


def test_constructor_with_wrong_credentials(postgres_host, postgres_database_name):

    # TODO: replace - use with pytest.raises(Exception): instead
    try:
        PostgresStorageAdapter(postgres_host, 'foo', postgres_database_name)
    except Exception as e:
        assert True
    else:
        assert False


def test_constructor_with_access(postgres_storage_adapter_instance):
    # On failure - make sure to run build_s3_storage.py on the relevant bucket and that your aws access have permissions
    assert True


def test_get_last_file_sequence(postgres_storage_adapter_instance: PostgresStorageAdapter):

    assert re.match('^[a-f0-9]{8}$', postgres_storage_adapter_instance.get_last_file_sequence())


def test_save_payments(postgres_storage_adapter_instance: PostgresStorageAdapter):
    payments = list()

    payments.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.payments_output_schema()))
    payments.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.payments_output_schema()))

    number_of_rows_before_saving = __get_count_of_table(postgres_storage_adapter_instance, 'payments')

    postgres_storage_adapter_instance._save_payments(payments)
    number_of_rows_after_saving = __get_count_of_table(postgres_storage_adapter_instance, 'payments')

    assert number_of_rows_after_saving == number_of_rows_before_saving + len(payments)

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_payments_with_null_value(postgres_storage_adapter_instance: PostgresStorageAdapter):
    # Trying to save a row with a null value where the value can be null
    selected_column = None

    for column, column_type in postgres_storage_adapter_instance.payments_output_schema().items():
        if 'not null' not in column_type.lower():
            selected_column = column
            break

    if not selected_column:
        assert True

    payments = list()

    payments.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.payments_output_schema()))
    payments[0][selected_column] = None

    number_of_rows_before_saving = __get_count_of_table(postgres_storage_adapter_instance, 'payments')

    postgres_storage_adapter_instance._save_payments(payments)
    number_of_rows_after_saving = __get_count_of_table(postgres_storage_adapter_instance, 'payments')

    assert number_of_rows_after_saving == number_of_rows_before_saving + len(payments)

    postgres_storage_adapter_instance.cursor.execute('SELECT source, {} FROM payments ORDER BY time DESC LIMIT 1'.format(selected_column))
    returned_row = postgres_storage_adapter_instance.cursor.fetchone()

    assert returned_row[0] == payments[0]['source']
    assert returned_row[1] == payments[0][selected_column]

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_payments_null_value_error(postgres_storage_adapter_instance: PostgresStorageAdapter):
    # Trying to save a row with a null value where the value must no be null
    selected_column = None

    for column, column_type in postgres_storage_adapter_instance.payments_output_schema().items():
        if 'not null' in column_type.lower():
            selected_column = column
            break

    if not selected_column:
        assert True

    payments = list()

    payments.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.payments_output_schema()))
    payments[0][selected_column] = None

    try:
        postgres_storage_adapter_instance._save_payments(payments)
    except IntegrityError as e:
        assert True
    else:
        assert False

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_creations(postgres_storage_adapter_instance: PostgresStorageAdapter):
    creations = list()

    creations.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.creations_output_schema()))
    creations.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.creations_output_schema()))

    number_of_rows_before_saving = __get_count_of_table(postgres_storage_adapter_instance, 'creations')

    postgres_storage_adapter_instance._save_creations(creations)
    number_of_rows_after_saving = __get_count_of_table(postgres_storage_adapter_instance, 'creations')

    assert number_of_rows_after_saving == number_of_rows_before_saving + len(creations)

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_creations_with_null_value(postgres_storage_adapter_instance: PostgresStorageAdapter):
    # Trying to save a row with a null value where the value can be null
    selected_column = None

    for column, column_type in postgres_storage_adapter_instance.creations_output_schema().items():
        if 'not null' not in column_type.lower():
            selected_column = column
            break

    if not selected_column:
        assert True

    creations = list()

    creations.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.creations_output_schema()))
    creations[0][selected_column] = None

    number_of_rows_before_saving = __get_count_of_table(postgres_storage_adapter_instance, 'creations')

    postgres_storage_adapter_instance._save_creations(creations)
    number_of_rows_after_saving = __get_count_of_table(postgres_storage_adapter_instance, 'creations')

    assert number_of_rows_after_saving == number_of_rows_before_saving + len(creations)

    postgres_storage_adapter_instance.cursor.execute('SELECT source, {} FROM creations ORDER BY time DESC LIMIT 1'.format(selected_column))
    returned_row = postgres_storage_adapter_instance.cursor.fetchone()

    assert returned_row[0] == creations[0]['source']
    assert returned_row[1] == creations[0][selected_column]

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_creation_null_value_error(postgres_storage_adapter_instance: PostgresStorageAdapter):
    # Trying to save a row with a null value where the value must no be null
    selected_column = None

    for column, column_type in postgres_storage_adapter_instance.creations_output_schema().items():
        if 'not null' in column_type.lower():
            selected_column = column
            break

    if not selected_column:
        assert True

    creations = list()

    creations.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.creations_output_schema()))
    creations[0][selected_column] = None

    try:
        postgres_storage_adapter_instance._save_creations(creations)
    except IntegrityError as e:
        assert True
    else:
        assert False

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save(postgres_storage_adapter_instance : PostgresStorageAdapter):
    # Test Setup
    payments = list()
    creations = list()

    timestamp_before_insertion = datetime.now()

    payments_row_count = random.randint(1, 4)
    for i in range(payments_row_count):
        payments.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.payments_output_schema()))

    creations_row_count = random.randint(1, 3)
    for i in range(creations_row_count):
        creations.append(__generate_row_based_on_schema(postgres_storage_adapter_instance.creations_output_schema()))

    postgres_storage_adapter_instance.cursor.execute('SELECT * FROM lastfile')
    pre_test_ledger_name = postgres_storage_adapter_instance.cursor.fetchone()[0]
    ledger_name = 'test'
    where_clause = 'time > \'{}\''.format(timestamp_before_insertion.strftime('%Y-%m-%d %H:%M:%S'))

    # Test
    postgres_storage_adapter_instance.save(payments, creations, ledger_name)
    assert payments_row_count == __get_count_of_table(postgres_storage_adapter_instance, 'payments', where_clause)
    assert creations_row_count == __get_count_of_table(postgres_storage_adapter_instance, 'creations', where_clause)
    assert postgres_storage_adapter_instance.get_last_file_sequence() == ledger_name

    # Test Cleanup
    # deletes all inserted rows based on timestamp
    postgres_storage_adapter_instance.cursor.execute('DELETE FROM payments where {}'.format(where_clause))
    postgres_storage_adapter_instance.conn.commit()

    # return the value of lastfile by saving again, but this time with empty lists and creations
    postgres_storage_adapter_instance.save([], [], pre_test_ledger_name)


def test_save_empty_file(postgres_storage_adapter_instance : PostgresStorageAdapter):
    # Test Setup
    payments = list()
    creations = list()

    postgres_storage_adapter_instance.cursor.execute('SELECT * FROM lastfile')
    pre_test_ledger_name = postgres_storage_adapter_instance.cursor.fetchone()[0]
    ledger_name = 'test'

    payments_count = __get_count_of_table(postgres_storage_adapter_instance, 'payments')
    creations_count = __get_count_of_table(postgres_storage_adapter_instance, 'creations')

    # Test
    postgres_storage_adapter_instance.save(payments, creations, ledger_name)

    assert payments_count == __get_count_of_table(postgres_storage_adapter_instance, 'payments')
    assert creations_count == __get_count_of_table(postgres_storage_adapter_instance, 'creations')
    assert postgres_storage_adapter_instance.get_last_file_sequence() == ledger_name

    # Test Cleanup
    # return the value of lastfile by saving again, but this time with empty lists and creations
    postgres_storage_adapter_instance.save([], [], pre_test_ledger_name)


def test_convert_payment(postgres_storage_adapter_instance: PostgresStorageAdapter):

    payment = __generate_row_based_on_schema(postgres_storage_adapter_instance.payments_output_schema())
    payment['time'] = 1535594286
    returned_dict = postgres_storage_adapter_instance.convert_payment(*payment.values())
    payment.update({'time': datetime.strptime('2018-08-30 01:58:06', '%Y-%m-%d %H:%M:%S')})
    assert returned_dict == payment


def test_convert_creations(postgres_storage_adapter_instance: PostgresStorageAdapter):

    creation = __generate_row_based_on_schema(postgres_storage_adapter_instance.creations_output_schema())
    creation['time'] = 1535594286
    returned_dict = postgres_storage_adapter_instance.convert_creation(*creation.values())
    creation.update({'time': datetime.strptime('2018-08-30 01:58:06', '%Y-%m-%d %H:%M:%S')})
    assert returned_dict == creation


def __get_count_of_table(postgres_storage_adapter_instance, table_name, where_clause='true'):
    postgres_storage_adapter_instance.cursor.execute('select count(*) from {table} where {where_clause}'.format(
        table=table_name, where_clause=where_clause))
    return int(postgres_storage_adapter_instance.cursor.fetchone()[0])


def __generate_row_based_on_schema(schema):
    row_dict = {}

    for column, column_type in schema.items():
        column_type = column_type.lower()
        value = None
        if any([curr_type in column_type for curr_type in ['varchar', 'text']]):
            matcher = re.match('.*\((\d+)\).*', column_type)
            if matcher:
                value = random.choice(string.ascii_lowercase).zfill(int(matcher.group(1)))
            else:
                # No specific text limit
                value = 'test'
        elif 'int' in column_type:
            value = 1
        elif 'float' in column_type:
            value = 0.5
        elif 'timestamp' in column_type:
            value = datetime.now()
        else:
            raise NotImplementedError('sql type has no value generator')

        row_dict[column] = value

    return row_dict
