import pytest
import re
import random
import string
from adapters.postgres_storage_adapter import PostgresStorageAdapter
from datetime import datetime
from psycopg2 import IntegrityError

global_ledger_inc = 1


@pytest.fixture('session')
def postgres_storage_adapter_instance(postgres_host, postgres_password, postgres_database_name):
    postgres = PostgresStorageAdapter(postgres_host, postgres_password, postgres_database_name)
    postgres._rollback()

    return postgres


def test_constructor_with_wrong_credentials(postgres_host, postgres_database_name):

    with pytest.raises(Exception):
        assert PostgresStorageAdapter(postgres_host, 'foo', postgres_database_name)


def test_constructor_with_access(postgres_storage_adapter_instance):
    # On failure - make sure to run build_s3_storage.py on the relevant bucket and that your aws access have permissions
    assert True


def test_get_last_file_sequence(postgres_storage_adapter_instance: PostgresStorageAdapter):

    assert re.match('^[a-f0-9]{8}$', postgres_storage_adapter_instance.get_last_file_sequence())


def test_save_operations(postgres_storage_adapter_instance: PostgresStorageAdapter):

    operations = __generate_row_based_on_schema(postgres_storage_adapter_instance.operation_output_schema(), 2)

    number_of_rows_before_saving = __get_count_of_table(postgres_storage_adapter_instance, 'operations')

    postgres_storage_adapter_instance._save_operations(operations)
    number_of_rows_after_saving = __get_count_of_table(postgres_storage_adapter_instance, 'operations')

    assert number_of_rows_after_saving == number_of_rows_before_saving + len(operations)

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_operations_with_null_value(postgres_storage_adapter_instance: PostgresStorageAdapter):
    # Trying to save a row with a null value where the value can be null
    selected_column = None

    for column, column_type in postgres_storage_adapter_instance.operation_output_schema().items():
        if 'not null' not in column_type.lower():
            selected_column = column
            break

    if not selected_column:
        assert True

    operations = __generate_row_based_on_schema(postgres_storage_adapter_instance.operation_output_schema())
    operations[0][selected_column] = None

    number_of_rows_before_saving = __get_count_of_table(postgres_storage_adapter_instance, 'operations')

    postgres_storage_adapter_instance._save_operations(operations)
    number_of_rows_after_saving = __get_count_of_table(postgres_storage_adapter_instance, 'operations')

    assert number_of_rows_after_saving == number_of_rows_before_saving + len(operations)

    postgres_storage_adapter_instance.cursor.execute('SELECT source, {} FROM operations ORDER BY timestamp DESC LIMIT 1'.format(selected_column))
    returned_row = postgres_storage_adapter_instance.cursor.fetchone()

    assert returned_row[0] == operations[0]['source']
    assert returned_row[1] == operations[0][selected_column]

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_operations_null_value_error(postgres_storage_adapter_instance: PostgresStorageAdapter):

    # Trying to save a row with a null value where the value must no be null
    selected_column = None

    for column, column_type in postgres_storage_adapter_instance.operation_output_schema().items():
        if 'not null' in column_type.lower():
            selected_column = column
            break

    if not selected_column:
        assert True

    operations = __generate_row_based_on_schema(postgres_storage_adapter_instance.operation_output_schema())
    operations[0][selected_column] = None

    with pytest.raises(IntegrityError):
        assert postgres_storage_adapter_instance._save_operations(operations)

    # Rollback
    postgres_storage_adapter_instance._rollback()


def test_save_empty_file(postgres_storage_adapter_instance : PostgresStorageAdapter):
    # Test Setup
    operations = list()

    postgres_storage_adapter_instance.cursor.execute('SELECT * FROM lastfile')
    pre_test_ledger_name = postgres_storage_adapter_instance.cursor.fetchone()[0]
    ledger_name = 'test'

    operations_count = __get_count_of_table(postgres_storage_adapter_instance, 'operations')

    # Test
    postgres_storage_adapter_instance.save(operations, ledger_name)

    assert operations_count == __get_count_of_table(postgres_storage_adapter_instance, 'operations')
    assert postgres_storage_adapter_instance.get_last_file_sequence() == ledger_name

    # Test Cleanup
    # return the value of lastfile by saving again, but this time with empty lists and creations
    postgres_storage_adapter_instance.save([], pre_test_ledger_name)


def test_save(postgres_storage_adapter_instance : PostgresStorageAdapter):
    # Test Setup
    timestamp_before_insertion = datetime.now()

    operations_row_count = random.randint(1, 4)
    # Generating records
    operations = __generate_row_based_on_schema(postgres_storage_adapter_instance.operation_output_schema(),
                                                operations_row_count)

    postgres_storage_adapter_instance.cursor.execute('SELECT * FROM lastfile')
    pre_test_ledger_name = postgres_storage_adapter_instance.cursor.fetchone()[0]
    ledger_name = 'test'
    where_clause = 'timestamp > \'{}\''.format(timestamp_before_insertion.strftime('%Y-%m-%d %H:%M:%S'))

    # Test
    postgres_storage_adapter_instance.save(operations, ledger_name)
    assert operations_row_count == __get_count_of_table(postgres_storage_adapter_instance, 'operations', where_clause)
    assert postgres_storage_adapter_instance.get_last_file_sequence() == ledger_name

    # No Test Cleanup - no permissions to delete records

    # return the value of lastfile by saving again, but this time with empty lists and creations
    postgres_storage_adapter_instance.save([], pre_test_ledger_name)


def test_convert_operation(postgres_storage_adapter_instance: PostgresStorageAdapter):

    operations = __generate_row_based_on_schema(postgres_storage_adapter_instance.operation_output_schema())
    operation = operations[0]
    operation['timestamp'] = 1535594286
    returned_dict = postgres_storage_adapter_instance.convert_operation(*operation.values())
    operation.update({'timestamp': datetime.strptime('2018-08-30 01:58:06', '%Y-%m-%d %H:%M:%S')})

    assert returned_dict == operation


def __get_count_of_table(postgres_storage_adapter_instance, table_name, where_clause='true'):
    postgres_storage_adapter_instance.cursor.execute('select count(*) from {table} where {where_clause}'.format(
        table=table_name, where_clause=where_clause))
    return int(postgres_storage_adapter_instance.cursor.fetchone()[0])


def __generate_row_based_on_schema(schema, amount_of_rows=1):

    global global_ledger_inc
    global_ledger_inc += 1

    list_of_rows = []

    for i in range(amount_of_rows):

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
            elif 'boolean' in column_type:
                value = False
            else:
                raise NotImplementedError('sql type has no value generator')

            if column == 'op_order':
                value = i

            if column == 'ledger_sequence':
                value = global_ledger_inc

            row_dict[column] = value

        list_of_rows.append(row_dict)

    return list_of_rows
