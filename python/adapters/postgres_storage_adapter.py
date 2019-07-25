import psycopg2
import logging
from datetime import datetime
from adapters.hc_storage_adapter import HistoryCollectorStorageAdapter
from psycopg2.extras import execute_values


class PostgresStorageAdapter(HistoryCollectorStorageAdapter):

    def __init__(self, postgres_host, python_password, database='kin'):
        super().__init__()
        """Set up a connection to the postgres database using the user 'python'."""
        # TODO: Allow passing port as a param
        self.conn = psycopg2.connect("postgresql://python:{password}@{host}:5432/{database}".format(
            password=python_password, host=postgres_host, database=database))
        self.cursor = self.conn.cursor()
        logging.debug('Successfully connected to the database')

    def get_last_file_sequence(self):

        """Get the sequence of the last file scanned."""
        self.cursor.execute('select * from lastfile;')
        self.conn.commit()
        last_file = self.cursor.fetchone()[0]

        return last_file

    def _save_operations(self, operations: list):
        if operations:
            operation_columns = self.operation_output_schema().keys()
            execute_values(self.cursor,
                           'INSERT INTO operations ({columns}) VALUES %s'.format(columns=', '.join(operation_columns)),
                           operations,
                           template='({mapping})'.format(
                               mapping=', '.join(['%({})s'.format(column) for column in operation_columns])
                           ))

    def _commit(self):
        # Update the 'lastfile' entry in the storage
        self.cursor.execute("UPDATE lastfile SET name = %s", (self.file_name,))
        self.conn.commit()

    def _rollback(self):
        self.conn.rollback()

    def convert_operation(self, source, destination, amount, tx_order, tx_memo, tx_account, tx_account_sequence,
                          tx_fee, tx_charged_fee, tx_status, tx_hash, op_order, op_status, op_type, timestamp,
                          is_signed_by_app, ledger_file_name, ledger_sequence):

        operation = dict.fromkeys(self.operation_output_schema())
        operation['source'] = source,
        operation['destination'] = destination,
        operation['amount'] = amount,
        operation['tx_order'] = tx_order,
        operation['tx_memo'] = tx_memo,
        operation['tx_account'] = tx_account,
        operation['tx_account_sequence'] = tx_account_sequence,
        operation['tx_fee'] = tx_fee,
        operation['tx_charged_fee'] = tx_charged_fee,
        operation['tx_status'] = tx_status,
        operation['tx_hash'] = tx_hash,
        operation['op_order'] = op_order,
        operation['op_status'] = op_status,
        operation['op_type'] = op_type
        operation['timestamp'] = datetime.utcfromtimestamp(timestamp),
        operation['is_signed_by_app'] = is_signed_by_app,
        operation['ledger_file_name'] = ledger_file_name,
        operation['ledger_sequence'] = ledger_sequence

        return operation

    @staticmethod
    def operation_output_schema():
        """
        :return: A dictionary of columns saved by the History collector.\
          Key - name, Value - string literal of postgres type
        """

        return {
            'source': 'varchar(64) not NULL',
            'destination': 'varchar(64) not NULL',
            'amount': 'BIGINT not NULL',
            'tx_order': 'INT not NULL',
            'tx_memo': 'varchar(28)',
            'tx_account': 'varchar(64) NOT NULL',
            'tx_account_sequence': 'BIGINT not NULL',
            'tx_fee': 'INT not NULL',
            'tx_fee_charged': 'INT not NULL',
            'tx_status': 'varchar(32) NOT NULL',
            'tx_hash': 'varchar(64) not NULL',
            'op_order': 'INT not NULL',
            'op_status': 'varchar(32) NOT NULL',
            'op_type': 'varchar(32) NOT NULL',
            'timestamp': 'TIMESTAMP without time zone not NULL',
            'is_signed_by_app': 'BOOLEAN',
            'ledger_file_name': 'varchar(10) not NULL',
            'ledger_sequence': 'INT not NULL',
        }

