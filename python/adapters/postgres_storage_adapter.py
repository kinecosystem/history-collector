import psycopg2
import logging
from python.adapters.hc_storage_adapter import HistoryCollectorStorageAdapter
from psycopg2.extras import execute_values


class PostgresStorageAdapter(HistoryCollectorStorageAdapter):

    def __init__(self, python_password, postgres_host):
        super().__init__()
        """Set up a connection to the postgres database using the user 'python'."""
        self.conn = psycopg2.connect("postgresql://python:{}@{}:5432/kin".format(python_password, postgres_host))
        self.cursor = self.conn.cursor()
        logging.info('Successfully connected to the database')

    def get_last_file_sequence(self):

        """Get the sequence of the last file scanned."""
        self.cursor.execute('select * from lastfile;')
        self.conn.commit()
        last_file = self.cursor.fetchone()[0]

        return last_file

    def __save_payments(self, payments: list):
        payments_columns = self.payments_output_schema().keys()
        execute_values(self.cursor,
                       'INSERT INTO payments ({columns}) values %s'.format(columns=', '.join(payments_columns)),
                       payments,
                       template=', '.join(['%({})s'.format(column) for column in payments_columns]))

    def __save_creations(self, creations: list):
        creations_columns = self.creations_output_schema().keys()
        execute_values(self.cursor,
                       'INSERT INTO creations ({columns}) values %s'.format(columns=', '.join(creations_columns)),
                       creations,
                       template=', '.join(['%({})s'.format(column) for column in creations_columns]))

    def __commit(self):
        # Update the 'lastfile' entry in the storage
        self.cursor.execute("UPDATE lastfile SET name = %s", (self.file_name,))
        self.conn.commit()

    def __rollback(self):
        self.conn.rollback()

    @staticmethod
    def payments_output_schema():
        """
        :return: A dictionary of columns saved by the History collector.
          Key - name, Value - string literal of postgres type
        """

        return {
            'source': 'varchar(56) not NULL',
            'destination': 'varchar(56) not NULL',
            'amount': 'FLOAT not NULL',  # TODO: change for Kin3
            'memo': 'varchar(28)',
            'tx_fee': 'INT not NULL',
            'tx_charged_fee': 'INT not NULL',
            'op_index': 'INT not NULL',
            'tx_status': 'text',
            'op_status': 'text',
            'tx_hash': 'varchar(64) not NULL',
            'timestamp': 'TIMESTAMP not NULL'
        }

    @staticmethod
    def creations_output_schema():
        """
        :return: A dictionary of columns saved by the History collector.
          Key - name, Value - string literal of postgres type
        """

        return {
            'source': 'varchar(56) not NULL',
            'destination': 'varchar(56) not NULL',
            'starting_balance': 'FLOAT not NULL',  # TODO: change for Kin3
            'memo': 'varchar(28)',
            'tx_fee': 'INT not NULL',
            'tx_charged_fee': 'INT not NULL',
            'op_index': 'INT not NULL',
            'tx_status': 'text',
            'op_status': 'text',
            'tx_hash': 'varchar(64) not NULL',
            'timestamp': 'TIMESTAMP not NULL'
        }

