import psycopg2
import logging
from datetime import datetime
from python.adapters.hc_storage_adapter import HistoryCollectorStorageAdapter
from psycopg2.extras import execute_values


class PostgresStorageAdapter(HistoryCollectorStorageAdapter):

    def __init__(self, postgres_host, python_password, database='kin'):
        super().__init__()
        """Set up a connection to the postgres database using the user 'python'."""
        # TODO: Allow passing port as a param
        self.conn = psycopg2.connect("postgresql://python:{password}@{host}:5432/{database}".format(
            password=python_password, host=postgres_host, database=database))
        self.cursor = self.conn.cursor()
        logging.info('Successfully connected to the database')

    def get_last_file_sequence(self):

        """Get the sequence of the last file scanned."""
        self.cursor.execute('select * from lastfile;')
        self.conn.commit()
        last_file = self.cursor.fetchone()[0]

        return last_file

    def _save_payments(self, payments: list):
        if payments:
            payments_columns = self.payments_output_schema().keys()
            execute_values(self.cursor,
                           'INSERT INTO payments ({columns}) VALUES %s'.format(columns=', '.join(payments_columns)),
                           payments,
                           template='({mapping})'.format(
                               mapping=', '.join(['%({})s'.format(column) for column in payments_columns])
                           ))

    def _save_creations(self, creations: list):
        if creations:
            creations_columns = self.creations_output_schema().keys()
            execute_values(self.cursor,
                           'INSERT INTO creations ({columns}) VALUES %s'.format(columns=', '.join(creations_columns)),
                           creations,
                           template='({mapping})'.format(
                               mapping=', '.join(['%({})s'.format(column) for column in creations_columns])
                           ))

    def _commit(self):
        # Update the 'lastfile' entry in the storage
        self.cursor.execute("UPDATE lastfile SET name = %s", (self.file_name,))
        self.conn.commit()

    def _rollback(self):
        self.conn.rollback()

    def convert_payment(self, source, destination, amount, memo, tx_fee, tx_charged_fee, op_index, tx_status, op_status,
                        tx_hash, timestamp):
        payment = dict.fromkeys(self.payments_output_schema())
        payment['source'] = source
        payment['destination'] = destination
        payment['amount'] = amount
        payment['memo_text'] = memo
        payment['fee'] = tx_fee
        payment['fee_charged'] = tx_charged_fee
        payment['operation_index'] = op_index
        payment['tx_status'] = tx_status
        payment['op_status'] = op_status
        payment['hash'] = tx_hash
        payment['time'] = datetime.utcfromtimestamp(timestamp)

        return payment

    def convert_creation(self, source, destination, balance, memo, tx_fee, tx_charged_fee, op_index, tx_status,
                         op_status, tx_hash, timestamp):
        creation = dict.fromkeys(self.creations_output_schema())
        creation['source'] = source
        creation['destination'] = destination
        creation['starting_balance'] = balance
        creation['memo_text'] = memo
        creation['fee'] = tx_fee
        creation['fee_charged'] = tx_charged_fee
        creation['operation_index'] = op_index
        creation['tx_status'] = tx_status
        creation['op_status'] = op_status
        creation['hash'] = tx_hash
        creation['time'] = datetime.utcfromtimestamp(timestamp)

        return creation

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
            'memo_text': 'varchar(28)',
            'fee': 'INT not NULL',
            'fee_charged': 'INT not NULL',
            'operation_index': 'INT not NULL',
            'tx_status': 'text',
            'op_status': 'text',
            'hash': 'varchar(64) not NULL',
            'time': 'TIMESTAMP not NULL'
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
            'memo_text': 'varchar(28)',
            'fee': 'INT not NULL',
            'fee_charged': 'INT not NULL',
            'operation_index': 'INT not NULL',
            'tx_status': 'text',
            'op_status': 'text',
            'hash': 'varchar(64) not NULL',
            'time': 'TIMESTAMP not NULL'
        }
