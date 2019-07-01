import logging
from abc import ABC, abstractmethod
from datetime import datetime


class HistoryCollectorStorageError(Exception):

    def __init__(self, message):
        super(HistoryCollectorStorageError, self).__init__(message)


class HistoryCollectorStorageAdapter(ABC):
    def __init__(self):
        super().__init__()
        self.file_name = None

    @abstractmethod
    def get_last_file_sequence(self):
        pass

    @abstractmethod
    def _save_payments(self, payments):
        pass

    @abstractmethod
    def _save_creations(self, creations):
        pass

    @abstractmethod
    def _commit(self):
        pass

    @abstractmethod
    def _rollback(self):
        pass

    @abstractmethod
    def convert_payment(self, source, destination, amount, memo, tx_fee, tx_charged_fee, op_index, tx_status, op_status,
                        tx_hash, timestamp):
        pass

    @abstractmethod
    def convert_creation(self, source, destination, balance, memo, tx_fee, tx_charged_fee, op_index, tx_status,
                         op_status, tx_hash, timestamp):
        pass

    def save(self, payments_operations_list: list, creations_operations_list: list, file_name: str):
        try:
            self.file_name = file_name
            self._save_payments(payments_operations_list)
            self._save_creations(creations_operations_list)
            self._commit()
            logging.info('Successfully stored the data of file: {} to storage'.format(file_name))

        except Exception:
            logging.warning('Exception occurred while trying to save file: {}'.format(file_name))
            self._rollback()
            logging.info('Rollback finished successfully')
            raise

    @staticmethod
    def payments_output_schema():
        """
        :return: A dictionary of columns saved by the History collector. Key - name, Value - type
        """

        return {
            'source': str,
            'destination': str,
            'amount': float,  # TODO: change for Kin3
            'memo': str,
            'tx_fee': int,
            'tx_charged_fee': int,
            'op_index': int,
            'tx_status': str,
            'op_status': str,
            'tx_hash': str,
            'timestamp': datetime
        }

    @staticmethod
    def creations_output_schema():
        """
        :return: A dictionary of columns saved by the History collector. Key - name, Value - type
        """

        return {
            'source': str,
            'destination': str,
            'starting_balance': float,  # TODO: change for Kin3
            'memo': str,
            'tx_fee': int,
            'tx_charged_fee': int,
            'op_index': int,
            'tx_status': str,
            'op_status': str,
            'tx_hash': str,
            'timestamp': datetime
        }
