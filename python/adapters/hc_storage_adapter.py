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
    def _save_operations(self, operations):
        pass

    @abstractmethod
    def _commit(self):
        pass

    @abstractmethod
    def _rollback(self):
        pass

    @abstractmethod
    def convert_operation(self, source, destination, amount, tx_order, tx_memo, tx_account, tx_account_sequence,
                          tx_fee, tx_charged_fee, tx_status, tx_hash, op_index, op_status, op_type, timestamp,
                          is_signed_by_app, ledger_file_name, ledger_sequence):
        pass

    def save(self, operations_list: list, file_name: str):
        try:
            self.file_name = file_name
            self._save_operations(operations_list)
            self._commit()
            logging.info('Successfully stored the data of file: {} to storage'.format(file_name))

        except Exception:
            logging.warning('Exception occurred while trying to save file: {}'.format(file_name))
            self._rollback()
            logging.info('Rollback finished successfully')
            raise

    @staticmethod
    def operation_output_schema():
        """
        :return: A dictionary of columns saved by the History collector. Key - name, Value - type
        """

        return {
            'source': str,
            'destination': str,
            'amount': int,
            'tx_order': int,
            'tx_memo': str,
            'tx_account': str,
            'tx_account_sequence': int,
            'tx_fee': int,
            'tx_charged_fee': int,
            'tx_status': str,
            'tx_hash': str,
            'op_index': int,
            'op_status': str,
            'op_type': str,
            'timestamp': datetime,
            'is_signed_by_app': bool,
            'ledger_file_name': str,
            'ledger_sequence': str,
        }
