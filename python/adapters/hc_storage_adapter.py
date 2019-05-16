import logging
from abc import ABC, abstractmethod
from datetime import datetime


class HistoryCollectorStorageAdapter(ABC):
    def __init__(self):
        super().__init__()
        self.file_name = None

    @abstractmethod
    def get_last_file_sequence(self):
        pass

    @abstractmethod
    def __save_payments(self, payments):
        pass

    @abstractmethod
    def __save_creations(self, creations):
        pass

    @abstractmethod
    def __commit(self):
        pass

    @abstractmethod
    def __rollback(self):
        pass

    def save(self, payments_operations_list: list, creations_operations_list: list, file_name: str):
        try:
            self.file_name = file_name
            self.__save_payments(payments_operations_list)
            self.__save_creations(creations_operations_list)
            self.__commit()
            logging.warning('Successfully stored the data of file: {} to storage'.format(file_name))

        except Exception:
            logging.warning('Exception occurred while trying to save file: {}'.format(file_name))
            self.__rollback()
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
