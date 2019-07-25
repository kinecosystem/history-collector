from abc import ABC, abstractmethod
from utils import OperationType


class BlockchainOperation(ABC):
    def __init__(self, tx_operation):
        super().__init__()
        self.tx_operation = tx_operation

    def get_type(self):
        return OperationType(self.tx_operation['body']['type'])

    def get_source(self):
        if len(self.tx_operation['sourceAccount']) > 0 and 'ed25519' in self.tx_operation['sourceAccount'][0]:
            return self.tx_operation['sourceAccount'][0]['ed25519']

        return None

    def get_status(self):
        return self._fetch_status_lambda()

    def get_destination(self):
        return self._fetch_destination_lambda()

    def get_amount(self):
        return self._fetch_amount_lambda()

    @abstractmethod
    def _fetch_status_lambda(self):
        pass

    @abstractmethod
    def _fetch_destination_lambda(self):
        pass

    @abstractmethod
    def _fetch_amount_lambda(self):
        pass
