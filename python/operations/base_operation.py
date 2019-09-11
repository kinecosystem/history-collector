from abc import ABC, abstractmethod
from enums import OperationType


class BlockchainOperation(ABC):
    def __init__(self, tx_operation, op_result):
        super().__init__()
        self.tx_operation = tx_operation
        self.op_result = op_result

    def get_source(self):
        if len(self.tx_operation['sourceAccount']) > 0 and 'ed25519' in self.tx_operation['sourceAccount'][0]:
            return self.tx_operation['sourceAccount'][0]['ed25519']

        return None

    @abstractmethod
    def get_status(self):
        pass

    @abstractmethod
    def get_destination(self):
        pass

    @abstractmethod
    def get_amount(self):
        pass

    @abstractmethod
    def get_type(self):
        pass
