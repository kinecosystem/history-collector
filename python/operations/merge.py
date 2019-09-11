from operations.base_operation import BlockchainOperation

SUCCESSFUL_OPERATION = 'ACCOUNT_MERGE_SUCCESS'


class MergeOperation(BlockchainOperation):

    def __init__(self, tx_operation, op_result):
        super().__init__(tx_operation, op_result)

    def get_amount(self):

        if self.get_status() == SUCCESSFUL_OPERATION:
            return self.op_result['tr']['accountMergeResult']['sourceAccountBalance']
        else:
            return 0

    def get_destination(self):
        return self.tx_operation['body']['destination']

    def get_status(self):
        return self.op_result['tr']['accountMergeResult']['code']

    def get_type(self):
        return 'merge'
