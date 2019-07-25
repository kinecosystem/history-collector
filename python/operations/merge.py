from operations.base_operation import BlockchainOperation

SUCCESSFUL_OPERATION = 'ACCOUNT_MERGE_SUCCESS'


class MergeOperation(BlockchainOperation):

    def __init__(self, tx_operation):
        super().__init__(tx_operation)

    def _fetch_amount_lambda(self):
        if self._fetch_status_lambda() == SUCCESSFUL_OPERATION:
            return lambda x: x['tr']['accountMergeResult']['sourceAccountBalance']
        else:
            return 0

    def _fetch_destination_lambda(self):
        return lambda x: x['body']['destination']

    def _fetch_status_lambda(self):
        return lambda x: x['tr']['accountMergeResult']['code']
