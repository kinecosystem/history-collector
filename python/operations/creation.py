from operations.base_operation import BlockchainOperation


class CreationOperation(BlockchainOperation):

    def __init__(self, tx_operation):
        super().__init__(tx_operation)

    def _fetch_amount_lambda(self):
        return lambda x: x['body']['createAccountOp']['startingBalance']

    def _fetch_destination_lambda(self):
        return lambda x: x['body']['createAccountOp']['destination']['ed25519']

    def _fetch_status_lambda(self):
        return lambda x: x['tr']['createAccountResult']['code']
