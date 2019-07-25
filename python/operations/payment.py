from operations.base_operation import BlockchainOperation


class PaymentOperation(BlockchainOperation):

    def __init__(self, tx_operation):
        super().__init__(tx_operation)

    def _fetch_amount_lambda(self):
        return lambda x: x['body']['paymentOp']['amount']

    def _fetch_destination_lambda(self):
        return lambda x: x['body']['paymentOp']['destination']['ed25519']

    def _fetch_status_lambda(self):
        return lambda x: x['tr']['paymentResult']['code']
