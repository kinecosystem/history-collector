from operations.base_operation import BlockchainOperation


class PaymentOperation(BlockchainOperation):

    def __init__(self, tx_operation, op_result):
        super().__init__(tx_operation, op_result)

    def get_amount(self):
        return self.tx_operation['body']['paymentOp']['amount']

    def get_destination(self):
        return self.tx_operation['body']['paymentOp']['destination']['ed25519']

    def get_status(self):
        return self.op_result['tr']['paymentResult']['code']

    def get_type(self):
        return 'payment'
