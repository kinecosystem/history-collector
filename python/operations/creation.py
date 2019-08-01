from operations.base_operation import BlockchainOperation


class CreationOperation(BlockchainOperation):

    def __init__(self, tx_operation, op_result):
        super().__init__(tx_operation, op_result)

    def get_amount(self):
        return self.tx_operation['body']['createAccountOp']['startingBalance']

    def get_destination(self):
        return self.tx_operation['body']['createAccountOp']['destination']['ed25519']

    def get_status(self):
        return self.op_result['tr']['createAccountResult']['code']

    def get_type(self):
        return 'creation'
