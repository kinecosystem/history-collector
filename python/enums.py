from enum import Enum, unique


@unique
class OperationType(Enum):
    # https://www.stellar.org/developers/horizon/reference/resources/operation.html
    CREATE_ACCOUNT = 0
    PAYMENT = 1
    PATH_PAYMENT = 2
    MANAGE_OFFER = 3
    CREATE_PASSIVE_OFFER = 4
    SET_OPTIONS = 5
    CHANGE_TRUST = 6
    ALLOW_TRUST = 7
    ACCOUNT_MERGE = 8
    INFLATION = 9
    MANAGE_DATA = 10
    BUMP_SEQUENCE = 11

    def __str__(self):
        """By default, enum prefixes members with the class name, i.e. OperationType.PAYMENT. Remove the prefix."""
        return self.name
