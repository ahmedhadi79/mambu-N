column_comments = {
    "clients_customfields": {
        "id": "The id of the loan, can be generated and customized, unique",
        "encoded_key": "The encoded key of the loan account, auto generated, unique",
        "creation_date": "The Date of creation for this wise customfields",
        "Card_originalAmount": "The original amount",
        "Card_originalCurrencyCode": "The original Currency Code",
        "Card_settlementAmount": "Amount used for settlement with payment processor",
        "Card_settlementCurrencyCode": "Currency used for settlement with payment processor",
        "Card_settlementStatus": "It contains card settlement status. After the transaction is moved from Mastercard Transit GL, the status gets updated.",
        "Fee_Override": "The Fee Override",
        "Override_Expiry_Date": "The Override Expiry Date"
    },
}

schemas = {
    "clients_customfields": {
        "id": "string",
        "encoded_key": "string",
        "creation_date": "timestamp",
        "_CC_contactId": "string",
        "CC_AccNoOrIBAN": "string",
        "CC_BicOrRoutingCode": "string",
        "_CC_subAccountId": "string",
        "Fee_Override": "string",
        "Override_Expiry_Date": "string"
    },
}
