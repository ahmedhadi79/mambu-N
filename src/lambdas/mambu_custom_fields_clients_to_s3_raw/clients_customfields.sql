SELECT id
    ,encoded_key
    ,creation_date
    ,custom_fields_0_id
    ,custom_fields_0_value
    ,custom_fields_1_id
    ,custom_fields_1_value
    ,custom_fields_2_id
    ,custom_fields_2_value
    ,custom_fields_3_id
    ,custom_fields_3_value
FROM datalake_raw.clients
WHERE custom_fields_0_id in (
    'CC_AccNoOrIBAN'
    ,'CC_BicOrRoutingCode'
    ,'_CC_contactId'
    ,'_CC_subAccountId'
    ,'Fee_Override'
    ,'Override_Expiry_Date'
)
OR custom_fields_1_id in (
    'CC_AccNoOrIBAN'
    ,'CC_BicOrRoutingCode'
    ,'_CC_contactId'
    ,'_CC_subAccountId'
    ,'Fee_Override'
    ,'Override_Expiry_Date'
)
OR custom_fields_2_id in (
    'CC_AccNoOrIBAN'
    ,'CC_BicOrRoutingCode'
    ,'_CC_contactId'
    ,'_CC_subAccountId'
    ,'Fee_Override'
    ,'Override_Expiry_Date'
)
OR custom_fields_3_id in (
    'CC_AccNoOrIBAN'
    ,'CC_BicOrRoutingCode'
    ,'_CC_contactId'
    ,'_CC_subAccountId'
    ,'Fee_Override'
    ,'Override_Expiry_Date'
)