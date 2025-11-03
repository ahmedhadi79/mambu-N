--WITH latest_deposit_accounts AS
--(
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
FROM datalake_raw.deposit_accounts
WHERE custom_fields_0_id in (
    'CBS_accountNumber'
    ,'CBS_assignedDepositUser'
    ,'CBS_ibanDeposit'
    ,'CBS_mandatesDetails'
    ,'CBS_sortCode'
    ,'CBS_virtualAccountId'
    ,'FTD_CLOSING_BALANCE'
    ,'FTD_CLOSING_PROFIT'
    ,'term_id'
)--AND date = cast('{0}' as varchar)
OR custom_fields_1_id in (
    'CBS_accountNumber'
    ,'CBS_assignedDepositUser'
    ,'CBS_ibanDeposit'
    ,'CBS_mandatesDetails'
    ,'CBS_sortCode'
    ,'CBS_virtualAccountId'
    ,'FTD_CLOSING_BALANCE'
    ,'FTD_CLOSING_PROFIT'
    ,'term_id'
)--AND date = cast('{0}' as varchar)
OR custom_fields_2_id in (
    'CBS_accountNumber'
    ,'CBS_assignedDepositUser'
    ,'CBS_ibanDeposit'
    ,'CBS_mandatesDetails'
    ,'CBS_sortCode'
    ,'CBS_virtualAccountId'
    ,'FTD_CLOSING_BALANCE'
    ,'FTD_CLOSING_PROFIT'
    ,'term_id'
)--AND date = cast('{0}' as varchar)
OR custom_fields_3_id in (
    'CBS_accountNumber'
    ,'CBS_assignedDepositUser'
    ,'CBS_ibanDeposit'
    ,'CBS_mandatesDetails'
    ,'CBS_sortCode'
    ,'CBS_virtualAccountId'
    ,'FTD_CLOSING_BALANCE'
    ,'FTD_CLOSING_PROFIT'
    ,'term_id'
)--AND date = cast('{0}' as varchar)
--)
--SELECT  id                          
--  ,encoded_key                    
--    ,creation_date                  
--    ,custom_fields_0_id             
--    ,custom_fields_0_value         
--    ,custom_fields_1_id             
--    ,custom_fields_1_value          
--    ,custom_fields_2_id             
--    ,custom_fields_2_value          
--    ,custom_fields_3_id             
--    ,custom_fields_3_value              
--FROM latest_deposit_accounts