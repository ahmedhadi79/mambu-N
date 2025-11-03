SELECT  MIN(id) as min_id,max(id) as max_id,MIN(timestamp_extracted) as min_timestam_extracted,max(timestamp_extracted) as max_timestam_extracted
        ,custom_fields_0_id
        ,custom_fields_1_id
        ,custom_fields_2_id
        ,custom_fields_3_id
        ,custom_fields_4_id
FROM "datalake_raw"."deposit_transactions" 
WHERE transaction_details_transaction_channel_id='Wise_Local_Payments' 
AND custom_fields_0_field_set_id = '_WiseChannelSet'
GROUP BY custom_fields_0_id,
        custom_fields_1_id,
        custom_fields_2_id,
        custom_fields_3_id,
        custom_fields_4_id;


--bb2/promote-quicksight-dashboards/sql/authors_access_mambu_transactions_all.sql

SELECT dt1."id" as "transaction_id"
       ,COALESCE(dt1.custom_fields_0_field_set_id,dt2.custom_fields_0_field_set_id) as custom_fields_0_field_set_id
       ,COALESCE(dt1.custom_fields_0_id,dt2.custom_fields_0_id) as custom_fields_0_id
       ,COALESCE(dt1.custom_fields_0_value,dt2.custom_fields_0_value) as custom_fields_0_value
       ,COALESCE(dt1.custom_fields_1_field_set_id,dt2.custom_fields_1_field_set_id) as custom_fields_1_field_set_id
       ,COALESCE(dt1.custom_fields_1_id,dt2.custom_fields_1_id) as custom_fields_1_id
       ,COALESCE(dt1.custom_fields_1_value,dt2.custom_fields_1_value) as custom_fields_1_value
       ,COALESCE(dt1.custom_fields_2_field_set_id,dt2.custom_fields_2_field_set_id) as custom_fields_2_field_set_id
       ,COALESCE(dt1.custom_fields_2_id,dt2.custom_fields_2_id) as custom_fields_2_id 
       ,COALESCE(dt1.custom_fields_2_value,dt2.custom_fields_2_value) as custom_fields_2_value
       ,COALESCE(dt1.custom_fields_3_field_set_id,dt2.custom_fields_3_field_set_id) as custom_fields_3_field_set_id
       ,COALESCE(dt1.custom_fields_3_id,dt2.custom_fields_3_id) as custom_fields_3_id
       ,COALESCE(dt1.custom_fields_3_value,dt2.custom_fields_3_value) as custom_fields_3_value
       ,COALESCE(dt1.custom_fields_4_field_set_id,dt2.custom_fields_4_field_set_id) as custom_fields_4_field_set_id
       ,COALESCE(dt1.custom_fields_4_id,dt2.custom_fields_4_id) as custom_fields_4_id
       ,COALESCE(dt1.custom_fields_4_value,dt2.custom_fields_4_value) as custom_fields_4_value
       ,CAST(From_iso8601_timestamp(dt1."creation_date") AS timestamp) as "creation_date"
       ,CAST(From_iso8601_timestamp(dt1."value_date") AS timestamp) as "value_date"
       ,dt1."type"
       ,dt1."amount"
       ,ABS(CAST(dt1."amount" as decimal(10,2))) as abs_amount
       ,dt1."currency_code"
       ,dt1."account_balances_total_balance"
       ,dt1."transaction_details_transaction_channel_id"
       ,dt1."transfer_details"
       ,dt1."branch_key"
       ,dt1."transaction_details"
       ,CAST(From_iso8601_timestamp(dt1."booking_date") AS timestamp) as "booking_date"
       ,dt1.date
FROM datalake_raw.deposit_transactions dt1
LEFT JOIN deposit_transactions_wise_custom_fields_backfill dt2
ON dt1.id=dt2.id


-------A.
--before
SELECT  id
        ,custom_fields_0_id
        ,custom_fields_1_id
        ,custom_fields_2_id
        ,custom_fields_3_id
        ,custom_fields_4_id
FROM "datalake_raw"."deposit_transactions" 
WHERE transaction_details_transaction_channel_id='Wise_Local_Payments' 
AND id IN ('12606','10850','17879')
AND custom_fields_0_field_set_id = '_WiseChannelSet'
GROUP BY id,custom_fields_0_id,
        custom_fields_1_id,
        custom_fields_2_id,
        custom_fields_3_id,
        custom_fields_4_id;

--backfill table
SELECT *
FROM "datalake_raw"."deposit_transactions_wise_custom_fields_backfill"
WHERE id IN ('12606','10850')

--after
SELECT dt1."id" as "transaction_id"
       ,COALESCE(dt1.custom_fields_0_field_set_id,dt2.custom_fields_0_field_set_id) as custom_fields_0_field_set_id
       ,COALESCE(dt1.custom_fields_0_id,dt2.custom_fields_0_id) as custom_fields_0_id
       ,COALESCE(dt1.custom_fields_0_value,dt2.custom_fields_0_value) as custom_fields_0_value
       ,COALESCE(dt1.custom_fields_1_field_set_id,dt2.custom_fields_1_field_set_id) as custom_fields_1_field_set_id
       ,COALESCE(dt1.custom_fields_1_id,dt2.custom_fields_1_id) as custom_fields_1_id
       ,COALESCE(dt1.custom_fields_1_value,dt2.custom_fields_1_value) as custom_fields_1_value
       ,COALESCE(dt1.custom_fields_2_field_set_id,dt2.custom_fields_2_field_set_id) as custom_fields_2_field_set_id
       ,COALESCE(dt1.custom_fields_2_id,dt2.custom_fields_2_id) as custom_fields_2_id 
       ,COALESCE(dt1.custom_fields_2_value,dt2.custom_fields_2_value) as custom_fields_2_value
       ,COALESCE(dt1.custom_fields_3_field_set_id,dt2.custom_fields_3_field_set_id) as custom_fields_3_field_set_id
       ,COALESCE(dt1.custom_fields_3_id,dt2.custom_fields_3_id) as custom_fields_3_id
       ,COALESCE(dt1.custom_fields_3_value,dt2.custom_fields_3_value) as custom_fields_3_value
       ,COALESCE(dt1.custom_fields_4_field_set_id,dt2.custom_fields_4_field_set_id) as custom_fields_4_field_set_id
       ,COALESCE(dt1.custom_fields_4_id,dt2.custom_fields_4_id) as custom_fields_4_id
       ,COALESCE(dt1.custom_fields_4_value,dt2.custom_fields_4_value) as custom_fields_4_value
       ,CAST(From_iso8601_timestamp(dt1."creation_date") AS timestamp) as "creation_date"
       ,CAST(From_iso8601_timestamp(dt1."value_date") AS timestamp) as "value_date"
       ,dt1."type"
       ,dt1."amount"
       ,ABS(CAST(dt1."amount" as decimal(10,2))) as abs_amount
       ,dt1."currency_code"
       ,dt1."account_balances_total_balance"
       ,dt1."transaction_details_transaction_channel_id"
       ,dt1."transfer_details"
       ,dt1."branch_key"
       ,dt1."transaction_details"
       ,CAST(From_iso8601_timestamp(dt1."booking_date") AS timestamp) as "booking_date"
       ,dt1.date
FROM datalake_raw.deposit_transactions dt1
LEFT JOIN deposit_transactions_wise_custom_fields_backfill dt2
ON dt1.id=dt2.id
WHERE dt1.id IN ('12606','17879')


-------B.
--15920
select count(*)
from(
SELECT "id" as "transaction_id"
       ,CAST(From_iso8601_timestamp("creation_date") AS timestamp) as "creation_date"
       ,CAST(From_iso8601_timestamp("value_date") AS timestamp) as "value_date"
       ,"type"
       ,"amount"
       ,ABS(CAST("amount" as decimal(10,2))) as abs_amount
       ,"currency_code"
       ,"account_balances_total_balance"
       ,"transaction_details_transaction_channel_id"
       ,"transfer_details"
       ,"branch_key"
       ,"transaction_details"
       ,CAST(From_iso8601_timestamp("booking_date") AS timestamp) as "booking_date"
FROM datalake_raw.deposit_transactions
)


--15920
select count(*)
from(
SELECT dt1."id" as "transaction_id"
       ,COALESCE(dt1.custom_fields_0_field_set_id,dt2.custom_fields_0_field_set_id)
       ,COALESCE(dt1.custom_fields_0_id,dt2.custom_fields_0_id)
       ,COALESCE(dt1.custom_fields_0_value,dt2.custom_fields_0_value)
       ,COALESCE(dt1.custom_fields_1_field_set_id,dt2.custom_fields_1_field_set_id)
       ,COALESCE(dt1.custom_fields_1_id,dt2.custom_fields_1_id)
       ,COALESCE(dt1.custom_fields_1_value,dt2.custom_fields_1_value)
       ,COALESCE(dt1.custom_fields_2_field_set_id,dt2.custom_fields_2_field_set_id)
       ,COALESCE(dt1.custom_fields_2_id,dt2.custom_fields_2_id)
       ,COALESCE(dt1.custom_fields_2_value,dt2.custom_fields_2_value)
       ,COALESCE(dt1.custom_fields_3_field_set_id,dt2.custom_fields_3_field_set_id)
       ,COALESCE(dt1.custom_fields_3_id,dt2.custom_fields_3_id)
       ,COALESCE(dt1.custom_fields_3_value,dt2.custom_fields_3_value)
       ,COALESCE(dt1.custom_fields_4_field_set_id,dt2.custom_fields_4_field_set_id)
       ,COALESCE(dt1.custom_fields_4_id,dt2.custom_fields_4_id)
       ,COALESCE(dt1.custom_fields_4_value,dt2.custom_fields_4_value)
       ,CAST(From_iso8601_timestamp(dt1."creation_date") AS timestamp) as "creation_date"
       ,CAST(From_iso8601_timestamp(dt1."value_date") AS timestamp) as "value_date"
       ,dt1."type"
       ,dt1."amount"
       ,ABS(CAST(dt1."amount" as decimal(10,2))) as abs_amount
       ,dt1."currency_code"
       ,dt1."account_balances_total_balance"
       ,dt1."transaction_details_transaction_channel_id"
       ,dt1."transfer_details"
       ,dt1."branch_key"
       ,dt1."transaction_details"
       ,CAST(From_iso8601_timestamp(dt1."booking_date") AS timestamp) as "booking_date"
       ,dt1.date
FROM datalake_raw.deposit_transactions dt1
LEFT JOIN deposit_transactions_wise_custom_fields_backfill dt2
ON dt1.id=dt2.id

)
