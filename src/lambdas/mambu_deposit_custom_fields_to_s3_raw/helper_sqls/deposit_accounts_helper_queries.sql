select MIN(last_modified_date), max(last_modified_date)
from
(
SELECT  CAST(From_iso8601_timestamp("last_modified_date") AS timestamp) as "last_modified_date"
FROM "datalake_raw"."deposit_accounts_backfill" 
)

SELECT count( DISTINCT id) FROM datalake_raw.deposit_accounts
