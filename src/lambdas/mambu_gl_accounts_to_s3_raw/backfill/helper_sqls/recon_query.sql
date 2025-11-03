select last_modified_date, count(*) as total_count
from
(
SELECT  CAST(From_iso8601_timestamp("last_modified_date") AS date) as "last_modified_date"
FROM "datalake_raw"."deposit_accounts_backfill" 
)
group by last_modified_date
order by last_modified_date desc