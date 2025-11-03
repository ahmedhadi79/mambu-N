# Mambu Generic API Client Lambda

## Sample Full Event Structure
```
{
    "table_name": "target_athena_table_name",
    "endpoint": "mambu:api_endpoint",
    "request_type": "Post",
    "cdc_field": "lastModifiedDate", # Optional only with "Get" requests
    "start_date": "2018-12-07",  # %Y-%m-%d %H:%M:%S Optional
    "end_date": "2024-12-04",  # %Y-%m-%d %H:%M:%S Optional
    "extra_params": "paginationDetails=ON", # Optional
    "auto_schema": "True",  # Optional
    "rename_columns": [],  # Optional
}
```

## Events To Manually Ingest Tables For The First Time.
- Modify `start_date` as needed, this way lambda handler will not request athena for CDC.
- Below tables are already included in `data_catalog.py`, if your table not included you can add `"auto_schema": "True"`
```
[
  {
    "table_name": "mambu_users",
    "endpoint": "users",
    "request_type": "get"
  },
  {
    "table_name": "mambu_gl_accounts",
    "endpoint": "glaccounts",
    "request_type": "get"
  },
  {
    "table_name": "mambu_loan_accounts_installments",
    "endpoint": "installments",
    "request_type": "get"
  },
  {
    "table_name": "mambu_gl_journal_entries",
    "endpoint": "gljournalentries:search",
    "request_type": "post",
    "cdc_field": "creationDate",
    "start_date": "2025-01-01 00:00:00"
  },
  {
    "table_name": "mambu_loan_accounts",
    "endpoint": "loans:search",
    "request_type": "post",
    "cdc_field": "lastModifiedDate",
    "start_date": "2025-01-01 00:00:00"
  },
  {
    "table_name": "mambu_loan_transactions",
    "endpoint": "loans/transactions:search",
    "request_type": "post",
    "cdc_field": "creationDate",
    "start_date": "2025-01-01 00:00:00"
  },
  {
    "table_name": "mambu_deposit_accounts",
    "endpoint": "deposits:search",
    "request_type": "post",
    "cdc_field": "lastModifiedDate",
    "start_date": "2025-01-01 00:00:00"
  },
  {
    "table_name": "mambu_deposit_transactions",
    "endpoint": "deposits/transactions:search",
    "request_type": "post",
    "cdc_field": "creationDate",
    "start_date": "2025-01-01 00:00:00"
  },
  {
    "table_name": "mambu_clients",
    "endpoint": "clients:search",
    "request_type": "post",
    "cdc_field": "lastModifiedDate",
    "start_date": "2025-01-01 00:00:00"
  },
  {
    "table_name": "mambu_accounting_interestaccrual",
    "endpoint": "accounting/interestaccrual:search",
    "request_type": "post",
    "cdc_field": "creationDate",
    "start_date": "2025-01-01 00:00:00"
  },
  {
    "table_name": "mambu_groups",
    "endpoint": "groups:search",
    "request_type": "post",
    "cdc_field": "lastModifiedDate",
    "start_date": "2025-01-01 00:00:00"
  },
]
```
