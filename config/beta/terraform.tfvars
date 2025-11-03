### Common
aws_account_id  = "466535336611" # beta
bespoke_account = "beta"

### Mambu
mambu_subdomain = "bb2ukbeta.sandbox"

mambu_api_events = {
  "mambu_users" = {
    endpoint     = "users"
    request_type = "get"
    cdc_field    = null
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_gl_accounts" = {
    endpoint     = "glaccounts"
    request_type = "get"
    cdc_field    = null
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_loan_accounts_installments" = {
    endpoint     = "installments"
    request_type = "get"
    cdc_field    = null
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_gl_journal_entries" = {
    endpoint     = "gljournalentries:search"
    request_type = "post"
    cdc_field    = "creationDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  # "mambu_loan_products" = {
  #   endpoint     = "loanproducts"
  #   request_type = "get"
  #   cdc_field    = null
  #   schedule     = "rate(2 days)"
  #   state        = "ENABLED"
  # }
  "mambu_loan_accounts" = {
    endpoint     = "loans:search"
    request_type = "post"
    cdc_field    = "lastModifiedDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_loan_transactions" = {
    endpoint     = "loans/transactions:search"
    request_type = "post"
    cdc_field    = "creationDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_deposit_accounts" = {
    endpoint     = "deposits:search"
    request_type = "post"
    cdc_field    = "lastModifiedDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_deposit_transactions" = {
    endpoint     = "deposits/transactions:search"
    request_type = "post"
    cdc_field    = "creationDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_clients" = {
    endpoint     = "clients:search"
    request_type = "post"
    cdc_field    = "lastModifiedDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  "mambu_accounting_interestaccrual" = {
    endpoint     = "accounting/interestaccrual:search"
    request_type = "post"
    cdc_field    = "creationDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  # "mambu_communications_messages" = {
  #   endpoint     = "communications/messages:searchSorted"
  #   request_type = "post"
  #   cdc_field    = "creationDate"
  #   schedule     = "rate(2 days)"
  #   state        = "ENABLED"
  # }
  "mambu_groups" = {
    endpoint     = "groups:search"
    request_type = "post"
    cdc_field    = "lastModifiedDate"
    schedule     = "rate(2 days)"
    state        = "ENABLED"
  }
  # "mambu_credit_arrangements" = {
  #   endpoint     = "creditarrangements:search"
  #   request_type = "post"
  #   cdc_field    = "approvedDate"
  #   schedule     = "rate(7 days)"
  #   state        = "ENABLED"
  # }
}
