# Backfill

## 1. Setup virtual env(Python:3.7)
```
virtualenv venv-backfill-mambu
source venv-backfill-mambu/bin/activate
pip3 install --upgrade pip
pip3 install -r requirements_aws.txt
pip3 install pip==19.2
pip3 install -r requirements_mambu.txt
```


## 2. Export Vars:

1. export env vars for:

SANDBOX
```
export MAMBU_USERNAME=blme_s3_exports \
export MAMBU_PASSWORD_NAME=sls/reporting \
export MAMBU_SUBDOMAIN=bb2uk.sandbox \
export MAMBU_USER_AGENT="tap-mambu andreas.adamides@bb2.tech" \
export S3_RAW=bb2-sandbox-datalake-raw
```

ALPHA
```
export MAMBU_USERNAME=blme_s3_exports \
export MAMBU_PASSWORD_NAME=sls/etl/mambuAuthDetails \
export MAMBU_SUBDOMAIN=bb2ukalpha.sandbox \
export MAMBU_USER_AGENT="tap-mambu andreas.adamides@bb2.tech" \
export S3_RAW=bb2-alpha-datalake-raw
```


BETA
```
export MAMBU_USERNAME=blme_s3_exports \
export MAMBU_PASSWORD_NAME=sls/etl/mambuAuthDetails \
export MAMBU_SUBDOMAIN=bb2ukbeta.sandbox \
export MAMBU_USER_AGENT="tap-mambu andreas.adamides@bb2.tech" \
export S3_RAW=bb2-beta-datalake-raw
```


PROD
```
export MAMBU_USERNAME=blme_s3_exports \
export MAMBU_PASSWORD_NAME=sls/etl/mambuAuthDetails \
export MAMBU_SUBDOMAIN=bb2uk \
export MAMBU_USER_AGENT="tap-mambu andreas.adamides@bb2.tech" \
export S3_RAW=bb2-prod-datalake-raw
```

## 3. Ensure you're connected to the VPN required for Mambu. 

## 4. According to stream run:
### Deposit Transactions

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- SANDBOX
```
aws configure sso --profile bb2-sandbox-admin
python mambu_generic_backfill.py --profile bb2-sandbox-admin --stream deposit_transactions --stream-pk encoded_key --backfill yes
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream deposit_transactions --stream-pk encoded_key --backfill yes
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream deposit_transactions --stream-pk encoded_key --backfill yes
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream deposit_transactions --stream-pk encoded_key --backfill no
python mambu_generic_backfill.py --profile bb2-prod-admin --stream deposit_transactions --stream-pk encoded_key --backfill yes
```

Finally:
```
rm *.csv
python mambu_generic_backfill.py --profile bb2-prod-admin --stream deposit_transactions --stream-pk encoded_key --backfill no
```

### Deposit Accounts

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream deposit_accounts --stream-pk id --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream deposit_accounts --stream-pk id --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream deposit_accounts --stream-pk id --backfill no
```

### Clients

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream clients --stream-pk id --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream clients --stream-pk id --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream clients --stream-pk id --backfill no
```

### Loan accounts

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream loan_accounts --stream-pk id --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream loan_accounts --stream-pk id --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream loan_accounts --stream-pk id --backfill yes
```

### Loan transactions

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream loan_transactions --stream-pk id --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream loan_transactions --stream-pk id --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream loan_transactions --stream-pk id --backfill yes
```

### Users

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- SANDBOX
```
aws configure sso --profile bb2-sandbox-admin
python mambu_generic_backfill.py --profile bb2-sandbox-admin --stream users --stream-pk id --backfill no
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream users --stream-pk id --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream users --stream-pk id --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream users --stream-pk id --backfill yes
```

### gl_journal_entries

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- SANDBOX
```
aws configure sso --profile bb2-sandbox-admin
python mambu_generic_backfill.py --profile bb2-sandbox-admin --stream gl_journal_entries --stream-pk entry_id --backfill no
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream gl_journal_entries --stream-pk entry_id --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream gl_journal_entries --stream-pk entry_id --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream gl_journal_entries --stream-pk entry_id --backfill no
```

### gl_accounts:

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- SANDBOX
```
aws configure sso --profile bb2-sandbox-admin
python mambu_generic_backfill.py --profile bb2-sandbox-admin --stream gl_accounts --stream-pk gl_code --backfill no
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream gl_accounts --stream-pk gl_code --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream gl_accounts --stream-pk gl_code --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream gl_accounts --stream-pk gl_code --backfill no
```

Finally:
```
rm *.csv
```

### installments

For all envs first execute this:
```
cd lambda/mambu-to-s3-raw/backfill
```

- SANDBOX
```
aws configure sso --profile bb2-sandbox-admin
python mambu_generic_backfill.py --profile bb2-sandbox-admin --stream installments --stream-pk encoded_key --backfill no
```

- ALPHA
```
aws configure sso --profile bb2-alpha-admin
python mambu_generic_backfill.py --profile bb2-alpha-admin --stream installments --stream-pk encoded_key --backfill no
```

- BETA
```
aws configure sso --profile bb2-beta-admin
python mambu_generic_backfill.py --profile bb2-beta-admin --stream installments --stream-pk encoded_key --backfill no
```

- PROD
```
aws configure sso --profile bb2-prod-admin
python mambu_generic_backfill.py --profile bb2-prod-admin --stream installments --stream-pk encoded_key --backfill no
```

Finally:
```
rm *.csv
```
#### (old) Multiple venvs approach for Wise Fields backfill
1. create main venv:
    ```
    virtualenv venv-aws-wrangler
    source venv-aws-wrangler/bin/activate
    pip install --upgrade pip
    pip install awswrangler==2.20.1 flatten-json==0.1.13
    ```
    
2. create tap-mambu env:

    ```
    virtualenv venv-tap-mambu
    source venv-tap-mambu/bin/activate
    pip install tap-mambu==2.0.1
    ```

3. create target-jsonl env:

    ```
    virtualenv venv-target-jsonl
    source venv-target-jsonl/bin/activate
    pip install target-jsonl==0.1.2
    ```

4. Switch to main env:
    ```
    source venv-aws-wrangler/bin/activate
    ```
This was done for 
```python 
python deposit_transactions_wise_custom_fields.py --profile-name bb2-beta-admin 
```
