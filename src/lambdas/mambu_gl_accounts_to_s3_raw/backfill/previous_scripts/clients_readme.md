# local dev

1. create venv / conda env
2. `pip install pip==19.2`
3. `pip install -r requirements_tests.txt`
4. `pip install --target package -r requirements.txt`
5. `cd package`
6. `mv bin/tap-mambu . && mv bin/target-jsonl .`


## to run backfill locally:

1. export env vars for:
    - MAMBU_USERNAME
    - MAMBU_PASSWORD_NAME
    - MAMBU_SUBDOMAIN
    - MAMBU_USER_AGENT
    - S3_RAW

2. ensure you're connected to the VPN required for Mambu.

3. run `python clients_backfill.py --profile_name <aws profile name for environment>`