region="eu-west-2"
bucket="bb2-beta-tfstate"
key="mambu-integration.tfstate"
encrypt="true"
use_lockfile="true"
assume_role = {
    role_arn="arn:aws:iam::521333308695:role/tfstate-mgnt-role-mambu-integration-beta"
    session_name="mambu-integration-beta"
}