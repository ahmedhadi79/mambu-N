module "tap_mambu" {
  source = "terraform-aws-modules/lambda/aws"

  create_layer = true

  layer_name          = "${local.prefix}-tap-mambu"
  description         = "Layer for tap-mambu"
  compatible_runtimes = ["python3.10"]

  store_on_s3 = true
  s3_bucket   = local.meta_datalake_bucket_name
  s3_prefix   = "${local.prefix}/layers/tap_mambu/"

  source_path = {
    path = "${path.module}/../src/layers/tap_mambu"
    commands = [
      "mkdir -p package",
      # target-jsonl is installed separately because of dependency conflicts with tap-mambu
      "python3.10 -m pip install target-jsonl==0.1.4 --target package --upgrade",
      "mv package/bin/target-jsonl package/",
      "python3.10 -m pip install -r requirements_mambu.txt --target package --upgrade",
      "mv package/bin/tap-mambu package/",
      # The following command is a workaround for a bug in tap-mambu, remove it when the bug is fixed in the new version
      "perl -i -pe's|entryId|creationDate|g' package/tap_mambu/tap_generators/gl_journal_entries_generator.py",
      "perl -i -pe 's|AFTER|AFTER_INCLUSIVE|g' package/tap_mambu/tap_generators/{clients_generator.py,communications_generator.py,deposit_accounts_generator.py,deposit_transactions_generator.py,groups_generator.py,interest_accrual_breakdown_generator.py,loan_accounts_generator.py,loan_transactions_generator.py}",
      ":zip",
    ]
  }
}
