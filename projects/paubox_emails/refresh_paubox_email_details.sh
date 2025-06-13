#!/bin/bash

set -u  # Treat unset variables as errors

rm -f "$ETL_HOME/downloads/"*_emails.csv
aws s3 rm s3://acp-data/paubox/ --recursive --exclude "*" --include "*_emails.csv"
python "$ETL_HOME/lib/update_email_sent_status.py"

EMAIL_TYPES=("quality" "recertification" "mdanalytics")

for type in "${EMAIL_TYPES[@]}"; do
  sql_template="$ETL_HOME/sql/unload_paubox_email_status.sql"
  output_sql="$ETL_HOME/sql/unload_${type}_email_status.sql"
  
  sed "s/EMAIL_TYPE/${type}/g" "$sql_template" > "$output_sql"
  aws s3 cp "$ETL_HOME/downloads/${type}_emails.csv" s3://acp-data/paubox/${type}_emails.csv
  "$ETL_HOME/scripts/ipsql.sh" "unload_${type}_email_status.sql"
done