#!/bin/bash

aws s3 rm s3://acp-data/paubox/quality_emails.csv
cd $ETL_HOME/downloads/
rm quality_emails.csv
python $ETL_HOME/lib/gic_email_providers.py
aws s3 cp /home/etl/etl_home/downloads/quality_emails.csv s3://acp-data/paubox/
sed -e "s/EMAIL_TYPE/quality/g" $ETL_HOME/sql/unload_paubox_email_status.sql > $ETL_HOME/sql/unload_quality_email_status.sql
$ETL_HOME/scripts/ipsql.sh unload_quality_email_status.sql