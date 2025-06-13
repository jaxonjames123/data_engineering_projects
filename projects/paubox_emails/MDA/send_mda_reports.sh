#!/bin/bash
aws s3 cp s3://sftp_test/mda_coders/coders.csv $ETL_HOME/downloads/
cd $ETL_HOME/downloads/
rm mdanalytics_emails.csv

python $ETL_HOME/lib/send_mda_report.py
aws s3 cp /home/etl/etl_home/downloads/mdanalytics_emails.csv s3://acp-data/paubox/
sed -e "s/EMAIL_TYPE/mdanalytics/g" $ETL_HOME/sql/unload_paubox_email_status.sql > $ETL_HOME/sql/unload_mdanalytics_email_status.sql
$ETL_HOME/scripts/ipsql.sh unload_mdanalytics_email_status.sql