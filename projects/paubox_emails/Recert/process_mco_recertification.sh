#!/bin/bash
year_month=$1
echo $year_month
rm /home/etl/etl_home/downloads/recertification_emails.csv
aws s3 rm s3://acp-data/paubox/recertification_emails.csv
touch /home/etl/etl_home/downloads/mco_recert_log_$year_month.txt

## Creates files with recertification data for the given month for each pcp_tin
python $ETL_HOME/lib/load_recertification_data.py $year_month
cd $ETL_HOME/downloads/recertification/new_recertification

## Uploads files to Vikram's scratch
aws s3 sync . s3://acp-data/scratch_vikram/Recertification/Practice_files/

## Grabs emails from Monday board, and sends email to them containing the recert data
python $ETL_HOME/lib/recert_email_providers.py >> /home/etl/etl_home/downloads/mco_recert_log_$year_month.txt

## Removes files from downloads directory
rm *.xlsx

## Moves files from Vikram's scratch to the proper S3 bucket for Pam
python $ETL_HOME/lib/mco_recertification_s3_upload.py >> /home/etl/etl_home/downloads/mco_recert_log_$year_month.txt
aws s3 cp /home/etl/etl_home/downloads/mco_recert_log_$year_month.txt s3://acp-data/Recertification/
aws s3 cp /home/etl/etl_home/downloads/recertification_emails.csv s3://acp-data/paubox/
sed -e "s/EMAIL_TYPE/recertification/g" $ETL_HOME/sql/unload_paubox_email_status.sql > $ETL_HOME/sql/unload_recertification_email_status.sql
$ETL_HOME/scripts/ipsql.sh unload_recertification_email_status.sql
