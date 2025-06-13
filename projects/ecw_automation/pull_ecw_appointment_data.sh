#!/bin/bash
cd $ETL_HOME/downloads/ecw/lastpass
aws s3 cp s3://sftp_test/ecw_automation/lastpass/lastpass_vault_export.csv .
python $ETL_HOME/lib/ecw_automation/lastpass/ingest_lastpass_names.py
aws s3 cp lastpass_vault_export_formatted.csv s3://sftp_test/ecw_automation/lastpass/
$ETL_HOME/scripts/ipsql.sh copy_lastpass_export_formatted.sql
rm lastpass_vault_export.csv
rm lastpass_vault_export_formatted.csv

python $ETL_HOME/lib/ecw_automation/firefox_pull_appointments.py
cd $ETL_HOME/downloads/ecw/appointments

ls */*appointments*.csv | while read filename; do
    curr_file=$(basename "$filename")
    echo "$curr_file"
    aws s3 cp "$filename" s3://sftp_test/ecw_automation/appointments/"$curr_file"
    sed -e "s/FILENAME/${curr_file}/g" $ETL_HOME/sql/ingest_ecw_appointment_data_template.sql > $ETL_HOME/sql/ingest_ecw_appointment_data.sql
    $ETL_HOME/scripts/ipsql.sh ingest_ecw_appointment_data.sql
    rm $filename
done

cd $ETL_HOME/downloads/ecw/errored_screenshots
aws s3 cp . s3://sftp_test/ecw_automation/errored_screenshots/ --recursive
rm *.png

$ETL_HOME/scripts/ipsql.sh create_appointments_patient_details.sql
python $ETL_HOME/lib/ecw_automation/ecw_gdw_fuzzy.py

python $ETL_HOME/lib/ecw_automation/sophia/retrieve_sophia_appointments.py
$ETL_HOME/scripts/ipsql_sophia_db.sh sophia_db_prod_appointments.sql

