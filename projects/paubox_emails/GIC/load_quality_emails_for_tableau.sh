#!/bin/bash

python $ETL_HOME/lib/group_quality_emails_for_tableau.py
aws s3 cp $ETL_HOME/downloads/transformed_quality_emails.csv s3://sftp_test/
"$ETL_HOME/scripts/ipsql.sh" ingest_grouped_quality_emails.sql