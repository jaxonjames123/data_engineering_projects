#!/bin/bash
export SSHPASS=$FTP_01_PW
date=$(date +%Y-%m)

cd $ETL_HOME/downloads/biotricity
echo "cd /data/findhelp/outbound" > $ETL_HOME/temp/biotricity_outbound_claims.sftp
echo "mget *$date*.csv" >> $ETL_HOME/temp/biotricity_outbound_claims.sftp
echo "cd /data/biotricity/Outgoing" >> $ETL_HOME/temp/biotricity_outbound_claims.sftp
echo "mget *$date*/*.txt" >> $ETL_HOME/temp/biotricity_outbound_claims.sftp

sshpass -e sftp -o BatchMode=no -b $ETL_HOME/temp/biotricity_outbound_claims.sftp $FTP_01_US@$FTP_01_HOST
AWS_ACCESS_KEY_ID=$OPTIMUS_S3_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$OPTIMUS_S3_ACCESS_KEY aws s3 sync . s3://optimusscn/Claims/