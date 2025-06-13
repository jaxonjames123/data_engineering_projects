#!/bin/bash

export SSHPASS=$EMBLEM_RISK_PWD

cd /home/etl/etl_home/downloads/Emblem_PPL || exit 1

# Clean up old files
rm -f *.xlsx

# Download new files from Emblem
echo "cd PROD/FromEmblem/" > $ETL_HOME/temp/emblem_ppl.sftp
echo "mget EH_SOMOS_REPORT_PPL_REPORT_SOMOS_*" >> $ETL_HOME/temp/emblem_ppl.sftp
echo "mget PPL_REPORT_SOMOS_*" >> $ETL_HOME/temp/emblem_ppl.sftp
sshpass -e sftp -o BatchMode=no -b $ETL_HOME/temp/emblem_ppl.sftp $EMBLEM_RISK_USER@$EMBLEM_RISK_HOST

# Rename downloaded files on remote server
echo "cd PROD/FromEmblem/" > $ETL_HOME/temp/emblem_ppl.sftp
for filename in EH_SOMOS_REPORT_PPL_REPORT_SOMOS_* PPL_REPORT_SOMOS_*; do
    [ -e "$filename" ] || continue  # skip if file doesn't exist
    echo "rename $filename ardir/$filename" >> $ETL_HOME/temp/emblem_ppl.sftp
done
sshpass -e sftp -o BatchMode=no -b $ETL_HOME/temp/emblem_ppl.sftp $EMBLEM_RISK_USER@$EMBLEM_RISK_HOST

# Upload files to Evolent
export SSHPASS=$EVOLENT_SFTP_PWD
echo "cd Production/inbound/Emblem" > $ETL_HOME/temp/transfer_emblem_ppl_file.sftp
echo "mput *.xlsx" >> $ETL_HOME/temp/transfer_emblem_ppl_file.sftp
sshpass -e sftp -o BatchMode=no -b $ETL_HOME/temp/transfer_emblem_ppl_file.sftp $EVOLENT_SFTP_USER@$EVOLENT_SFTP_HOST
