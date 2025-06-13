#!/bin/bash
set -x
rm -f $ETL_HOME/downloads/277_files/*
export SSHPASS=$FTP_01_PW
echo "cd /data/emblem-ftp" > $ETL_HOME/temp/eh_submitted_277.sftp
echo "lcd /home/etl/etl_home/downloads/277_files" >> $ETL_HOME/temp/eh_submitted_277.sftp
echo "mget HN*" >> $ETL_HOME/temp/eh_submitted_277.sftp
echo "mget TR*" >> $ETL_HOME/temp/eh_submitted_277.sftp
sshpass -e sftp -o BatchMode=no -b $ETL_HOME/temp/eh_submitted_277.sftp $FTP_01_US@10.0.12.217 > $ETL_HOME/temp/eh_submitted_277.txt
cd $ETL_HOME/downloads/277_files/   
unzip -o emblem_837.zip
rename .dat .DAT *.dat

ls $ETL_HOME/downloads/277_files/HN*837* |
while read filename
do
    curr_file=`echo $filename | awk -F "/" '{ print $7 }'`
    aws s3api head-object --bucket acp-data --key Emblem/277/$curr_file || NOT_EXIST=true
    while read ff
    do
        echo "rename $ff ardir/$ff" >> $ETL_HOME/temp/archive_837.sftp
    done
    sshpass -e sftp -o BatchMode=no -b $ETL_HOME/temp/archive_837.sftp $FTP_01_US@10.0.12.217
    if [ $NOT_EXIST ]; then
        sed -e "s/~/\\n/g" $filename | awk -F "~" '{ print NR "|" $0}'  > $filename".txt"
        aws s3 cp $filename".txt" "s3://acp-data/Emblem/277/$curr_file"
        rm $filename".txt"
        mv '$filename' ardir/'$filename'
        sed -e "s/FILENAME/${curr_file}/g" $ETL_HOME/sql/parse_277_template.sql > $ETL_HOME/sql/parse_277.sql
        $ETL_HOME/scripts/ipsql.sh parse_277.sql
    fi
done

ls $ETL_HOME/downloads/277_files/TR*837* |
while read filename
do
    curr_file=`echo $filename | awk -F "/" '{ print $7 }'`
    aws s3api head-object --bucket acp-data --key Emblem/837/$curr_file || NOT_EXIST=true
    if [ $NOT_EXIST ]; then
        sed -e "s/~/\\n/g" $filename | awk -F "~" '{ print NR "|" $0}'  > $filename".txt"
        aws s3 cp $filename".txt" "s3://acp-data/Emblem/837/$curr_file"
        rm $filename".txt"
        mv '$filename' ardir/'$filename'
        sed -e "s/FILENAME/${curr_file}/g" $ETL_HOME/sql/parse_837_template.sql > $ETL_HOME/sql/parse_837.sql
        $ETL_HOME/scripts/ipsql.sh parse_837.sql
    fi
done


