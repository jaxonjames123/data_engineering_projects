$ETL_HOME/scripts/ipsql.sh copy_scn_assessments_historic_data.sql
cd /home/etl/etl_home/lib
python copy_remote_db_data.py SOMOS
python copy_remote_db_data.py SOMOS_MEMBER_INSIGHTS
OUTPUT_FILE="/home/etl/etl_home/downloads/scn_latest_update_date.txt"
/home/etl/etl_home/scripts/ipsql.sh scn_get_last_update_date.sql > "$OUTPUT_FILE"
mail -s "SCN Findhelp Table Ingestion" ssundararaman@somoscommunitycare.org jterrell@somoscommunitycare.org kyu@somoscommunitycare.org < "$OUTPUT_FILE"