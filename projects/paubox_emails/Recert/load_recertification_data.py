import pandas as pd
import pyodbc
import sys
from get_practices_emails_monday import monday_dictionary

practices = monday_dictionary

year_month = sys.argv[1]
year = year_month[0:4]
filename_month = int(year_month[4:6])

if filename_month < 10:
    filename_month = f"0{filename_month}"

conn = pyodbc.connect(dsn="somos_redshift_1")


tin_cac_data = {}
print(len(practices.items()))
for key, value in practices.items():
    monday_practice_name = list(value.keys())[0]
    monday_practice_data = value[monday_practice_name][0]
    monday_tin = monday_practice_data["TIN"]
    cac_practice = monday_practice_data["CAC"]
    tin_cac_data[monday_tin] = cac_practice


sql_monday_tins = ",".join(map(str, tin_cac_data.keys()))


query = f"""
select mco, member_id, member_recertification_date, member_first_name, member_last_name, 
        member_age, member_gender, member_dob, member_address1, member_address2, member_city, member_state, 
        member_zip, member_phone, pcp_npi, pcp_name, pcp_tin, 
        replace(replace(replace(replace(replace(replace(practice_name,' ','-'),',','-'),'.','-'),'&','-'),'-','-'),'/','-') as practice_name, 
        pcp_address1, pcp_address2, pcp_city, pcp_state, pcp_zip, priority_bucket, recertification_assignment
from payor.vw_mco_recertification 
where pcp_tin is not NULL 
and mco in ('HEALTHFIRST', 'EMBLEM', 'MOLINA', 'METROPLUS', 'ANTHEM', 'UHC', 'FIDELIS')
and substring(file_name, 1, 6) = '{year_month}';
"""

df = pd.read_sql(query, conn)

grouped = df.groupby(["pcp_tin", "practice_name"])
for pcp_tin, group_df in grouped:
    print(pcp_tin[0])
    try:
        if tin_cac_data[pcp_tin[0]] == "No":
            group_df.drop(columns="recertification_assignment", inplace=True)
    except Exception:
        print("Not a Monday TIN, will generate a file but will not send an email")
    group_df.to_excel(
        f"/home/etl/etl_home/downloads/recertification/new_recertification/{pcp_tin[0]}_{year}{filename_month}_{pcp_tin[1].upper()}.xlsx",
        index=False,
        header=True,
    )
