import os

import pandas as pd
import psycopg2
import pyodbc


def get_schedule_id(conn):
    cursor = conn.cursor()
    query = "select max(schedule_id) from aim.schedule_all;"
    cursor.execute(query)
    return cursor.fetchone()[0]


def get_future_appointment_ids(conn):
    cursor = conn.cursor()
    query = "select mid, schedule_id from aim.schedule_all where cast(date_time as date) > CURRENT_DATE;"
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    return df


def remove_future_sophia_appointments(conn):
    cursor = conn.cursor()
    query = "delete from aim.schedule_all where cast(date_time as date) > CURRENT_DATE;"


def retrieve_appointments_data(conn, schedule_id):
    query = f"""
    select distinct member_id                                                                            as mid,
                    cast((appointment_date || ' ' || appointment_start_time) as timestamp)               as date_time,
                    t3.somos_provider_id                                                                 as somos_provider_id,
                    t3.provider_location_id                                                              as somos_practice_id,
                    null                                                                                 as create_date,
                    null                                                                                 as synchronization_id,
                    null                                                                                 as synchronization,
                    null                                                                                 as synchronization_status,
                    appointment_provider_npi                                                             as appointment_npi,
                    tin                                                                                  as practice_tin,
                    {schedule_id} +
                    ROW_NUMBER() OVER (ORDER BY mid, date_time, t3.somos_provider_id, somos_practice_id) as schedule_id,
                    status                                                                               as appointment_status
    from appointments.ecw_appointments t1
            inner join sdw.dim_provider t2
                        on t1.appointment_provider_npi = t2.npi
            inner join (select somos_provider_id, provider_location_id
                        from (SELECT somos_provider_id,
                                    provider_location_id,
                                    ROW_NUMBER() OVER (PARTITION BY somos_provider_id ORDER BY added_tz DESC) AS RN
                            FROM sdw.sdw_fact
                            where provider_location_id is not null) AS sub_t1
                        WHERE RN = 1) t3
                        on t2.somos_provider_id = t3.somos_provider_id
            left join (select tin, status
                        from appointments.appointment_download_status
                        where date_of_exec >= dateadd(hour, 17,
                                                    trunc(CONVERT_TIMEZONE('UTC', 'America/New_York', SYSDATE)) -
                                                    INTERVAL '1 day')) t4
                    on split_part(t1.file_name, '_', 1) = t4.tin
    where member_id is not null
        and practice_tin != '0000000000'
        and appointment_date >= getdate();
    """
    df = pd.read_sql(query, conn)
    df.to_csv(
        "/home/etl/etl_home/downloads/sophia/schedule_all_data.csv",
        sep=",",
        index=False,
    )
    print("Appointment data saved in downloads/sophia/schedule_all_data.csv")


def main():
    sophia_connection_creds = {
        "dbname": os.environ.get("SOPHIA_DB_DEV"),
        "user": os.environ.get("SOPHIA_USER_DEV"),
        "password": os.environ.get("SOPHIA_PWD_DEV"),
        "host": os.environ.get("SOPHIA_HOST"),
        "port": os.environ.get("SOPHIA_PORT"),
    }
    sophia_conn = psycopg2.connect(**sophia_connection_creds)
    appointments_conn = pyodbc.connect(dsn="somos_redshift_1")

    schedule_id = get_schedule_id(sophia_conn)
    retrieve_appointments_data(appointments_conn, schedule_id)


if __name__ == "__main__":
    main()
