import pyodbc
from fuzzywuzzy import fuzz
from collections import defaultdict


def get_ecw_patients(conn):
    ecw_patients = defaultdict(list)
    cur = conn.cursor()
    query = "select * from appointments.patient_details;"
    cur.execute(query)
    for row in cur.fetchall():
        ecw_patients[row[0]].append(row[1])
    cur.close()
    return ecw_patients


def get_garage_patient_ids(conn):
    gdw_patients = defaultdict(list)
    cur = conn.cursor()
    query = """
    select distinct cast(dateofbirth as date)           as dob,
                    upper(firstname || ' ' || lastname) as patient_name,
                    id                                  as garage_patient_id
    from gdw.patients
    where id is not null
    and patient_name is not null
    and patients.dateofbirth is not null; 
    """
    cur.execute(query)
    for row in cur.fetchall():
        gdw_patients[row[0].strftime('%Y-%m-%d')].append((row[1], row[2]))
    cur.close()
    return gdw_patients


def update_appointmets_table(conn, query):
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()
    cur.close()


def fuzzy_compare(ecw_patients, gdw_patients, threshold, conn):
    matched_pairs = []
    for ecw_dob, ecw_names in ecw_patients.items():
        if ecw_dob in gdw_patients:
            for ecw_name in ecw_names:
                best_match = None
                best_score = 0
                for gdw_patient in gdw_patients[ecw_dob]:
                    score = fuzz.ratio(ecw_name, gdw_patient[0])
                    if score > best_score and score >= threshold:
                        best_match = gdw_patient
                        best_score = score
                if best_match:
                    matched_pairs.append(
                        (ecw_name, best_match[0], best_match[1], best_score))
    if matched_pairs:
        with conn.cursor() as cur:
            query_delete = "DELETE FROM appointments.ecw_gdw_fuzzy_match;"
            cur.execute(query_delete)
            query = """
                insert into appointments.ecw_gdw_fuzzy_match (ecw_name, gdw_name, gdw_patient_id, match_confidence)
                values (?, ?, ?, ?);
            """
            cur.executemany(query, matched_pairs)
            conn.commit()
    return matched_pairs


def main():
    conn = pyodbc.connect(dsn="somos_redshift_1")
    ecw_patients = get_ecw_patients(conn)
    gdw_patients = get_garage_patient_ids(conn)
    fuzzy_compare(ecw_patients, gdw_patients, 70, conn)
    garage_patient_ids_query = """
        update appointments.ecw_appointments
        set garage_patient_id = gdw_patient_id
        from appointments.ecw_gdw_fuzzy_match
        where upper(patient_first_name || ' ' || patient_last_name) = ecw_name
          and added_tz in (select max(added_tz) from appointments.ecw_appointments);
    """
    garage_practice_ids_query = """
        update appointments.ecw_appointments
        set garage_practice_id = t1.practice_id
        from (select t1.tin as tin,
                      t2.id  as practice_id
              from appointments.lastpass_export t1
                      inner join gdw.practice_details t2 on t1.tin = t2.tin) t1
        where split_part(file_name, '_', 1) = t1.tin
          and added_tz in (select max(added_tz) from appointments.ecw_appointments);
    """
    temp_table_member_ids_query = """
        create temp table temp_member_id as
            with cte_1 as (select mco_id, max(incurred_month_id) as max_month_id, product_id
                          from sdw.sdw_fact
                          group by mco_id, product_id)
            select distinct t2.mco_patient_member_id as member_id, t1.garage_patient_id
            from appointments.ecw_appointments t1
                    inner join gdw.emr_to_mco_map t2 on t1.garage_patient_id = t2.garage_patient_id
                    inner join sdw.dim_member t3 on t2.mco_patient_member_id = t3.health_plan_subscriber_id
                    inner join sdw.sdw_fact t4 on t3.somos_member_id = t4.somos_member_id
                    inner join cte_1 on t4.incurred_month_id = cte_1.max_month_id
                and t4.product_id = cte_1.product_id;
    """
    update_member_ids_query = """
        update appointments.ecw_appointments
        set member_id = temp_member_id.member_id
        from temp_member_id
        where appointments.ecw_appointments.garage_patient_id = temp_member_id.garage_patient_id;
    """
    update_appointmets_table(conn, garage_patient_ids_query)
    update_appointmets_table(conn, garage_practice_ids_query)
    update_appointmets_table(conn, temp_table_member_ids_query)
    update_appointmets_table(conn, update_member_ids_query)


if __name__ == '__main__':
    main()
