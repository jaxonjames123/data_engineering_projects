import pyodbc
from fuzzywuzzy import fuzz

conn = pyodbc.connect(dsn="somos_redshift_1")
cursor = conn.cursor()
threshold = 91


ecw_query = """select distinct concat(concat(concat(Upper(patient_first_name), ' '), Upper(patient_last_name)),
                                        concat(' ', to_char(cast(patient_dob as date), 'YYYY-MM-DD'))) as name,
                                encounter_id,
                                appointment_provider_npi                                              as npi,
                                appointment_facility_name                                       as facility_name
                from appointments.encounter_patient
                where encounter_id is not null
                    and name is not null
                    and appointment_provider_npi is not null
                    and garage_patient_id is null
                    or garage_practice_id is null;"""
cursor.execute(ecw_query)
result = cursor.fetchall()
ecw_full_names_list = [row.name for row in result]
encounter_ids = [row.encounter_id for row in result]
npis = [str(row.npi).split(".")[0] for row in result]
ecw_patient_data = dict(zip(ecw_full_names_list, encounter_ids))
keys = list()
for name in ecw_full_names_list:
    keys.append(name[-10:])
gdw_query = f"""select distinct concat(concat(concat(upper(firstname), ' '), Upper(lastname)),
                                    concat(' ', to_char(cast(dateofbirth as date), 'YYYY-MM-DD'))) as name,
                                id
                from gdw.patients
                where id is not null
                    and name is not null
                    and to_char(cast(dateofbirth as date), 'YYYY-MM-DD') in ({', '.join(map(repr, keys))});"""
cursor.execute(gdw_query)
result = cursor.fetchall()
possible_matches = [row.name for row in result]
print(len(possible_matches))
patient_ids = [row.id for row in result]
gdw_patient_data = dict(zip(possible_matches, patient_ids))

practice_id_query = f"""select distinct t2.practiceid as practice_id, npi, t3.practicename as practice_name
                        from gdw.physician_details t1
                            left join gdw.practice_physician t2
                                on t1.id = t2.physicianid
                            left join gdw.practice_details t3
                                on t2.practiceid = t3.id
                        where npi in ({', '.join(map(repr, npis))});"""
cursor.execute(practice_id_query)
results = cursor.fetchall()
practice_ids = [row.practice_id for row in results]
gdw_npis = [row.npi for row in results]
practice_ids_dict = dict(zip(gdw_npis, practice_ids))
dictionary_to_fuzzy = {}
i = 0
while i < len(keys):
    matches = []
    for value in possible_matches:
        if keys[i][-10:] in value[-10:]:
            matches.append(value)
    print(matches)
    dictionary_to_fuzzy[ecw_full_names_list[i]] = matches
    i += 1
print(dictionary_to_fuzzy)
matches_dict = {}
for key in dictionary_to_fuzzy.items():
    for value in key:
        for possible_match in value:
            match_percentage = fuzz.ratio(key[0], possible_match)
            if match_percentage >= threshold:
                matches_dict[key[0]] = [
                    ecw_patient_data[key[0]],
                    gdw_patient_data[possible_match],
                ]
                print(matches_dict[key[0]])
print(matches_dict)
for name in matches_dict.items():
    encounter_id = name[1][0]
    patient_id = name[1][1]
    update_patient_id_query = f"""update appointments.encounter_patient set garage_patient_id = '{patient_id}' where encounter_id = '{encounter_id}';"""
    cursor.execute(update_patient_id_query)
    print(
        f"Query executed for patient_id: {patient_id} on encounter_id: {encounter_id}"
    )

for npi in practice_ids_dict.keys():
    practice = practice_ids_dict[npi]
    update_practice_id_query = f""" update appointments.encounter_patient set garage_practice_id = '{practice}' where appointment_provider_npi = '{npi}';"""
    cursor.execute(update_practice_id_query)
    print(f"Query executed for practice_id: {practice} on npi: {npi}")

cursor.commit()
print("Queries committed")
cursor.close()
conn.close()
