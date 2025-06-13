import pandas as pd
import pyodbc
from fuzzywuzzy import fuzz

conn = pyodbc.connect(dsn="somos_redshift_1")
cursor = conn.cursor()
with open("Lab Result Name.csv", mode="r") as file:
    lab_result_name_file = pd.read_csv(file)
with open("Loinc code mapping.csv", mode="r") as file:
    loinc_code_mapping_file = pd.read_csv(file)


def check_data(code_mapping, lab_result_name):
    target_dict = {}
    keys_list = []
    full_name_list = []
    possible_values = []
    matched_list = []
    k = 0
    ratio = 91
    matched_codes = 0
    for i in range(len(code_mapping)):
        keys_list.append(code_mapping.iloc[i, 0].lower()[0:5])
    for i in range(len(code_mapping)):
        full_name_list.append(code_mapping.iloc[i, 0].lower())
    for i in range(len(lab_result_name)):
        possible_values.append(lab_result_name.iloc[i, 0].lower())
    while k < len(keys_list):
        values_list = []
        for value in possible_values:
            if keys_list[k] in value[0:5]:
                values_list.append(value)
        target_dict[full_name_list[k]] = values_list
        k += 1
    print(target_dict.items())
    for loinc_code_mapping_name in target_dict.items():
        for row in loinc_code_mapping_name:
            for lab_result in row:
                new_ratio = fuzz.ratio(loinc_code_mapping_name[0], lab_result)
                if new_ratio >= ratio:
                    matched_list.append(
                        [
                            lab_result,
                            loinc_code_mapping_name[0],
                            grab_loinc_code(loinc_code_mapping_name[0], code_mapping),
                            new_ratio,
                        ]
                    )
                    matched_codes += 1
    total_non_matches = len(lab_result_name) - matched_codes
    print(total_non_matches)
    return pd.DataFrame(
        matched_list,
        columns=["lab_result_name", "loinc_code_mapping_name", "loinc_code", "score"],
    )


def grab_loinc_code(name, file):
    for row in range(len(file)):
        if name == file.iloc[row, 0].lower():
            return file.iloc[row, 1]


def add_to_db(match_data):
    cursor.execute("DELETE FROM gdw.loinc_code_match;")
    df = match_data
    for index, row in df.iterrows():
        cursor.execute(
            "INSERT INTO gdw.loinc_code_match (lab_result_name, loinc_code_mapping_name, loinc_code, score) values(?,?,?,?)",
            row.lab_result_name,
            row.loinc_code_mapping_name,
            row.loinc_code,
            row.score,
        )
    conn.commit()
    conn.close()


def main():
    add_to_db(check_data(loinc_code_mapping_file, lab_result_name_file))


if __name__ == "__main__":
    main()
