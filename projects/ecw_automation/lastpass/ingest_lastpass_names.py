import os
import pandas as pd
import re

VALID_URLS = ('eclinicalweb.com', 'ecwcloud.com', 'mobiledoc')


def filter_csv(file_path, output_file_path):
    column_name = 'grouping'
    value = 'Shared-SOMOS Practices'
    df = pd.read_csv(file_path, usecols=[
                     'url', 'username', 'password', 'name', 'grouping'])
    filtered_df = df[df[column_name] == value]
    filtered_df[['name', 'tin']] = filtered_df['name'].str.split(
        ' - ', n=1, expand=True)
    filtered_df['tin'] = filtered_df['tin'].str.strip()
    filtered_df = filtered_df.drop(columns=['grouping'])
    filtered_df.to_csv(output_file_path, index=False)


def format_df(df):
    df[['name', 'tin']] = df['name'].str.split(' - ', n=1, expand=True).trim()
    df.loc[:, 'url'] = df['url'].apply(lambda x: re.search(r'http[s]?://(.*?)(?:app\.|\.netstem|\:8080)', x).group(
        1) if re.search(r'http[s]?://(.*?)(?:app\.|\.netstem|\:8080)', x) else x)
    pass


def generate_usernames_passwords_dict(df):
    usernames_passwords_dict = {}
    for index, row in df.iterrows():
        if any(keyword in row['url'] for keyword in VALID_URLS):
            tin = row['name'].str.split(' - ', n=1)
            usernames_passwords_dict[row['url']] = (
                row['username'], row['password'])
    return usernames_passwords_dict


def main():
    file_path = f"{os.environ.get('ETL_HOME')}/downloads/ecw/lastpass/lastpass_vault_export.csv"
    output_file_path = f"{os.environ.get('ETL_HOME')}/downloads/ecw/lastpass/lastpass_vault_export_formatted.csv"
    filter_csv(file_path=file_path, output_file_path=output_file_path)


if __name__ == '__main__':
    main()
