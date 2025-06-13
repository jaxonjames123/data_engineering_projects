import pandas as pd
import pyodbc


def load_practices_data():
    conn = pyodbc.connect(dsn="somos_redshift_1")
    query = """SELECT DISTINCT REPLACE(account_name, ',', '') AS practice_name,
                                practice_tin                   AS tin,
                                quality_indicator              AS grouping,
                                lower(a.email)                        AS primary_email,
                                lower(secondary_email)                AS secondary_email,
                                lower(other_email)                    AS other_email,
                                CASE
                                    WHEN COALESCE(lower(a.email), lower(secondary_email)) IN (lower(pta_email), lower(fpa_email)) THEN NULL
                                    WHEN pta_email IS NOT NULL AND fpa_email IS NOT NULL THEN lower(pta_email) || ',' || lower(fpa_email)
                                    ELSE COALESCE(lower(fpa_email), lower(pta_email))
                                    END                        AS bcc
                FROM zoho.zoho_accounts a
                WHERE legal_entity1 = 'true'
                and quality_indicator IS NOT NULL
                and email is not null
                and practice_tin is not null
                and account_name is not null
                and upper(practice_status) = 'ACTIVE';"""
    # query = """select REPLACE(account_name, ',', '') AS practice_name,
    #                 practice_tin                   AS tin,
    #                 quality_indicator              AS grouping,
    #                 email                          AS primary_email,
    #                 secondary_email                AS secondary_email,
    #                 other_email                    AS other_email,
    #                 CASE
    #                     WHEN COALESCE(email, secondary_email) IN (pta_email, fpa_email) THEN NULL
    #                     WHEN pta_email IS NOT NULL AND fpa_email IS NOT NULL THEN pta_email || ',' || fpa_email
    #                     ELSE COALESCE(fpa_email, pta_email)
    #                     END                        AS bcc
    #             from scratch_kayla.temp_quality_emails
    #             WHERE quality_indicator IS NOT NULL
    #             and email is not null
    #             and practice_tin is not null
    #             and account_name is not null;"""
    # test_2_query = """
    #             select DISTINCT REPLACE(practice_name, ',', '')             AS practice_name,
    #                             tin                                         as tin,
    #                             quality_indicator                           as grouping,
    #                             'jterrell@optimusha.com,jterrell@somoscommunitycare.org'                         as email,
    #                             '' as bcc
    #             from scratch_kayla.temp_quality_emails
    #             WHERE quality_indicator in ('Individual')
    #             order by grouping, email asc limit 5;"""
    df = pd.read_sql(query, conn)
    return df.fillna('')


def main():
    print(load_practices_data())


if __name__ == "__main__":
    main()
