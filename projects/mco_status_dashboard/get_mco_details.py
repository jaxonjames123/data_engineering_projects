import os

import pandas as pd
import pyodbc

etlhome = os.environ.get("ETL_HOME")
conn = pyodbc.connect(dsn="somos_redshift_1")


def get_mco_details(connection):
    with open(etlhome + "/sql/get_mco_details.sql", "r") as file:
        query = file.read()
        pd.read_sql(query, connection).to_pickle(etlhome + "/downloads/mco_data.pkl")


def get_roster_details(connection):
    with open(etlhome + "/sql/get_roster_details.sql", "r") as file:
        query = file.read()
        pd.read_sql(query, connection).to_pickle(etlhome + "/downloads/roster_data.pkl")


def get_claim_details(connection):
    with open(etlhome + "/sql/get_claim_details.sql", "r") as file:
        query = file.read()
        pd.read_sql(query, connection).to_pickle(etlhome + "/downloads/claim_data.pkl")


def get_pharmacy_details(connection):
    with open(etlhome + "/sql/get_pharmacy_details.sql", "r") as file:
        query = file.read()
        pd.read_sql(query, connection).to_pickle(etlhome + "/downloads/rx_data.pkl")


def get_gic_details(connection):
    df = pd.DataFrame()
    mco_ipas = [
        ("Anthem", "SOMOS"),
        ("Anthem", "Balance"),
        ("Elderplan", "Balance"),
        ("Emblem", "SOMOS"),
        ("Emblem", "Balance"),
        ("Emblem", "Corinthian"),
        ("Fidelis", "SOMOS"),
        ("Healthfirst", "SOMOS"),
        ("Healthfirst", "Excelsior"),
        ("Healthfirst", "Corinthian"),
        ("Humana", "SOMOS"),
        ("MetroPlus", "SOMOS"),
        ("United", "SOMOS"),
        ("VNS", "Balance"),
        ("VillageCare", "SOMOS"),
        ("Wellcare", "Balance"),
    ]
    with open(etlhome + "/sql/get_gic_details.sql", "r") as file:
        query = file.read()

        for mco, ipa in mco_ipas:
            currentquery = query.format(mco=mco, ipa=ipa)
            df = pd.concat([df, pd.read_sql(currentquery, connection)])

        df.to_pickle(etlhome + "/downloads/gic_data.pkl")


get_mco_details(conn)
get_roster_details(conn)
get_claim_details(conn)
get_pharmacy_details(conn)
get_gic_details(conn)
