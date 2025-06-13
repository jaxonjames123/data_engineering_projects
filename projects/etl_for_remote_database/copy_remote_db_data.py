import argparse
import csv
import logging
import os
import re

import mysql.connector
import pyodbc
from retry import retry
from s3fs.core import S3FileSystem
from select_queries import QUERY_REGISTRY

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


class RedshiftConnector:
    def __init__(self, dsn="somos_redshift_1"):
        self.dsn = dsn

    def connect(self):
        return pyodbc.connect(dsn=self.dsn)


class MSSQLConnector:
    def __init__(self, dsn="health_catalyst_sql") -> None:
        self.dsn = dsn
        self.uid = os.environ.get("HEALTH_CATALYST_PYODBC_UID")
        self.pwd = os.environ.get("HEALTH_CATALYST_PYODBC_PWD")
        self.timeout = "connection_timeout=0"

    def connect(self):
        return pyodbc.connect(dsn=f"{self.dsn};uid={self.uid};pwd={self.pwd};{self.timeout}")


class MySQLConnector:
    def __init__(self, env_prefix="SCN_FINDHELP"):
        self.env = {k: os.getenv(f"{env_prefix}_{k}") for k in [
            "HOST", "UID", "PWD", "DB", "SSL_CA", "SSL_CERT", "SSL_KEY"]}

    def connect(self):
        return mysql.connector.connect(
            host=self.env["HOST"],
            port=3306,
            user=self.env["UID"],
            password=self.env["PWD"],
            database=self.env["DB"],
            ssl_ca=self.env["SSL_CA"],
            ssl_cert=self.env["SSL_CERT"],
            ssl_key=self.env["SSL_KEY"]
        )


def clean_csv_none(input_path: str, output_path: str):
    with open(input_path, "r") as infile, open(output_path, "w") as outfile:
        for line in infile:
            outfile.write(line.replace("None", ""))


def remove_file(path: str):
    try:
        os.remove(path)
    except FileNotFoundError:
        log.warning(f"File not found: {path}")


class DataPipeline:
    def __init__(self, redshift_name, file_path, conn, source_schema, index_query):
        self.redshift_name = redshift_name
        self.file_path = file_path
        self.conn = conn
        self.source_schema = source_schema
        self.index_query = index_query
        self.database_name = ''

    def get_max_load_index(self, table_name):
        cur = self.conn.cursor()
        try:
            query = self.index_query + f"{table_name};"
            log.info(query)
            cur.execute(query)
            result = cur.fetchone()
            log.info(result)
            return result[0] if result and result[0] else 0
        finally:
            cur.close()

    def extract_table_name(self, query: str):
        try:
            pattern = re.compile(
                rf"{self.source_schema + '.'}(\w+)", re.IGNORECASE)
            match = pattern.search(query)
            log.info(
                msg=f'Match: {match.group(1).lower() if match else "unknown_table"}')
            return match.group(1).lower() if match else "unknown_table"
        except Exception:
            return "unknown_table"

    @retry(tries=3, delay=2)
    def copy_to_redshift(self, file_path: str, table_name: str):
        if not os.path.exists(file_path):
            log.warning(f"File does not exist for COPY: {file_path}")
            return

        s3 = S3FileSystem(anon=False)
        s3_key = f"sftp_test/{self.redshift_name.replace('.', '/')}{self.source_schema}/{table_name}.csv"
        s3.put(file_path, s3_key)
        log.info(f"Uploaded {file_path} to s3://{s3_key}")

        copy_query = f"""
            COPY {self.redshift_name}{self.source_schema}_{table_name}
            FROM 's3://{s3_key}'
            IAM_ROLE 'arn:aws:iam::042108671686:role/myRedshiftRole'
            REGION 'us-east-1'
            FORMAT AS CSV
            DELIMITER '\\t'
            BLANKSASNULL
        """
        log.info(copy_query)
        with RedshiftConnector().connect() as conn:
            cur = conn.cursor()
            cur.execute(copy_query)
            conn.commit()
            log.info(
                f"Executed COPY for {self.redshift_name}{self.source_schema}_{table_name}")

        remove_file(file_path)

    def process_file(self, rows, file_name, file_number, schema_name):
        log.info(f'Schema Name: {schema_name}\nFile Name: {file_name}')
        path = f"{self.file_path}{schema_name}/{file_name}_{file_number}.csv"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "a", newline="") as f:
            writer = csv.writer(f, delimiter="\t")
            for row in rows:
                # cleaned_row = [str(col).replace(
                #     "\n", "").replace("\r", "") for col in row]
                writer.writerow(row)
        return path

    def run_query_pipeline(self, query: str):
        try:
            cur = self.conn.cursor()
            if 'USE' in query:
                self.database_name = query.split('USE ')[1].split(';')[0]
                source_table_name = query.split("FROM ")[1].split()[0]
                self.source_schema = source_table_name.split('.')[0]
                table_name = self.database_name + '.' + source_table_name
                cur.execute(query.split(';')[0])
                query = query.split(';')[1]
            try:
                max_index = self.get_max_load_index(
                    table_name=table_name)
                log.info(f'Max INdex: {max_index}')
                query = query.replace("?", f"'{str(max_index).split(' ')[0]}'")
            except Exception:
                log.info("No Indexing Needed")
            table_name = self.extract_table_name(query)
            log.info(table_name)
            file_name = table_name
            log.info(f'Query: {query}')
            cur.execute(query)
            with RedshiftConnector().connect() as conn:
                red_cur = conn.cursor()
                delete_query = f'delete from {self.redshift_name}{self.source_schema}_{table_name}'
                log.info(delete_query)
                red_cur.execute(delete_query)
                conn.commit()
                red_cur.close()
                log.info(
                    f"Executed DELETE for {self.redshift_name}{self.source_schema}_{table_name}")
            batch_size = 100000
            file_number = 1
            rows_processed = 0

            while True:
                rows = cur.fetchmany(batch_size)
                if not rows:
                    break
                file_path = self.process_file(
                    rows, file_name, file_number, self.source_schema)
                rows_processed += len(rows)
                log.info(f"{rows_processed} rows have been processed")
                if rows_processed >= 1000000:
                    self.copy_to_redshift(file_path, table_name)
                    rows_processed = 0
                    file_number += 1
            if rows_processed == 0 and file_number == 1:
                log.error("No rows were processed, table is empty")
                return
            cur.close()
            log.info(table_name)
            # Final batch
            self.copy_to_redshift(file_path, table_name)
        except Exception:
            log.exception(
                f"Failed to process query for table {self.extract_table_name(query)}")


# --- Argument Interface ---
def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", type=str, help="Data source: HC or FINDHELP")
    parser.add_argument("tables", nargs="*", help="Optional specific tables")
    return parser.parse_args()


# --- Main Orchestration ---
def main():
    args = parse_args()
    source = args.source.upper()

    if source == "HC":
        selected_queries = [
            QUERY_REGISTRY[source][name] for name in args.tables if name in QUERY_REGISTRY[source]
        ] if args.tables else [
            v for k, v in QUERY_REGISTRY[source].items()
        ]

        conn = MSSQLConnector().connect()
        pipeline = DataPipeline(redshift_name="health_catalyst.", file_path="/home/etl/etl_home/downloads/healthcatalyst/hc_sql_server_csv_data/",
                                conn=conn, source_schema=r"(?:Summary|Reference|Result|ResultLoad|Terminology|Claim|CareManagement|Patient|IntegratedCore|DataCore|Measures)", index_query="select MAX(lastloaddts) from ")
        for q in selected_queries:
            pipeline.run_query_pipeline(q)

    elif source == "SOMOS" or source == 'SOMOS_MEMBER_INSIGHTS':
        selected_queries = [
            QUERY_REGISTRY[source][name] for name in args.tables if name in QUERY_REGISTRY[source]
        ] if args.tables else [
            v for k, v in QUERY_REGISTRY[source].items()
        ]

        conn = MySQLConnector().connect()
        pipeline = DataPipeline(redshift_name="scn.", file_path="/home/etl/etl_home/downloads/scn/scn_sql_server_data/",
                                conn=conn, source_schema=source, index_query=None)
        for q in selected_queries:
            pipeline.run_query_pipeline(q)

    else:
        log.error(f"Unsupported source: {source}")


if __name__ == "__main__":
    main()
