import os
import re

import mysql.connector

tables = []


conn = mysql.connector.connect(
    host=os.environ.get("SCN_FINDHELP_HOST"),
    port=3306,
    user=os.environ.get("SCN_FINDHELP_UID"),
    password=os.environ.get("SCN_FINDHELP_PWD"),
    database=os.environ.get("SCN_FINDHELP_DB"),
    ssl_ca=os.environ.get("SCN_SSL_CA"),
    ssl_cert=os.environ.get("SCN_SSL_CERT"),
    ssl_key=os.environ.get("SCN_SSL_KEY"),
)
for table in tables:
    table_name = table
    cursor = conn.cursor()
    cursor.execute(f"SHOW CREATE TABLE {table_name}")
    create_stmt = cursor.fetchone()[1]
    # Replace MySQL data types with Redshift equivalents
    type_mapping = {
        r"\bint\b": "INTEGER",
        r"\bbigint\b": "BIGINT",
        r"\btinyint\(1\)\b": "BOOLEAN",
        r"\btext\b": "VARCHAR(MAX)",
        r"\bmediumtext\b": "VARCHAR(MAX)",
        r"\blongtext\b": "VARCHAR(MAX)",
        r"\bdatetime\b": "TIMESTAMP",
        r"\bdecimal\((\d+),(\d+)\)\b": r"NUMERIC(\1,\2)",
        r"\bvarchar\(\d+\)\b": "VARCHAR(MAX)",  # Convert all VARCHAR(n) to VARCHAR(MAX)
    }
    for mysql_type, redshift_type in type_mapping.items():
        create_stmt = re.sub(
            mysql_type, redshift_type, create_stmt, flags=re.IGNORECASE
        )
    # Remove constraints, defaults, keys, and identity columns
    create_stmt = re.sub(
        r"(?i)AUTO_INCREMENT", "", create_stmt
    )  # Remove auto_increment
    create_stmt = re.sub(r"(?i)PRIMARY KEY.*?,", "", create_stmt)  # Remove primary keys
    create_stmt = re.sub(
        r"(?i)PRIMARY KEY.*?\)", ")", create_stmt
    )  # If at end, close parenthesis
    create_stmt = re.sub(r"(?i)FOREIGN KEY.*?,", "", create_stmt)  # Remove foreign keys
    create_stmt = re.sub(
        r"(?i)FOREIGN KEY.*?\)", ")", create_stmt
    )  # If at end, close parenthesis
    create_stmt = re.sub(
        r"(?i)DEFAULT .*?(,|\))", "\1", create_stmt
    )  # Remove default values
    create_stmt = re.sub(r"(?i)CONSTRAINT .*?,", "", create_stmt)  # Remove constraints
    create_stmt = re.sub(
        r"(?i)CONSTRAINT .*?\)", ")", create_stmt
    )  # If at end, close parenthesis
    create_stmt = re.sub(
        r"(?i)UNIQUE.*?,", "", create_stmt
    )  # Remove unique constraints
    create_stmt = re.sub(
        r"(?i)UNIQUE.*?\)", ")", create_stmt
    )  # If at end, close parenthesis
    create_stmt = re.sub(r"(?i)INDEX .*?,", "", create_stmt)  # Remove indexes
    create_stmt = re.sub(
        r"(?i)INDEX .*?\)", ")", create_stmt
    )  # If at end, close parenthesis
    # Clean up empty parentheses and commas left from removals
    create_stmt = re.sub(
        r",\s*\)", ")", create_stmt
    )  # Remove trailing commas before closing parentheses
    create_stmt = re.sub(
        r"\(\s*,", "(", create_stmt
    )  # Remove leading commas inside parentheses
    # Remove MySQL-specific options
    create_stmt = re.sub(r"(?i)ENGINE=\w+", "", create_stmt)  # Remove ENGINE=...
    create_stmt = re.sub(
        r"(?i)DEFAULT CHARSET=\w+", "", create_stmt
    )  # Remove DEFAULT CHARSET
    create_stmt = re.sub(r"(?i)COLLATE \w+", "", create_stmt)  # Remove COLLATE
    print(create_stmt)

conn.close()
