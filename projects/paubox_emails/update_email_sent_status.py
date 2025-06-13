import pandas as pd
import pyodbc
from send_secure_email import PauboxMailer

mailer = PauboxMailer()


def get_processing_emails():
    conn = pyodbc.connect(dsn="somos_redshift_1")
    query = """select * from paubox.emails_sent_status where email_status in ('processing', 'soft bounced');"""
    df = pd.read_sql_query(query, conn)  # type: ignore
    df = df.drop_duplicates()
    df_multi = df.set_index(["tracking_id", "email"])
    df_multi.to_dict(orient="index")
    return df_multi


def write_updates_to_file(df):
    records = df
    for (tracking_id, email), row in records.iterrows():
        status = mailer.check_delivery_status(tracking_id)
        file = mailer.update_delivery_status_table(
            status,
            row["email_type"],
            row["tin"].split(", "),
            row["practice_name"].split(", "),
            row["for_month"],
            row["file_name"].split(", "),
        )
    return file if file is not None else None


def main():
    emails = get_processing_emails()
    output_file = write_updates_to_file(emails)
    print(f"Finished writing to: {output_file}")


if __name__ == "__main__":
    main()
