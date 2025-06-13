from datetime import datetime, timedelta

import pandas as pd
import pyodbc
from paubox_email_footers import somos_it_reports_footer as footer
from send_secure_email import PauboxMailer

formatted_final_day = (datetime.today() - timedelta(days=1)
                       ).date().strftime('%m/%d/%Y')
formatted_start_day = (datetime.today() - timedelta(days=7)
                       ).date().strftime('%m/%d/%Y')
mailer = PauboxMailer()


def load_query_from_file(filename: str) -> str:
    with open(f'/home/etl/etl_home/sql/{filename}', 'r') as file:
        query = file.read()
    return query


def update_query_params(mda_query: str, coders: pd.DataFrame, conn: pyodbc.Connection) -> pd.DataFrame:
    end_date = datetime.today().strftime('%Y%m%d')
    start_date = (datetime.today() - timedelta(days=7)
                  ).date().strftime('%Y%m%d')
    if not coders.empty:
        names = []
        for _, row in coders.iterrows():
            names.append(row['coder_first_name'] +
                         " " + row['coder_last_name'])
        name_placeholders = ', '.join(['?'] * len(names))
        mda_query = mda_query.replace(
            '{{names_placeholder}}', name_placeholders)
        params = [start_date, end_date] + names
    else:
        params = [start_date, end_date]
    print(params)
    df = pd.read_sql(sql=mda_query, params=params, con=conn)  # type: ignore
    return df


def main():
    date = datetime.today().strftime('%Y%m%d')
    filenames = {
        'MDA-RMM.sql': {
            'filename': f'{date}_MDAnalytics_Code_Capture_Report_Ranga.xlsx',
            'recipient': 'ranga@reliablemedicalmgmt.com',
            'cc': ['fjilani@somoscommunitycare.org', 'jvasquez@somoscommunitycare.org'],
            'name': 'Ranga'
        },
        'MDA-Optimus.sql': {
            'filename': f'{date}_MDAnalytics_Code_Capture_Report.xlsx',
            'recipient': 'jvasquez@somoscommunitycare.org',
            'cc': ['fjilani@somoscommunitycare.org'],
            'name': 'Jellie'
        }
    }
    conn = pyodbc.connect(dsn="md_analytics_prd")
    coders = pd.read_csv('/home/etl/etl_home/downloads/coders.csv')
    for file in filenames:
        message = f"""
            <style>
                .spaced-text {{ line-height: 1; }}
                .double-spaced-text {{ line-height: 2; }}
                .underline {{ text-decoration: underline; }}
            </style>
            <p class="double-spaced-text">Hi {filenames[file]['name']},</p>
            <p>Attached to this email, please find this week's Code Capture Report.</p>
            <p><span class="underline">Reporting Period</span>: {formatted_start_day} - {formatted_final_day}</p>
            <p class="double-spaced-text">Data Source: MD Analytics as of {formatted_final_day}</p>
            <p class="double-spaced-text">Please reach out to us with any questions.</p>
            <p class="spaced-text">Thank You,</p>
            """
        output_file = f'/home/etl/etl_home/downloads/{filenames[file]["filename"]}'
        print(output_file)
        query = load_query_from_file(file)
        df = update_query_params(mda_query=query, coders=coders if file == 'MDA-RMM.sql' else pd.DataFrame(),
                                 conn=conn)
        df.to_excel(output_file, header=True, index=False)
        response = mailer.send_mail(recipients=filenames[file]['recipient'], subject='Optimus Code Capture Report',
                                    attachments=output_file, message=message, footer=footer, cc=','.join(filenames[file]['cc']), delivery_email_address="no-reply-itreports@somoscommunitycare.org")
        status = mailer.check_delivery_status(response)
        mailer.update_delivery_status_table(recipients_data=status, email_type='mdanalytics', tins=[],
                                            practice_names=[], for_month=datetime.today().strftime('%Y%m'), filenames=[output_file.split('/')[5]])


if __name__ == '__main__':
    main()
