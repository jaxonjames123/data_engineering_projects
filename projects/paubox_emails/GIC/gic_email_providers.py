import json
import os
from collections import defaultdict
from datetime import datetime

import pandas as pd
import pyodbc
from get_gic_email_practices import load_practices_data
from paubox_email_footers import quality_footer as footer
from send_secure_email import PauboxMailer

conn = pyodbc.connect(dsn="somos_redshift_1")
current_month = datetime.now().strftime("%B")
current_month_str = datetime.today().strftime('%Y%m')
mailer = PauboxMailer()
subject = f"Action Required: {current_month} SOMOS Quality Report - Close Care Gaps Now"
email_message = f"""
<style>
    .spaced-text {{ margin-bottom: 20px; }}
    .double-spaced-text {{ margin-bottom: 20px; }}
</style>
<p class="spaced-text"><strong>Good Morning,</strong></p>
<p class="spaced-text">We are pleased to share your <strong>{current_month} Care Gap Report</strong>, which includes both HEDIS quality measure data and EMR-documented supplemental data from our in-house analytics. Use this report to <strong>proactively engage members and close care gaps for 2025.</strong></p>
<p class="spaced-text"><strong>How to Use Your Care Gap Report: </strong></p>
<ol><li> <strong>Review all Health Plan contracted measures</strong> to identify care gap opportunities.</li>
<li><strong>Leverage key data fields</strong> to enhance member outreach, documentation, claims submission, and overall care quality:
<ul><li><strong>Column AG - Deadline Date:</strong> Focus on care gaps with approaching deadlines to ensure they are addressed in time.</li>
<li><strong>Column AM - SOMOS Numerator:</strong> Identifies gaps closed based on supplemental data, claims, and clinical documentation.</li>
<li><strong>Column AW - Latest PCP Claim Date:</strong> Confirms the most recent claim received. Open gaps may indicate missing codes or pending claim processing.</li>
<li><strong>Column AZ - Non-User Flag:</strong> Highlights members without a PCP claim in the measurement year. Prioritize these members to ensure outreach and engagement.</li></ul>
</li>
</ol>
<p><strong>Take Immediate Action: </strong></p>
<ul>
<li><strong>Schedule Appointments:</strong> Filter <strong>Column AM (Numerator = 0)</strong> to identify open gaps and coordinate necessary services.</li>
<li><strong>Ensure Accurate Billing:</strong> Billers should verify <strong>CPTII codes and condition-specific specifications -</strong> consult the SOMOS Quality Tip Sheet.</li>
<li><strong>Optimize EMR Connectivity:</strong> Properly <strong>lock encounters and generate claims</strong> to ensure accurate submission to health plans.</li>
<li><strong>Utilize Incentives:</strong> Leverage <strong>the Bill Above & Vaccine Administration Program</strong> for Innovator arrangements.</li>
</ul>
<p class="double-spaced-text"><strong>Need Assistance? </strong> Reply to this email to connect with your SOMOS Practice Transformation & Field Practice Associate for support. </p>
<p class="spaced-text"><strong>Thank you for your commitment to quality care.</strong></p>
<p>Best regards,</p>
<p><strong>SOMOS Quality Team</strong></p>
"""


def generate_filename(tins):
    joined_tins = "_".join(tins) if isinstance(tins, list) else tins
    return f"/home/etl/etl_home/downloads/SOMOS_Quality_Report_{current_month_str}_{joined_tins}.xlsx"


def run_query_to_excel(tins):
    tin_str = "', '".join(tins) if isinstance(tins, list) else tins
    query = f"""
        SELECT * FROM zoho.monthly_measurable_data 
        WHERE contracted = 'YES' 
        AND pcp_tin IN ('{tin_str}');
    """
    print(f"Query: {query}")
    df = pd.read_sql(query, conn)
    file = generate_filename(tins)
    df.to_excel(file, index=False)
    return file


def send_email_and_log(recipients, bcc, tins, practice_names, files, grouping):
    response = mailer.send_mail(
        recipients=",".join(recipients),
        subject=subject,
        attachments=",".join(files),
        message=email_message,
        bcc=",".join(bcc),
        delivery_email_address="quality@somosipa.com",
        footer=footer,
        logo="somos_innovation_logo"
    )
    delivery_status = mailer.check_delivery_status(response)
    mailer.update_delivery_status_table(
        recipients_data=delivery_status,
        email_type="quality",
        tins=tins,
        practice_names=practice_names,
        for_month=current_month_str,
        filenames=[os.path.basename(f) for f in files]
    )
    print(f"Grouping: {grouping}")
    print(f"Practice Names: {practice_names}")
    print(f"Email To: {', '.join(recipients)}")
    print(f"BCC: {', '.join(bcc)}")
    print(f"Files attached to email: {', '.join(files)}\n")


def organize_practices():
    practices = defaultdict(lambda: defaultdict(dict))
    for _, row in load_practices_data().iterrows():
        grouping = row['grouping']
        tin = row['tin']
        primary_email = row['primary_email']
        practices[grouping][primary_email][tin] = {
            'name': row['practice_name'],
            'secondary_email': row['secondary_email'],
            'other_email': row['other_email'],
            'bcc': row['bcc']
        }
    return json.loads(json.dumps(practices))


def load_data(practices, grouping):
    for primary_email, tin_dict in practices[grouping].items():
        tins = list(tin_dict.keys())
        practice_names = [d['name'] for d in tin_dict.values()]
        bcc_emails = {d['bcc'] for d in tin_dict.values() if d.get('bcc')}
        files = []

        if grouping == 'Practice Group':
            for tin in tins:
                data = tin_dict[tin]
                file = run_query_to_excel(tin)
                files.append(file)
            send_email_and_log([primary_email], bcc_emails,
                               tins, practice_names, files, grouping)

            for tin, data in tin_dict.items():
                file = generate_filename(tin)
                recipients = list(filter(None, [data.get('secondary_email'), data.get('other_email')]))
                if recipients:
                    send_email_and_log(
                        recipients=recipients,
                        bcc=[data['bcc']] if data.get('bcc') else [],
                        tins=[tin],
                        practice_names=[data['name']],
                        files=[file],
                        grouping=grouping
                    )

        elif grouping == 'Combined TIN':
            file = run_query_to_excel(tins)
            # All emails (primary, secondary, other) = recipients
            recipient_emails = set()
            for data in tin_dict.values():
                recipient_emails.update(filter(
                    None, [primary_email, data.get('secondary_email'), data.get('other_email')]))
            send_email_and_log(
                recipients=list(recipient_emails),
                bcc=bcc_emails,
                tins=tins,
                practice_names=practice_names,
                files=[file],
                grouping=grouping
            )

        elif grouping == 'Individual':  # Individual
            for tin, data in tin_dict.items():
                file = run_query_to_excel(tin)
                recipients = list(filter(None, [primary_email, data.get(
                    'secondary_email'), data.get('other_email')]))
                bcc = [data['bcc']] if data.get('bcc') else []
                if recipients:
                    send_email_and_log(
                        recipients=recipients,
                        bcc=bcc,
                        tins=[tin],
                        practice_names=[data['name']],
                        files=[file],
                        grouping=grouping
                    )
    # if grouping == 'Practice Group':
    #     # Step 1: Group by primary_email
    #     for primary_email, tin_dict in practices[grouping].items():
    #         grouped_tins = list(tin_dict.keys())
    #         grouped_practice_names = []
    #         grouped_bcc_emails = set()
    #         grouped_files = []

    #         for tin, data in tin_dict.items():
    #             grouped_practice_names.append(data['name'])
    #             if data.get('bcc'):
    #                 grouped_bcc_emails.add(data['bcc'])

    #             df = run_query(tin)
    #             file = generate_filename(tin)
    #             df.to_excel(file, index=False)
    #             grouped_files.append(file)

    #         # Send to shared primary_email only once for all grouped TINs
    #         send_email_and_log(
    #             recipients=[primary_email],
    #             bcc=grouped_bcc_emails,
    #             tins=grouped_tins,
    #             practice_names=grouped_practice_names,
    #             files=grouped_files,
    #             grouping=grouping
    #         )

    #     # Step 2: Send individual emails for secondary and other emails
    #     for primary_email, tin_dict in practices[grouping].items():
    #         for tin, data in tin_dict.items():
    #             indiv_tins = [tin]
    #             indiv_practice_names = [data['name']]
    #             indiv_bcc = [data['bcc']] if data.get('bcc') else []

    #             df = run_query(tin)
    #             file = generate_filename(tin)
    #             df.to_excel(file, index=False)

    #             for recipient in filter(None, [data.get('secondary_email'), data.get('other_email')]):
    #                 send_email_and_log(
    #                     recipients=[recipient],
    #                     bcc=indiv_bcc,
    #                     tins=indiv_tins,
    #                     practice_names=indiv_practice_names,
    #                     files=[file],
    #                     grouping=grouping
    #                 )
    # else:
    #     for primary_email, tin_dict in practices[grouping].items():
    #         tins = list(tin_dict.keys())
    #         files, practice_names = [], []
    #         to_emails, bcc_emails = set(), set()

    #         for tin, data in tin_dict.items():
    #             practice_names.append(data['name'])
    #             to_emails.update(
    #                 filter(None, [data.get('secondary_email'), data.get('other_email')]))
    #             if data.get('bcc'):
    #                 bcc_emails.add(data['bcc'])

    #             if grouping == 'Practice Group':
    #                 df = run_query(tin)
    #                 file = generate_filename(tin)
    #                 df.to_excel(file, index=False)
    #                 files.append(file)

    #                 if to_emails:
    #                     send_email_and_log(
    #                         recipients=to_emails,
    #                         bcc=[data['bcc']] if data['bcc'] else [],
    #                         tins=[tin],
    #                         practice_names=[data['name']],
    #                         files=[file],
    #                         grouping=grouping
    #                     )

    #         if grouping != 'Practice Group':
    #             df = run_query(tins)
    #             file = generate_filename(tins)
    #             df.to_excel(file, index=False)
    #             files.append(file)

    #         to_emails.add(primary_email)
    #         send_email_and_log(
    #             recipients=to_emails,
    #             bcc=bcc_emails,
    #             tins=tins,
    #             practice_names=practice_names,
    #             files=files,
    #             grouping=grouping
    #         )


def main():
    try:
        practices = organize_practices()
        for grouping in ['Combined TIN', 'Practice Group', 'Individual']:
            load_data(practices, grouping)
    except Exception as e:
        print(f"Error occurred: {e}")


if __name__ == '__main__':
    main()
