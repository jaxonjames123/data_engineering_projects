import os
from paubox_email_footers import providerrelations_footer as footer
from get_practices_emails_monday import monday_dictionary
from send_secure_email import PauboxMailer
from datetime import datetime


files = os.listdir(".")
current_date = datetime.now()
current_month = datetime.now().strftime('%Y%m')
practices = monday_dictionary
messages_dict = {
    'non_cac_message': """<p>Dear SOMOS Partner,</p>
                        <p>Attached, please find the current Medicaid recertification roster.</p>
                        <p>We kindly remind you that Medicaid beneficiaries have a 30-day grace period after their term date to recertify to avoid a break in coverage. Failure to recertify within this timeframe will result in a loss of Medicaid coverage for the beneficiary.</p>
                        <p>We highly recommend for you to outreach these members and connect them to services for Recertification.</p>
                        <p>SOMOS supports a few options:</p>
                        <ul>
                            <li>Additionally, we offer monthly CAC (Certified Application Counselor) certification courses. Enrolling your practice can help you better assist your patients with Medicaid recertification, potentially saving you significant cost by preventing coverage gaps.  If you are interested, please click the following link and register your staff member <a href="https://docs.google.com/forms/d/e/1FAIpQLSctCQuKC8ec2b5pwHX8ySe2oeIKaMeU4D9LG_egrynrAjDv4g/viewform?usp=sf_link">CAC Registration Form</a></li>
                            <li>If you do not have resources at the moment, you may work with your designated Health Plan representative.  Please contact your SOMOS Field Practice Associate for introduction and strategy planning.</li>
                        </ul>
                        <p>Should you have any additional questions, you may submit a request through the following link: <a href="https://somosinnovation.com/contact-us/">https://somosinnovation.com/contact-us/ </a>and our representative will contact you.</p>
                        <p>Thank you,</p>""",
    'cac_message': """<p>Dear [Provider Office],</p>
                    <p>Please find attached the current recertification roster of patients enrolled with the following health plans: <strong>Anthem, Emblem, Fidelis, Healthfirst, MetroPlus, Molina and United.</strong> Please note that only patients who are eligible for recertification in the current month are listed in the roster, regardless of MCO.</p>
                    <p>Medicaid beneficiaries have a <strong>30-day grace period</strong> after their term date to recertify and avoid a break in coverage. Failure to recertify within this timeframe will result in the <strong>loss of Medicaid coverage</strong>. Ensuring your patients maintain continuous coverage can potentially save your practice <strong>up to $700 per beneficiary</strong> by preventing lapses in their coverage.</p>
                    <p>To further assist your team, we offer <strong>Certified Application Counselor (CAC) training courses</strong>. If you wish to enroll an additional staff member in the program, please contact Daylin Cuevas at <strong>dcuevas@somosipa.com</strong> for details on upcoming sessions.</p>
                    <p>Thank you for your partnership and commitment to providing excellent care. Please reach out if you have any questions or need further assistance.</p>
                    <p>Sincerely,</p>""",
}

sender = "providerrelations@somoscommunitycare.org"
mailer = PauboxMailer()


def main():
    for key, value in practices.items():
        practice_name = list(value.keys())[0]
        try:
            practice_data = value[practice_name][0]
            tin = practice_data["TIN"]
            message = messages_dict['non_cac_message'] if practice_data["CAC"] == "No" else messages_dict['cac_message'].replace(
                "[Provider Office]", practice_name)
            send_email = True if practice_data["Active Email?"] == "Active" else False
            email = practice_data["Email"]
            print(
                f"Practice: {practice_name}\nTIN: {tin}\nEmail: {email}\nSend Email: {send_email}")
            if send_email:
                for file in files:
                    if tin in file:
                        response = mailer.send_mail(recipients=email, subject=f'SOMOS {current_date.strftime("%B")} {current_date.year} MCD Recert Roster for {practice_name}',
                                             attachments=file, message=message, footer=footer, delivery_email_address=sender, logo='somos_logo')
                        status = mailer.check_delivery_status(response)
                        mailer.update_delivery_status_table(recipients_data=status, email_type='recertification', tins=[tin], practice_names=[
                                                     practice_name], for_month=current_month, filenames=[file])
                        print('File Sent')
        except Exception as e:
            print(
                f'Failed to send email due to {e}, check Monday board for {practice_name}')


if __name__ == '__main__':
    main()
