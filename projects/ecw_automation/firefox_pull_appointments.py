import concurrent.futures
import os
import re
import time

import pandas as pd
import pyodbc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

# Constants
DOWNLOAD_PATH = "/home/etl/etl_home/downloads/ecw/"
SCREENSHOT_PATH = DOWNLOAD_PATH + "errored_screenshots/"
FILE_PATH = DOWNLOAD_PATH + "appointments/"
SCREENSHOT_S3_PATH = "s3://sftp_test/ecw_automation/"

practices_dict = {}
failed_practices = []


def get_practice_info(conn):
    query = "select * from appointments.lastpass_export"
    cur = conn.execute(query)
    res = cur.fetchall()
    for practice in res:
        practices_dict[practice[0]] = {
            "username": practice[1],
            "password": practice[2],
            "name": practice[3],
            "tin": practice[4],
        }


def get_driver(tin):
    download_path = rf"{FILE_PATH}{tin}/"
    if not os.path.exists(download_path):
        os.makedirs(download_path)
    print(download_path)
    options = Options()
    options.set_preference("plugin.default.state", 0)
    options.set_preference("plugin.state.flash", 0)  # Disable Flash
    options.set_preference("plugin.state.java", 0)  # Disable Java plugins
    options.set_preference("extensions.enabled", False)
    options.set_preference("extensions.autoDisableScopes", 15)
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.dir", download_path)
    options.set_preference(
        "browser.helperApps.neverAsk.saveToDisk",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet,application/vnd.ms-excel",
    )
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("pdfjs.disabled", True)
    options.set_preference("browser.download.manager.closeWhenDone", True)
    options.add_argument("-headless")
    # service = Service("C:/Users/jterrell/Desktop/geckodriver.exe",
    #   log_path="geckodriver.log")
    # firefox_path = 'C:/Users/jterrell/AppData/Local/Mozilla Firefox/firefox.exe'
    # options.binary_location = firefox_path
    # Initialize WebDriver with options
    service = Service(log_path=os.devnull)
    driver = webdriver.Firefox(service=service, options=options)
    return driver


def save_screenshot_to_s3(driver, tin):
    driver.save_screenshot(
        f"{SCREENSHOT_PATH}{time.strftime('%Y%m%d')}_{tin}_error.png"
    )
    driver.quit()


def bypass_login(username, password, wait):
    time.sleep(60)
    try:
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@name='CAMUsernameForm']")
            )
        ).send_keys(username)
        time.sleep(5)
        wait.until(
            EC.presence_of_element_located(
                (By.XPATH, "//input[@name='CAMPasswordForm']")
            )
        ).send_keys(password)
        time.sleep(5)
        wait.until(
            EC.presence_of_element_located((By.XPATH, "(//input[@value='Login'])[2]"))
        ).click()
    except Exception:
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@name='CAMUsername']"))
        ).send_keys(username)
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@name='CAMPassword']"))
        ).send_keys(password)
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//a[@class='loginButton']"))
        ).click()
    time.sleep(360)
    print("\nSuccessfully Logged In")


def download_report(wait):
    wait.until(
        EC.presence_of_element_located((By.XPATH, "//a[normalize-space()='eCWEBO']"))
    ).click()
    wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//a[normalize-space()='4 - Administrative Reports']")
        )
    ).click()
    wait.until(
        EC.presence_of_element_located(
            (
                By.XPATH,
                "//img[@title='Run with options - 4.02 - Encounter Patient Download']",
            )
        )
    ).click()
    Select(
        wait.until(
            EC.presence_of_element_located((By.XPATH, "//select[@id='outputFormat']"))
        )
    ).select_by_visible_text("Delimited text (CSV)")
    wait.until(
        EC.presence_of_element_located((By.XPATH, "//a[@id='IDS_OTHERRUN_RUN']"))
    ).click()
    time.sleep(300)
    Select(
        wait.until(
            EC.presence_of_element_located((By.XPATH, "(//select[@role='listbox'])[1]"))
        )
    ).select_by_visible_text("Next X Months")
    print("Selected dropdown Custom Date for Next X Months")
    days_element = wait.until(
        EC.presence_of_element_located(
            (By.XPATH, "//input[@aria-label='Text box prompt']")
        )
    )
    days_element.click()
    days_element.send_keys(Keys.BACKSPACE)
    days_element.send_keys("1")
    print("Set Date to next 1 Months")
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//span[normalize-space()='Encounter Details']")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "(//a[@href='javascript:;'][normalize-space()='Select all'])[1]")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//span[normalize-space()='Patient Details']")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "(//a[@href='javascript:;'][normalize-space()='Select all'])[2]")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable((By.XPATH, "//span[normalize-space()='S.O./G.I']"))
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "(//a[@href='javascript:;'][normalize-space()='Select all'])[3]")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//span[normalize-space()='Facility Details']")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "(//a[@href='javascript:;'][normalize-space()='Select all'])[4]")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//span[normalize-space()='Provider Details']")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "(//a[@href='javascript:;'][normalize-space()='Select all'])[5]")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "//span[normalize-space()='Payer Details']")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, "(//a[@href='javascript:;'][normalize-space()='Select all'])[6]")
        )
    ).click()
    wait.until(
        EC.element_to_be_clickable((By.XPATH, "//span[normalize-space()='OK']"))
    ).click()
    time.sleep(360)


def transform_file(tin):
    path = f"{FILE_PATH}{tin}/"
    os.chdir(path)
    files = os.listdir()
    files.sort(key=os.path.getmtime, reverse=True)
    print(files)
    columns_to_keep = [
        "Appointment Date",
        "Admission Date",
        "Discharge Date",
        "Appointment Start Time",
        "Is Sunoh.ai",
        "Is Televisit",
        "Call Start Time",
        "Call End Time",
        "Call Duration",
        "Encounter ID",
        "Visit Type",
        "Visit Sub-Type",
        "Visit Status",
        "Case Label",
        "Appointment Created By User",
        "Visit Count",
        "Patient Count",
        "Patient Name",
        "Patient First Name",
        "Patient Last Name",
        "Patient Middle Initial",
        "Patient Acct No",
        "Patient DOB",
        "Patient Gender",
        "Patient Address Line 1",
        "Patient Address Line 2",
        "Patient City",
        "Patient State",
        "Patient ZIP Code",
        "Patient Full Address",
        "Patient Race",
        "Patient Ethnicity",
        "Patient Language",
        "Patient Home Phone",
        "Patient Cell Phone",
        "Patient Work Phone",
        "Patient E-mail",
        "Patient Status",
        "Don't Send Statements",
        "Patient Deceased",
        "Patient Age Group",
        "Birth Sex",
        "Gender Identity Name",
        "Gender Identity SNOMED Code",
        "Sexual Orientation Name",
        "Sexual Orientation SNOMED Code",
        "Appointment Facility Name",
        "Appointment Facility POS",
        "Appointment Facility Group Name",
        "Department Name",
        "Practice Name",
        "Appointment Provider Name",
        "Appointment Provider NPI",
        "Appointment Referring Provider Name",
        "Appointment Referring Provider NPI",
        "Resource Provider Name",
        "Resource Provider NPI",
        "Demographics PCP Name",
        "Demographics PCP NPI",
        "Demographics Referring Provider Name",
        "Demographics Referring Provider NPI",
        "Demographics Rendering Provider Name",
        "Demographics Rendering Provider NPI",
        "Primary Insurance Name",
        "Primary Insurance Subscriber No",
        "Secondary Insurance Name",
        "Secondary Insurance Subscriber No",
        "Tertiary Insurance Name",
        "Tertiary Insurance Subscriber No",
        "Sliding Fee Schedule",
        "Appointment Employer",
    ]
    if (
        len(files) > 0
        and re.match(r".*- Encounter Patient Download(.*).csv", files[0]) is not None
    ):
        file_name = files[0]
        print(file_name)
        df = pd.read_csv(file_name, encoding="utf-16", sep="\t")
        missing_columns = [col for col in columns_to_keep if col not in df.columns]
        for col in missing_columns:
            df[col] = None
        df = df[columns_to_keep]
        output_file = f"{tin}_appointments.csv"
        df.to_csv(output_file, header=True, index=False, sep="|")
        os.remove(file_name)
        print("File has been transformed")
        return "success" if not df.empty else "failure"
    else:
        print("File not downloaded")
        return "failure"


def update_status_table(tin, practice_name, update_status):
    query = """
    INSERT INTO appointments.appointment_download_status 
    (tin, practice_name, status, date_of_exec) 
    VALUES (?, ?, ?, GETDATE())
    """
    conn = pyodbc.connect(dsn="somos_redshift_1")
    try:
        conn.execute(query, (tin, practice_name, update_status))
        conn.commit()
        print("Appointments Status Table Updated")
    except Exception as e:
        print("Error Updating Appointments Status:", e)
    finally:
        conn.close()


def process_practice(url, username, password, name, tin):
    print(f"\nProcessing practice: {tin} {name}")
    try:
        driver = get_driver(tin)
        wait = WebDriverWait(driver, 10)
        driver.get(url)
        bypass_login(username, password, wait)
        download_report(wait)
        status = transform_file(tin)
        print(status)
        driver.quit()
    except Exception as e:
        print(f"Exception in process_practice: {e}")
        status = "failure"
        save_screenshot_to_s3(driver, tin)
        failed_practices.append(tin)
    finally:
        update_status_table(tin, name, status)


def process_wrapper(practice, details):
    username = details["username"]
    password = details["password"]
    name = details["name"]
    tin = details["tin"]

    if ":8080" in practice:
        url = practice.split(":8080")[0] + ":9300/p2pd/servlet/dispatch"
    elif "netstem" in practice:
        print(
            f"Netstem practices do not have EBO reports, PLEASE CONFIRM for this tin: {tin} {name}"
        )
        return
    elif "eclinicalweb" in practice:
        url = practice.split("app")[0] + "ebo.eclinicalweb.com/bi/?legacyLogin"
    elif "ecwcloud" in practice:
        url = practice.split("app")[0] + "ebo.ecwcloud.com/bi/?legacyLogin"
    else:
        print(f"Practice does not belong to ECW: {tin} {name}")
        return
    print(url)
    process_practice(url=url, username=username, password=password, name=name, tin=tin)


def main():
    conn = pyodbc.connect(dsn="somos_redshift_1")
    get_practice_info(conn)
    conn.execute(
        "delete from appointments.ecw_appointments where appointment_date > DATEADD(day, -1, CURRENT_DATE);"
    )
    conn.commit()
    conn.close()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(process_wrapper, practice, details): practice
            for practice, details in practices_dict.items()
        }

        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # To catch exceptions if any occur
            except Exception as e:
                print(f"Error processing {futures[future]}: {e}")
    print(f"Following Practices Failed: {failed_practices}")


if __name__ == "__main__":
    main()
