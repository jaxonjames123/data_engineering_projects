import pandas as pd
import requests
from bs4 import BeautifulSoup

URL = "https://x12.org/codes/claim-status-codes"
page = requests.get(URL)
bucket = "sftp_test"

soup = BeautifulSoup(page.content, "html.parser")
results = soup.find(id="codelist")
code_nums = results.find_all("td", class_="code")
code_descriptions = results.find_all("td", class_="description")
codes_dict = {}
codes_dict = {
    code_nums[i].text: code_descriptions[i].text.split("Start:")[0]
    for i in range(len(code_nums))
}

df = pd.DataFrame.from_dict(codes_dict, orient="index")
df.to_csv("s3://sftp_test/277/Claim_Line_Status_Codes.csv", header=False)
