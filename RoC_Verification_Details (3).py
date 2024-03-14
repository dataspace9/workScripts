from bs4 import BeautifulSoup
import requests
from time import sleep
import pandas as pd
from datetime import datetime as dt

from dbConnectionScript import connectionScript
import pandas as pd

env = "dev"

dbName = "assurekitds"

collection = "companyPincodeData"

p = connectionScript(env = env, dbName = dbName, collectionName= collection)

def verify_roc_year(CompanyName, UIN):
    """def verifies the name of the company and the UIN and the year in which the company
    was incorporated using zaubacorp as a reference
    full company name should be provided as input.
    for e.g ASSUREKIT-TECHNOLOGY-AND-SERVICES-PRIVATE-LIMITED
    U72900MH2019PTC323645
    """
    sleep(3)
    r = requests.get("https://www.zaubacorp.com/"+CompanyName + "/" +UIN)
    soup = BeautifulSoup(r.content, 'html.parser')
    volumes = soup('p')
    # print(volumes)

    finlist = []
    for v in volumes:
        finlist.append(v.contents[0])
    doi = finlist.index("Date of Incorporation") + 1
    return finlist[doi]


df = pd.read_excel(r"C:\Users\sheld\OneDrive\projects\allScripts\April 2023\company202304111609.xlsx","companyList")
dfx = df[['LLPIN','LIMITED LIABILITY PARTNERSHIP NAME']]
# print(dfx)
x = dfx.to_dict("records")[14386:]

final = []
for i in x:
    sleep(3)
    url = "https://cleartax.in/f/company/assurekit-technology-and-services-private-limited/"
    r = requests.get("https://www.quickcompany.in/company/" +i['LIMITED LIABILITY PARTNERSHIP NAME'].lower().replace(" ","-"))
    soup = BeautifulSoup(r.content, 'html.parser')
    volumes = soup('p')
    status = str(volumes[-4:-3])
    i.update({"status":status})
    p.collection.insert_many([i])

















