import bs4
import requests
import time
from Core.datautils import DataStruct
import os
import pandas as pd
import re
__per_site_api_path = os.path.dirname(__file__)
__root_path = os.path.dirname(__per_site_api_path)
__core_path = os.path.join(__root_path, "Core")
__data_path = os.path.join(__root_path, "Data")
__database_file = os.path.join(__data_path, "database.csv")
__zipcode_file = os.path.join(__data_path, "zipcodes.csv")
__root_url = r"https://www.logic-immo.be"
__addresses = list()
__pattern = re.compile(r'/en/(buy|rent)/[a-zA-Z\-]+-for-sale/all-[0-9]+,[0-9],.+.html')
'en/rent/real-estate-for-rent/all-5101,[0-9]+,--------16776966-,-,---.html'
__zipcode_df = pd.read_csv(__zipcode_file)
for i, row in __zipcode_df.iterrows():
    print(row["local"])
    buy_url =  __root_url + f'/en/buy/immo-for-sale/all-{row["zipcode"]},1,--------16776966-,---,---.html'
    buy_url = f'https://www.logic-immo.be/en/buy/immo-for-sale/all-{5101},1,--------16776966-,---,---.html'
    response = requests.get(buy_url)
    buy_soup = bs4.BeautifulSoup(response.content, features="html.parser")
    for link in (f.get("href") for f in buy_soup.findAll("a")):
        print(link)
        print(re.search(__pattern, link))
        if re.search(__pattern, link):
            print(link)

    time.sleep(4)
    # rent_url = __root_url + f'/en/rent/real-estate-for-sale/all-{row["zipcode"]},1,--------16776966-,-,---.html'
    # response = requests.get(rent_url)
    # rent_soup = bs4.BeautifulSoup(response.content, features="html.parser")
    #
    # time.sleep(4)



#
# # url = r'https://www.logic-immo.be/en/real-estate-properties-belgium.html'
#
#
# 'https://www.logic-immo.be/en/buy/immo-for-sale/all-5101,1,--------16776966-,---,---.html'
# 'https://www.logic-immo.be/en/buy/immo-for-sale/all-5000,1,--------16776966-,---,---.html'
# response = requests.get(url)
# soup = bs4.BeautifulSoup(response.content, features="html.parser")
# print(soup.text)

