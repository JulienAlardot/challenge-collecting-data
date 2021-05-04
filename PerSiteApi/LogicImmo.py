import csv
import os
import random
import re
import time

import bs4
import pandas as pd
import requests
from Core import bs4utils

__per_site_api_path = os.path.dirname(__file__)
__root_path = os.path.dirname(__per_site_api_path)
__core_path = os.path.join(__root_path, "Core")
__data_path = os.path.join(__root_path, "Data")
__database_file = os.path.join(__data_path, "database.csv")
__zipcode_file = os.path.join(__data_path, "zipcodes.csv")
__logic_immo_url = os.path.join(__data_path, "logic_immo_url.csv")
__root_url = r"https://www.logic-immo.be"
__addresses = set()
__pattern = re.compile(r'/fr/vente/.+/[a-zA-Z]+-[0-9]+/.+.html')

__zipcode_df = pd.read_csv(__zipcode_file)
__headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate",
    "Accept-Language": "en-GB,en-US;q=0.9,en;q=0.8",
    "Dnt": "1",
    "Host": "httpbin.org",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    "X-Amzn-Trace-Id": "Root=1-5ee7bae0-82260c065baf5ad7f0b3a3e3"
}
def search_for_urls():
    for i, row in __zipcode_df.iterrows():
        print(i)
        print(row["local"])
        page = 1
        found = True
        text = "all"
        while found:
            found = False
            url = __root_url + f'/fr/vente/immo-a-vendre/{text}-{row["zipcode"]},{page},--------16776966-,---,---.html'
            response = requests.get(url, headers=__headers)
            buy_soup = bs4.BeautifulSoup(response.content, features="html.parser")
            for link in (f.get("href") for f in buy_soup.findAll("a")):
                if link:
                    if re.search(__pattern, link):
                        found = True
                        __addresses.add(__root_url + link)

            if not found:
                text = row["local"].lower().replace(" ", "-").replace("é", "e").replace("è", "e").replace("â", "a")
                text = text.replace("ç", "c").replace("û", "u").replace("ê", "e").replace("î", "i")
                url = __root_url + f'/fr/vente/immo-a-vendre/{text}-{row["zipcode"]},{page},--------16776966-,---,---.html'

                    response = requests.get(url)
                    buy_soup = bs4.BeautifulSoup(response.content, features="html.parser")

                for link in (f.get("href") for f in buy_soup.findAll("a")):
                    if link:
                        if re.search(__pattern, link):
                            found = True
                            __addresses.add(__root_url + link)

            print(url, response.status_code, page, len(__addresses))
            # time.sleep(random.random()/4)
            page += 1

            if len(__addresses) % 480 == 0:
                with open(__logic_immo_url, 'wt+', newline='', encoding="utf-8") as url_file:
                    f_w = csv.writer(url_file)
                    f_w.writerows(([address] for address in __addresses))


def search_raw_infos():
    with open(__logic_immo_url, "rt", encoding='utf-8') as url_file:
        f_r = csv.reader(url_file)
        f_r = ['https://www.logic-immo.be/en/buy/apartments-for-sale/erpent-5101/penthouse-4-rooms-207c419f-bdb8-0d36-4c0a-182a2b9df7b2.html#5']
        for url in f_r:
            response = requests.get(url, headers=__headers)
            soup = bs4.BeautifulSoup(response.content, features="html.parser")
            title = ''
            desc = ''
            icons = ""
            for element in soup.findAll("h1", class_="c-details_title c-details_title--primary"):
                title += bs4utils.extract_text_from_tag(element)
            print(title)

            for element in soup.findAll('p', class_="js-description"):
                desc += bs4utils.extract_text_from_tag(element)

            print(desc)
            for element in soup.findAll("ul", id="property-details-icons"):
                icons += bs4utils.extract_text_from_tag(element)

            print(icons)
            time.sleep(random.random()+0.2)


if __name__ == "__main__":
    search_for_urls()
    search_raw_infos()
