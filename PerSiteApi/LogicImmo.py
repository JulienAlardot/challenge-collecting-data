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
__pattern = re.compile(r'/en/(buy|rent)/.+/[a-zA-Z]+-[0-9]+/.+.html')

__zipcode_df = pd.read_csv(__zipcode_file)
__headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}


def search_for_urls():
    for sell_type_contect in ('/en/buy/immo-for-sale/'):
        for i, row in __zipcode_df.iterrows():
            print(row["local"])
            page = 1
            found = True
            text = "all"
            while found:
                found = False
                url = __root_url + f'{sell_type_contect}{text}-{row["zipcode"]},{page},--------16776966-,---,---.html'
                response = requests.get(url, headers=__headers)
                buy_soup = bs4.BeautifulSoup(response.content, features="html.parser")

                for link in (f.get("href") for f in buy_soup.findAll("a")):
                    if link:
                        if re.search(__pattern, link):
                            found = True
                            __addresses.add(__root_url + link)

                if not found:
                    text = row["local"].lower().replace(" ", "-").replace("é", "e").replace("è", "e").replace("â", "a")
                    url = __root_url + f'{sell_type_contect}{text}-{row["zipcode"]},{page},--------16776966-,---,---.html'

                    response = requests.get(url)
                    buy_soup = bs4.BeautifulSoup(response.content, features="html.parser")

                    for link in (f.get("href") for f in buy_soup.findAll("a")):
                        if link:
                            if re.search(__pattern, link):
                                found = True
                                __addresses.add(__root_url + link)

                print(url, response.status_code, page, len(__addresses))
                time.sleep(random.random() + 0.5)
                page += 1

    with open(__logic_immo_url, 'w+', encoding=True) as url_file:
        f_w = csv.writer(url_file)
        for element in sorted(__addresses):
            f_w.writerow(element)


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
    # search_for_urls()
    search_raw_infos()
