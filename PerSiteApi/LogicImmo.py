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
__logic_immo_raw_data = os.path.join(__data_path, "logic_immo_raw_data.csv")
__root_url = r"https://www.logic-immo.be"
__addresses = set()
__pattern = re.compile(r'/fr/vente/.+/.+/.+\.html')

__zipcode_df = pd.read_csv(__zipcode_file)
__headers = {
    # "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    # "Accept-Encoding": "gzip, deflate",
    # "Accept-Language": "fr-FR,en-US;q=0.9,en;q=0.8",
    # "Dnt": "1",
    # "Host": "httpbin.org",
    # "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
    # "X-Amzn-Trace-Id": "Root=1-5ee7bae0-82260c065baf5ad7f0b3a3e3"
}
def search_for_urls():
    old_len_address = 0
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
            if response.status_code == 500 or response.status_code == 503:
                while response.status_code == 500 or response.status_code == 503:
                    response = requests.get(url)
                    time.sleep(1)
            buy_soup = bs4.BeautifulSoup(response.content, features="html.parser")
            for link in (f.get("href") for f in buy_soup.findAll("a")):
                if link:
                    if re.search(__pattern, link):
                        found = True
                        __addresses.add(__root_url + link)

            if not found:
                text = row["local"].lower().replace(" ", "-").replace("é", "e").replace("è", "e").replace("â", "a")
                text = text.replace("ç", "c").replace("û", "u").replace("ê", "e").replace("î", "i").replace("'", "")
                text = text.replace("ô", "o")
                url = __root_url + f'/fr/vente/immo-a-vendre/{text}-{row["zipcode"]},{page},--------16776966-,---,---.html'

                response = requests.get(url)

                if response.status_code == 500 or response.status_code == 503:
                    while response.status_code == 500 or response.status_code == 503:
                        response = requests.get(url)
                        time.sleep(1)
                buy_soup = bs4.BeautifulSoup(response.content, features="html.parser")

                for link in (f.get("href") for f in buy_soup.findAll("a")):
                    if link:
                        if re.search(__pattern, link):
                            found = True
                            __addresses.add(__root_url + link)

            print(url, response.status_code, page, len(__addresses))
            # time.sleep(random.random()/4)
            page += 1

            if len(__addresses) > old_len_address + 1000:
                old_len_address = len(__addresses)
                with open(__logic_immo_url, 'wt+', newline='', encoding="utf-8") as url_file:
                    f_w = csv.writer(url_file)
                    f_w.writerows(([address] for address in sorted(__addresses)))


def search_raw_infos():
    with open(__logic_immo_url, "rt", encoding='utf-8') as url_file:
        with open(__logic_immo_raw_data, "w+", encoding="utf-8", newline="") as text_file:
            f_r = csv.reader(url_file)
            f_w = csv.writer(text_file)
            for url in f_r:
                url = url[0]
                print(url)
                response = requests.get(url, headers=__headers)
                soup = bs4.BeautifulSoup(response.content, features="html.parser")
                title = ''
                desc = ''
                icons = ""
                for element in soup.findAll("h1", class_="c-details_title c-details_title--primary"):
                    title += bs4utils.extract_text_from_tag(element).replace("\n", " ").replace('"', "").replace("'",
                                                                                                                 '')

                for element in soup.findAll('p', class_="js-description"):
                    desc += bs4utils.extract_text_from_tag(element).replace("\n", " ").replace('"', "").replace("'", '')

                for element in soup.findAll("ul", id="property-details-icons"):
                    for link in element.findAll("li", attrs={"data-toggle": 'tooltip'}):
                        icons += link.get("title") + bs4utils.extract_text_from_tag(link).replace("\n", " ") + ','

                f_w.writerow((title.replace(",", "."), desc.replace(",", "."), icons.replace('"', "").replace("'", '')))



if __name__ == "__main__":
    search_for_urls()
    # search_raw_infos()
