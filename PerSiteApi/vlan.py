import requests
from bs4 import BeautifulSoup
import os
import re
import pandas as pd
import scrapy
import csv
from lxml import etree

# Url of website
from pandas.io.common import urlopen

url = 'https://www.2ememain.be/l/immo/'
other_url = 'https://immo.vlan.be/fr/immobilier?propertytypes=appartement,maison,business,terrain,garage,' \
            'bien-d%27investissement,kot,divers&transactiontypes=a-vendre,a-louer,en-vente-publique,' \
            'en-colocation&propertysubtypes=appartement,rez-de-chaussee,duplex,penthouse,studio,loft,triplex,maison,' \
            'villa,immeuble-mixte,maison-de-maitre,fermette,bungalow,chalet,chateau,surface-commerciale,' \
            'bureaux,surface-industrielle,fonds-de-commerce,ferme,terrain-a-batir,terrain,terrain-agricole,' \
            'parking,garage,maison-de-rapport,kot-etudiant,bien-divers&countries=belgique&noindex=1'
user_agent_desktop = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '\
'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.149 '\
'Safiri/537.36'

url_page_offset = '&pageOffset='

headers = { 'User-Agent': user_agent_desktop}

__per_site_api_path = os.path.dirname(__file__)
__root_path = os.path.dirname(__per_site_api_path)
__core_path = os.path.join(__root_path, "Core")
__data_path = os.path.join(__root_path, "Data")
__database_file = os.path.join(__data_path, "database.csv")
__zipcode_file = os.path.join(__data_path, "zipcodes.csv")
__vlan_url = os.path.join(__data_path, "vlan.csv")
__vlan_link = os.path.join(__data_path, "vlan_link.csv")
__vlan_data = os.path.join(__data_path, "vlan_data.csv")
__root_url = r"https://immo.vlan.be"
__addresses = set()
__pattern = re.compile(r'/en/(buy|rent)/.+/[a-zA-Z]+-[0-9]+/.+.html')

__zipcode_df = pd.read_csv(__zipcode_file)




# I send my HTTP request with a "GET" to the site server to identify in the url

r = requests.get(other_url, headers=headers)
# I display the requested url and the return of the server

# I ask beautifulSoup to keep in a soup variable the web page to scrape (url) an html script
soup = BeautifulSoup(r.content,'lxml')
for elem in soup.find_all('article', attrs={"class": "list-view-item"}):
    print(elem.get('href'))
#tree = etree.parse(other_url)
# Quel joli petit dictionnaire
#xmlObject = etree.fromstring(r.content)
#xmlTree = xmlObject.getroottree()
#print(soup.text)
""" propertytype"""
page_list = []
htmlparser = etree.HTMLParser()
with open(__vlan_url, 'a+') as url_file:
    with open(__vlan_link, 'w+') as url_index_file:
        with open(__vlan_data, 'a+') as url_data_file:
            for offset in range(1,168):
                link_list = []
                offset_url = other_url+url_page_offset+str(offset)
                r = requests.get(offset_url, headers=headers)
                url_index_file.write(str(r.status_code))
                url_index_file.write(offset_url)
                soup = BeautifulSoup(r.content, 'lxml')
                for element in soup.findAll('a'):
                    link = element.get("href")
                    if 'a-vendre' in link and '/fr/detail/'  in link:
                        link_list.append(link)
                for links in sorted(link_list):
                    url_file.write(links)
                    url_file.write("\n")
                    item_url = __root_url + links
                    r = urlopen(item_url)
                    tree = etree.parse(r, htmlparser)
                    xpathselector = '//div[@id="collapse_general_info"]/div[@class="row"]'
                    # catch HTTPError
                    results = tree.xpath(xpathselector)
                    for info in results:
                        url_data_file.write(info)
                        url_data_file.write("\n")

                url_index_file.write("\n")

