import csv
import os

from Core.datautils import DataStruct
import re
import pandas as pd

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from typing import List, Dict
import requests
import json


class Immoweb:

    def __init__(self):
        self.data_immo: Dict = {}
        self.url_error: List[str] = []
        self.zip_code: List = DataStruct().get_zipcode_data()

        self._per_site_api_path = os.path.dirname(__file__)
        self._root_path = os.path.dirname(self._per_site_api_path)
        self._data_path = os.path.join(self._root_path, "Data")
        self._data_immoweb_path = os.path.join(self._data_path, "data_immoweb")
        self.path_results_search = os.path.join(self._data_immoweb_path, "url_results_search.csv")
        self.path_immo = os.path.join(self._data_immoweb_path, "url_immo.csv")
        self.path_data_immoweb = os.path.join(self._data_immoweb_path, "datas_immoweb.csv")

        s = pd.read_csv(self.path_results_search)["0"]
        self.url_results_search = set(s)
        s = pd.read_csv(self.path_immo)["0"]
        self.url_immo = set(s)
        self.datas_immoweb = pd.read_csv(self.path_data_immoweb, index_col=0)

        self.r_num_page = re.compile("\d{1,3}$")
        self.r_zip_code = re.compile("/\d{4}/")

        self.url_vente = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&orderBy=cheapest&postalCodes=BE-"
        self.immo_path: Dict = {
            "Zip": ["customers", "location","postalCode"],
            "Locality": ["customers", "location", 'locality'],
            "Type of property": ["property", "type"],
            "Subtype of property": ["property", "subtype"],
            "Price": ["transaction", "sale", "price"],
            "Type of sale": ["transaction", "subtype"],
            "Number of rooms": ["property", "bedroomCount"],
            "Area": ["property", "netHabitableSurface"],
            "Fully equipped kitchen": ["property", "kitchen","type"],  # !!! Kitchen peut être null !!
            "Furnished": ["transaction", "sale", "isFurnished"],
            "Open fire": ["property", "fireplaceExists"],
            "Terrace": ["property", "hasTerrace"],
            "Terrace Area": ["property", "terraceSurface"],
            "Garden": ["property", "hasGarden"],
            "Garden Area": ["property", "gardenSurface"],
            "Surface of the land": ["property", "land", "surface"],  # !!! land peut être null !
            "Surface area of the plot of land": ["property", "land","surface"],  # !!! land peut être null !
            "Number of facades": ["property", "building", "facadeCount"],
            "Swimming pool": ["property", "hasSwimmingPool"],
            "State of the building": ["property", "building", "condition"]
        }
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}
        self.driver = webdriver.Firefox()

    # retourne le milieu du texte entre les deux balises et le texte qui suit
    def coupepage(self, texte, debut, fin, n=1):
        debutT = texte.index(debut) + len(debut)
        texte = texte[debutT:]
        finT = texte.index(fin)

        if finT:
            return texte[:finT], texte[finT:]
        else:
            return False, False

    def _finditem(self, obj, key):
        if key in obj: return obj[key]
        for k, v in obj.items():
            if isinstance(v, dict):
                return self._finditem(v, key)  # added return statement

    def json_to_dic (self, json_immo: dict):
        # print("immo_path", self.immo_path)
        new_sale = {}
        for key, path in self.immo_path.items():
            n = len(path)
            i = 0
            element = json_immo[path[i]]
            if type(element) is list:
                element = element[0]

            i += 1
            while i < n and element is not None:
                element = element[path[i]]
                i += 1
            new_sale[key] = element
        return new_sale

    def scan_page_bien_immobilier(self, url: str) -> Dict:
        print(url)
        page = requests.get(url)
        texte = page.text
        texte, suite = self.coupepage(texte, "window.classified = ", "};")
        texte += "}"
        json_immo = json.loads(texte)

        new_sale = self.json_to_dic(json_immo)
        if new_sale["Zip"] is None:
            zip = re.search(self.r_zip_code, url).group(0)
            zip = zip[1:-1]
            new_sale["Zip"] = int(zip)

        if new_sale["Locality"] is None:
            new_sale["Locality"] = DataStruct.get_locality(new_sale["Zip"])[0]

        new_sale["Url"] = url
        new_sale["Source"] = "Immoweb"
        print("type et new_sale", type(new_sale), new_sale)

        return pd.DataFrame(new_sale, index=[len(self.datas_immoweb.index)])

    def loop_immo(self):

        i = 0
        max = 20
        for url in self.url_immo:
            if url not in self.datas_immoweb["Url"]:
                new_sale = self.scan_page_bien_immobilier(url)
                self.datas_immoweb = self.datas_immoweb.append(new_sale)
                i += 1
            if i > max:
                break

        print(type(self.datas_immoweb), self.datas_immoweb)
        self.save_data_to_csv()

    def save_data_to_csv(self):
        self.datas_immoweb.to_csv(self.path_data_immoweb)

    def run(self):
        self._generator_db_url()
        # self._scan_page_list('https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&postalCodes=BE-1348')
        #self._add_set_to_csv()
        # self.scan_page_bien_immobilier('https://www.immoweb.be/fr/annonce/kot/a-vendre/bruxelles-ville/1000/9303884?searchId=60914b580d2b7')
        self._save_set_to_csv()
        self.driver.close()
        self.loop_immo()
        self.save_data_to_csv()
        print("fini")

    def _save_set_to_csv(self):
        with open(self.path_results_search, 'w') as file:
            df = pd.DataFrame(self.url_results_search)
            df.to_csv(file)

        with open(self.path_immo, "w") as file:
            df = pd.DataFrame(self.url_immo)
            df.to_csv(file)

    # https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&orderBy=cheapest&postalCodes=BE-1341,1348
    def _scan_page_list(self, url: str, num_pages: int = 1) -> bool:
        """
        Scan the page with result of research.
        :param url: An String url without page=xx
        :param num_pages: Number of the page to scan
        :return: Now, I dont know.
        """

        pager = f"&page={num_pages}"
        url_paged = url + pager
        pagination = [0]
        if url_paged not in self.url_results_search:
            self.url_results_search.add(url_paged)
            self.driver.get(url_paged)
            text = self.driver.page_source

            if "Désolé. Aucun résultat trouvé." in text:
                print(url_paged, " pas trouvé")
                return False
            elif "pagination__item" in text:
                li_pagination_item = self.driver.find_elements_by_css_selector('li.pagination__item')
                pagination = [1]
                for element in li_pagination_item:
                    number = re.search(self.r_num_page, element.text)
                    if number is not None:
                        pagination.append(int(number.group(0)))
                print(pagination)
                if max(pagination) == 333:
                    print("333 pages a scaner => url passée : ", url_paged)
                    return False

            a_elements = self.driver.find_elements_by_css_selector('a.card__title-link')
            for element in a_elements:
                self.url_immo.add(element.get_attribute('href'))
            print(len(self.url_immo), " adresses url de biens")

            #  Section who verify if we must turn to next page
            if max(pagination) == num_pages:
                return True
            else:
                self._scan_page_list(url, num_pages+1)

    def _generator_db_url(self) -> bool:

        count_limit = 12 + len(self.url_results_search)
        count_sale = 0
        for zip in self.zip_code.zipcode:
            url_list = f"{self.url_vente}{zip}"
            if count_sale < count_limit:
                                                # temporaire, pour éviter que le programme tourne de trop lors du dev.
                print("run on : ", url_list)
                self._scan_page_list(url_list)
                count_sale += 1
                print(count_sale)
                self._save_set_to_csv()

        print('page de ventes', count_sale)

        return True

if __name__ == "__main__":
    immoweb = Immoweb()
    immoweb.run()


"""count_rent = 0
url_location = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-louer?countries=BE&orderBy=cheapest&postalCodes=BE-"
for zip in self.zip_code.zipcode:
    url_list = f"{url_location}{zip}"
    self._scan_page_list(url_list)
    count_rent += 1"""
