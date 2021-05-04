from Core.datautils import DataStruct
import re
import pandas as pd
from selenium import webdriver
from typing import List, Dict
import requests
import json


class Immoweb:

    def __init__(self):
        self.data_immo: Dict = {}
        self.url_error: List[str] = []
        self.zip_code: List = DataStruct().get_zipcode_data()
        self.addresses: set = {"https://www.immoweb.be/fr/annonce/appartement/a-vendre/sint-pieters-leeuw/1600/9312492?searchId=609139890de6c"}
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}
        self.regex = re.compile("\d{1,3}$")
        self.url_vente = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&orderBy=cheapest&postalCodes=BE-"
        self.immo_path: Dict = {
            "Zip": ["customers", "location","postalCode"],
            "Locality": ["customers", "location"],
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

    # retourne le milieu du texte entre les deux balises et le texte qui suit
    def coupepage(self, texte, debut, fin, n=1):
        debutT = texte.index(debut) + len(debut)
        texte = texte[debutT:]
        finT = texte.index(fin)

        if finT:
            return texte[:finT], texte[finT:]
        else:
            return False, False


    def scan_page_bien_immobilier(self, url: str) -> Dict:
        page = requests.get(url)
        texte = page.text
        texte, suite = self.coupepage(texte, "window.dataLayer = [", "];")
        texte, suite = self.coupepage(suite, "window.classified = ", ";")

        json_immo = json.loads(texte)
        print(type(json_immo), json_immo)
        return True

    def run(self):
        # self._generator_db_url()
        #self._scan_page_list('https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&postalCodes=BE-1348')
        # self._add_set_to_csv()
        self.scan_page_bien_immobilier('https://www.immoweb.be/fr/annonce/kot/a-vendre/bruxelles-ville/1000/9303884?searchId=60914b580d2b7')

    def _add_set_to_csv(self):
        with open("url_set_immoweb.txt", 'w') as fichier :
            df = pd.DataFrame(self.addresses)
            df.to_csv(fichier)

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
        print("pager : ", url_paged)
        driver = webdriver.Firefox()
        driver.get(url_paged)

        li_pagination_item = driver.find_elements_by_css_selector('li.pagination__item')
        pagination = [0]
        for element in li_pagination_item:
            nombre = re.search(self.regex, element.text)
            if nombre != None:
                pagination.append(int(nombre.group(0)))
        print(pagination)
        if max(pagination) == 333:
            driver.close()
            print("333 pages a scaner => url passée : ", url_paged)
            return False

        a_elements = driver.find_elements_by_css_selector('a.card__title-link')
        for element in a_elements:
            self.addresses.add(element.get_attribute('href'))
        print(len(self.addresses), " adresses de biens - ", self.addresses)

        driver.close()

        #  Section who verify if we must turn to next page
        if max(pagination) == num_pages:
            return True
        else:
            self._scan_page_list(url, num_pages+1)

    def _generator_db_url(self) -> bool:

        count_limit = 12
        count_sale = 0

        for zip in self.zip_code.zipcode:
            if count_sale < count_limit: # temporaire, pour éviter que le programme tourne de trop lors du dev.
                url_list = f"{self.url_vente}{zip}"
                self._scan_page_list(url_list)
                count_sale += 1
                print(count_sale)

        """count_rent = 0
        url_location = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-louer?countries=BE&orderBy=cheapest&postalCodes=BE-"
        for zip in self.zip_code.zipcode:
            url_list = f"{url_location}{zip}"
            self._scan_page_list(url_list)
            count_rent += 1"""

        print('page de ventes', count_sale)

        return True



immoweb = Immoweb()
immoweb.run()
