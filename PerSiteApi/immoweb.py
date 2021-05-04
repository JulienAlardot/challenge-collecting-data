import sys
from Core.datautils import DataStruct
import re
import pandas as pd
import bs4
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from typing import List, Dict


class Immoweb():

    def __init__(self):
        self.data_immo: Dict = {}
        self.list_URL: List[str] = []
        self.url_error: List[str] = []
        self.zip_code: List = DataStruct().get_zipcode_data()
        self.addresses: set = {}
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}
        self.regex = re.compile("\d{1,3}$")


    def _ajouteURL(self, url, valeur=False):
        # vérifie si une url est dans le fichier
        if (url not in self.list_URL) and (url != ""):
            self.list_URL.append(url)

    def _coupepage(self, text: str, debut: str, fin: str, n=1):
        # a remplacer par les fonctions de Beautifull soup
        debutT = text.index(debut) + len(debut)
        texte = text[debutT:]
        finT = text.index(fin)

        if finT:
            return texte[:finT], texte[finT:]
        else:
            return False, False

    def scan_page_bien_immobilier(self, text: str, url: str) -> Dict:
        return {}

    def run(self):
        #  print (self.zip_code.zipcode)
        self._generator_db_url()
        #self._scan_page_list('https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&postalCodes=BE-1348')

    # https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&orderBy=cheapest&postalCodes=BE-1341,1348
    def _scan_page_list(self, url: str, num_pages: int=1) -> bool:
        """
        Scan the page with result of research.
        :param url: An String url without page=xx
        :param nbr_pages: Number of the page to scan
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
            print("333 pages a scaner => url passée : ", url_paged)
            return False

        a_elements = driver.find_elements_by_css_selector('a.card__title-link')
        for element in a_elements:
            self.addresses.add(element.get_attribute('href'))
        print(len(self.addresses), " adresses de biens - ", self.addresses)

        #  Section who verify if we must turn to next page
        if max(pagination) == num_pages:
            driver.close()
            return True
        else:
            self._scan_page_list(url, num_pages+1 )
            driver.close()

    def _generator_db_url(self) -> bool:

        count_limit = 3
        count_sale = 0
        url_vente = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&orderBy=cheapest&postalCodes=BE-"
        for zip in self.zip_code.zipcode:
            if count_sale < count_limit: # temporaire, pour éviter que le programme tourne de trop lors du dev.
                url_list = f"{url_vente}{zip}"
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
