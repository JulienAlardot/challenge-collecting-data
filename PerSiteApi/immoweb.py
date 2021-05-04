
import re
from typing import List, Dict
from Core.datautils import DataStruct
import pandas as pd
import bs4
import requests
from selenium import webdriver
from selenium.webdriver.common.keys import Keys


class Immoweb():

    def __init__(self):
        self.data_immo: Dict = {}
        self.list_URL: List[str] = []
        self.url_error: List[str] = []
        self.zip_code: List = DataStruct().get_zipcode_data()
        self.adresses: set = {}
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:20.0) Gecko/20100101 Firefox/20.0'}

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
        #  self._generator_db_url()
        self._scan_page_list('https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre/louvain-la-neuve/1348?countries=BE&orderBy=cheapest')

    def _scan_page_list(self, url: str) -> bool:
        print(url)
        driver = webdriver.PhantomJS()
        driver.get(my_url)
        p_element = driver.find_element_by_id(id_='main-content')
        print(p_element.text)

    def _generator_db_url(self) -> bool:
        end_url = '&orderBy=cheapest'

        count_sale = 0
        url_vente = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&postalCodes=BE-"
        for zip in self.zip_code.zipcode:
            url_list = f"{url_vente}{zip}{end_url}"
            self._scan_page_list(url_list)
            count_sale += 1

        count_rent = 0
        url_location = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-louer?countries=BE&postalCodes=BE-"
        for zip in self.zip_code.zipcode:
            url_list = f"{url_location}{zip}{end_url}"
            self._scan_page_list(url_list)
            count_rent += 1

        print('page de ventes', count_sale, 'pages de locations', count_rent)

        return True



immoweb = Immoweb()
immoweb.run()
