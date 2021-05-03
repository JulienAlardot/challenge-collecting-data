
import re
from typing import List, Dict
from Core.datautils import DataStruct
import pandas as pd


class Immoweb():

    def __init__(self):
        self.data_immo: Dict = {}
        self.list_URL: List[str] = []
        self.url_error: List[str] = []
        self.zip_code: List = DataStruct().get_zipcode_data()

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

    def _scan_page_list(self, url: str) -> bool:
        print(url)

    def _generator_db_url(self) -> bool:
        """https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&page=3&orderBy=relevance
        https://www.immoweb.be/fr/recherche/maison-et-appartement/a-louer?countries=BE&minPrice=700&maxPrice=1000&page=315&orderBy=cheapest
        https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&postalCodes=BE-1341,BE-1650&orderBy=cheapest"""

        init_url = "https://www.immoweb.be/fr/recherche/maison-et-appartement/"
        middle_url = [
                   'a-louer?countries=BE&minPrice=0&maxPrice=700&page=',
                   'a-louer?countries=BE&minPrice=700&maxPrice=1099&page=',
                   'a-louer?countries=BE&minPrice=1100&maxPrice=1499&page=',
                   'a-louer?countries=BE&minPrice=1500page='
                   ]

        middle_url = '?countries=BE&'
        end_url = '&orderBy=cheapest'
        num_max_page = 333

        for type_in in middle_url:
            for pager in range(1, num_max_page + 1):
                url_list = f"{init_url}{type_in}{pager}{end_url}"

            self._scan_page_list(url_list)

        url_vente = "https://www.immoweb.be/fr/recherche/maison-et-appartement/a-vendre?countries=BE&postalCodes=BE-"
        for zip in self.zip_code.zipcode:
            url_list = f"{url_vente}{zip}{end_url}"
            self._scan_page_list(url_list)

        return True



immoweb = Immoweb()
immoweb.run()
