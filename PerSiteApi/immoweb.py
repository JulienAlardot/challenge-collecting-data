#!/usr/bin/python3.8

import re
from typing import List, Dict


class Immoweb():

    def __init__(self):
        self.data_immo: Dict = {}
        self.list_URL: List[str] = []
        self.url_error: List[str] = []


    def _ajouteURL(self, url, valeur=False):
        # vérifie si une url est dans le fichier
        if (url not in self.list_URL) and (url != ""):
            self.list_URL.append(url)


    def _coupepage(self, text: str, debut: str, fin: str, n=1):
        # a remplacer avec les fonctions de Beautifull soup
        debutT = text.index(debut) + len(debut)
        texte = text[debutT:]
        finT = text.index(fin)

        if finT:
            return texte[:finT], texte[finT:]
        else:
            return False, False


