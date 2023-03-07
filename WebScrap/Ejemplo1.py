# Ejemplo de Scrapping 1
# Link YTB https://www.youtube.com/watch?v=HpRsfpPuUzE 

import httpx
from selectolax.parser import HTMparser
from colorama import Fore, Back, Style
from dataclasses import dataclass, asdict


@dataclass
class Product:
    fabricante: str
    modelo: str
    precio: str

def get_html(page):
    url =f'https://www.thomann.de/intl/{page}guitarras_electricas_para_zurdos.html'
    resp = httpx.get(url)
    return HTMparser(resp.text)

def main():
    html = get_html( )
    print(html.css_first('title').text())


if __name__ == '__main__':
    main()
