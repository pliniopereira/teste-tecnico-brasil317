import os
from time import sleep

import requests
from bs4 import BeautifulSoup as bs

from Py_mongo import import_content as json_mongo


def remove_repetidos(lista):
    lista_sem_repetidos = []
    for i in lista:
        if i not in lista_sem_repetidos:
            lista_sem_repetidos.append(i)
    lista_sem_repetidos.sort()
    return lista_sem_repetidos


def find_datasets(url_site):
    """
    Funcao que procura links de possiveis locais para futuro scraping
    """

    try:
        res = requests.get(url_site)
        res.raise_for_status()
        soup = bs(res.text, 'html.parser')
        datasets_links = []

        for a in soup.find_all('a', href=True):
            if "dataset/" in a['href']:
                datasets_links.append(a['href'])
        datasets_links = remove_repetidos(datasets_links)

        sleep(1)

        return datasets_links
    except Exception as e:
        print("Failed!\n{}".format(e))


def check_and_downloadind_csv(datasets_links, base_url):
    """
    Funcao que procura links contendo .csv no final e faz o download para a pasta dataset
    """
    try:
        for c in datasets_links:
            url_site = f'{base_url + c}'

            res = requests.get(url_site)
            res.raise_for_status()
            # TODO: raise_for_status check
            soup = bs(res.text, 'html.parser')

            for link in soup.findAll('a', href=True):
                current_link = link.get("href")
                if ".csv" in link['href']:
                    print('Found CSV: ' + current_link)
                    only_filename = current_link.split('/')
                    print('Downloading ' + only_filename[-1])

                    sleep(10)

                    response = requests.get(current_link, stream=True)

                    filename = str(only_filename[-1])

                    os.makedirs(str(c[1:] + '/'), exist_ok=True)

                    name_path = str(c[1:]) + '/' + os.path.basename(filename)

                    csvFile = open(os.path.join(str(name_path)), 'wb')
                    # TODO: Colocar datas no arquivo MM DD YYYY
                    for data in response.iter_content():
                        csvFile.write(data)
                    csvFile.close()
                    # utiliza a funcao json_mongo
                    json_mongo(name_path)
                    # TODO: diferenciar arquivos HTML csv
                    # TODO: deletar arquivos csv
            sleep(1)

    except Exception as e:
        print("Failed check_and_downloadind_csv!\n{}".format(e))


def main():
    """Função principal da aplicação."""

    base_url = 'http://dados.gov.br'
    url_site = f'{base_url}/dataset?q=licita%C3%A7%C3%B5es&sort=metadata_modified+desc'
    os.makedirs('dataset', exist_ok=True)

    datasets_found = find_datasets(url_site)
    check_and_downloadind_csv(datasets_found, base_url)

    print("EOF")


if __name__ == "__main__":
    main()

