import pandas as pd
import requests 
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin

def authenticate():
    user_and_password = {"username": "julukasga@gmail.com", "password": "Real10barcelon@"}
    authentication = requests.post(urljoin(base_url, 'token'), data=user_and_password)

    if authentication.status_code == 200:
        return authentication.json()["access_token"]
    else:
        raise Exception(f"Authentication failed: {authentication.text}")
    

if __name__ == '__main__':
    base_url = 'http://tyke-app-load-balancer-962786073.us-east-2.elb.amazonaws.com/'

    archivo_resultado_modelo = pd.read_csv('/Users/test/Desktop/Trabajo Tyke/simulacion_resultados_emis_200000_empresas.csv', index_col=0)
    archivo_resultado_modelo = archivo_resultado_modelo.drop_duplicates(['NIT'])


    urls = []

    number_of_url = 3000

    print(f"CREANDO {number_of_url} PARA HACER REQUEST...")
    for i in range(number_of_url):
        nit_random = archivo_resultado_modelo['NIT'].sample(1).values[0]
        url = base_url + "predict?nit=" + str(nit_random)
        urls.append(url)
    print("LISTA CREADA CORRECTAMENTE")
    print("ENVIANDO LOS REQUEST...............")

    access_token = authenticate()
    def get_url(url):
        return requests.get(url, headers={"Authorization": f"Bearer {access_token}"}).text

    res = []
    with ThreadPoolExecutor(max_workers=200) as pool:
        res = list(pool.map(get_url,urls))

    print("LOS RESULTADOS SON: ")
    for r in res:
        print(r)