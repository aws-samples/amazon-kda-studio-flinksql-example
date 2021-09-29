import requests
import os


def download_connector(url: str, filename: str) -> None:
    if not os.path.isfile(f'../connectors/{filename}'):
        r = requests.get(url, allow_redirects=True)
        open(f'../connectors/{filename}', 'wb').write(r.content)
        print('Connector Downloaded')
    else:
        print('Connector already downloaded')
