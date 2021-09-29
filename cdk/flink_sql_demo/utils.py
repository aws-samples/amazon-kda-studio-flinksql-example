import boto3
import requests


def check_glue_database(db_name: str) -> bool:
    client = boto3.client('glue', region_name='us-east-1')
    resp = client.get_databases()
    databases = resp['DatabaseList']

    for database in databases:
        if database['Name'] == db_name:
            print(f'Found database: {db_name}')
            return True

    return False


def get_public_cidr() -> str:
    url = 'https://checkip.amazonaws.com'
    resp = requests.get(url).text
    ip = resp.replace('\n', '')
    print(f'Using {ip} as source')
    return f'{ip}/32'


def check_es_service_policy() -> bool:
    client = boto3.client('iam', region_name='us-east-1')
    resp = client.list_roles(
        PathPrefix='/aws_service_role/es.amazonaws.com/'
    )
    roles = resp['Roles']
    if len(roles) > 0:
        print('ES Service Role Found')
        return True
    else:
        print('ES Service Role not found! Will be created.')
        return False
