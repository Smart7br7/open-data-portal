import os
import requests
import google.auth.transport.requests
import tempfile
from pathlib import Path
import shutil
from google.oauth2.service_account import IDTokenCredentials

# This script is used to upload multiple resources from the local CKAN instance to data.gov.il.
#
# Provide data.gov credentials using environment variables: CREDS_FILENAME, CLIENT_ID, DATAGOV_CKAN_API_KEY
# Provide local configuration using environment variables: DATASET_ID, CKAN_HOSTNAME, CKAN_API_KEY
#
# Configuration is provided in the DATAGOV_RESOURCES environment variable:
# It's a '\n' separated list of items, each item is of the form '<local_resource_name>:<datagov_resource_id>'
# For each resource in the dataset, if it appears in the configuration it will duplicate it to the specified datagov resource.


def get_datagov_session():
    CREDS_FILENAME = os.environ['CREDS_FILENAME']
    CLIENT_ID = os.environ['CLIENT_ID']
    CKAN_API_KEY = os.environ['DATAGOV_CKAN_API_KEY']

    credentials = IDTokenCredentials.from_service_account_file(CREDS_FILENAME, target_audience=CLIENT_ID)

    request = google.auth.transport.requests.Request()
    credentials.refresh(request)

    token = credentials.token

    session = requests.Session()
    headers = {
        'User-Agent': 'datagov-internal-client',
        'Authorization': 'Bearer {}'.format(token),
        'X-Non-Standard-CKAN-API-Key': CKAN_API_KEY,
    }
    session.headers = headers
    return session


def update_datagov_resource(session, parameters, filename):
    URL = 'https://e.data.gov.il/api/3/action/resource_update'
    response = session.post(URL, data=parameters, files={'upload': open(filename, 'rb')})
    print('UPDATED DATAGOV {} with {}'.format(parameters['id'], filename, response.content))


if __name__ == '__main__':
    local_dataset = os.environ.get('DATASET_ID')

    datagov_resources = os.environ.get('DATAGOV_RESOURCES')
    datagov_resources = datagov_resources.split('\n')
    datagov_resources = [i.split(':') for i in datagov_resources]
    datagov_resources = dict((i[0], i[1]) for i in datagov_resources)
    print('GETTING DATAGOV SESSION...', flush=True)
    datagov_session = get_datagov_session()
    print('DONE!')

    base_url = os.environ['CKAN_HOSTNAME']
    headers = {
        'Authorization': os.environ['CKAN_API_KEY']
    }
    dataset = requests.get('%s/api/action/package_show?id=%s' % (base_url, local_dataset), headers=headers).json()

    for resource in dataset['result']['resources']:
        resource_name = resource['name']
        resource_format = resource['format']
        last_modified = resource['last_modified']
        url = resource['url']
        filename = Path(url).name

        print('CONSIDERING: %s (%s)' % (resource_name, resource_format))
        if datagov_resources.get(resource_name):
            print('UPLOADING TO DATAGOV: %s' % resource_name)
            resource_dict = {
                'id': datagov_resources[resource_name],
                'name': resource_name,
                'format': resource_format,
                'created': resource['created'],
                'last_modified': last_modified,
            }
            with tempfile.TemporaryDirectory() as temp:
                temp_file = os.path.join(temp, filename)
                with open(temp_file, 'wb') as out:
                    print('GETTING DATA FROM: %s' % url, flush=True)
                    stream = requests.get(url, headers=headers, stream=True).raw
                    shutil.copyfileobj(stream, out)
                    out.flush()
                    update_datagov_resource(datagov_session, resource_dict, out.name)

