import os
import requests
import datetime
import json
import dataflows as DF

# This script loads data from a SharePoint list and creates/updates a resource in a CKAN dataset with its data.
# The Sharepoint URL is provided in the 'URL' environment variable.

now = datetime.datetime.now().isoformat()

BASE_URL = os.environ['CKAN_HOSTNAME']
HEADERS = {
    'Authorization': os.environ['CKAN_API_KEY']
}
DATASET_NAME = os.environ['DATASET_NAME']

if __name__=='__main__':
    data = requests.get(os.environ['URL']).text\
                .replace('\t', '')\
                .replace('\n', ' ')\
                .replace('&quot;', '\\"')
    data = json.loads(data)
    data = data['Root']['Items']['Item']
    data = [
        dict(
            ((x['Caption'], x['Value'])
             for x in item['Fields']['Field']),
            URL=item.get('URL')
        )
        for item in data
    ]
    filename = DATASET_NAME+'.csv'
    DF.Flow(
        data,
        DF.update_resource(-1, name=DATASET_NAME, path=filename),
        DF.dump_to_path(),
    ).process()

    print(f'GETTING DATASET {DATASET_NAME} from {BASE_URL}')
    dataset = requests.get(f'{BASE_URL}/api/3/action/package_show?id={DATASET_NAME}&cachebusting={now}',
                            headers=HEADERS).json()['result']
    assert dataset.get('id')

    resources = dataset.get('resources', [])
    selected = None
    for resource in resources:
        if resource['name'].upper() == 'CSV':
            selected = resource
            break

    new_resource = dict(
        package_id=dataset['id'],
        name='CSV',
        format='CSV',
        last_modified=now,
    )

    if selected:
        new_resource.update(dict(
            created=selected['created'],
            position=selected['position'],
            id=selected['id'],
        ))

    if new_resource.get('id'):
        ret = requests.post('%s/api/action/resource_update' % BASE_URL,
                data=new_resource, headers=HEADERS,
                files=[('upload', open(filename, 'rb'))]).json()
        print('RESOURCE UPDATED: %s' % ret)
    else:
        ret = requests.post('%s/api/action/resource_create' % BASE_URL,
                            data=new_resource, headers=HEADERS,
                            files=[('upload', open(filename, 'rb'))]).json()
        print('RESOURCE CREATED: %s' % ret)

