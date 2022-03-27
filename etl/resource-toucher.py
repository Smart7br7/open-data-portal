import os
import requests
import datetime

# This (quite specific) script ensures that the last modified dates of URL resources in datasets are more 'reasonable'.
# The logic goes as follows:
# For all datasets and all resources, when a resource's format is 'URL':
# - When the dataset's update period is 'ONLINE', we set the date to be today
# - When the dataset's update period is NOT 'ONLINE', we force the date to be the creation date of the dataset

now = datetime.datetime.now().isoformat()

def all_datasets(base_url, headers):
    print(f'GETTING ALL IDS from {base_url}')
    dataset_ids = requests.get(f'{base_url}/api/3/action/package_list', headers=headers).json()['result']
    for id in dataset_ids:
        print('DATASET ID', id)
        yield requests.get(f'{base_url}/api/3/action/package_show?id={id}&cachebusting={now}', headers=headers).json()['result']

if __name__=='__main__':
    base_url = os.environ['CKAN_HOSTNAME']
    headers = {
        'Authorization': os.environ['CKAN_API_KEY']
    }

    for dataset in all_datasets(base_url, headers):
        resources = dataset['resources']
        for resource in resources:
            if resource['format'].upper() == 'URL':
                if dataset['update_period'].upper() == 'ONLINE':
                    resource['last_modified'] = datetime.datetime.now().isoformat()
                else:
                    resource['last_modified'] = resource['created']
                ret = requests.post('%s/api/action/resource_update' % base_url,
                                    data=resource, headers=headers).json()
                print('RESOURCE UPDATED: {}, {}, {}'.format(dataset, resource['name'], resource['url']))
