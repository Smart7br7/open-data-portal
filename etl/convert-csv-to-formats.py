import os
import json
from pathlib import PurePath
import requests
import tempfile
import datetime
import tabulator
import dateutil.parser
from json2xml import json2xml

# This script runs over all datasets in the source CKAN instance,
# locates all resources of CSV format and creates identical resources in the same dataset with different formats:
# - XLSX
# - JSON
# - XML

now = datetime.datetime.now().isoformat()
session = requests.Session()
session.headers.update({
    'Authorization': os.environ['CKAN_API_KEY']
})
session.headers.update(tabulator.config.HTTP_HEADERS)

def lm(resource, next=False, iso=True):
    ret = resource.get('last_modified') or resource.get('created')
    if ret:
        ret = dateutil.parser.isoparse(ret)
        if next:
            ret += datetime.timedelta(seconds=1)
        if iso:
            return ret.isoformat()
    return ret
    
    

def all_datasets(base_url):
    print(f'GETTING ALL IDS from {base_url}')
    datasets = session.get(f'{base_url}/api/3/action/current_package_list_with_resources?limit=1000').json()['result']
    for d in datasets:
        id = d['name']
        print('DATASET ID', id)
        yield session.get(f'{base_url}/api/3/action/package_show?id={id}&cachebusting={now}').json()['result']

def convert_to_XLSX(csv_url, filename):
    with tabulator.Stream(csv_url, headers=1, http_session=session) as s:
        s.save(filename, sheet=dataset['name'])

def convert_to_JSON(csv_url, filename):
    with tabulator.Stream(csv_url, headers=1, http_session=session) as s:
        with open(filename, 'w', encoding='utf8') as o:
            o.write('[\n')
            first = True
            for row in s.iter(keyed=True):
                if not first:
                    o.write(',\n')
                first = False
                o.write(json.dumps(row, ensure_ascii=False))
            o.write('\n]\n')

def convert_to_XML(csv_url, filename):
    with tabulator.Stream(csv_url, headers=1, http_session=session) as s:
        with open(filename, 'w', encoding='utf8') as o:
            o.write('<?xml version="1.0" encoding="UTF-8" ?>\n<root>\n')
            for row in s.iter(keyed=True):
                xml = json2xml.Json2xml(row, wrapper='item').to_xml()
                if xml is not None:
                    o.write(xml + '\n')
                else:
                    print('BAD ROW when converting to XML: %r' % row)
            o.write('</root>\n')

if __name__=='__main__':
    base_url = os.environ['CKAN_HOSTNAME']

    for dataset in all_datasets(base_url):
        resources = dataset['resources']
        names = dict()
        for resource in resources:
            names.setdefault(resource['name'], []).append(resource)
        for name, res in names.items():
            old = sorted(res, key=lm, reverse=True)
            old = old[1:]
            if len(old) > 0:
                print('%s: FOUND %d extra resoruces, will delete' % (name, len(old)))
                for res in old:
                    id = res['id']
                    resp = session.post('%s/api/action/resource_delete' % base_url, data=dict(id=id))
                    print(resp)

    for dataset in all_datasets(base_url):
        resources = dataset['resources']

        CSVs=[]
        for resource in resources:
            if resource['format'].upper() == 'CSV' and resource['state'] == 'active':
                CSVs.append(resource)
        print('FOUND {} CSV RESOURCES'.format(len(CSVs)))
        while len(CSVs) > 0:
            csv_resource = CSVs.pop(0)
            for new_format, new_suffix in (('XLSX', '.xlsx'), ('JSON', '.json'), ('XML', '.xml')):
                new_name = csv_resource['name'].upper()
                if 'CSV' in new_name:
                    new_name = new_name.replace('CSV', new_format)
                else:
                    new_name = new_name + ' - ' + new_format
                new_resource = dict(
                    package_id=dataset['id'],
                    name=new_name,
                    format=new_format,
                )
                print('NEW RESOURCE NAME: {}'.format(new_resource['name']))
                for resource in resources:
                    if resource['format'].upper() == new_format and resource['name'] == new_resource['name']:
                        print('FOUND EXISTING {} RESOURCE'.format(new_format))
                        new_resource.update(dict(
                            created=resource['created'],
                            position=resource['position'],
                            last_modified=resource['last_modified'],
                            id=resource['id'],
                        ))
                        break
                if lm(new_resource) and lm(new_resource) == lm(csv_resource, next=True):
                    print('{} resource slightly newer than csv resource: {} > {}'.format(new_format, lm(new_resource), lm(csv_resource)))
                    continue
                new_resource['last_modified'] = lm(csv_resource, next=True)
                csv_url = csv_resource['url']
                print(f'PROCESSING {csv_url}')
                filename = PurePath(csv_url).with_suffix(new_suffix).name
                with tempfile.TemporaryDirectory() as tmpdir:
                    filename = os.path.join(tmpdir, filename)
                    globals()['convert_to_' + new_format](csv_url, filename)
                    if new_resource.get('id'):
                        ret = session.post('%s/api/action/resource_update' % base_url,
                                data=new_resource,
                                files=[('upload', open(filename, 'rb'))]).json()
                        print('RESOURCE UPDATED: %s' % ret)
                    else:
                        ret = session.post('%s/api/action/resource_create' % base_url,
                                            data=new_resource,
                                            files=[('upload', open(filename, 'rb'))]).json()
                        print('RESOURCE CREATED: %s' % ret)

