#!/usr/bin/env python
# coding: utf-8

import ckanapi
import requests
import os
import sys


SRC=os.environ['MIGRATE_FROM']
DST='http://localhost:5000'
API=os.environ['ADMIN_API_KEY']
PREFIX=SRC + '/dataset'


source = ckanapi.RemoteCKAN(SRC)
dest = ckanapi.RemoteCKAN(DST, apikey=API)


for org in source.action.organization_list():
    print(org)
    org = source.action.organization_show(id=org)
    dest.action.organization_create(**org)
    

session = requests.Session()
for dataset_id in source.action.package_list():
    print(dataset_id)
    dataset = source.action.package_show(id=dataset_id)
    resources = dataset['resources']
    dataset['resources'] = []
    try:
        ret = dest.action.package_create(**dataset)
    except Exception as e:
        print('Failed to create %s' % e)
        ret = dest.action.package_update(**dataset)
    for resource in resources:
        resource_url = resource['url']
        print(resource_url)
        if resource_url.startswith(PREFIX):
            # resource['url'] = os.path.basename(resource_url)
            print(resource)
            print(os.path.basename(resource_url))
            nr = dict((k, v) for k, v in resource.iteritems() if k in ('package_id', 'created', 'last_modified', 'id', 'name', 'description', 'format', 'position', 'mimetype') and v)
            with open('tmp-dl', 'wb') as temp:
                temp.write(session.get(resource_url).content)
            basename = resource_url.split('/')[-1]
            # {'clear_upload': u'', 'format': u'', 'url': u'unnamed-br.png', 'description': u'', 'upload': FieldStorage('upload', u'unnamed-br.png'), 'package_id': u'test3', 'name': u'unnamed-br.png'}
            nr['url'] = os.path.basename(resource_url)
            nr['upload'] = (os.path.basename(resource_url), open('tmp-dl', 'rb'))
            nr['clear_upload'] = ''
            print(nr)
            dest.action.resource_create(**nr)
        else:
            dest.action.resource_create(**resource)
    
print(len(source.action.package_list()))