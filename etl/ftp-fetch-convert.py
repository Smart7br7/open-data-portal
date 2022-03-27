import sys
import os
import ftplib
import logging
import tempfile
import dataflows as DF
import requests
import datetime

# This script will pull a file from an FTP server and create/update a CSV resource in a CKAN dataset with its data.
# The script will look for a csv/excel file matching a certain pattern in the FTP root directory.
# If it finds it, it will move it to the 'old/' directory so that it's not processed again.
# 
# FTP server parameters are provided in env vars: FTP_HOST, FTP_USER, FTP_PASSWORD
# File pattern is provided in env var: FILE_PATTERN
# The file is processed using these parameters:
# - Headers row can be specified using HEADERS_ROW env var
# - Some columns can be ignored using DELETE_FIELDS env var
# CKAN server parameters are provided in env vars: CKAN_FILENAME, CKAN_DATASET_ID, CKAN_HOSTNAME, CKAN_API_KEY, CKAN_RESOURCE_NAME

FTP_HOST = os.environ['FTP_HOST']
FTP_USER = os.environ['FTP_USER']
FTP_PASSWORD = os.environ['FTP_PASSWORD']
FILE_PATTERN = os.environ['FILE_PATTERN']

HEADERS_ROW = int(os.environ.get('HEADERS_ROW', 1))

CKAN_FILENAME = os.environ['CKAN_FILENAME']
CKAN_DATASET_ID = os.environ['CKAN_DATASET_ID']
CKAN_HOSTNAME = os.environ['CKAN_HOSTNAME']
CKAN_API_KEY = os.environ['CKAN_API_KEY']

CKAN_RESOURCE_NAME = os.environ.get('CKAN_RESOURCE_NAME', 'CSV')

DELETE_FIELDS = os.environ.get('DELETE_FIELDS', '').split(',') or None

print('CONFIGURATION')
print('FILE_PATTERN: %r' % FILE_PATTERN)
print('HEADERS_ROW: %r' % HEADERS_ROW)
print('CKAN_RESOURCE_NAME: %r' % CKAN_RESOURCE_NAME)

headers = {
    'Authorization': os.environ['CKAN_API_KEY']
}

def normalize_filename(filename):
    try:
        return bytes(map(lambda x: ord(x), filename)).decode('utf8')
    except:
        print('FAILED to normalize file', filename)
        return 'dummy'

if __name__=='__main__':
    logging.getLogger().setLevel(logging.INFO)
    with ftplib.FTP_TLS(FTP_HOST, FTP_USER, FTP_PASSWORD) as ftp:
        ftp.prot_p()
        candidates = sorted(
            (int(props['modify']), filename)
            for filename, props in ftp.mlsd()
            if FILE_PATTERN in normalize_filename(filename) and props['type'] == 'file'
        )
        logging.info('CONNECTED!')
        candidates = [x[1] for x in candidates]
        logging.info('FOUND {} CANDIDATES'.format(len(candidates)))
        if len(candidates) == 0:
            logging.info('Failed to find any candidate, bailing out')
            sys.exit(0)
        candidate = candidates[-1]
        with tempfile.NamedTemporaryFile('wb', suffix=candidate, delete=False) as tmpfile:
            ftp.retrbinary('RETR {}'.format(candidate), lambda block: tmpfile.write(block))
            tmpfile.close()
            DF.Flow(
                DF.load(tmpfile.name, headers=HEADERS_ROW),
                DF.update_resource(-1, path=CKAN_FILENAME),
                *([DF.delete_fields(DELETE_FIELDS)] if DELETE_FIELDS else []),
                DF.printer(),
                DF.dump_to_path('.')
            ).process()
            os.unlink(tmpfile.name)
            package = requests.get('%s/api/action/package_show' % CKAN_HOSTNAME,
                                    params=dict(id=CKAN_DATASET_ID), headers=headers).json()['result']
            resources = package['resources']
            new_resource = dict(
                package_id=CKAN_DATASET_ID,
                name=CKAN_RESOURCE_NAME,
                format='CSV',
            )
            logging.info('NEW RESOURCE NAME: {}'.format(new_resource['name']))
            for resource in resources:
                if resource['format'].upper() == 'CSV' and resource['name'] == new_resource['name']:
                    print('FOUND EXISTING RESOURCE')
                    new_resource.update(dict(
                        created=resource['created'],
                        position=resource['position'],
                        id=resource['id'],
                    ))
                    break
            new_resource['last_modified'] = datetime.datetime.now().isoformat()
            if new_resource.get('id'):
                ret = requests.post('%s/api/action/resource_update' % CKAN_HOSTNAME,
                        data=new_resource, headers=headers,
                        files=[('upload', open(CKAN_FILENAME, 'rb'))]).json()
                logging.info('RESOURCE UPDATED: %s' % ret)
            else:
                ret = requests.post('%s/api/action/resource_create' % CKAN_HOSTNAME,
                                    data=new_resource, headers=headers,
                                    files=[('upload', open(CKAN_FILENAME, 'rb'))]).json()
                logging.info('RESOURCE CREATED: %s' % ret)
        suffix = datetime.date.today().strftime('%Y%m%d')
        for candidate in candidates:
            logging.info('MOVING {} to old/'.format(candidate))
            ftp.rename(candidate, 'old/' + candidate + '-' + suffix)
