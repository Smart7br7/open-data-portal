import os
import shutil
import dataflows as DF
from dataflows_ckan import dump_to_ckan
import datetime

# This script is used to fetch data from the Defi app and create a CKAN dataset with it.

URL = 'https://us-central1-eifo-defi.cloudfunctions.net/api/defis?key=apikey'

if __name__ == '__main__':
    shutil.rmtree('.checkpoints/defi', ignore_errors=True)
    DF.Flow(
        DF.load(URL, name='defi', format='json'),
        DF.checkpoint('defi'),
    ).process()

    DF.Flow(
        DF.checkpoint('defi'),
        DF.filter_rows(lambda row: row['id'] != 'copyrights-1'),
        DF.set_type('contactName', type='string', transform=str),
        DF.add_field('lat', 'number', lambda row: row['coordinates']['geopoint']['latitude']),
        DF.add_field('lon', 'number', lambda row: row['coordinates']['geopoint']['longitude']),
        DF.add_field('itm-x', 'number', lambda row: row['coordinates']['itm']['x']),
        DF.add_field('itm-y', 'number', lambda row: row['coordinates']['itm']['y']),
        DF.set_type('createdAt', type='date', transform=lambda v: datetime.date.fromtimestamp(v['seconds'])),
        DF.set_type('updatedAt', type='date', transform=lambda v: datetime.date.fromtimestamp(v['seconds'])),
        DF.delete_fields(['coordinates']),
        DF.update_resource(-1, path='defi.csv', name='CSV'),
        DF.duplicate('CSV', 'GeoJSON'),
        DF.add_field('geometry', 'geopoint', lambda row: [float(row['lon']), float(row['lat'])], resources=-1),
        DF.delete_fields(['lat', 'lon'], resources=-1),
        DF.update_resource(-1, path='defi.geojson'),
        DF.update_package(name='defi', title='איפה דפי'),
        DF.printer(),
        dump_to_ckan(
            os.environ['CKAN_HOSTNAME'],
            os.environ['CKAN_API_KEY'],
            os.environ['DATASET_ORG_ID'],
            force_format=False,
        )
    ).process()