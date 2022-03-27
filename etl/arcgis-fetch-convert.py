import sys
import os

# This script:
# - connects to a Windows ArcGIS server via SSH
# - Connects to the ARCGis server using arcpy and an SDE connection file
# - Downloads a layer from the server
# - Selects rows based on an SQL query
# - Renames columns based on configuration
# - Converts the layer to:
#   - CSV
#   - KML
#   - GeoJSON
#   - GeoXML
# - Uploads the original SHP and all converted files to CKAN
# - Uploads the GeoJSON to an Azure Blobstore

def parse_shapefile(layername, convert):
    import shapefile
    if convert:
        from pyproj import Transformer
        from shapely.ops import transform
        from shapely.geometry import shape, mapping
        transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)
   
    reader = shapefile.Reader(layername)
    fields = reader.fields[1:]
    field_names = [field[0] for field in fields] + ['lat', 'lon']
    buffer = []
    for sr in reader.shapeRecords():
        try:
            geom = sr.shape.__geo_interface__
            rec = sr.record.as_dict()
            rec = dict((k,v) for k,v in rec.items() if isinstance(v, (int, float, str, bool, type(None))))
            if convert:
                geom = transform(transformer.transform, shape(geom))
                geom = mapping(geom)
            try:
                if geom['type'] == 'Point':
                    rec['lon'], rec['lat'] = geom['coordinates']
                else:
                    rec['lon'], rec['lat'] = None, None
            except:
                logging.exception('Failed to extract points %r %r', geom, dict(geom))
            buffer.append((geom, rec))
        except:
            print('Failed to add row %r' % sr)
            continue
    print('Parsed shapefile, first few entries: %r' % buffer[:3])
    return field_names, buffer
   

def convert_to_csv(layername, out_filename, convert):
    import csv

    fieldnames, buffer = parse_shapefile(layername, convert)

    with open(out_filename, 'w') as csvfile:
        w = csv.DictWriter(csvfile, fieldnames)
        w.writeheader()
        w.writerows(x[1] for x in buffer)
    
    return out_filename


def convert_to_kml(layername, out_filename, convert):
    from fastkml import KML, Document, Placemark
    from fastkml.config import KMLNS as NS

    fieldnames, buffer = parse_shapefile(layername, convert)


    with open(out_filename, 'w') as kmlfile:
        kml = KML()
        doc = Document(ns=NS, name=layername)
        kml.append(doc)

        for geom, rec in buffer:
            description = ''.join('{}: {}<br/>'.format(f, rec.get(f, '')) for f in fieldnames)
            try:
                pm = Placemark(name=str(rec[fieldnames[0]]),
                               description=description)
                pm.geometry = geom
                doc.append(pm)
            except:
                print('BAD GEOMETRY for KML %r' % geom)
        try:
            kmlfile.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            kmlfile.write(kml.to_string(prettyprint=True))
        except:
            print('FAILED TO WRITE KML')

    
    return out_filename


def get_geo_obj(layername, convert):
    _, buffer = parse_shapefile(layername, convert)
    return dict(
            type='FeatureCollection', features=[
                dict(type='Feature', geometry=geom, properties=p)
                for geom, p in buffer 
            ]
        )


def convert_to_geojson(layername, out_filename, convert):
    import json

    with open(out_filename, 'w') as geojson:
        json.dump(get_geo_obj(layername, convert), geojson, indent=2)
    return out_filename


def convert_to_geoxml(layername, out_filename, convert):
    from json2xml import json2xml

    with open(out_filename, 'w') as geoxml:
        geoxml.write(
            json2xml.Json2xml(
                get_geo_obj(layername, convert),
                wrapper='root'
            ).to_xml()
        )
    
    return out_filename


def main():
    import fabric
    import requests
    import zipfile
    import codecs
    import datetime
    import os
    from lxml import etree

    HOST = os.environ['SSH_HOST']
    USER = os.environ['SSH_USER']
    PW = os.environ['SSH_PASSWORD']
    REMOTE_PYTHON = os.environ['REMOTE_PYTHON'] # e.g. 'c:\\python27\\ArcGISx6410.9\\python.exe'
    OUTPUT_LOCATION = os.environ['OUTPUT_LOCATION'] # e.g. '//gis-server/e$/directory'

    LAYER_NAME = os.environ['LAYER_NAME']
    DATASET_NAME = os.environ['DATASET_NAME']
    PREFIX = os.environ.get('RESOURCE_NAME_PREFIX')

    def prepare_arg(x):
        return codecs.encode(x.encode('utf8'), 'hex').decode('ascii')

    args = [
        os.environ['SDE_PATH'],
        LAYER_NAME,
        prepare_arg(os.environ.get('DELETE_FIELDS', '')),
        prepare_arg(os.environ.get('RENAME_FIELDS', '')),
        prepare_arg(os.environ.get('SELECT_EXPRESSION', '')),
        prepare_arg(OUTPUT_LOCATION),
    ]

    dataset_dict = {
        'name': DATASET_NAME,
        'title': os.environ['DATASET_TITLE'],
        'notes': os.environ['DATASET_DESCRIPTION'],
        'owner_org': os.environ['DATASET_ORG_ID'],
        'category': os.environ['DATASET_CATEGORY'],
        'update_period': os.environ['DATASET_UPDATE_PERIOD'],
        'private': os.environ['DATASET_PRIVATE'] == 'true',
    }

    # Convert dataset
    c = fabric.Connection(HOST, user=USER, connect_kwargs=dict(password=PW))
    script_file = os.path.abspath(sys.argv[0])
    c.put(script_file, '/scripts/remote.py')
    cmd = REMOTE_PYTHON + ' c:\\scripts\\remote.py {}'.format(' '.join('"%s"' % x for x in args))
    print('running\n%s' % cmd)
    c.run(cmd)

    FORMATS = ['shp', 'dbf', 'shx', 'prj', 'shp.xml']

    # Fetch result
    for ext in FORMATS:
        filename = OUTPUT_LOCATION + '/%s.%s' % (LAYER_NAME, ext)
        print('fetching %s' % filename)
        c.get(filename)
    
    # remove shp.xml
    FORMATS = FORMATS[:-1] 

    if PREFIX:
        FILENAME = '%s - %s' % (DATASET_NAME, PREFIX)
    else:
        FILENAME = DATASET_NAME
    # Create ZIP
    out_filename = '%s.zip' % FILENAME
    with zipfile.ZipFile(out_filename, 'w') as final:
        for ext in FORMATS:
            final.write('%s.%s' % (LAYER_NAME, ext), arcname='%s.%s' % (FILENAME, ext))

    to_upload = [
        ('SHP', out_filename)
    ]

    to_upload.append(('GeoJSON', convert_to_geojson(LAYER_NAME, '%s.%s' % (FILENAME, 'geojson'), True)))
    to_upload.append(('CSV', convert_to_csv(LAYER_NAME, '%s.%s' % (FILENAME, 'csv'), True)))
    to_upload.append(('GeoXML', convert_to_geoxml(LAYER_NAME, '%s.%s' % (FILENAME, 'xml'), True)))
    to_upload.append(('KML', convert_to_kml(LAYER_NAME, '%s.%s' % (FILENAME, 'kml'), True)))
    to_upload.append(('GeoJSON-ITM', convert_to_geojson(LAYER_NAME, '%s.itm.%s' % (FILENAME, 'geojson'), False)))
    to_upload.append(('CSV-ITM', convert_to_csv(LAYER_NAME, '%s.itm.%s' % (FILENAME, 'csv'), False)))
    to_upload.append(('GeoXML-ITM', convert_to_geoxml(LAYER_NAME, '%s.itm.%s' % (FILENAME, 'xml'), False)))
    to_upload.append(('KML-ITM', convert_to_kml(LAYER_NAME, '%s.itm.%s' % (FILENAME, 'kml'), False)))

    # Upload to CKAN
    base_url = os.environ.get('CKAN_HOSTNAME')
    if base_url:
        headers = {
            'Authorization': os.environ['CKAN_API_KEY']
        }
        print('Creating dataset...')
        dataset = requests.post('%s/api/action/package_create' % base_url, 
                                headers=headers, json=dataset_dict).json()
        if not dataset.get('success'):
            print('Already exists, updating dataset')
            for f in ('title', 'notes', 'category', 'update_period'):
                del dataset_dict[f]
            dataset = requests.get('%s/api/action/package_show?id=%s' % (base_url, dataset_dict['name']), headers=headers).json()
            assert dataset['success']
            dataset = dataset['result']
            dataset.update(dataset_dict)
            dataset_dict = dataset
            dataset = requests.post('%s/api/action/package_update' % base_url, 
                                    headers=headers, json=dataset_dict).json()
            assert dataset['success']
        print('Package create/fetch retval %s' % dataset)
        dataset = dataset['result']
        existing_resources = dataset['resources']

        ## LAST MODIFIED
        r = etree.fromstring(open('%s.shp.xml' % LAYER_NAME).read())
        mod_dates = sorted(set(a.attrib['Date'] for a in r.xpath('//Esri/DataProperties/lineage/Process')))
        if len(mod_dates) > 1:
            last_modified = mod_dates[-2]
            last_modified = '%s-%s-%sT12:00:00' % (last_modified[:4], last_modified[4:6], last_modified[6:8])
        else:
            last_modified = dataset['metadata_created']
        print('MODIFICATION DATES: %r -> %s' % (mod_dates, last_modified))
            
        for to_upload_format, to_upload_filename in to_upload:
            updated = False
            orig_to_upload_format = to_upload_format
            to_upload_name = to_upload_format
            if PREFIX is not None:
                to_upload_name = PREFIX + ' - ' + to_upload_name
            print('CONSIDERING UPLOAD: FMT %s, FN %s, NAME %s' % (to_upload_format, to_upload_filename, to_upload_name))
            for resource in existing_resources:
                existing_filename = resource['url'].split('/')[-1]
                if existing_filename == to_upload_filename and resource.get('name') == to_upload_name:
                    to_upload_format = to_upload_format.split('-')[0]
                    resource_dict = {
                        'package_id': dataset['id'],
                        'name': to_upload_name,
                        'format': to_upload_format,
                        'created': resource['created'],
                        'position': resource['position'],
                        'last_modified': last_modified,
                        'id': resource['id'],
                    }
                    ret = requests.post('%s/api/action/resource_update' % base_url,
                                        data=resource_dict, headers=headers,
                                        files=[('upload', open(to_upload_filename, 'rb'))]).json()
                    print('RESOURCE UPDATED: %s' % ret)
                    updated = True
                    break
            if not updated:
                resource_dict = {
                    'package_id': dataset['id'],
                    'name': to_upload_name,
                    'format': to_upload_format,
                }
                ret = requests.post('%s/api/action/resource_create' % base_url,
                                    data=resource_dict, headers=headers,
                                    files=[('upload', open(to_upload_filename, 'rb'))]).json()
                print('RESOURCE CREATED:%s' % ret)

    # Upload to BlobStore
    blobstore_connection_str = os.environ.get('BLOBSTORE_CONNECTION_STRING')
    if blobstore_connection_str:
        from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
        filename = '%s.%s' % (FILENAME, 'geojson')
        src_filename = convert_to_geojson(LAYER_NAME, filename, True)
        blob_service_client = BlobServiceClient.from_connection_string(blobstore_connection_str)
        container = os.environ['BLOBSTORE_CONTAINER']
        container_client = blob_service_client.get_container_client(container)
        with open(src_filename, 'rb') as data:
            container_client.upload_blob(filename, data, overwrite=True)
            print('UPLOADED: %s to CONTAINER %s' % (filename, container))


def main_remote():
    import arcpy
    import shutil
    import time

    def prepare_arg(x):
        return x.decode('hex').decode('utf8')    

    sde, layer, fields2delete, rename, select_expression, output_base = sys.argv[1:]
    fields2delete, rename, select_expression = prepare_arg(fields2delete), prepare_arg(rename), prepare_arg(select_expression)

    output_base = output_base.replace('/', '\\')
    shutil.rmtree(output_base, ignore_errors=True)
    time.sleep(3)
    os.mkdir(output_base)

    # Local variables:
    source = sde

    # Set Geoprocessing environments
    arcpy.env.outputCoordinateSystem = ""

    # Process: Feature Class to Geodatabase (multiple)
    tempdb = output_base + 'convert.gdb'
    arcpy.CreateFileGDB_management(output_base, 'convert.gdb')
    time.sleep(3)

    arcpy.FeatureClassToGeodatabase_conversion("'%s'" % source, tempdb)
    templayer = tempdb + '\\' + layer

    if select_expression:
        tempdb = output_base + 'select.gdb'
        arcpy.CreateFileGDB_management(output_base, 'select.gdb')
        templayer2 = tempdb + '\\' + layer
        arcpy.Select_analysis(templayer, templayer2, select_expression)
        templayer = templayer2

    if fields2delete:
        arcpy.DeleteField_management(templayer, fields2delete)

    if rename:
        rename = [x.split(':') for x in rename.split(';')]
        existing_fields = [[f.name, f.type, f.length] for f in arcpy.ListFields(templayer)]
        existing_fields = dict((x[0], x[1:]) for x in existing_fields)
        processed_rename = []
        for x in rename:
            if x[1] in existing_fields:
                processed_rename.append(x)
            else:
                print('ERROR - no such field in source "%s"' % x[1])

        for to_field, from_field in processed_rename:
            arcpy.AlterField_management(templayer, from_field, to_field)

        print('---')
        print([(f.name, f.type, f.length) for f in arcpy.ListFields(templayer)])
        print('---')


    arcpy.FeatureClassToShapefile_conversion(templayer, output_base)




if __name__ == '__main__':
    if sys.argv[0].endswith('remote.py'):
        from arcpy import ExecuteError
        try:
            main_remote()
        except ExecuteError:
            print(repr(arcpy.GetMessages()))
            raise
    else:
        main()