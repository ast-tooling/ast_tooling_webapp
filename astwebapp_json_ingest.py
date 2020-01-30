import django
import json
import os

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "astwebapp.settings")
django.setup()

# from django.db import models
from app.models import GMCCustomer, GMCTemplate


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_DIR = os.path.join(BASE_DIR, 'json_drop')
ARCHIVE_DIR = os.path.join(JSON_DIR, 'archive')

def gather_filenames(dir):
    json_files = []
    for f in os.listdir(dir):
        print('checking {} for files'.format(dir))
        if os.path.isfile(os.path.join(dir,f)) and f.split('.')[-1] == 'json':
            print('Appending %s' % os.path.join(dir,f))
            json_files.append(os.path.join(dir,f))
        elif os.path.isdir(os.path.join(dir,f)):
            continue
        else:
            raise FileNotFoundError('Object at path {} either was not found or is not json'.format(os.path.join(dir,f)))
    return json_files

def parse_json(json_file):
    with open(json_file,'r') as f_json:
        data = json.load(f_json)
        f_json.close()

    cust_id = int(data['wfd']['csr_params']['customer_id'])
    ffd_id = int(data['wfd']['csr_params']['ffdid'])
    if GMCCustomer.objects.filter(cust_id=cust_id).count() == 0:
        gmc_cust = GMCCustomer(
            cust_name = data['wfd']['csr_params']['username'],
            cust_id = cust_id
        )
        gmc_cust.save()

    if GMCTemplate.objects.filter(ffd_id=ffd_id).count() == 0:
        gmc_temp = GMCTemplate(
            ffd_id = ffd_id,
            ffd_name = '',
            wfd_input_type = data['wfd']['data_input']['input_type'],
            wfd_input_name = data['wfd']['data_input']['input_name'],
            wfd_name = os.path.basename(json_file).split('.')[0],
            wfd_multiple_records = data['wfd']['data_input']['multiple_records'],
            wfd_delimiter = data['wfd']['data_input']['delimiter'],
            wfd_text_qualifier = data['wfd']['data_input']['text_qualifier'],
            wfd_props = str(data['wfd']['data_input']['properties']),
            gmccustomer = gmc_cust
        )
        gmc_temp.save()

    return None

if __name__ == '__main__':

    l_files = gather_filenames(JSON_DIR)

    for f in l_files:
        parse_json(f)
        os.rename(f,os.path.join(ARCHIVE_DIR, os.path.basename(f)))
