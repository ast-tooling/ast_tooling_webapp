import json
import os

from app.models import GMCCustomer, GMCTemplate

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
JSON_DIR = os.path.join(BASE_DIR, 'json_dumps')

def gather_filenames(dir):
    json_files = []
    for f in os.listdir(JSON_DIR):
        if os.path.isfile(os.path.join(JSON_DIR,f)) and f.split('.')[-1] == 'json':
            json_files.append(os.path.abspath(f))
        else:
            raise FileNotFoundException('File either was not found or is not json')
    return json_files





if __name__ == '__main__':
    print(BASE_DIR)
    l_files = gather_filenames(JSON_DIR)
    print(l_files)
