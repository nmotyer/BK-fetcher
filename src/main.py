import requests
import shutil
import boto3
import os

os.environ['AWS_PROFILE'] = 'personal'

# Below URL probably changes per datasource update
base_url = 'https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip'
s3 = boto3.client('s3')

def get_download_links():
    # TODO: Programatically generate these URLs via scraping https://data.gov.au/dataset/ds-dga-5bd7fcab-e315-42cb-8daf-50b7efc2027e/details
    url_part1 = 'https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip'
    url_part2 = 'https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip'
    return [url_part1, url_part2]

def retrieve(url):
    """Downloads the file contained at the url endpoint in chunks and streams it to disk -> s3, then deletes the file"""
    local_filename = url.split('/')[-1]
    #with requests.get(url, stream=True) as r:
    #    with open(local_filename, 'wb') as f:
    #        shutil.copyfileobj(r.raw, f, length=16*1024*1024)
    return local_filename

def upload_s3(filename):
    """Uploads to s3 in chunks to avoid RAM/memory issues. ideas borrowed from https://amalgjose.com/2020/08/13/python-program-to-stream-data-from-a-url-and-write-it-to-s3/"""
    with open(filename, 'rb') as f:
        #f.raw.decode_content = True
        conf = boto3.s3.transfer.TransferConfig(multipart_threshold=10000, max_concurrency=4)
        s3.upload_fileobj(f, 'bk-abn-raw', f'ABN/{filename}', Config=conf)
    return

def entry_point(event=None, context=None):
    urls = get_download_links()
    for url in urls:
        filename = retrieve(url)    # download file to local storage and store the filename
        upload_s3(filename)         # upload the file to s3
        shutil.rmtree(filename)     # delete file from local storage
    return

entry_point()