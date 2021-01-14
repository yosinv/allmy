import logging
import json
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.blob._shared.base_client import create_configuration
from azure.storage.blob import ContainerClient
# from ..shared_code import splunk_data
# from . import test_for_IT
# from ..config import logs
import logging.handlers
import pprint
import os ,sys
import time
import datetime
import pathlib
import splunklib
import splunklib.results as results
import splunklib.client as client
import pprint
import logging
import json
import collections
import codecs
from time import sleep
from pathlib import Path
import urllib
import httplib2
from xml.dom import minidom

# pip install -r requirements.txt

HOST = "https://31.154.3.103"
# HOST = "https://31.154.3.103"
PORT = 8089
USERNAME = "yosin"
PASSWORD = "Corona10!"
pp = pprint.PrettyPrinter(indent=4)

# new logs due to azurefunction structure 18052020:
logger = logging.getLogger('splunk_data')
logger.setLevel(logging.INFO)

sh = logging.StreamHandler()
sh.setLevel(logging.INFO)

logger.addHandler(sh)
logger.debug('Hello debug mode')


def splunk_setServer(hostname, port, splunkuser, splunkpassword):
    # client = pyimport("splunklib.client")
    logger.debug('splunk_data module  - setServer function')
    # Connect to Splunk Enterprise
    try:
        service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD, autologin=True)
        logger.debug('service success')
        return service
    except ValueError:
        logger.error('service error')



def splunk_new_job_csv_k4ug(service, query, earliest_time, output_mode, **kwargs):
    """CSV MODE ,return k4u result in csv"""
    logger.debug("function k4u: START k4u function")
    logger.debug("**********************************************************")
    # Working query:
    # query = '"""' + query + '"""'
    # query  = 'search * | head 5'
    query = """search {}""".format(query)
    logger.info("query = " + query)
    # query = """search * | head 5"""
    kwargs_export = {"output_mode": str(output_mode),
                     "earliest_time": str(earliest_time)}
    logger.info(f' "output_mode": "{output_mode}", "earliest_time": "{earliest_time}" ')
    try:
        # rr = service.jobs.export(query, **kwargs_export)
        rr = service.jobs.export(query, **kwargs_export)

        logger.info("...done!\n")
        print("...done!\n")
        sys.stdout.write("...done!\n")
        # f = codecs.open('/home/site/k4u_logs_bi.csv', 'w')
        for result in rr:
            # if isinstance(result, results.Message):
            #     # Diagnostic messages may be returned in the results
            #     logger.info('%s: %s' % (result.type, result.message))

            # elif isinstance(result, dict):
            #     # Normal events are returned as dicts
            #     logger.info('result of type dict')

            result = result.decode('utf8')
            logger.info(str(result))
            # f.write(str(result))
        # f.close()
        querystat = 1
    except ValueError:
        logger.error('service error could not complete query execution')
        logger.error('Status query execution: * Failed * ')
        querystat = 0
    sys.stdout.write('end k4u_logs_bi function')
    sys.stdout.write('\n')
    logger.info("end k4u function")
    return bool(querystat)

def splunk_query_splunk(query, earliest_time, output_mode):
    word = "שלום"
    logger.info(word)
    # word = word.decode('UTF-8')
    # print(word)
    word = word.encode('utf8')
    logger.info(word)
    word = word.decode('utf8')
    logger.info(word)
    service = splunk_setServer(HOST, PORT, USERNAME, PASSWORD)
    # querystat = splunk_new_job_csv_360g(service, query, earliest_time, output_mode)
    querystat = splunk_new_job_csv_k4ug(service, query, earliest_time, output_mode)
    return querystat

query_stat = None
text_sent ="K4U_Log , ClaimNr ,claimant choose k4u "
query_stat= splunk_query_splunk(text_sent, "-15min", "csv")
