#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pathlib
import splunklib
import splunklib.results as results
import splunklib.client as client
import pprint
import logging
import json
import collections
import codecs
import sys, os
from time import sleep
from pathlib import Path

HOST = "172.16.227.30"
PORT = 8089
USERNAME = "yosin"
PASSWORD = "Corona10!"
pp = pprint.PrettyPrinter(indent=4)

# LOG_FILENAME = 'local_splunk_data_func_logs.log'
# file_handler = logging.FileHandler(filename=LOG_FILENAME)
# formatter = logging.Formatter('[%(asctime)s] {%(filename)s: %(lineno)d} %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)

# Set up a specific logger with our desired output level
# logging.basicConfig(
#     level=logging.INFO,
# )

# logger = logging.getLogger('Splunk_data')
# logger.addHandler(file_handler)



logger = logging.getLogger('splunk_data')
logger.setLevel(logging.DEBUG)
sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
logger.addHandler(sh)

def setServer(hostname, port, splunkuser, splunkpassword):
    # client = pyimport("splunklib.client")
    logger.debug('splunk_data module  - setServer function')
    # Connect to Splunk Enterprise
    try:
        service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD, autologin=True)
        logger.debug('service success')
        return service
    except ValueError:
        logger.error('service error')


def new_job_csv_360g(service, query, earliest_time, output_mode, **kwargs):
    """CSV MODE"""
    logger.debug("function new_job_csv_360g: START new_job_csv_360g function")
    logger.debug("**********************************************************")
    # Working query:
    # query = '"""' + query + '"""'
    # query  = 'search * | head 5'
    query = """search {}""".format(query)
    logger.info("query = " + query)
    query = """search * | head 5"""
    kwargs_export = {"output_mode": str(output_mode),
                     "earliest_time": str(earliest_time)}
    logger.info(f' "output_mode": "{output_mode}", "earliest_time": "{earliest_time}" ')
    try:
        # rr = service.jobs.export(query, **kwargs_export)
        rr = service.jobs.export(query, **kwargs_export)

        logger.info("...done!\n")
        print("...done!\n")
        sys.stdout.write("...done!\n")
        # f = codecs.open('splunk_data_360g.csv', 'w')
        for result in rr:
            if isinstance(result, result.Message):
                # Diagnostic messages may be returned in the results
                logger.info('%s: %s' % (result.type, result.message))

            elif isinstance(result, dict):
                # Normal events are returned as dicts
                logger.info('result of type dict')

            result = result.decode('utf8')
            logger.info(str(result))
            # f.write(str(result))
        # f.close()
        querystat = 1
    except ValueError:
        logger.error('service error could not complete query execution')
        logger.error('Status query execution: * Failed * ')
        querystat = 0
    sys.stdout.write('end new_job_csv_360g function')
    sys.stdout.write('\n')
    logger.info("end new_job_csv_360g function")
    return bool(querystat)


def new_job_csv_k4ug(service, query, earliest_time, output_mode, **kwargs):
    """CSV MODE"""
    logger.debug("function k4u: START k4u function")
    logger.debug("**********************************************************")
    # Working query:
    # query = '"""' + query + '"""'
    # query  = 'search * | head 5'
    query = """search {}""".format(query)
    logger.info("query = " + query)
    query = """search * | head 5"""
    kwargs_export = {"output_mode": str(output_mode),
                     "earliest_time": str(earliest_time)}
    logger.info(f' "output_mode": "{output_mode}", "earliest_time": "{earliest_time}" ')
    try:
        # rr = service.jobs.export(query, **kwargs_export)
        rr = service.jobs.export(query, **kwargs_export)

        logger.info("...done!\n")
        print("...done!\n")
        sys.stdout.write("...done!\n")
        f = codecs.open('/home/site/k4u_logs_bi.csv', 'w')
        for result in rr:
            if isinstance(result, results.Message):
                # Diagnostic messages may be returned in the results
                logger.info('%s: %s' % (result.type, result.message))

            elif isinstance(result, dict):
                # Normal events are returned as dicts
                logger.info('result of type dict')

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


def query_splunk(query, earliest_time, output_mode):
    word = "שלום"
    print(word)
    # word = word.decode('UTF-8')
    # print(word)
    word = word.encode('utf8')
    print(word)
    word = word.decode('utf8')
    print(word)
    service = setServer(HOST, PORT, USERNAME, PASSWORD)
    # querystat = new_job_csv_360g(service, query, earliest_time, output_mode)
    querystat = new_job_csv_k4ug(service, query, earliest_time, output_mode)
    return querystat



OWNER = "yosin"       # Replace this with a valid username
APP   = "search"

service2 = client.connect(
    host=HOST,
    port=PORT,
    username=USERNAME,
    password=PASSWORD,
    owner=OWNER,
    app=APP )

savedsearches2 = service2.saved_searches

# a = service2.apps["idi"]
# print(a)
for savedsearch in savedsearches2:
    print ("  " + savedsearch.name)