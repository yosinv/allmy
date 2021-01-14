# !/usr/bin/python -u
import logging
import logging.handlers
import json
import azure.functions as func
from azure.storage.filedatalake import DataLakeServiceClient

# from ..shared_code import splunk_data
# from . import test_for_IT
# from ..config import logs
import pprint
import os, sys
import datetime
import pathlib
import splunklib.results as results
import splunklib.client as client
import pprint
import logging
import codecs

# pip install -r requirements.txt

HOST = "31.154.3.103"
PORT = 8089
USERNAME = "yosin"
PASSWORD = "Corona10!"
pp = pprint.PrettyPrinter(indent=4)

# DATALAKE PARAMS:
sourcepath = '/home/site/data'
source_filename = "test_uploadfile.txt"
target_filename = "test_uploadfile.txt"
file_system_name = "datalakedev"
datalake_directory = "test"

# new logs due to azurefunction structure 18052020:
logger = logging.getLogger('splunk_data')
logger.setLevel(logging.INFO)

sh = logging.StreamHandler()
sh.setLevel(logging.INFO)

logger.addHandler(sh)
logger.debug('Hello debug mode')


def splunk_setServer():
    # client = pyimport("splunklib.client")
    logger.debug('splunk_data module  - setServer function')
    # Connect to Splunk Enterprise
    try:
        service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD, autologin=True)
        logger.debug('service success')
        return service
    except ValueError:
        logger.error('service error')
    return service


def splunk_new_job_csv_k4ug(service, query, earliest_time, output_mode, **kwargs):
    """CSV MODE ,return k4u result in csv"""
    logger.debug("function k4u: START k4u function")
    logger.debug("**********************************************************")
    query = """search {}""".format(query)
    logger.info("query = " + query)
    # query = """search * | head 5"""
    kwargs_export = {"output_mode": str(output_mode),
                     "earliest_time": str(earliest_time)}
    logger.info(f' "output_mode": "{output_mode}", "earliest_time": "{earliest_time}" ')
    try:
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
    logger.info("end k4u function")
    return bool(querystat)


def splunk_query_splunk(query, earliest_time, output_mode):
    word = "שלום"
    logger.info(word)
    word = word.encode('utf8')
    logger.info(word)

    # CREATE CONNECTION
    service = splunk_setServer()
    # querystat = splunk_new_job_csv_360g(service, query, earliest_time, output_mode)
    querystat = splunk_new_job_csv_k4ug(service, query, earliest_time, output_mode)
    return querystat


def splunkapp_datalake_service_create():
    logger.info('service_create azure datalake storage function')
    storage_account_name = "bigdatalake"
    storage_account_key = "B003wbMTvViMXVe0Aisc51vRDkpQK2c4GOPxJjkuvlvoG6Q9/JbXFM9Vc6R3u9X7WDrmNjD6u0nFmW4KEXlMfg=="
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        logger.error(e)
    return service_client


def splunkapp_create_file(foldername, filename):
    logging.info('Python *CREATE FILE* test.')
    t = datetime.datetime.now().strftime("%Y-%m-%d")
    logfoldername = "/home/site" + "/LogsAz2Sp" + t
    foldername = "/home/site" + foldername + t
    logging.info("foldername" + foldername)
    if not os.path.exists(foldername):
        os.makedirs(foldername)
    suffix = ".txt"
    newfile = filename + t + suffix
    logging.info("newfile" + newfile)
    os.getcwd()
    os.chdir(foldername)
    if not os.path.exists(newfile):
        f = open(newfile, 'w')
        f.write("test")
    f.close()
    return foldername


def splunkapp_upload_file_to_directory_bulk(service_client, file_system_name, target_filename, splunkapp_data_path,
                                            targetpath):
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        directory_client = file_system_client.get_directory_client(targetpath)
        file_client = directory_client.get_file_client(target_filename)
        # local_file = open(sourcepath + target_filename + ".csv", 'r')
        logger.info(splunkapp_data_path)
        local_file = open(splunkapp_data_path, 'r')
        file_contents = local_file.read()
        file_client.upload_data(file_contents, overwrite=True)
    except Exception as e:
        logger.error(e)


def splunkapp_check_or_create_dir(sourcepath):
    """Create dir for file if not exists """
    # CHANGE DIR
    os.chdir(sourcepath)
    # detect the current working directory and print it
    path = os.getcwd()
    logger.info("The current working directory is %s" % path)
    print("The current working directory is %s" % path)
    if not os.path.exists(sourcepath):
        logger.error('source path NOT EXISTS')
        print('source path NOT EXISTS')
        os.makedirs(sourcepath)
        logger.info('create source path')
    else:
        logger.info('source path exists')
        print('source path exists')


def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info('*********MAIN*******')
    logger.info('Checking sql server connection!')
    # check datalakestorage mounted
    logger.info('Python HTTP trigger function processed a request.')
    text_sent = None
    query_stat = None

    logger.info('DATALAKE PARAMS: ')
    logger.info(sourcepath)
    logger.info(source_filename)
    logger.info(target_filename)
    logger.info(file_system_name)
    logger.info(datalake_directory)
    logger.info('****************')
    # CREATE DATALAKE SERVICE
    logger.info('create data lake service connection "key based"')
    datalake_service = splunkapp_datalake_service_create()
    # SPLUNK*******************************************
    splunkapp_check_or_create_dir(sourcepath)
    logger.info('SPLUN******search K4U_Log ,NEW_CLAIM, ClaimNr ,claimant choose k4u*****')
    query = """search K4U_Log ,NEW_CLAIM, ClaimNr ,claimant choose k4u"""
    kwargs_export = {"output_mode": "csv", "earliest_time": "-15min"}
    service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD, autologin=True)
    rr = service.jobs.export(query, **kwargs_export)

    # for result in rr:
    #     result = result.decode('utf8')
    #     print(str(result))
    #     logger.info(str(result))
    # SPLUNK*****************END************************

    # WRITE RESULT TO FILE
    newfile = os.path.join(sourcepath, source_filename)
    logger.info('****** FILE NAME: ******')
    logger.info(newfile)

    logger.info(f'file name {newfile} is not exists')
    try:
        myfile = open(newfile, 'w')
        myfile.write("test YOSI TEST............................................................TEST")
        for result in rr:
            result = result.decode('utf8')
            logger.info(str(result))
            myfile.write(str(result))
        # SPLUNK*****************END************************
        logger.info('write result to file')
        myfile.close()
    except Exception as e:
        logger.error(e)

        # # UPLOAD TO DATALAKE SPLUNK RESULT FILE
    splunkapp_data_path = newfile
    splunkapp_upload_file_to_directory_bulk(datalake_service, file_system_name, target_filename, splunkapp_data_path,
                                            datalake_directory)

    try:
        text_sent = req.get_body().decode('utf-8')

        logger.info('text_sent decode(utf-8):' + text_sent)
        body = json.loads(text_sent)
        content = body['content']
        logger.info('content:' + content)

        logger.info('text_sent:' + str(text_sent))
    except ValueError:
        logger.info('text_sent IS ERROR:' + str(text_sent))
        pass

    logger.info(f'HOST, PORT, USERNAME, PASSWORD: {HOST} , {PORT}, {USERNAME}, {PASSWORD}')
    try:
        logger.info('check splunk service status')
        logger.info(f'setServer:  {HOST, PORT, USERNAME, PASSWORD}')
        # query_stat = splunk_query_splunk(text_sent, "-15min", "csv")
        logger.info(query_stat)
        if query_stat:
            logger.info(f"Splunk query success result")
            logger.info(f"service success result: {str(query_stat)}")
        else:
            logging.error(f"Splunk query Failed or not set")

    except ValueError:
        logger.error('service error')
        pass

    # if text_sent:
    #     return func.HttpResponse(json.dumps(text_sent), mimetype="application/json", charset="utf-8", status_code=200)
    if text_sent:
        logger.info('*********ENDMAIN*******')
        return func.HttpResponse(
            json.dumps([{"splunk_query": f"{str(text_sent)}"}]), status_code=200)


    else:
        logger.info('*********ENDMAIN*******')
        return func.HttpResponse(
            "AZFUNC - Please pass a file in the request body",
            status_code=400
        )


