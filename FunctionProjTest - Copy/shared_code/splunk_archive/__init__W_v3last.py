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

# SPLUNK PARAMS:
HOST = "31.154.3.103"
PORT = 8089
USERNAME = "yosin"
PASSWORD = "Corona10!"
pp = pprint.PrettyPrinter(indent=4)
reportname = 'k4u_logs_bi'
output_mode = "csv"
earliest_time = "-60min"

# DATALAKE PARAMS:
STORAGE_ACCOUNT_NAME = "bigdatalake"
STORAGE_ACCOUNT_KEY = "B003wbMTvViMXVe0Aisc51vRDkpQK2c4GOPxJjkuvlvoG6Q9/JbXFM9Vc6R3u9X7WDrmNjD6u0nFmW4KEXlMfg=="
sourcepath = '/home/site/data'
source_filename = "input_k4u_logs_bi.csv"
target_filename = "output_k4u_logs_bi"
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


def splunk_new_job_csv_k4ug(reportname, sourcepath, source_filename, service, query, earliest_time, output_mode,
                            **kwargs):
    """CSV MODE ,return k4u result in csv"""
    logger.info("function k4u: START k4u function")
    logger.info("**********************************************************")

    query = """search {}""".format(query)
    logger.info("query = " + query)
    kwargs_export = {"output_mode": str(output_mode),
                     "earliest_time": str(earliest_time)}
    logger.info(f' "output_mode": "{output_mode}", "earliest_time": "{earliest_time}" ')

    # WRITE RESULT TO FILE
    newfile_splunk = os.path.join(sourcepath, source_filename)
    logger.info('****** FILE NAME: ******')
    logger.info(newfile_splunk)

    try:

        myfile = open(newfile_splunk, 'w')
        # f = codecs.open('/home/site/data/k4u_logs_bi.csv', 'w')
        rr = service.jobs.export(query, **kwargs_export)
        splunk_rows_count = 0
        logger.info("...done!\n")
        for result in rr:
            # if isinstance(result, results.Message):
            #     # Diagnostic messages may be returned in the results
            #     logger.info('%s: %s' % (result.type, result.message))

            # elif isinstance(result, dict):
            #     # Normal events are returned as dicts
            #     logger.info('result of type dict')

            result = result.decode('utf8')
            logger.info(str(result))
            myfile.write(str(result))
            splunk_rows_count += 1
            # f.write(str(result))
        querystat = 1

    except ValueError:
        logger.error('service error could not complete query execution')
        logger.error('Status query execution: * Failed * ')
        querystat = 0
    finally:
        myfile.close()
        # f.close()
        # NEED TO ADD -if result is set then query stat 1 else 0
    logger.info(f"end k4u function , wrote: {splunk_rows_count} rows")
    return bool(querystat), newfile_splunk, splunk_rows_count


def splunkapp_datalake_service_create(storage_account_name, storage_account_key):
    logger.info('service_create azure datalake storage function')
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        logger.error(e)
    return service_client


def splunkapp_create_logfile(foldername, logfilename):
    logging.info('Python *CREATE FILE* test.')
    t = datetime.datetime.now().strftime("%Y-%m-%d")
    logfoldername = "/home/site" + "/Logsaz2sp" + t
    # foldername = "/home/site" + foldername + t
    logging.info("foldername" + logfoldername)
    if not os.path.exists(logfoldername):
        os.makedirs(logfoldername)
    suffix = ".log"
    newlogfile = logfilename + t + suffix
    logging.info("new log file" + newlogfile)
    os.getcwd()
    os.chdir(foldername)
    if not os.path.exists(newlogfile):
        f = open(newlogfile, 'w')
        f.write("test")
    f.close()
    return foldername


def splunkapp_upload_file_to_directory_bulk(service_client, file_system_name, target_filename, splunkapp_data_path,
                                            targetpath, rows_count, output_mode):
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        directory_client = file_system_client.get_directory_client(targetpath)
        t = datetime.datetime.now().strftime("%Y-%m-%d")
        suffix = "." + output_mode
        target_filename = target_filename + "_" + t + suffix
        file_client = directory_client.get_file_client(target_filename)
        # local_file = open(sourcepath + target_filename + ".csv", 'r')
        logger.info(splunkapp_data_path)
        logger.info('LOADING CONTENT TO DATA LAKE!')
        local_file = open(splunkapp_data_path, 'r')
        file_contents = local_file.read()
        # with open(splunkapp_data_path, 'r') as local_file:
        #     if len(local_file.readlines()) == rows_count:
        #         # ORIGIN:
        #         logger.info('LOADING CONTENT TO DATA LAKE!')
        #         file_contents = local_file.read()
        #     else:
        #         logger.error('error in rows count')

        # # UPDATED - NOT W
        # # Read and print the entire file line by line
        # for line in local_file:
        #     file_contents = line

        # NOTE , OVERWRITE =TRUE !!!!!!
        file_client.upload_data(file_contents, overwrite=True)
        return target_filename


    except Exception as e:
        logger.error(e)


def splunkapp_check_or_create_dir(sourcepath):
    """Create dir for file if not exists """
    # CHANGE DIR
    # NEW
    os.chdir("/home/site")
    # ORIGIN:
    # os.chdir(sourcepath)

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


def splunkapp_query_splunk(reportname, sourcepath, source_filename, query, earliest_time, output_mode):
    word = " ספלאנק שלום"
    logger.info(word)
    word = word.encode('utf8')
    logger.info(word)

    # CREATE CONNECTION
    service = splunk_setServer()
    querystat, newfile, q_rows_count = splunk_new_job_csv_k4ug(reportname, sourcepath, source_filename, service, query,
                                                               earliest_time, output_mode)
    return querystat, newfile, q_rows_count


def main(req: func.HttpRequest) -> func.HttpResponse:
    #  (req: func.HttpRequest,context: func.Context) -> func.HttpResponse:
    logger.info('*********MAIN*******')
    logger.info('Checking sql server connection!')
    # check datalakestorage mounted
    logger.info('Python HTTP trigger function processed a request.')
    text_sent = None
    query_stat = None

    logger.info(reportname)
    logger.info('DATALAKE PARAMS: ')
    logger.info(sourcepath)
    logger.info(source_filename)
    logger.info(target_filename)
    logger.info(file_system_name)
    logger.info(datalake_directory)
    logger.info('****************')

    # CREATE DATALAKE SERVICE
    logger.info('create data lake service connection "key based"')
    datalake_service = splunkapp_datalake_service_create(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)

    # CREATE DATA DIR IF NOT EXISTS
    splunkapp_check_or_create_dir(sourcepath)

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
        query_stat, newfile, rows_count = splunkapp_query_splunk(reportname, sourcepath, source_filename, text_sent,
                                                                 earliest_time, output_mode)
        logger.info(query_stat)
        if query_stat:
            logger.info(f"Splunk query success result")
            logger.info(f"service success result: {str(query_stat)}")
            # # UPLOAD TO DATALAKE SPLUNK RESULT FILE
            splunkapp_data_path = newfile
            logger.info(f"splunkapp_data_path: {str(splunkapp_data_path)}")
            newtarget_filename = splunkapp_upload_file_to_directory_bulk(datalake_service, file_system_name,
                                                                         target_filename,
                                                                         splunkapp_data_path, datalake_directory,
                                                                         rows_count, output_mode)
            targetpath = os.path.join(file_system_name, datalake_directory, newtarget_filename)
            logger.info(f"newtarget_filename: {newtarget_filename}")
            logger.info(f"Path: {targetpath}")
        else:
            logging.error(f"Splunk query Failed or not set")

    except ValueError:
        logger.error('service error')
        pass

    if text_sent:
        finalResult = {"Report": f"{text_sent}", "FileName": f"{targetpath}"}
        logger.info(f"finalResult: {str(finalResult)}")
        return func.HttpResponse(json.dumps(finalResult), mimetype="application/json", charset="utf-8", status_code=200)
    # if text_sent:
    #     logger.info('*********ENDMAIN*******')
    #     return func.HttpResponse(
    #         json.dumps([{"splunk_query": f"{str(text_sent)}"}]), status_code=200)

    else:
        logger.info('*********ENDMAIN*******')
        return func.HttpResponse(
            "AZFUNC - Please pass a file in the request body",
            status_code=400
        )
