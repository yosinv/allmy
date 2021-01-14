# !/usr/bin/python -u


from datetime import  datetime,date
import logging
# import logging.handlers
import json
from azure.storage.filedatalake import DataLakeServiceClient
import datetime


logger = logging.getLogger('datagov_data')
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
logger.addHandler(sh)
logger.debug('Hello debug mode')

def read_app_settings():
    with open('config.json', 'r', encoding='utf-8') as f:
        try:
            config = json.load(f)
        except Exception as e:
            logger.error("Error occurred while trying to upload app settings!!! Empty settings are set.")
            config = {}
    return config

def datalake_service_create(storage_account_name, storage_account_key):
    logger.info('service_create azure datalake storage function')
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        logger.error(e)
    return service_client

def upload_file_to_directory_bulk(service_client, file_system_name, target_filename, splunkapp_data_path,
                                            targetpath, rows_count, output_mode):
    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        directory_client = file_system_client.get_directory_client(targetpath)
        t = datetime.now().strftime("%Y-%m-%d")
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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    APP_SETTINGS = read_app_settings()
    datalake = datalake_service_create(APP_SETTINGS["STORAGE_ACCOUNT_NAME"], APP_SETTINGS["STORAGE_ACCOUNT_KEY"])





    earliest_time = earliest_time_v1
    source_filename = "input_" + str(splunk_query_report_name) + ".csv"
    target_filename = "output_" + str(splunk_query_report_name)


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

    target_filename =
    upload_file_to_directory_bulk(datalake, APP_SETTINGS["file_system_name"], target_filename, splunkapp_data_path,targetpath, rows_count, output_mode)