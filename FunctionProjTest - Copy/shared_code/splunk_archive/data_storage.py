import os, uuid, sys
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from azure.storage.blob._shared.base_client import create_configuration

storage_account_name = "bigdatalake"
storage_account_key = "B003wbMTvViMXVe0Aisc51vRDkpQK2c4GOPxJjkuvlvoG6Q9/JbXFM9Vc6R3u9X7WDrmNjD6u0nFmW4KEXlMfg=="
file_system = "datalakedev"
file_system_name = "datalakedev"
directory = "test"
connection_string = "BlobEndpoint=https://bigdatalake.blob.core.windows.net/;QueueEndpoint=https://bigdatalake.queue.core.windows.net/;FileEndpoint=https://bigdatalake.file.core.windows.net/;TableEndpoint=https://bigdatalake.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwlacupx&se=2020-06-01T15:09:57Z&st=2020-05-20T07:09:57Z&spr=https&sig=DetL57VoSwDm832zXa2ok8%2BzvH9%2F0mQln31zrRbT7vo%3D"
sas_token = "?sv=2019-10-10&ss=bfqt&srt=sco&sp=rwlacupx&se=2020-05-31T15:09:57Z&st=2020-05-20T07:09:57Z&spr=https&sig=orzlSQfv%2B9t4xJ9yohSpAzQaWhuQWIHoZXfRU5kIxdo%3D"


# LOGS in LOCAL mode
import logging
logger = logging.getLogger(__name__)
FORMAT = "[%(filename)s:%(lineno)s - %(funcName)20s() ] %(message)s"
logging.basicConfig(format=FORMAT)
logger.setLevel(logging.DEBUG)



def service_create():
    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    except Exception as e:
        logger.error(e)
    return service_client


def GetSasToken():
    # blobService = BlockBlobService(account_name=accountName, account_key=accountKey)
    # sas_token = blobService.generate_container_shared_access_signature(containerName, ContainerPermissions.READ,
    #                                                                    datetime.utcnow() + timedelta(hours=1))

    blobService = DataLakeServiceClient(account_name=storage_account_name, account_key=storage_account_key)
    sas_token = blobService.generate_container_shared_access_signature(containerName, ContainerPermissions.READ,
                                                                       datetime.utcnow() + timedelta(hours=1))
    return sas_token


def service_create_sas():
    try:
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=sas_token)

    except Exception as e:
        logger.error(e)
    return service_client


def test_conn_msupport():
    logger.debug('start con')
    try:
        logger.debug('try DatalakeCon con')

        BlobServiceClient = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name),
            credential=storage_account_key)

        # TODO: Update this with your actual proxy information.
        http_proxy = 'http://192.168.220.21:8080'

        https_proxy = 'http://192.168.220.21:8080'

        # Create a storage Configuration object and update the proxy policy.
        config = create_configuration(storage_sdk='filedatalake')
        config.proxy_policy.proxies = {
            'http': http_proxy,
            'https': https_proxy
        }
        # Construct the BlobServiceClient, including the customized configuation.
        service_client = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name),
            credential=storage_account_key, configuration=config)
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        paths = file_system_client.get_paths(path=directory)

        for path in paths:
            logger.debug(path.name + '\n')
        # containers = list(service_client.list_containers(logging_enable=True))
        # print("{} containers.".format(len(containers)))

        # Alternatively, proxy settings can be set using environment variables, with no
        # custom configuration necessary.
        HTTP_PROXY_ENV_VAR = 'HTTP_PROXY'
        HTTPS_PROXY_ENV_VAR = 'HTTPS_PROXY'
        # os.environ[HTTPS_PROXY_ENV_VAR] = https_proxy

        service_client = BlobServiceClient.from_connection_string(connection_string)
        # containers = list(service_client.list_containers(logging_enable=True))
        # print("{} containers.".format(len(containers)))

    except Exception as e:
        logger.error(e)


def test_conn_msupport_v2noproxy():
    logger.debug('start con')
    try:
        logger.debug('try DatalakeCon con')

        config = create_configuration(storage_sdk='filedatalake')

        service_client = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", storage_account_name),
            credential=storage_account_key, configuration=config)
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        paths = file_system_client.get_paths(path=directory)

        for path in paths:
            logger.debug(path.name + '\n')

    except Exception as e:
        logger.error(e)


def test_conn_msupport_connectionstr_v2noproxy():
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.storage.blob._shared.base_client import create_configuration
    logger.debug('start con')
    try:

        config = create_configuration(storage_sdk='filedatalake')

        connection_string = "BlobEndpoint=https://bigdatalake.blob.core.windows.net/;QueueEndpoint=https://bigdatalake.queue.core.windows.net/;FileEndpoint=https://bigdatalake.file.core.windows.net/;TableEndpoint=https://bigdatalake.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sc&sp=rwlacupx&se=2020-06-16T19:29:04Z&st=2020-04-26T11:29:04Z&spr=https&sig=cNzYYuvfxY6%2BkrfBghjqGnBmLFWgTur%2FM0gPtjDZbRQ%3D"

        # Construct the BlobServiceClient, including the customized configuation.
        service_client = DataLakeServiceClient.from_connection_string(connection_string, configuration=config)

        # NEW:
        file_systems = service_client.list_file_systems()

        for file_system in file_systems:
            logger.debug(file_system.name + '\n')

    except Exception as e:
        logger.error(e)

def splunkapp_service_create():
    logger.info('service_create azure datalake storage function')
    storage_account_name = "bigdatalake"
    storage_account_key = "B003wbMTvViMXVe0Aisc51vRDkpQK2c4GOPxJjkuvlvoG6Q9/JbXFM9Vc6R3u9X7WDrmNjD6u0nFmW4KEXlMfg=="
    try:
        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    except Exception as e:
        logger.error(e)
    return service_client


def splunkapp_list_directory_contents(service_client, file_system):
    logger.info('list_directory_contents function')
    try:
        file_system_client = service_client.get_file_system_client(file_system="datalakedev")
        paths = file_system_client.get_paths(path="test")
        for path in paths:
            logger.info(str(path.name) + '\n')
    except Exception as e:
        logger.error(e)

def splunkapp_upload_file_to_directory_bulk(service_client, file_system_name, filename, sourcepath, targetpath):
    try:

        file_system_client = service_client.get_file_system_client(file_system=file_system_name)
        directory_client = file_system_client.get_directory_client(targetpath)
        file_client = directory_client.get_file_client(filename)
        os.path.join(sourcepath,filename)
        # local_file = open(sourcepath + filename + ".csv", 'r')
        local_file = open(sourcepath + filename , 'r')
        file_contents = local_file.read()
        file_client.upload_data(file_contents, overwrite=True)
    except Exception as e:
        logger.error(e)

def upload_file_to_directory_bulk():
    try:

        file_system_client = service_client.get_file_system_client(file_system=file_system_name)

        directory_client = file_system_client.get_directory_client("test")

        file_client = directory_client.get_file_client("uploaded-file.txt")

        local_file = open("C:\\file-to-upload.txt", 'r')

        file_contents = local_file.read()

        file_client.upload_data(file_contents, overwrite=True)

    except Exception as e:
        logger.error(e)

    # try:
    #     service_client = DataLakeServiceClient(account_url="https://bigdatalake.blob.core.windows.net",
    #                                            credential=storage_account_key)
    #
    #     file_system_client = service_client.get_file_system_client(file_system=file_system)
    #
    #     paths = file_system_client.get_paths(path=directory)
    #
    #     for path in paths:
    #         logger.debug(path.name + '\n')
    #
    # except Exception as e:
    #     print(e)


# def initialize_storage_account_ad(storage_account_name, client_id, client_secret, tenant_id):
#     try:
#         global service_client
#
#         credential = ClientSecretCredential(tenant_id, client_id, client_secret)
#
#         service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
#             "https", storage_account_name), credential=credential)
#
#     except Exception as e:
#         print(e)
#
#
# def create_directory(file_system_client):
#     try:
#         file_system_client.create_directory("my-directory")
#
#     except Exception as e:
#         print(e)


def list_directory_contents(service_client, file_system):
    try:

        file_system_client = service_client.get_file_system_client(file_system="datalakedev")

        paths = file_system_client.get_paths(path="test")

        for path in paths:
            logger.debug(path.name + '\n')

    except Exception as e:
        logger.error(e)


# service = service_create()

# list_directory_contents(service, file_system)
# # create_directory(service, file_system)

# test_conn_msupport_v2noproxy()
# test_conn_msupport_connectionstr_v2noproxy()

storage_account_name = "bigdatalake"
storage_account_key = "B003wbMTvViMXVe0Aisc51vRDkpQK2c4GOPxJjkuvlvoG6Q9/JbXFM9Vc6R3u9X7WDrmNjD6u0nFmW4KEXlMfg=="
file_system = "datalakedev"

datalake_directory = "test"
connection_string = "BlobEndpoint=https://bigdatalake.blob.core.windows.net/;QueueEndpoint=https://bigdatalake.queue.core.windows.net/;FileEndpoint=https://bigdatalake.file.core.windows.net/;TableEndpoint=https://bigdatalake.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rwlacupx&se=2020-06-01T15:09:57Z&st=2020-05-20T07:09:57Z&spr=https&sig=DetL57VoSwDm832zXa2ok8%2BzvH9%2F0mQln31zrRbT7vo%3D"

sourcepath = "\home\site\/"

sourcepath = r"C:\Users\yosin\Desktop\YOSI\\"
filename ="test_uploadfile.txt"

file_system_name = "datalakedev"
datalake_directory = "test"

# DATALAKE PARAMS:
sourcepath = "\home\site\data"
source_filename = "test_uploadfile.txt"
target_filename = "test_uploadfile.txt"
file_system_name = "datalakedev"
datalake_directory = "test"

logger.info('DATALAKE PARAMS: ')
logger.info(sourcepath)
logger.info(source_filename)
logger.info(target_filename)
logger.info(file_system_name)
logger.info(datalake_directory)
logger.info('****************')
logger.info()
logger.info('create data lake service connection "key based"')
datalake_service = splunkapp_service_create()

if not os.path.exists(sourcepath):
    logger.error('source path NOT EXISTS')
    print('source path NOT EXISTS')
    os.makedirs(sourcepath)
    logger.info('create source path')
else:
    logger.info('source path exists')
    print('source path exists')

newfile = source_filename
os.getcwd()
newfile = sourcepath + '\\' + newfile
os.chdir(sourcepath)
if not os.path.exists(newfile):
    f = open(newfile, 'w')
    f.write("test YOSI TEST............................................................TEST")
f.close()

splunkapp_data_path = os.path.join(sourcepath, source_filename)
splunkapp_upload_file_to_directory_bulk(datalake_service, file_system_name, target_filename, splunkapp_data_path,
                                        datalake_directory)