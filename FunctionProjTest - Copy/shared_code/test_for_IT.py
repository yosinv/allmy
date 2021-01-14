from azure.storage.blob import ContainerClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.blob._shared.base_client import create_configuration
from datetime import datetime, timedelta



storage_account_name = "bigdatalake"
storage_account_key = "B003wbMTvViMXVe0Aisc51vRDkpQK2c4GOPxJjkuvlvoG6Q9/JbXFM9Vc6R3u9X7WDrmNjD6u0nFmW4KEXlMfg=="
file_system_name = "datalakedev"
directory = "test"
connection_string = "BlobEndpoint=https://bigdatalake.blob.core.windows.net/;QueueEndpoint=https://bigdatalake.queue.core.windows.net/;FileEndpoint=https://bigdatalake.file.core.windows.net/;TableEndpoint=https://bigdatalake.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rlapx&se=2020-05-14T22:40:33Z&st=2020-05-06T14:40:33Z&spr=https,http&sig=QnTbtsD9ZwbOCpL6tih6KqjcT0hV3u%2B2x%2F92OUYuHxA%3D"

accountName = "bigdatalake"
accountKey = "B003wbMTvViMXVe0Aisc51vRDkpQK2c4GOPxJjkuvlvoG6Q9/JbXFM9Vc6R3u9X7WDrmNjD6u0nFmW4KEXlMfg=="
containerName = "datalakedev"
blobName = "test"

# def GetSasToken():
#
#     blobService = BlockBlobService(account_name=accountName, account_key=accountKey)
#     sas_token = blobService.generate_container_shared_access_signature(containerName,ContainerPermissions.READ, datetime.utcnow() + timedelta(hours=1))
#     return sas_token
#
#
# def AccessTest(token):
#     blobService = BlockBlobService(account_name = accountName, account_key = None, sas_token = token)
#     blobService.get_blob_to_path(containerName,blobName,"E://test.txt")




def test_conn_idi_it():
    container_client = ContainerClient.from_connection_string(connection_string, container_name="datalakedev")
    # [START list_blobs_in_container]
    blobs_list = container_client.list_blobs()

    try:
        for blob in blobs_list:
            print(blob.name + '\n')
        # [END list_blobs_in_container]
    except Exception as e:
        print(e)


def test_conn_msupport():
    print('start con')
    try:
        print('try DatalakeCon con')

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
            print(path.name + '\n')
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
        print(e)


def test_conn_msupport_connectionstr():
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.storage.blob._shared.base_client import create_configuration
    print('start con')
    try:
        print('try DatalakeCon with proxy')
        # TODO: Update this with your actual proxy information.
        http_proxy = 'http://192.168.220.21:8080'
        # Create a storage Configuration object and update the proxy policy.
        config = create_configuration(storage_sdk='filedatalake')
        config.proxy_policy.proxies = {
            'http': http_proxy
        }
        # Setting up connection string - ***NEED TO BE FUNCTION***
        connection_string = "BlobEndpoint=https://bigdatalake.blob.core.windows.net/;QueueEndpoint=https://bigdatalake.queue.core.windows.net/;FileEndpoint=https://bigdatalake.file.core.windows.net/;TableEndpoint=https://bigdatalake.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sco&sp=rlapx&se=2020-05-14T22:40:33Z&st=2020-05-06T14:40:33Z&spr=https,http&sig=QnTbtsD9ZwbOCpL6tih6KqjcT0hV3u%2B2x%2F92OUYuHxA%3D"

        # Construct the BlobServiceClient, including the customized configuation.
        service_client = DataLakeServiceClient.from_connection_string(connection_string, configuration=config)

        # OLD NEED TO CHECK IF WORKS
        # file_system_client = service_client.get_file_system_client(file_system=file_system)

        # NEW:
        file_systems = service_client.list_file_systems()

        # OLD:
        # paths = file_system_client.get_paths(path=directory)
        # NEW:
        for file_system in file_systems:
            print(file_system.name + '\n')

    except Exception as e:
        print(e)

# test_conn_idi_it()
# test_conn_msupport()
# test_conn_msupport_connectionstr()

# token=GetSasToken()
# print (token)
# AccessTest(token)