# LOGS in LOCAL mode 
# LOG_FILENAME = 'Splunk_data_logs.log'
# file_handler = logging.FileHandler(filename=LOG_FILENAME)
# formatter = logging.Formatter('[%(asctime)s] {%(filename)s: %(lineno)d} %(levelname)s - %(message)s')
# file_handler.setFormatter(formatter)

# Set up a specific logger with our desired output level
# logging.basicConfig(
#     level=logging.INFO,
# )

# logger = logging.getLogger('Splunk_data')
# logger.addHandler(file_handler)

# LOGS USING CLASS
curr_dir = os.path.dirname(os.path.realpath(__file__))


# Logs_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) USING: -> /HOME/SITE INSTEAD
# Logs_dir = '/home/site'
# setup_logging(Logs_dir,)
# logger = logging.getLogger(__name__)
# logger.info("logger start logging")

# def splunk_test_splunk1():
#     # !/usr/bin/python -u
#     logger.info('test_splunk1 function')



def splunk_new_job_csv_360g(service, query, earliest_time, output_mode, **kwargs):
    """CSV MODE"""
    logger.debug("function new_job_csv_360g: START new_job_csv_360g function")
    logger.debug("**********************************************************")
    query = """search {}""".format(query)
    logger.info("query = " + query)
    query = """search * | head 5"""
    kwargs_export = {"output_mode": str(output_mode),
                     "earliest_time": str(earliest_time)}
    logger.info(f' "output_mode": "{output_mode}", "earliest_time": "{earliest_time}" ')
    try:

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

    
    # test_conn_msupport_connectionstr_v2noproxy()
    # test_conn_idi_it()
    # file_system = "datalakedev"
    # list_directory_contents(service_create(), file_system)


def new_search_with_type(service):
    import sys
    from time import sleep
    import splunklib.results as results

    # ...
    #
    # Initialize your service like so
    # import splunklib.client as client
    # service = client.connect(username="admin", password="yourpassword")

    # searchquery_normal = "search * | head 10"
    # searchquery_normal_bi = """search successPos=Agent status ,claims found, at AgentStatusLoginService   agentStatusLogin() - login success" | convert timeformat="%H:%M" ctime(_time) AS Time| convert timeformat="%d/%m/%y" ctime(_time) AS Date| eval DateTime=Date+" "+Time | eval Brand=brand | eval FRM=case(like(sid, "518#%"), "Internet", like(sid, "6551#%"), "Mobile") | eval InsuredRegMark=insured_reg_mark | eval SideCRegMark=side_c_reg_mark | eval TrimmedDate=substr(eventDate, 16, 6) | eval TrimmedYear=substr(eventDate, 36, 4) | eval EventDate=TrimmedDate+" "+TrimmedYear | eval Claim=claim_nr | eval Policy=policy_nr | eval CIP=cip | table Claim Policy EventDate InsuredRegMark SideCRegMark Brand DateTime FRM CIP"""
    searchquery_normal_360g = """search index=asindex gcid=* gcid!=err "GA360 onboarding" source=Prod 
| eval phone=replace(telAndPre , "-","") 
| eval id_type=entityType 
| eval source_type=sourcValue 
| eval is_mobile=insertUser 
| eval idi_id=idiId 
| eval insert_date=strftime(_time, "%Y-%m-%d %H:%M:%S") 
| eval clientid = if(isnotnull(clientid),clientid, "null") 
| search NOT ( clientid=null email=null phone=null source_type=260) 
| table ts gcid idi_id sid ip clientid email phone brand id_type source_type is_mobile insert_date """

    kwargs_normalsearch = {"exec_mode": "normal", "output_mode": "csv",
                           "earliest_time": "-15min"}
    job = service.jobs.create(searchquery_normal_360g, **kwargs_normalsearch)

    # A normal search returns the job's SID right away, so we need to poll for completion
    while True:
        while not job.is_ready():
            pass
        stats = {"isDone": job["isDone"],
                 "doneProgress": float(job["doneProgress"]) * 100,
                 "scanCount": int(job["scanCount"]),
                 "eventCount": int(job["eventCount"]),
                 "resultCount": int(job["resultCount"])}

        status = ("\r%(doneProgress)03.1f%%   %(scanCount)d scanned   "
                  "%(eventCount)d matched   %(resultCount)d results") % stats

        sys.stdout.write(status)
        sys.stdout.flush()
        if stats["isDone"] == "1":
            sys.stdout.write("\n\nDone!\n\n")
            break
        sleep(2)

    # Get the results and display them
    for result in results.ResultsReader(job.results()):
        print
        result

    job.cancel()
    sys.stdout.write('\n')


def data_extract(result):
    print(f'result type : {type(result)}')
    # print(f'__dict__: {result.__dict__}')
    # for item in result:
    #     print(item)
    for k, v in result.items():
        print(f'key= {k} : value = {v}')

    # for item in result["_raw"]:
    #     print(item)
    #     print()

    if type(result) is dict:
        try:
            decoded = json.loads(result)
            for x in decoded:
                print(x)
        except (ValueError, KeyError, TypeError):
            print("JSON format error")

    elif type(result) is list:
        print(result)

    elif type(result) is collections.OrderedDict():
        for k, v in result.items():
            print(k, v)


def new_job_csv(service):
    """CSV MODE"""
    query = """search * |head 50 """

    # query = """search index=asindex source=Prod sourcetype=Mobile application="xxx" sourcetype=xxx|
    #      spath visitId  | join type ..."""

    # query = """search index=asindex source=Prod sourcetype=Mobile | head 100"""

    # Working query:
    query = """search successPos | head 10"""

    # kwargs_oneshot = {'output_mode': 'csv', "search_mode": "normal"}
    # oneshotsearch_results = service.jobs.oneshot(query, **kwargs_oneshot)
    # print("...done!\n")
    # f = codecs.open('myresults.csv', 'w','utf-8')
    # f.write(str(oneshotsearch_results.read()))

    kwargs_export = {"output_mode": "csv"}
    rr = service.jobs.export(query, **kwargs_export)
    print("...done!\n")
    f = codecs.open('splunk_data.csv', 'w')
    for item in rr:
        item = item.decode('utf8')
        print(item)
        f.write(str(item))
    f.close()
    # f = codecs.open(myresults.csv, "r", encoding)
    # with codecs.open('myresults.csv', "r", encoding = 'utf-8') as f:

def new_job(service):
    """XML preview """
    job = service.jobs.create("search * | head 100", **{"exec_mode": "blocking"})
    print("...done!\n")

    print("Search results:\n")

    # Prints a parsed, formatted XML stream to the console
    result_stream = job.results()
    reader = results.ResultsReader(result_stream)
    for item in reader:
        print(item)
    print("Results are a preview: %s" % reader.is_preview)
def read_results_job(service):
    """DICT MODE"""
    logger.info('******read_results_job start*****')
    job = service.jobs.create("search * | head 10 ")
    while not job.is_done():
        sleep(.2)
    rr = results.ResultsReader(job.results())
    for result in rr:
        if isinstance(result, results.Message):
            # Diagnostic messages may be returned in the results
            logger.info('%s: %s' % (result.type, result.message))

        elif isinstance(result, dict):
            # Normal events are returned as dicts
            logger.info('result of type dict')

            # pp.pprint(result)

    if rr.is_preview:
        print("Preview of a running search job.")
    else:
        print("Job is finished. Results are final.")
    return result

    # assert rr.is_preview == False


def splunkapp_list_directory_contents(service_client, file_system):
    logger.info('list_directory_contents function')
    try:
        file_system_client = service_client.get_file_system_client(file_system="datalakedev")
        paths = file_system_client.get_paths(path="test")
        for path in paths:
            logger.info(str(path.name) + '\n')
    except Exception as e:
        logger.error(e)


def test_conn_msupport_connectionstr_v2noproxy():
    from azure.storage.filedatalake import DataLakeServiceClient
    from azure.storage.blob._shared.base_client import create_configuration
    logger.info('start ****test_conn_msupport_connectionstr_v2noproxy')
    try:
        config = create_configuration(storage_sdk='filedatalake')
        connection_string = "BlobEndpoint=https://bigdatalake.blob.core.windows.net/;QueueEndpoint=https://bigdatalake.queue.core.windows.net/;FileEndpoint=https://bigdatalake.file.core.windows.net/;TableEndpoint=https://bigdatalake.table.core.windows.net/;SharedAccessSignature=sv=2019-10-10&ss=bfqt&srt=sc&sp=rwlacupx&se=2020-06-16T19:29:04Z&st=2020-04-26T11:29:04Z&spr=https&sig=cNzYYuvfxY6%2BkrfBghjqGnBmLFWgTur%2FM0gPtjDZbRQ%3D"

        # Construct the BlobServiceClient, including the customized configuation.
        service_client = DataLakeServiceClient.from_connection_string(connection_string, configuration=config)
        # NEW:
        file_systems = service_client.list_file_systems()
        for file_system in file_systems:
            logger.info(file_system.name + '\n')
    except Exception as e:
        logger.error(e)


# **************

# if text_sent:
#     return func.HttpResponse(
#         json.dumps([{
#             "splunk_query": f"{str(text_sent)}",
#             "message": "TEST azure function query 1",
#             "splunk execution result": f'{query_stat}'
#         }]),
#         status_code=200
#     )


# ___________________________________
# import logging
# import os
# import time
# import datetime
# import json
# import azure.functions as func
# import splunklib 


# def main(req: func.HttpRequest,context: func.Context) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')

#     t = datetime.datetime.now().strftime("%H-%M-%S-%f")
#     foldername="/home/site"+"/newfolder"+t
#     os.makedirs(foldername)
#     suffix = ".txt"
#     newfile= t+suffix
#     os.getcwd()
#     os.chdir(foldername)
#     if not os.path.exists(newfile):
#         f = open(newfile,'w')
#         f.write("test")
#         f.close()
#     data=[]
#     for filename in os.listdir(context.function_directory):
#         print(filename)
#         d1={"filename":filename}
#         data.append(d1)
#     jsondata=json.dumps(data)


# return func.HttpResponse(jsondata)