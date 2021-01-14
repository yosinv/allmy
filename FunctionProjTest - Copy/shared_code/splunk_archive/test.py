import logging
import azure.functions as func
import splunklib.client as client
import splunklib.results as results
import pprint
from time import sleep

HOST = "172.16.227.30"
PORT = 8089
USERNAME = "yosin"
PASSWORD = "Yashir12!"
pp = pprint.PrettyPrinter(indent=4)

# def setServer(hostname, port, splunkuser, splunkpassword):
#     #   client = pyimport("splunklib.client")
#
#     # Connect to Splunk Enterprise
#     service = client.connect(host=HOST, port=PORT, username=USERNAME, password=PASSWORD, autologin=True)
#
#     #   assert isinstance(service, client.Service)
#     #   print(service.__dict__)
#     # for app in service.apps:
#     #     print(app.name)
#     return service


# def read_results_job(service):
#     job = service.jobs.create("search * | head 5 ")
#     while not job.is_done():
#         sleep(.2)
#     rr = results.ResultsReader(job.results())
#     for result in rr:
#         if isinstance(result, results.Message):
#             # Diagnostic messages may be returned in the results
#             pp.pprint('%s: %s' % (result.type, result.message))
#         elif isinstance(result, dict):
#             # Normal events are returned as dicts
#             print('dict')
#             pp.pprint(result)
#     assert rr.is_preview == False


# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')
#
#     # service = setServer(HOST, PORT, USERNAME, PASSWORD)
#     logging.info('setServer: ' + HOST, PORT, USERNAME, PASSWORD)
#
#     # read_results_job(service)
#     text_sent = None
#
#     try:
#         text_sent = req.get_body()
#     except ValueError:
#         pass
#
#     if text_sent:
#         return func.HttpResponse(text_sent)
#         logging.info(text_sent)
#     else:
#         logging.error('status code: 400 ')
#         return func.HttpResponse(
#             "IDI AZFunc: Please pass a file in the request body",
#             status_code=400
#
#         )


# import json
# import azure.functions as func
# # from ..shared_code import splunk_data
# import logging.handlers
# import logging, os
# # from config.logs import setup_logging
# import pprint
#
# # LOG_FILENAME = 'Splunk_data_logs.log'
# # file_handler = logging.FileHandler(filename=LOG_FILENAME)
# # formatter = logging.Formatter('[%(asctime)s] {%(filename)s: %(lineno)d} %(levelname)s - %(message)s')
# # file_handler.setFormatter(formatter)
#
# # Set up a specific logger with our desired output level
# # logging.basicConfig(
# #     level=logging.INFO,
# # )
# #
# # logger = logging.getLogger('Splunk_data')
# # logger.addHandler(file_handler)
#
# HOST = "172.16.227.30"
# PORT = 8089
# USERNAME = "yosin"
# PASSWORD = "Corona10!"
# pp = pprint.PrettyPrinter(indent=4)
#
#
# def main(req: func.HttpRequest) -> func.HttpResponse:
#     # from config.logs import setup_logging
#     # To get the full path to the directory a Python file is contained in:
#
#     # setup_logging(logs_dir)
#     logger = logging.getLogger(__name__)
#     curr_dir = os.path.dirname(os.path.realpath(__file__))
#     logs_dir = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
#     logger.info(logs_dir)
#     logger.info("logger start logging")
#     logger.info('Python HTTP trigger function processed a request.')
#     text_sent = None
#     try:
#         text_sent = req.get_body()
#         # logger.info('text_sent:' + text_sent)
#     except ValueError:
#         # logger.error('text_sent:' + text_sent)
#         pass
#
#     # logger.info(f'HOST, PORT, USERNAME, PASSWORD: {HOST} , {PORT}, {USERNAME}, {PASSWORD}')
#     try:
#         logging.info('setServer: ' + HOST, PORT, USERNAME, PASSWORD)
#         # splunk_data.query_splunk()
#         # logging.info(f"service success result: {str(service)}")
#     except ValueError:
#         pass
#     else:
#         logger.error('service error')
#
#     if text_sent:
#         return func.HttpResponse(
#             json.dumps([{
#                 "splunk_query": "insert the splunk query here",
#                 "message": "TEST azure function query 1"
#             }]),
#             status_code=200
#         )
#     else:
#         return func.HttpResponse(
#             "Please pass a file in the request body",
#             status_code=400
#         )
#
