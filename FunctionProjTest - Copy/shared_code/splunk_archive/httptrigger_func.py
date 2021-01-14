import logging
import json
import azure.functions as func


# def main(req: func.HttpRequest) -> func.HttpResponse:
#     logging.info('Python HTTP trigger function processed a request.')

#     text_sent = None

#     try:
#         text_sent = req.get_body()
#     except ValueError:
#         pass

#     if text_sent:
#         return func.HttpResponse(text_sent)
#         logging.info(text_sent)
#     else:
#         logging.error('status code: 400 ')
#         return func.HttpResponse(
#              "IDI AZFunc: Please pass a file in the request body",
#              status_code=400

#         )


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    # loggin.info(str(name))
    if not name:
        try:
            req_body = req.get_json()
            # loggin.info(str(req_body))
        except ValueError:
            pass
        else:
            name = req_body.get('name')
            # same as  name = req_body['name']

    if name:
        # return func.HttpResponse(f"Hello {name}!")
        finalResult = {"result": f"Hello {name}!"}
        return func.HttpResponse(json.dumps(finalResult), mimetype="application/json", charset="utf-8")

    else:
        return func.HttpResponse(
            "YOSI TEST FUNCPlease pass a name on the query string or in the request body",
            status_code=400
        )
