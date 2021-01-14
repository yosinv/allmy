# coding=utf-8
# !/usr/bin/python -u


import configparser
import requests
import json
import logging
import pandas as pd
from pandas import json_normalize
import urllib
import re

logger = logging.getLogger(__name__)

http_proxy = "http://websense-proxy-vip:8080"  # NOT WWORKING IN IDI
https_proxy = "https://websense-proxy-vip:8080"
# https_proxy="192.168.220.21:8080"
# https_proxy="192.168.227.230:8080"

proxies = {"https": https_proxy,
           "http": http_proxy}


def read_json_like_api():
    with open('response.json', 'r', encoding='utf-8') as f:
        try:
            response = json.load(f)
        except Exception as e:
            logger.error("Error occurred while trying to upload app settings!!! Empty settings are set.")
            response = {}
    return response


def get_datagov_m():
    limit = 10000
    resource_id = 'bb68386a-a331-4bbc-b668-bba2766d517d'
    # url = "https://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid={}".format(location, api_key)
    url = (f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=20000')
    # https://data.gov.il/api/3/action/datastore_search?resource_id=bb68386a-a331-4bbc-b668-bba2766d517d&limit=100
    response = requests.request("GET", url, proxies=proxies, verify=False)
    data_from_api = json.loads(response.text.encode('utf-8'))

    return data_from_api


def get_datagov_s():
    limit = 10000
    resource_id = '4a434d65-3ca2-45e5-8026-5d9819c3f95c'
    # url = "https://api.openweathermap.org/data/2.5/weather?q={}&units=metric&appid={}".format(location, api_key)
    url = (f'https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}&limit=20000')
    # https://data.gov.il/api/3/action/datastore_search?resource_id=bb68386a-a331-4bbc-b668-bba2766d517d&limit=100
    response = requests.request("GET", url, proxies=proxies, verify=False)
    data_from_api = json.loads(response.text.encode('utf-8'))

    return data_from_api

def filter_m_data(df, **kwargs):
    df_mosah_morshe = df[df['sug_mosah']=='מוסך מורשה']



def set_pandas_display_options() -> None:
    """Set pandas display options."""
    # Ref: https://stackoverflow.com/a/52432757/
    display = pd.options.display

    display.max_columns = 1000
    display.max_rows = 1000
    display.max_colwidth = 199
    display.width = None
    # display.precision = 2  # set as needed


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    data_from_api = r'{"help": "https://data.gov.il/api/3/action/help_show?name=datastore_search", "success": true, "result": {"include_total": false, "resource_id": "bb68386a-a331-4bbc-b668-bba2766d517d", "fields": [{"type": "int", "id": "_id"}, {"type": "numeric", "id": "mispar_mosah"}, {"type": "text", "id": "shem_mosah"}, {"type": "numeric", "id": "cod_sug_mosah"}, {"type": "text", "id": "sug_mosah"}, {"type": "text", "id": "ktovet"}, {"type": "text", "id": "yishuv"}, {"type": "text", "id": "telephone"}, {"type": "numeric", "id": "mikud"}, {"type": "numeric", "id": "cod_miktzoa"}, {"type": "text", "id": "miktzoa"}, {"type": "text", "id": "menahel_miktzoa"}], "records_format": "objects", "records": [{"_id":1,"mispar_mosah":16,"shem_mosah":"נירים מוסך הקבוץ","cod_sug_mosah":6,"sug_mosah":"מוסך מורשה","ktovet":"דנ הנגב","yishuv":"נירים","telephone":"054-7916219","mikud":85125,"cod_miktzoa":10,"miktzoa":"מכונאות רכב בנזין","menahel_miktzoa":"אליהו ציון"},{"_id":2,"mispar_mosah":16,"shem_mosah":"נירים מוסך הקבוץ","cod_sug_mosah":6,"sug_mosah":"מוסך מורשה","ktovet":"דנ הנגב","yishuv":"נירים","telephone":"054-7916219","mikud":85125,"cod_miktzoa":20,"miktzoa":"מכונאות רכב דיזל","menahel_miktzoa":"אליהו ציון"},{"_id":3,"mispar_mosah":16,"shem_mosah":"נירים מוסך הקבוץ","cod_sug_mosah":6,"sug_mosah":"מוסך מורשה","ktovet":"דנ הנגב","yishuv":"נירים","telephone":"054-7916219","mikud":85125,"cod_miktzoa":190,"miktzoa":"טרקטורים ומכונות ניידות","menahel_miktzoa":"אליהו ציון"},{"_id":4,"mispar_mosah":16,"shem_mosah":"נירים מוסך הקבוץ","cod_sug_mosah":6,"sug_mosah":"מוסך מורשה","ktovet":"דנ הנגב","yishuv":"נירים","telephone":"054-7916219","mikud":85125,"cod_miktzoa":194,"miktzoa":"תיקון מלגזות הרמה","menahel_miktzoa":"אליהו ציון"},{"_id":5,"mispar_mosah":23,"shem_mosah":"קבוץ רביבים","cod_sug_mosah":6,"sug_mosah":"מוסך מורשה","ktovet":"דנ חלוצה","yishuv":"רביבים","telephone":"08-6562541","mikud":85515,"cod_miktzoa":10,"miktzoa":"מכונאות רכב בנזין","menahel_miktzoa":"בן יוסף יאיר"}], "limit": 5, "_links": {"start": "/api/3/action/datastore_search?limit=5&resource_id=bb68386a-a331-4bbc-b668-bba2766d517d", "next": "/api/3/action/datastore_search?offset=5&limit=5&resource_id=bb68386a-a331-4bbc-b668-bba2766d517d"}}}'
    set_pandas_display_options()
    # json.loads take a string as input and returns a dictionary as output.
    # json.dumps take a dictionary as input and returns a string as output.

    # datagov = read_json_like_api()
    datagov_m = get_datagov_m()

    df = json_normalize(datagov_m['result']['records'])

    # df.head(10)
    # print (df)
    df =df.drop(columns =['miktzoa', 'cod_miktzoa'])
    df = df.drop_duplicates(subset=['mispar_mosah'])


    df_mosah_morshe = df[df['sug_mosah'] == 'מוסך מורשה']
    # df_mosah_morshe = re.sub(r"[\\\"/:<>`|*?]", "", df_mosah_morshe["shem_mosah"])
    # df_mosah_morshe.applymap(lambda x: x.replace('"', ''))
    # df_mosah_morshe["shem_mosah"] = df_mosah_morshe["shem_mosah"].apply(lambda x: x.replace('"', ''))
    df_mosah_morshe.to_csv(r'\\nacenter2\generalfs\מערכות מידע\כללי\DWH\datagov\datagov_m.csv', header=True,
              index=False, sep='|', mode='w',
              encoding='utf-8', date_format='iso')



    datagov_s = get_datagov_s()
    df_s = json_normalize(datagov_s['result']['records'])
    df_s['shem_prati'] = df_s[['shem_prati', 'shem_mishpaha']].apply(lambda x: ' '.join(x), axis=1)
    df_s.rename(columns={"shem_prati": "NAME"})
    # df_s = re.sub(r"[\\\"/:<>`|*?]", "", df_s["shem_mosah"])
    # df_s.applymap(lambda x: x.replace('"', ''))

    df_s = df_s.drop(columns=['shem_mishpaha'])
    df_s.to_csv(r'\\nacenter2\generalfs\מערכות מידע\כללי\DWH\datagov\datagov_s.csv', header=True,
                index=False, sep='|', mode='w',
                encoding='utf-8', date_format='ISO 8601')
# doublequote=False,escapechar="\\"

# בpyspark:
# removeSpecialChars = udf(lambda x : re.sub(r'[^\w\s]', '', str(x)), StringType())
# Service_Provider_arrangment = Service_Provider_arrangment.withColumn('Expert_Name',removeSpecialChars(Service_Provider_arrangment.Expert_Name))
