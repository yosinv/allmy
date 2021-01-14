from __future__ import print_function, division
from future.utils import iteritems

import sys, os, json, inspect, time, copy
from datetime import datetime as dt
import joblib
import pickle
import numpy as np
import pandas as pd

import logging




def init():
    global model, QuitoClassifier_obj

    logging.basicConfig(level=logging.DEBUG)
    logging.debug('start deubg mode_TESTinit TESTinit TESTinit')
    logger = logging.getLogger(__name__)
    logger.debug('start deubg mode_TESTinit TESTinit TESTinit222222')
    logger.info('*****INIT****')
    logger.info('current working directory:  ' + str(os.getcwd()))
    threshold = 55
    QuitoClassifier_obj = QuitoClassifier('55')

def run(data):
  '''
  :param df
  :type Pandas data frame
  '''
  logger.info('start info mode_TESTinit TESTinit TESTinit222222')
  logger.info('****RUN EXECUTION****')
  json_str = data
  # logger.info('INPUT DATA: ', str(json_str))
  # logger.info('TYPE DATA: ', str(type(json_str)))
  mode = 'xgb'
  threshold = 55
  try:

      proba_df = QuitoClassifier_obj.query(json_str)
      result = proba_df
      # logger.info('proba_df, result: ' )
      return result.to_json(orient='records', date_format='iso', date_unit='s', force_ascii=False)
  except Exception as e:
      error = str(e)
      logger.error('error')
      return error


class QuitoClassifier(object):
    global dtypes_dct, ordinalEncoders, labelEncoders, scalers, dimensionality, relevant_features, xgb_model, lgb_model, select_best_features, feat_for_pca, pca, pca_reduced_values_scaler, chi2_features_for_poly, poly_obj, important_feat, ocs, scaler_after_score, rel_feat_feature_importance, rel_feat_feature_importance_client
    def __init__(self, threshold):
        self.logger = logging.getLogger(type(self).__name__)
        # self.logger = logger
        self.prediction_types = {0: 'נזק נמוך',
                                 1: 'נזק גבוה',
                                 2: 'נזק בינוני',
                                 3: 'כשלון בחילוץ הפיצרים'}
        xgb_model_path = '\mdb'
        self.logger.info('xgb_model_path: xgb_model_path')

    def query(self, json_obj, mode='xgb'):
        '''
        :param df
        :type Pandas data frame
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))

logging.basicConfig(level=logging.DEBUG)
logging.debug('start deubg mode_TEST TEST TEST')
logger = logging.getLogger(__name__)
logger.debug('start deubg mode_TEST TEST TEST222222')
logger.info('start deubg mode_TEST TEST TEST222222')

data= {
      "Claim_Nr":1006900,
      "Client_Claim":"032253122",
      "CLAIM_DATE":"2016-04-04T17:20:00Z",
      "Claim_Place_Code":"102",
      "Claim_Place_Zone_Code":"016",
      "Fualt_Insured_Code":1,
      "Vehicle_Towed":None,
      "Police_arrive":None,
      "Photos_taken":None,
      "Damage_Dsc_array":[
         "תאונת דרכים רב צדדית"
      ]
   }


data  = pd.DataFrame(data).to_json(orient='records',date_format='iso',date_unit='s',force_ascii=False)
init()
run(data)