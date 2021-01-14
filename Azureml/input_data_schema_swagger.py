from __future__ import print_function, division
from future.utils import iteritems

import sys, os, json, inspect, time, copy
from datetime import datetime as dt
import joblib
import pickle
import numpy as np
import pandas as pd
print(pd.__version__)
import logging


# from inference_schema.schema_decorators import input_schema, output_schema
# from inference_schema.parameter_types.numpy_parameter_type import NumpyParameterType
# from inference_schema.parameter_types.pandas_parameter_type import PandasParameterType
# from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType

d= {
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


temp_d  = pd.DataFrame(d)
standard_sample_input = temp_d.to_json(orient='records',date_format='iso',date_unit='s',force_ascii=False)
print(str(standard_sample_input ))

print(type(standard_sample_input ))

logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
logging.debug('start deubg mode_TEST TEST TEST')
logger = logging.getLogger(__name__)

logger.debug('start deubg mode_TEST TEST TEST222222')