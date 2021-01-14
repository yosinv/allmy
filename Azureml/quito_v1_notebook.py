% % writefile / dbfs / mnt / bigdatalake / datascience / azureml / score / quito / score.py

import os
import joblib
import pickle
import numpy as np
import pandas as pd
from azureml.core import Model

from inference_schema.schema_decorators import input_schema, output_schema
from inference_schema.parameter_types.numpy_parameter_type import NumpyParameterType
from inference_schema.parameter_types.pandas_parameter_type import PandasParameterType
from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType

import logging

logger = logging.basicConfig(level=logging.DEBUG)

# import debugpy
# # Allows other computers to attach to debugpy on this IP address and port.
# debugpy.listen(('0.0.0.0', 5678))
# # Wait 30 seconds for a debugger to attach. If none attaches, the script continues as normal.
# debugpy.wait_for_client()
# print("Debugger attached...")

# The init() method is called once, when the web service starts up.
#
# Typically you would deserialize the model file, as shown here using joblib,
# and store it in a global variable so your run() method can access it later.
sub_models = ['label_encoders_claim_feat', 'ordinal_encoders_claim_feat', 'scalers_claim_feat',
              'dimensionality_claim_feat', 'rel_feat_claim_feat', 'xgb_model', 'lgb_model',
              'select_best_features_claim_feat', 'feat_for_pca_claim_feat', 'pca_claim_feat',
              'pca_reduced_values_scaler_claim_feat', 'chi2_features_for_poly_claim_feat', 'poly_obj_claim_feat',
              'ocs_claim_feat', 'scaler_after_score_claim_feat', 'important_feat_claim_feat',
              'rel_feat_feature_importance_claim_feat', 'rel_feat_feature_importance']


def load_sub_model(modelname):
    if (modelname != 'xgb_model'):
        modelfilename = modelname + '.pkl'
        print(modelfilename)
        #         logger.debug(modelfilename)
        model_path = os.path.join(os.environ['AZUREML_MODEL_DIR'], modelfilename)
        # #   model_path = Model.get_model_path(model_name='lima_trained_model')
        print(model_path)
        try:
            submodel = joblib.load(model_path)
            return submodel
        except Exception as e:
            error = str(e)
            print('error loding model: ', modelfilename)
            return error


def init():
    global model

    bins_path = os.path.join('Quito_register_dataset', 'bins_df_claim_feat.csv')
    clipped_dct_path = os.path.join('Quito_register_dataset', 'clipping_values_df_claim_feat.csv')

    #    MAIN MODEL
    xgb_model_path = Model.get_model_path(model_name='xgb_model')  # SAV file extension
    #     SUB MODELS
    label_encoders_claim_feat_path = Model.get_model_path(model_name='label_encoders_claim_feat')
    ordinal_encoders_claim_feat_path = Model.get_model_path(model_name='ordinal_encoders_claim_feat')
    scalers_path = Model.get_model_path(model_name='scalers_claim_feat')
    dimensionality_path = Model.get_model_path(model_name='dimensionality_claim_feat')
    rel_feat_path = Model.get_model_path(model_name='rel_feat_claim_feat')
    lgb_model_path = Model.get_model_path(model_name='lgb_model')  # SAV
    select_best_features_path = Model.get_model_path(model_name='select_best_features_claim_feat')
    feat_for_pca_path = Model.get_model_path(model_name='feat_for_pca_claim_feat')
    pca_path = Model.get_model_path(model_name='pca_claim_feat')
    pca_reduced_values_scaler_path = Model.get_model_path(model_name='pca_reduced_values_scaler_claim_feat')
    chi2_features_for_poly_path = Model.get_model_path(model_name='chi2_features_for_poly_claim_feat')
    poly_obj_path = Model.get_model_path(model_name='poly_obj_claim_feat')
    ocs_path = Model.get_model_path(model_name='ocs_claim_feat')
    scaler_after_score_path = Model.get_model_path(model_name='scaler_after_score_claim_feat')
    important_feat_path = Model.get_model_path(model_name='important_feat_claim_feat')

    rel_feat_feature_importance_path = Model.get_model_path(model_name='rel_feat_feature_importance_claim_feat')
    rel_feat_feature_importance_path_client = Model.get_model_path(model_name='rel_feat_feature_importance')
    client_df_rt_path = client_df_for_real_time.csv

    # LOAD MODELS
    print('AZURE ML SUB MODEL "ordinal_encoders_claim_feat_path" : ', ordinal_encoders_claim_feat_path)
    ordinalEncoders = pickle.load(open(ordinal_encoders_claim_feat_path, 'rb'))

    print('AZURE ML SUB MODEL "label_encoders_claim_feat_path" : ', label_encoders_claim_feat_path)
    labelEncoders = pickle.load(open(label_encoders_claim_feat_path, 'rb'))

    print('AZURE ML SUB MODEL "scalers_path" : ', scalers_path)
    scalers = pickle.load(open(scalers_path, 'rb'))

    print('AZURE ML SUB MODEL "dimensionality_path" : ', dimensionality_path)
    dimensionality = pickle.load(open(dimensionality_path, 'rb'))

    print('AZURE ML SUB MODEL "rel_feat_path" : ', rel_feat_path)
    relevant_features = pickle.load(open(rel_feat_path, 'rb'))

    print('AZURE ML MAIN MODEL "xgb_model_path" : ', xgb_model_path)
    xgb_model = pickle.load(open(xgb_model_path, 'rb'))

    print('AZURE ML SUB MODEL "lgb_model_path" : ', lgb_model_path)
    lgb_model = pickle.load(open(lgb_model_path, 'rb'))

    print('AZURE ML SUB MODEL "select_best_features_path" : ', select_best_features_path)
    select_best_features = pickle.load(open(select_best_features_path, 'rb'))

    print('AZURE ML SUB MODEL "feat_for_pca_path" : ', feat_for_pca_path)
    feat_for_pca = pickle.load(open(feat_for_pca_path, 'rb'))

    print('AZURE ML SUB MODEL "pca_path" : ', pca_path)
    pca = pickle.load(open(pca_path, 'rb'))

    print('AZURE ML SUB MODEL "pca_reduced_values_scaler_path" : ', pca_reduced_values_scaler_path)
    pca_reduced_values_scaler = pickle.load(open(pca_reduced_values_scaler_path, 'rb'))

    print('AZURE ML SUB MODEL "chi2_features_for_poly_path" : ', chi2_features_for_poly_path)
    chi2_features_for_poly = pickle.load(open(chi2_features_for_poly_path, 'rb'))

    print('AZURE ML SUB MODEL "poly_obj_path" : ', poly_obj_path)
    poly_obj = pickle.load(open(poly_obj_path, 'rb'))

    print('AZURE ML SUB MODEL "important_feat_path" : ', important_feat_path)
    important_feat = pickle.load(open(important_feat_path, 'rb'))

    print('AZURE ML SUB MODEL "ocs_path" : ', ocs_path)
    ocs = pickle.load(open(ocs_path, 'rb'))

    print('AZURE ML SUB MODEL "scaler_after_score_path" : ', scaler_after_score_path)
    scaler_after_score = pickle.load(open(scaler_after_score_path, 'rb'))

    print('AZURE ML SUB MODEL "rel_feat_feature_importance_path" : ', rel_feat_feature_importance_path)
    rel_feat_feature_importance = pickle.load(open(rel_feat_feature_importance_path, 'rb'))

    print('AZURE ML SUB MODEL "rel_feat_feature_importance_path_client" : ', rel_feat_feature_importance_path_client)
    rel_feat_feature_importance_client = pickle.load(open(rel_feat_feature_importance_path_client, 'rb'))

    client_df_rt = pd.read_csv(client_df_rt_path)
    client_df_rt.drop_duplicates('CLIENT_CLAIM', inplace=True)


standard_sample_input = [{
    "Claim_Nr": 1350302,
    "Client_Claim": "034124149",
    "CLAIM_DATE": 1556035200000,
    "Claim_Place_Code": "4",
    "Claim_Place_Zone_Code": "\\u05d1\\u05d0\\u05e8",
    "Fualt_Insured_Code": 4.0,
    "Vehicle_Towed": 0.0,
    "Police_arrive": "nodata",
    "Photos_taken": "nodata"
}, ]
standard_sample_output = [
    {
        "Client": 208140,
        "Policy_Nr": 1127397702,
        "Endors_Nr": 1,
        "proba": 0.305826,
        "Lima_prediction_id": 2,
        "Lima_prediction": "ללא העדפה"
    }, ]


@input_schema('data', StandardPythonParameterType(standard_sample_input))
@output_schema(StandardPythonParameterType(standard_sample_output))
# The run() method is called each time a request is made to the scoring API.
#
# Shown here are the optional input_schema and output_schema decorators
# from the inference-schema pip package. Using these decorators on your
# run() method parses and validates the incoming payload against
# the example input you provide here. This will also generate a Swagger
# API document for your web service.
# @input_schema('data', NumpyParameterType(np.array([[0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.0]])))
# @output_schema(NumpyParameterType(np.array([4429.929236457418])))

# @input_schema('data', NumpyParameterType(np.array([[0.1, 1.2, 2.3, 3.4, 4.5, 5.6, 6.7, 7.8, 8.9, 9.0]])))
def run(data):
    try:
        result = data
        #     return result.tolist()
        print('predict')
        return result

    except Exception as e:
        error = str(e)
        print('error')
        return error
    # ts = 0.7
    # metadata = ['Client', 'Policy_Nr', 'Endors_Nr']
    # prediction_types = {0: 'הסדר', 1: 'פרטי', 2: 'ללא העדפה', 3: 'כשלון בחילוץ הפיצרים', }
    # client_claim=''
    # policy_nr=''
    # try:
    #   df = client_df_rt[(client_df_rt['Client'].isin(client_claim)) &
    #                     (client_df_rt['Policy_Nr'].isin(policy_nr))]
    #   client_metadata = df[metadata]
    #   values_to_predict = df.drop(metadata, axis=1)

    #   # Use the model object loaded by init().
    #   model_proba = model.predict(values_to_predict)

    #   probas_df = pd.DataFrame(model_proba, columns=['proba'])

    #   def f(x):
    #       if (x > 0.5):
    #           return 1
    #       elif (x <= (0.5 - (ts - 0.5))):
    #           return 0
    #       else:
    #           return 2

    #   probas_df['Lima_prediction_id'] = probas_df['proba'].apply(f)
    #   probas_df['Lima_prediction'] = probas_df['Lima_prediction_id'].apply(lambda x: prediction_types[x])
    #   probas_df.reset_index(drop=True, inplace=True)
    #   probas_df = pd.concat([client_metadata, probas_df], axis=1)

    #   result = probas_df
    #   return result

    # except Exception as e:
    #   probas_df = pd.DataFrame([99], columns=['proba'])
    #   probas_df['Lima_prediction_id'] = 3
    #   probas_df['Lima_prediction'] = probas_df['Lima_prediction_id'].apply(lambda x: prediction_types[x])
    #   probas_df.reset_index(drop=True, inplace=True)
    #   client_metadata = pd.DataFrame(list(zip(client_claim, policy_nr)), columns=['Client', 'Policy_Nr'])
    #   probas_df = pd.concat([client_metadata, probas_df], axis=1)

    #   result = probas_df
    #   return result
    # result =model.predict(data) """)
