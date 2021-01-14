from __future__ import print_function, division
from future.utils import iteritems
import sys, os, json, inspect, time, pickle, logging, copy
from datetime import datetime as dt
from ast import literal_eval
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import warnings

warnings.filterwarnings('ignore')


class QuitoClassifier(object):
    def __init__(self, base_dir, model_date, threshold):

        # model_dir = 'quito/quito'
        # model_date = '2020_08_30_06_33AM'

        threshold = 55
        prediction_types = {0: 'נזק נמוך',1: 'נזק גבוה',2: 'נזק בינוני',3: 'כשלון בחילוץ הפיצרים'}
        metadata_cols = ['Claim_Nr', 'Client_Claim']
        metadata_cols = ['Client_Claim']
        ordinal_feat_lst = list(set(['Claim_Date_month_year', 'Claim_Date_day', 'Claim_Date_month', 'Claim_Date_year']))
        cat_feat_lst = list(set(['Claim_Place_Code', 'Claim_Place_Zone_Code', 'Fualt_Insured_Code', 'Vehicle_Towed', 'Police_arrive','Photos_taken', 'Damage_Dsc_array']))
        feat_to_clipp = list(set(['Claim_Place_Zone_Code', ]))
        decomp_by_range = list(set(['Claim_Date_month_year']))

        all_features = list(set(ordinal_feat_lst + cat_feat_lst + feat_to_clipp + decomp_by_range + metadata_cols))

        ORDINAL_COLS = tuple(ordinal_feat_lst + decomp_by_range)
        CATEGORICAL_COLS = tuple(cat_feat_lst + feat_to_clipp)
        NUMERICAL_COLS = ()


        # encoders+model paths:
        # model_date = model_date
        # base_dir = '{}_{}'.format(base_dir, model_date)
        bins_path = os.path.join('Quito_register_dataset', 'bins_df_claim_feat.csv')
        clipped_dct_path = os.path.join('Quito_register_dataset', 'clipping_values_df_claim_feat.csv')

        #    MAIN MODEL
        xgb_model_path = Model.get_model_path(model_name='xgb_model')  # SAV file extension

        #     SUB MODELS
        dtypes_dct_path = Model.get_model_path(model_name='dtypes_dct_claim_feat')
        labelEncoders_path = Model.get_model_path(model_name='label_encoders_claim_feat')
        ordinalEncoders_path = Model.get_model_path(model_name='ordinal_encoders_claim_feat')
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


        # dtypes_dct_path = '{}/dtypes_dct_claim_feat.pickle'.format(self.base_dir)
        # labelEncoders_path = os.path.join(self.base_dir, 'label_encoders_claim_feat.pickle')
        # ordinalEncoders_path = os.path.join(self.base_dir, 'ordinal_encoders_claim_feat.pickle')
        # scalers_path = os.path.join(self.base_dir, 'scalers_claim_feat.pickle')
        # dimensionality_path = os.path.join(self.base_dir, 'dimensionality_claim_feat.pickle')
        # rel_feat_path = os.path.join(self.base_dir, 'rel_feat_claim_feat.pickle')
        # # rfc_model_path = os.path.join(self.base_dir,'rfc_model_claim_feat.sav')
        # xgb_model_path = os.path.join(self.base_dir, 'xgb_model.sav')
        # lgb_model_path = os.path.join(self.base_dir, 'lgb_model.sav')
        # select_best_features_path = '{}/select_best_features_claim_feat.pickle'.format(self.base_dir)
        # feat_for_pca_path = '{}/feat_for_pca_claim_feat.pickle'.format(self.base_dir)
        # pca_path = '{}/pca_claim_feat.pickle'.format(self.base_dir)
        # pca_reduced_values_scaler_path = '{}/pca_reduced_values_scaler_claim_feat.pickle'.format(self.base_dir)
        # chi2_features_for_poly_path = '{}/chi2_features_for_poly_claim_feat.pickle'.format(self.base_dir)
        # poly_obj_path = '{}/poly_obj_claim_feat.pickle'.format(self.base_dir)
        # ocs_path = '{}/ocs_claim_feat.pickle'.format(self.base_dir)
        # scaler_after_score_path = '{}/scaler_after_score_claim_feat.pickle'.format(self.base_dir)
        # important_feat_path = '{}/important_feat_claim_feat.pickle'.format(self.base_dir)
        # rel_feat_feature_importance_path = '{}/rel_feat_feature_importance_claim_feat.pickle'.format(self.base_dir)
        # rel_feat_feature_importance_path_client = '{}/rel_feat_feature_importance.pickle'.format(self.base_dir)
        client_df_rt_path = os.path.join(self.base_dir, 'client_df_for_real_time.csv')

        # encoders+model objects:
        self.bins_df = pd.read_csv(bins_path, converters={'bins': self._converter})
        self.clipped_vals_df = pd.read_csv(clipped_dct_path)

        self.dtypes_dct = pickle.load(open(dtypes_dct_path, 'rb'))
        self.ordinalEncoders = pickle.load(open(ordinalEncoders_path, 'rb'))
        self.labelEncoders = pickle.load(open(labelEncoders_path, 'rb'))
        self.scalers = pickle.load(open(scalers_path, 'rb'))
        self.dimensionality = pickle.load(open(dimensionality_path, 'rb'))
        self.relevant_features = pickle.load(open(rel_feat_path, 'rb'))
        # self.rfc_model = pickle.load(open(rfc_model_path, 'rb'))
        self.xgb_model = pickle.load(open(xgb_model_path, 'rb'))
        self.cols_when_model_builds = self.xgb_model.get_booster().feature_names
        self.lgb_model = pickle.load(open(lgb_model_path, 'rb'))
        self.select_best_features = pickle.load(open(select_best_features_path, 'rb'))
        self.feat_for_pca = pickle.load(open(feat_for_pca_path, 'rb'))
        self.pca = pickle.load(open(pca_path, 'rb'))
        self.pca_reduced_values_scaler = pickle.load(open(pca_reduced_values_scaler_path, 'rb'))
        self.chi2_features_for_poly = pickle.load(open(chi2_features_for_poly_path, 'rb'))
        self.poly_obj = pickle.load(open(poly_obj_path, 'rb'))
        self.important_feat = pickle.load(open(important_feat_path, 'rb'))
        self.ocs = pickle.load(open(ocs_path, 'rb'))
        self.scaler_after_score = pickle.load(open(scaler_after_score_path, 'rb'))
        self.rel_feat_feature_importance = pickle.load(open(rel_feat_feature_importance_path, 'rb'))
        self.rel_feat_feature_importance_client = pickle.load(open(rel_feat_feature_importance_path_client, 'rb'))

        self.client_df_rt = pd.read_csv(client_df_rt_path)
        self.client_df_rt.drop_duplicates('CLIENT_CLAIM', inplace=True)
        # threshold
        self.ts = round(int(threshold) / float(100), 2)



    def query(self, json_obj, mode='xgb'):
        '''
        :param df
        :type Pandas data frame
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            client_df = json_normalize(json.loads(json_obj))
            client_df['CLAIM_DATE'] = pd.to_datetime(client_df['CLAIM_DATE'])
            # client_df['Damage_Dsc_array'] = client_df['Damage_Dsc_array'].str[0]
            # client_df['Client_Claim'] = client_df['Client_Claim'].astype(str).apply(lambda x: x.zfill(9))
            client_df = client_df.merge(self.client_df_rt, left_on=['Client_Claim'],
                                        right_on=['CLIENT_CLAIM'])
            if client_df.shape[0] == 0:
                return 3

            client_df['CLAIM_DATE'].fillna(client_df['CLAIM_DATE'].value_counts().head(1).index[0], inplace=True)
            client_df['Claim_Date_month_year'] = pd.to_datetime(client_df['CLAIM_DATE']).dt.to_period('M')
            client_df['Claim_Date_month_year'] = client_df['Claim_Date_month_year'].apply(lambda x: x.to_timestamp())
            client_df['Claim_Date_month_year'] = client_df['Claim_Date_month_year'].apply(dt.toordinal)
            client_df['Claim_Date_day'] = client_df['CLAIM_DATE'].dt.day
            client_df['Claim_Date_month'] = client_df['CLAIM_DATE'].dt.month
            client_df['Claim_Date_year'] = client_df['CLAIM_DATE'].dt.year

            # replace_dtypes
            client_df = self._replace_dtypes(client_df)

            for col in list(client_df[self.all_features].select_dtypes(include=[np.float]).columns):
                client_df[col] = client_df[col].round(1)
            client_df.reset_index(drop=True, inplace=True)
            # replace_missing
            replace_missing_df = self._replace_missing(client_df)

            # decomp:
            decomp_df = self._sorted_numbers_decomposition(replace_missing_df)  # , self.bins_df, self.decomp_by_range)
            decomp_df = self._rename_rest_decomposition(decomp_df)

            # clipping:
            clipped_df = self._clipping(decomp_df)  # , self.clipped_vals_df, self.decomp_cols, self.decomp_by_range)

            # encode:
            encoded_df = self._encoding(clipped_df)

            # new_features:
            new_features_df = self._produce_new_features(encoded_df)

            # predict and save:
            values_to_predict = pd.concat([new_features_df, client_df], axis=1)
            self.values_to_predict = values_to_predict
            self.proba_df = self._predict(values_to_predict[self.metadata_cols],
                                          values_to_predict[self.cols_when_model_builds], mode)
            # self._save_data()
            return self.proba_df

            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))

        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            return ['cannot extract features']

    def _converter(self, instr):
        return np.fromstring(instr[1:-1], sep=' ')

    def _replace_dtypes(self, df):
        '''
        :param df
        :type Pandas data frame
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))

        def _isfloatnumber(x):
            try:
                float(x)
                return True
            except:
                return False

        def _isintnumber(x):
            try:
                int(x)
                return True
            except:
                return False

        try:
            # todo! tmp error col - change this
            # df = df[df['Municipal_Code']!='315P']
            # df['past_private_num'].fillna(0,inplace=True)
            # df['past_claim_num'].fillna(0,inplace=True)
            # df['past_arrangement_num'].fillna(0,inplace=True)
            for col in self.dtypes_dct.keys():
                try:
                    # if self.dtypes_dct[col]==np.dtype('float64'):
                    # df[col] = df[col].apply(lambda x:x if _isfloatnumber(x) else np.nan)
                    # if self.dtypes_dct[col]==np.dtype('int32'):
                    # df[col] = df[col].apply(lambda x:x if _isintnumber(x) else np.nan)
                    # col_shape = df[df[col].isnull()].shape[0]
                    # if (col_shape>0):
                    # self.logger.info('step: {} col: {} null values: {}'.format(step, col, col_shape))
                    # if col in ['Young_Driver_Vetek','Young_Driver_Age_Key','Area_Code','Young_Driver_Age']:
                    # fill_val = df[col].value_counts().head(1).index[0]
                    # df[col] = df[col].fillna(fill_val)
                    df[col] = df[col].astype(self.dtypes_dct[col].name)
                except Exception as e:
                    self.logger.info('step: {} col: {} error: {}'.format(step, col, e))

            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return df
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _replace_missing(self, df):
        '''
        :param df
        :type Pandas data frame
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))

        def _isnumber(x):
            try:
                float(x)
                return True
            except:
                return False

        try:
            # standard method of replacement for numerical columns is median
            for col in self.NUMERICAL_COLS:
                if np.any(df[col].isnull()):
                    med = np.median(df[col][df[col].notnull()])
                    df.loc[df[col].isnull(), col] = med
            for col in self.ORDINAL_COLS:
                df[col] = df[col].apply(lambda x: x if _isnumber(x) else np.nan)
                df[col] = df[col].astype('float')
                if np.any(df[col].isnull()):
                    med = np.median(df[col][df[col].notnull()])
                    df.loc[df[col].isnull(), col] = med
                    # set a special value = 'missing'
            for col in self.CATEGORICAL_COLS:
                if np.any(df[col].isnull()):
                    df.loc[df[col].isnull(), col] = 'missing'
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return df
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _sorted_numbers_decomposition(self, data):  # , bins_data, columns):
        '''
        :param data
        :type Pandas data frame
        :return data with columns decomposed from input column
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            for col in self.decomp_by_range:
                if ((data[col].dtype != float) and (data[col].dtype != np.int64)):
                    data.loc[incident_data[data[col] == 'None'].index, [col]] = np.NaN
                    data[col] = pd.to_numeric(data[col])
                bins = self.bins_df[self.bins_df['features'] == col]['bins'].values[0]
                labels = literal_eval(self.bins_df[self.bins_df['features'] == col]['labels'].values[0])
                data['decomp_{}'.format(col)] = pd.cut(data[col], bins=bins, labels=labels).values.add_categories(
                    'None').fillna('None')
                # add_categories('None').fillna('None')? todo need to delete it
                data = data.drop(col, axis=1)
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return data
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _rename_rest_decomposition(self, data):
        '''
        :param data
        :type Pandas data frame with incident
        :return data with columns renamed to decomposition
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            features_to_rename = self.ordinal_feat_lst + self.cat_feat_lst + self.feat_to_clipp
            data = data.rename(columns={x: 'decomp_{}'.format(x) for x in features_to_rename})
            self.decomp_cols = [x for x in data.columns if x.startswith('decomp_')]
            # self.feat_to_clipp = ['decomp_{}'.format(col) for col in self.feat_to_clipp]
            # self.decomp_by_range = ['decomp_{}'.format(col) for col in self.decomp_by_range]
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return data
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _clipping(self, data):  # , clipped_vals_data, columns, bin_cols):
        '''
        :param incident_data
        :type Pandas data frame with incident
        :param clipped_vals_data
        :type Pandas data frame with clipped values for every feature, clipped by training time
        :param columns
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            def f(val, head_vals, tail_vals):
                if val in head_vals:  # .lower()
                    return val
                elif val in tail_vals:  # .lower()
                    return 'gen_tail'
                else:
                    # self.logger.info('step: {}, col: {}, val: {}, not in clipped vals'.format(step, col, val))
                    return 'gen_change'

            for col in self.decomp_cols:
                try:
                    clipped_head_vals = literal_eval(self.clipped_vals_df.loc[0, col])
                    clipped_tail_vals = literal_eval(self.clipped_vals_df.loc[1, col])
                    if col in self.decomp_by_range:
                        data_vc = data[col].value_counts()
                        idx = [x for x in data_vc[data_vc != 0].index]
                        data['clipped_{}'.format(col)] = data[data[col].isin(idx)][col].apply(
                            lambda x: f(x, clipped_head_vals, clipped_tail_vals))
                    else:
                        data['clipped_{}'.format(col)] = data[col].astype(str).apply(
                            lambda x: f(x, clipped_head_vals, clipped_tail_vals))
                        data = data.drop(col, axis=1)
                except Exception as e:
                    self.logger.info('step: {} col: {} error: {}'.format(step, col, e))
                    data['clipped_{}'.format(col)] = 'gen_tail'
                    data = data.drop(col, axis=1)
            self.clipped_decomp_cols = [x for x in data.columns if x.startswith('clipped_decomp_')]
            for col in self.clipped_decomp_cols:
                change_val_shape = data[data[col] == 'gen_change'].shape[0]
                if change_val_shape > 0:
                    self.logger.info('step: {} col: {}, num_of_values: {}'.format(step, col, change_val_shape))
                    pop_value = data[col].value_counts().head(1).index.values[0]
                    if pop_value == 'gen_change':
                        try:
                            pop_value = data[col].value_counts().head(2).index.values[1]
                        except:
                            pop_value = 'missing'
                    data.loc[data[data[col] == 'gen_change'].index, col] = pop_value
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return data
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _encoding(self, df):
        '''
        :param df
        :type Pandas data frame with clipped features to encode
        :return Pandas data frame with encoded features
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            N, _ = df.shape
            X = np.zeros((N, self.dimensionality['dimensionality']))
            i = 0
            cols_lst = []
            for col, scaler in iteritems(self.scalers):
                X[:, i] = scaler.transform(df[col].values.reshape(-1, 1)).flatten()
                cols_lst.append(col)
                i += 1
            for col, ordinalEncoder in iteritems(self.ordinalEncoders):
                # self.logger.info('col: {}'.format(col))
                X[:, i] = ordinalEncoder.transform(df[col].values.reshape(-1, 1)).flatten()
                cols_lst.append(col)
                i += 1
            for col, labelEncoder in iteritems(self.labelEncoders):
                # print "transforming col:", col
                K = len(labelEncoder.classes_)
                cols_lst.extend(['{}_{}'.format(col, val) for val in labelEncoder.classes_])
                X[np.arange(N), labelEncoder.transform(df[col]) + i] = 1
                i += K
            encoded_df = pd.DataFrame(X, columns=cols_lst)
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return encoded_df
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _produce_new_features(self, X):
        '''
        :param client_metadata
        :type Pandas data frame with client_metadata
        :param values_to_predict
        :type Pandas data frame with all encoded features features to predict
        :return Pandas data frame with prediction and probas
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            X_select_best = X[self.select_best_features]
            pca_reduced_values = self.pca.transform(X[self.feat_for_pca])
            pca_reduced_values = self.pca_reduced_values_scaler.transform(pca_reduced_values)
            pca_cols = ['clipped_decomp_claim_pca_feat_{}'.format(x + 1) for x in range(self.pca.n_components)]
            pca_df = pd.DataFrame(data=pca_reduced_values, columns=pca_cols)

            num_df = X_select_best[self.chi2_features_for_poly]
            poly_array = self.poly_obj.transform(num_df)
            poly_df = pd.DataFrame(poly_array, index=num_df.index, columns=list(range(poly_array.shape[1])))
            poly_df.rename(columns={col: ('clipped_decomp_claim_pol_feat_{}'.format(col)) for col in poly_df.columns},
                           inplace=True)
            X_poly = pd.concat([X_select_best, poly_df], axis=1)

            # anomaly_scores = self.ocs.score_samples(num_df)
            anomaly_scores = self.ocs.score_samples(X_poly)
            X_anom_scor = X_poly.copy()
            X_anom_scor['clipped_decomp_claim_anomaly_score_feat'] = self.scaler_after_score.transform(
                anomaly_scores.reshape(-1, 1))

            X_for_predict = pd.concat([X_anom_scor, pca_df], axis=1)
            X_for_predict = X_for_predict[self.rel_feat_feature_importance]
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return X_for_predict
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _predict(self, client_metadata, values_to_predict, mode):
        '''
        :param client_metadata
        :type Pandas data frame with client_metadata
        :param values_to_predict
        :type Pandas data frame with all encoded features features to predict
        :return Pandas data frame with prediction and probas
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            probas_df = pd.DataFrame(self.xgb_model.predict_proba(values_to_predict),
                                     columns=['proba_0_low', 'proba_1_high', 'proba_2_medium'])
            probas_df['Quito_prediction_id'] = self.xgb_model.predict(values_to_predict)
            probas_df['Quito_prediction'] = probas_df['Quito_prediction_id'].apply(lambda x: self.prediction_types[x])
            # proba_df['prediction_date'] = dt.now()#.strftime("%Y/%m/%d_%I:%M%p")
            probas_df.reset_index(drop=True, inplace=True)
            self.probas_df = probas_df
            self.client_metadata = client_metadata
            probas_df = pd.concat([client_metadata, probas_df], axis=1)
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
            return probas_df
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e

    def _save_data(self):
        '''
        :param data
        :type Pandas data frame with clipped features
        :return data with columns renamed to clipping
        '''
        frame = inspect.currentframe()
        step = 'class: {}, func: {}'.format(self.__class__.__name__, inspect.getframeinfo(frame).function)
        start_time = dt.now()
        self.logger.info('step: {} started. at: {}'.format(step, dt.now()))
        try:
            '''
            self.proba_df = self.proba_df[(~self.proba_df['Client'].isnull())&(~self.proba_df['Policy_Nr'].isnull())]
            self.proba_df['Client'] = self.proba_df['Client'].astype(str)
            self.proba_df['Policy_Nr'] = self.proba_df['Policy_Nr'].astype(int)
            self.proba_df['Lima_model_date'] = self.model_date
  
            proba_df_final = self.proba_df[['Policy_Nr','Client','Lima_prediction_id','Lima_model_date']]
            proba_df_final.drop_duplicates(inplace=True)
  
            proba_df_path = '{}/proba_df.csv'.format(self.base_dir)
            proba_df_final_path = '{}/proba_df_final.csv'.format(self.base_dir)
            self.proba_df.to_csv(proba_df_path,index=False)
            proba_df_final.to_csv(proba_df_final_path,index=False)
            '''
            client_df_path = '{}/client_df_for_real_time.csv'.format(self.base_dir)
            self.client_df.to_csv(client_df_path, index=False)
            time_running = (dt.now() - start_time).total_seconds()
            self.logger.info('step: {} time running: {} time finish: {}'.format(step, time_running, dt.now()))
        except Exception as e:
            self.logger.exception('Failed to: step: {}, Reason: {}'.format(step, str(e)))
            raise e