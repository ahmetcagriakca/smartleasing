import logging
import os
import warnings

import seaborn as  sns

from smartleasing.application.services.EnrichmentProcessService import EnrichmentProcessService
from smartleasing.domain.process.ProcessInfo import ProcessInfo

matplotlib_logger = logging.getLogger(name="matplotlib")
matplotlib_logger.setLevel(level=logging.WARNING)

import matplotlib.pyplot as plt
import numpy as np
import pandas
import sklearn
import xgboost as xgb
from geopy.distance import geodesic
from injector import inject
from pandas import DataFrame
from pdip.configuration.models.application import ApplicationConfig
from pdip.dependency import IScoped
from pdip.io import FolderManager
from pdip.logging.loggers.database import SqlLogger
from sklearn.cluster import KMeans
from sklearn.metrics import r2_score, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.neighbors import KNeighborsClassifier
from sklearn.preprocessing import LabelEncoder, MinMaxScaler

from smartleasing.domain.configs.AiConfig import AiConfig


class AiService(IScoped):
    @inject
    def __init__(self,
                 application_config: ApplicationConfig,
                 folder_manager: FolderManager,
                 ai_config: AiConfig,
                 logger: SqlLogger,
                 enrichment_process_service: EnrichmentProcessService,
                 ):
        self.enrichment_process_service = enrichment_process_service
        self.ai_config = ai_config
        self.application_config = application_config
        self.folder_manager = folder_manager
        self.logger = logger

    def find_base_stations(self, df_building: DataFrame, df_cell: DataFrame):
        # Base station building calculation
        knn_all = KNeighborsClassifier(n_neighbors=1, n_jobs=-1)
        training_dataset_x = df_building[['LONGITUDE', 'LATITUDE']].values
        training_dataset_y = [i for i in range(len(training_dataset_x))]
        knn_all.fit(training_dataset_x, training_dataset_y)
        pred_list = knn_all.predict(df_cell[['LONGITUDE', 'LATITUDE']])
        df_building['BS_ON_BUILDING'] = "NOT_LOCATED"
        df_building.at[list(pred_list), 'BS_ON_BUILDING'] = list(df_cell.SITE_CODE)
        df_building.at[list(pred_list), 'SERVICED_BASE_STATION'] = list(df_cell.SITE_CODE)
        df_building.at[list(pred_list), 'SERVICED_BS_SITE_NO'] = list(df_cell.SITE_NO)
        df_building.at[list(pred_list), 'SERVICED_BS_LATITUDE'] = list(df_cell.LATITUDE)
        df_building.at[list(pred_list), 'SERVICED_BS_LONGITUDE'] = list(df_cell.LONGITUDE)
        df_building.at[list(pred_list), 'SERVICED_BS_YEARLY_TL'] = list(df_cell.YEARLY_AMOUNT_NOSTOPPAGE)
        return df_building

    def find_nearest_base_station(self, df_building: DataFrame, df_cell: DataFrame):
        knn_cell = KNeighborsClassifier(n_neighbors=1, n_jobs=-1)
        test = df_building[df_building['BS_ON_BUILDING'] != "NOT_LOCATED"]
        training_dataset_x = test[['LATITUDE', 'LONGITUDE']].values
        training_dataset_y = [i for i in range(len(training_dataset_x))]
        knn_cell.fit(training_dataset_x, training_dataset_y)
        sklearn.neighbors.DistanceMetric.get_metric('haversine')
        pred_list = knn_cell.predict(df_building[['LATITUDE', 'LONGITUDE']])
        df_building['SERVICED_BASE_STATION'] = [test.SERVICED_BASE_STATION.iloc[item] for item in pred_list]
        df_building['SERVICED_BS_SITE_NO'] = [test.SERVICED_BS_SITE_NO.iloc[item] for item in pred_list]
        df_building['SERVICED_BS_LATITUDE'] = [test.SERVICED_BS_LATITUDE.iloc[item] for item in pred_list]
        df_building['SERVICED_BS_LONGITUDE'] = [test.SERVICED_BS_LONGITUDE.iloc[item] for item in pred_list]
        df_building['SERVICED_BS_YEARLY_TL'] = [test.SERVICED_BS_YEARLY_TL.iloc[item] for item in pred_list]
        return df_building

    def find_base_station_distances(self, df_all: DataFrame):
        # Base station distance calculation
        locations = df_all[['LATITUDE', 'LONGITUDE', 'SERVICED_BS_LATITUDE', 'SERVICED_BS_LONGITUDE']].values
        df_all['SERVICED_BS_DISTANCE'] = [geodesic((location[0], location[1]), (location[2], location[3])).m for
                                          location in
                                          locations]

        df_all['SERVICED_BS_DISTANCE'] = df_all.SERVICED_BS_DISTANCE.astype(np.int)
        return df_all

    def find_region_with_kmeans(self, cluster_count, df_all: DataFrame):
        # Kmeans calculation for cities
        warnings.filterwarnings("ignore")
        km = KMeans(n_clusters=int(cluster_count), n_jobs=-1).fit(df_all[['LONGITUDE', 'LATITUDE']].values);
        df_all['REGION'] = km.labels_
        return df_all

    def save_region_figures(self, file_name, df_all: DataFrame):
        fig, ax = plt.subplots(dpi=150)
        ax.scatter(df_all.LONGITUDE, df_all.LATITUDE, c=df_all.REGION, cmap='prism', s=[1])
        plt.xlabel('Longitude')
        plt.ylabel('Latitude')
        folder = os.path.join(self.application_config.root_directory, 'files', 'figures')
        self.folder_manager.create_folder_if_not_exist(folder)
        path = os.path.join(folder, f'{file_name}-regions.png')
        plt.savefig(path)
        plt.cla()
        plt.close()
        return

    def save_correlation_figures(self, default_message, df_corr, target_name, file_name):
        corr_matrix = df_corr.corr()
        most_correlated_features = corr_matrix[target_name].sort_values(ascending=False).drop(target_name)
        most_correlated_feature_names = most_correlated_features.index.values
        related_columns = most_correlated_feature_names[:30].tolist()
        self.logger.info(f'{default_message}.1-{related_columns}')
        k = 10  # number of variables for heatmap
        cols = corr_matrix.nlargest(k, 'SERVICED_BS_YEARLY_TL')['SERVICED_BS_YEARLY_TL'].index
        cm = np.corrcoef(df_corr[cols].values.T)
        sns.set(font_scale=1.25)
        fig, ax = plt.subplots(figsize=(25, 25))
        hm = sns.heatmap(cm, cbar=True, annot=True, square=True, fmt='.2f', annot_kws={'size': 10},
                         yticklabels=cols.values, xticklabels=cols.values)
        folder = os.path.join(self.application_config.root_directory, 'files', 'figures')
        self.folder_manager.create_folder_if_not_exist(folder)

        heatmap_path = os.path.join(folder, f'{file_name}-heatmap.png')
        plt.savefig(heatmap_path)
        plt.cla()
        plt.close()

    def normalization(self, X):
        scaler = MinMaxScaler()
        scaled_values = scaler.fit_transform(X)
        X.loc[:, :] = scaled_values
        return X

    def prepare_dataframe(self, df_all):
        df = df_all
        df['R150'] = df.TOTAL_ALAN
        indexlist = list()
        valuelist = list()
        for reg in df.REGION.unique():
            r150 = pandas.to_numeric(df[df.REGION == reg].R150)
            norm = r150 / r150.max()
            indexlist.extend(list(norm.index))
            valuelist.extend(list(norm.values))

        df.at[indexlist, 'SCORE'] = valuelist
        return df

    def data_enrichment(self, df_all: DataFrame, enrich_data, process_info: ProcessInfo) -> DataFrame:
        new_df_categorical = df_all.select_dtypes(include="O")
        new_df_cat = new_df_categorical.apply(LabelEncoder().fit_transform)
        df_new = df_all.select_dtypes(exclude='O').join(new_df_cat)
        if process_info.Columns is not None and len(process_info.Columns) > 0:
            columns = process_info.Columns
        else:
            columns = self.ai_config.columns

        prepared_data = df_new[columns]
        cor_matrix = prepared_data.corr().abs()
        upper_tri = cor_matrix.where(np.triu(np.ones(cor_matrix.shape), k=1).astype(np.bool))
        to_drop = [column for column in upper_tri.columns if any(upper_tri[column] > 0.95) and column.startswith('NEW_')]
        self.logger.info(to_drop)
        prepared_data = prepared_data.drop(to_drop, axis=1)
        if enrich_data:
            result_data = self.enrichment_process_service.start(prepared_data)
        else:
            result_data = prepared_data
        return result_data

    def calculate_prediction(self, default_message, df: DataFrame, enriched_data: DataFrame) -> DataFrame:
        columns = enriched_data.columns.tolist()
        columns.remove('SERVICED_BS_YEARLY_TL')
        columns.append('SERVICED_BS_YEARLY_TL')
        dataset = enriched_data[columns]
        del enriched_data
        dataset = dataset.dropna()
        X = dataset.iloc[:, 0:len(dataset.columns) - 1]
        y = dataset.iloc[:, -1]

        trainx, testx, trainy, testy = train_test_split(X, y, test_size=0.33, random_state=33)

        bst = xgb.XGBRegressor()
        bst.fit(trainx, trainy)
        preds = bst.predict(testx)
        r2 = r2_score(testy, preds)

        if self.application_config.environment == 'DEVELOPMENT':
            predictions = [round(value) for value in preds]
            accuracy = accuracy_score(testy, predictions)
            self.logger.info(f"{default_message}.1 Accuracy: %.2f%%" % (accuracy * 100.0))
        self.logger.info(f"{default_message}.2 r2:{r2}")

        whole_input = dataset[columns[:-1]]
        whole_result = bst.predict(whole_input)

        df['KIRA_TAHMIN'] = whole_result

        df['KIRA_TAHMIN'] = pandas.to_numeric(df['KIRA_TAHMIN'], errors='coerce')
        df["RATE"] = df['KIRA_TAHMIN'] / df['SERVICED_BS_YEARLY_TL']
        df["DEVIATION"] = df["RATE"] - 1
        return df
