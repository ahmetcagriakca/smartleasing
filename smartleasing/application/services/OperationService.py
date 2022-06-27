import functools
import gc
import os
import sys
import time

import pandas as pd
from injector import inject
from pdip.configuration.models.application import ApplicationConfig
from pdip.dependency import IScoped
from pdip.io import FolderManager
from pdip.logging.loggers.console import ConsoleLogger
from pdip.logging.loggers.database import SqlLogger

from smartleasing.application.StartOperation.StartOperationCommandRequest import ProcessInfo, ProcessInfoDetail
from smartleasing.application.services.AiService import AiService
from smartleasing.application.services.DataService import DataService
from smartleasing.application.services.PredictionDataService import PredictionDataService
from smartleasing.domain.utils import Utils


class OperationService(IScoped):
    @inject
    def __init__(self,
                 logger: SqlLogger,
                 data_service: DataService,
                 ai_service: AiService,
                 folder_manager: FolderManager,
                 application_config: ApplicationConfig,
                 prediction_data_service: PredictionDataService,
                 ):
        self.prediction_data_service = prediction_data_service
        self.application_config = application_config
        self.folder_manager = folder_manager
        self.ai_service = ai_service
        self.data_service = data_service
        self.logger = logger
        self.count = 0
        self.console_logger = ConsoleLogger()
        self.df_building_calc = None

    def time_calculate(self, message=None):
        def decorate(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                start = time.time()
                try:
                    return func(*args, **kwargs)
                finally:
                    end = time.time()
                    elapsed_time = end - start
                    self.logger.info(f'{message}, elapsed time: {elapsed_time}')

            return wrapper

        return decorate

    def obj_size_fmt(self, num):
        if num < 10 ** 3:
            return "{:.2f}{}".format(num, "B")
        elif ((num >= 10 ** 3) & (num < 10 ** 6)):
            return "{:.2f}{}".format(num / (1.024 * 10 ** 3), "KB")
        elif ((num >= 10 ** 6) & (num < 10 ** 9)):
            return "{:.2f}{}".format(num / (1.024 * 10 ** 6), "MB")
        else:
            return "{:.2f}{}".format(num / (1.024 * 10 ** 9), "GB")

    def memory_usage(self):
        memory_usage_by_variable = pd.DataFrame({k: sys.getsizeof(v) \
                                                 for (k, v) in globals().items()}, index=['Size'])
        memory_usage_by_variable = memory_usage_by_variable.T
        memory_usage_by_variable = memory_usage_by_variable \
            .sort_values(by='Size', ascending=False).head(10)

        memory_usage_by_variable['Size'] = memory_usage_by_variable['Size'].apply(lambda x: self.obj_size_fmt(x))
        self.logger.info(f'{str(memory_usage_by_variable.values.tolist())}')
        return memory_usage_by_variable

    # def calculate_total(self, x):
    #     self.count += 1
    #     start = time.time()
    #     total = self.df_building_calc.query(
    #         f'LATITUDE<({x["LATITUDE_MAX"]}) and LATITUDE>({x["LATITUDE_MIN"]}) and LONGITUDE< ({x["LONGITUDE_MAX"]}) and LONGITUDE> ({x["LONGITUDE_MIN"]})')[
    #         'ALANM2'].sum()
    #     end = time.time()
    #     elapsed_time = end - start
    #     self.console_logger.debug(f'{self.count}-{x["MI_PRINX"]},elapsed time: {elapsed_time}')
    #
    #     return total

    def calculate_total(self, x):
        self.count += 1
        start = time.time()
        total = self.df_building_calc.query(
            f'LATITUDE<({x["LATITUDE_MAX"]}) and LATITUDE>({x["LATITUDE_MIN"]}) and LONGITUDE< ({x["LONGITUDE_MAX"]}) and LONGITUDE> ({x["LONGITUDE_MIN"]})'
        )['ALANM2'].sum()
        end = time.time()
        elapsed_time = end - start
        self.console_logger.debug(f'{self.count}-{x["MI_PRINX"]},elapsed time: {elapsed_time}')

        return total

    def calculate_total_area(self, df_building):
        df_building_calc = df_building[['MI_PRINX', 'LATITUDE', 'LONGITUDE', 'ALANM2']].copy()
        df_building_calc['LATITUDE_MIN'] = df_building_calc['LATITUDE'] - 0.001348982409
        df_building_calc['LATITUDE_MAX'] = df_building_calc['LATITUDE'] + 0.001348982409
        df_building_calc['LONGITUDE_MIN'] = df_building_calc['LONGITUDE'] - 0.001786544172
        df_building_calc['LONGITUDE_MAX'] = df_building_calc['LONGITUDE'] + 0.001786544172
        calculated_values = df_building_calc.apply(lambda x: df_building_calc.query(
            f'LATITUDE<({x["LATITUDE_MAX"]}) and LATITUDE>({x["LATITUDE_MIN"]}) and LONGITUDE< ({x["LONGITUDE_MAX"]}) and LONGITUDE> ({x["LONGITUDE_MIN"]})'
        )['ALANM2'].sum(), axis=1)
        del df_building_calc
        return calculated_values

    def do_calculation(self, default_message, process_info: ProcessInfo, detail: ProcessInfoDetail, unicode_county_name,
                       cluster_count,
                       df_all, df_cell):
        region = process_info.Region
        len_df_cell = len(df_cell)
        unicode_city_name = Utils.translate(detail.City)
        df_all = self.time_calculate(f"{default_message}02-find_base_stations")(
            self.ai_service.find_base_stations)(
            df_building=df_all, df_cell=df_cell)
        df_all = self.time_calculate(f"{default_message}03-find_nearest_base_station")(
            self.ai_service.find_nearest_base_station)(df_building=df_all,
                                                       df_cell=df_cell)
        del df_cell
        df_all = self.time_calculate(f"{default_message}04-find_base_station_distances")(
            self.ai_service.find_base_station_distances)(df_all=df_all)
        df_all = self.time_calculate(
            f"{default_message}05-find_region_with_kmeans with {cluster_count} cluster")(
            self.ai_service.find_region_with_kmeans)(
            cluster_count=cluster_count,
            df_all=df_all
        )
        file_name = f'{process_info.Name}-{region}-{unicode_city_name}-{unicode_county_name}'
        if process_info.SaveFigures:
            self.time_calculate(f"{default_message}06-save_region_figures")(
                self.ai_service.save_region_figures)(
                file_name=file_name,
                df_all=df_all
            )

        df_all = self.ai_service.prepare_dataframe(df_all=df_all)
        enriched_data = self.time_calculate(f"{default_message}07-data_enrichment")(
            self.ai_service.data_enrichment)(
            df_all=df_all,
            enrich_data=detail.EnrichData,
            process_info=process_info
        )

        if process_info.SaveFigures:
            self.time_calculate(f"{default_message}08-save_correlation_figures")(
                self.ai_service.save_correlation_figures)(
                default_message=default_message + '08',
                df_corr=enriched_data,
                target_name='SERVICED_BS_YEARLY_TL', file_name=file_name)
        df = self.time_calculate(f"{default_message}09-calculate_prediction")(
            self.ai_service.calculate_prediction)(
            default_message=default_message + '09',
            df=df_all,
            enriched_data=enriched_data)
        del enriched_data
        del df_all
        return df

    def calculate_for_city(self, process_id, process_info, detail, df_building_all, df_cell_all):
        start = time.time()
        len_df_cell_all = len(df_cell_all)
        region = process_info.Region
        city = detail.City
        unicode_city_name = Utils.translate(city)
        unicode_region_name = Utils.translate(region)
        county = 'All'

        total_len_all = 0
        total_len_building_cell = 0
        total_len_cell = 0
        total_overrate_count = 0
        try:
            default_message = f'{process_id}-region:{unicode_region_name}-city:{unicode_city_name}-county:{county},operation:'
            df_cell = df_cell_all.query(f"CITY.str.contains('{city}')")
            len_df_cell = len(df_cell)
            if len_df_cell <= 0:
                self.logger.info(f"{default_message} City doesn't have Base Station")
            df_cell = df_cell.reset_index(drop=True)
            df_all = df_building_all.reset_index(drop=True)
            len_df_all = len(df_all)
            self.logger.info(
                f"{default_message}01-basestation count:{len_df_cell}, all data count:{len_df_all}")
            cluster_count = round(df_all["TOTAL_ALAN"].sum() / (len_df_cell * len(df_all)))
            if cluster_count > len_df_cell:
                cluster_count = int(len_df_cell / detail.ClusterRate)
            df = self.do_calculation(default_message=default_message, process_info=process_info,
                                     detail=detail,
                                     unicode_county_name=county,
                                     cluster_count=cluster_count,
                                     df_all=df_all, df_cell=df_cell)
            overrate_count = \
                df.query("(RATE<0.8 or RATE>1.2) and BS_ON_BUILDING!='NOT_LOCATED'")[
                    ['RATE']].count().values.tolist()[
                    0]

            file_name = f'{process_info.Name}-{unicode_region_name}-{unicode_city_name}-All'
            self.time_calculate(f"{default_message}10-write_prediction_to_csv")(
                self.data_service.write_prediction_to_csv)(
                df=df,
                file_name=file_name,
                recreate=True)
            len_building_cell = df[df['BS_ON_BUILDING'] != "NOT_LOCATED"]['BS_ON_BUILDING'].count()
            del df
            total_len_all += len_df_all
            total_len_building_cell += len_building_cell
            total_len_cell += len_df_cell
            total_overrate_count += overrate_count
            self.logger.info(
                f'{default_message}11-all data count:{len_df_all}, basestation count:{len_df_cell}, building basestation count:{len_building_cell}, cluster_count:{cluster_count}, overrate_count:{overrate_count}')
        except Exception as ex:
            raise

        end = time.time()
        elapsed_time = end - start
        self.logger.info(
            f'{process_id}-region:{unicode_region_name}-city:{unicode_city_name}-Operation Completed -elapsed time: {elapsed_time} - all data count:{total_len_all},All basestation count:{len_df_cell_all},  basestation count:{total_len_cell}, building basestation count:{total_len_building_cell}, overrate_count:{total_overrate_count}')

    def calculate_for_county(self, process_id, process_info, detail, df_building_all, df_cell_all):
        start = time.time()
        len_df_cell_all = len(df_cell_all)
        region = process_info.Region
        city = detail.City
        unicode_city_name = Utils.translate(city)
        unicode_region_name = Utils.translate(region)
        groups = df_building_all.groupby('ILCE_ADI')
        del df_building_all
        # result_df = DataFrame()
        # input_preprocess = DataFrame()
        # result_preprocess = DataFrame()
        county_count = 0
        first_record = True
        total_len_all = 0
        total_len_building_cell = 0
        total_len_cell = 0
        total_overrate_count = 0
        for county, frame in groups:
            try:
                if detail.Counties is not None and len(detail.Counties) > 0 and county not in detail.Counties:
                    continue
                len_building_cell = 0
                if county_count >= 1:
                    first_record = False
                unicode_county_name = Utils.translate(county)
                default_message = f'{process_id}-region:{unicode_region_name}-city:{unicode_city_name},county:{unicode_county_name},operation:'
                df_cell = df_cell_all.query(f"CITY.str.contains('{city}') and COUNTY.str.contains('{county}')")
                len_df_cell = len(df_cell)
                if len_df_cell <= 0:
                    self.logger.info(f"{default_message} County doesn't have Base Station")
                    continue
                df_cell = df_cell.reset_index(drop=True)
                df_all = frame.reset_index(drop=True)
                del frame
                len_df_all = len(df_all)
                self.logger.info(
                    f"{default_message}01-basestation count:{len_df_cell}, all data count:{len_df_all}")
                cluster_count = int(len_df_cell / detail.ClusterRate)
                if cluster_count < 5:
                    cluster_count = len_df_cell/2
                df = self.do_calculation(default_message=default_message, process_info=process_info,
                                         detail=detail,
                                         unicode_county_name=unicode_county_name,
                                         cluster_count=cluster_count,
                                         df_all=df_all, df_cell=df_cell)
                overrate_count = \
                    df.query("(RATE<0.8 or RATE>1.2) and BS_ON_BUILDING!='NOT_LOCATED'")[
                        ['RATE']].count().values.tolist()[
                        0]

                file_name = f'{process_info.Name}-{unicode_region_name}-{unicode_city_name}-All'
                self.time_calculate(f"{default_message}10-write_prediction_to_csv")(
                    self.data_service.write_prediction_to_csv)(
                    df=df,
                    file_name=file_name,
                    recreate=first_record)
                file_name = f'{process_info.Name}-{unicode_region_name}-{unicode_city_name}-{unicode_county_name}'
                self.time_calculate(f"{default_message}10-write_prediction_to_csv")(
                    self.data_service.write_prediction_to_csv)(
                    df=df,
                    file_name=file_name,
                    recreate=True)
                del df

                total_len_all += len_df_all
                total_len_building_cell += len_building_cell
                total_len_cell += len_df_cell
                total_overrate_count += overrate_count
                self.logger.info(
                    f'{default_message}11-all data count:{len_df_all}, basestation count:{len_df_cell}, building basestation count:{len_building_cell}, cluster_count:{cluster_count}, overrate_count:{overrate_count}')
                county_count += 1
            except Exception as ex:
                self.logger.exception(ex,
                                      f'CHECK THIS ERROR FOR COUNTY!!!! {process_id}-{unicode_region_name}-{city}-{county}-Operation getting error  ')
        del groups

        end = time.time()
        elapsed_time = end - start
        self.logger.info(
            f'{process_id}-region:{unicode_region_name}-city:{unicode_city_name},county_count:{county_count}-Operation Completed -elapsed time: {elapsed_time} - all data count:{total_len_all},All basestation count:{len_df_cell_all},  basestation count:{total_len_cell}, building basestation count:{total_len_building_cell}, overrate_count:{total_overrate_count}')

    def start(self, process_id, process_info: ProcessInfo, detail: ProcessInfoDetail):
        try:
            start = time.time()

            region = process_info.Region
            city = detail.City
            unicode_city_name = Utils.translate(city)
            unicode_region_name = Utils.translate(region)
            default_message = f'{process_id}-region:{unicode_region_name}-city:{unicode_city_name},operation:'
            folder = os.path.join(self.application_config.root_directory, 'files', 'temp')
            self.folder_manager.create_folder_if_not_exist(folder)

            df_cell_all = self.time_calculate(f"{default_message}1-get_cell_data")(self.data_service.get_cell_data)(
                city=city)
            df_building_all = self.time_calculate(f"{default_message}2-get_all_data")(self.data_service.get_all_data)(
                region=region, city=city)
            len_df_cell_all = len(df_cell_all)
            self.logger.info(
                f"{default_message}3-all building count:{len(df_building_all)}, all basestation count:{len_df_cell_all}")
            if 'TOTAL_ALAN' not in df_building_all:
                df_building_all['TOTAL_ALAN'] = self.time_calculate(f"{default_message}4-calculate_total_area")(
                    self.calculate_total_area)(
                    df_building=df_building_all)
                file_path = f'building-{unicode_region_name}-{unicode_city_name}.csv'
                data_folder = os.path.join(self.application_config.root_directory, 'files', 'data')
                path = os.path.join(data_folder, file_path)
                df_building_all.to_csv(path, index=False, sep=';', decimal=',')
            if detail.ClusterRate is None or detail.ClusterRate == 0:
                detail.ClusterRate = 3

            if detail.PredictForCity is not None and detail.PredictForCity:
                self.calculate_for_city(process_id=process_id, process_info=process_info, detail=detail,
                                        df_building_all=df_building_all, df_cell_all=df_cell_all)
            else:
                self.calculate_for_county(process_id=process_id, process_info=process_info, detail=detail,
                                          df_building_all=df_building_all, df_cell_all=df_cell_all)
            del df_cell_all
            del df_building_all

            end = time.time()
            elapsed_time = end - start
            self.logger.info(
                f'{process_id}-region:{unicode_region_name}-city:{unicode_city_name}-Prediction finished. elapsed time: {elapsed_time} ')

            self.memory_usage()
            gc.collect()
            self.memory_usage()
            # return df
        except Exception as ex:
            self.logger.exception(ex,
                                  f'CHECK THIS ERROR FOR CITY!!!! {process_id}-{unicode_region_name}-{city}-Operation getting error')
