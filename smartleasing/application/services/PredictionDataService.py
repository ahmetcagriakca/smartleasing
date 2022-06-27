import functools
import os
import time

import pandas
from injector import inject
from pandas import DataFrame, notnull
from pdip.configuration.models.application import ApplicationConfig
from pdip.connection.database.base import DatabaseProvider
from pdip.dependency import IScoped
from pdip.dependency.container import DependencyContainer
from pdip.logging.loggers.database import SqlLogger
from pdip.processing import ProcessManager

from smartleasing.domain.configs.BuildingConfig import BuildingConfig
from smartleasing.domain.configs.OneMapDatabaseConfig import OneMapDatabaseConfig
from smartleasing.domain.configs.PredictionDataConfig import PredictionDataConfig


class PredictionDataService(IScoped):
    @inject
    def __init__(self,
                 application_config: ApplicationConfig,
                 logger: SqlLogger,
                 one_map_database_config: OneMapDatabaseConfig,
                 database_provider: DatabaseProvider,
                 building_config: BuildingConfig,
                 prediction_data_config: PredictionDataConfig,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prediction_data_config = prediction_data_config
        self.building_config = building_config
        self.application_config = application_config
        self.logger = logger
        self.database_provider = database_provider
        self.one_map_database_config = one_map_database_config
        self.one_map_database_context = self.database_provider.get_context_by_config(
            config=self.one_map_database_config)

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

    def truncate_prediction_data_table(self):
        truncate_building_data_script = f'DELETE FROM {self.prediction_data_config.prediction_data_table_name}'
        self.one_map_database_context.execute(truncate_building_data_script)

    def read_file(self, file_name) -> DataFrame:
        folder = os.path.join(self.application_config.root_directory, 'files', 'predictions')
        path = os.path.join(folder, f'{file_name}.csv')
        df = pandas.read_csv(path, sep=';', decimal=',')
        df = df.replace({pandas.NaT: None})
        return df

    def get_cell_data(self, cities):
        cities_str = '\'' + '\',\''.join(cities) + '\''
        path = os.path.join(self.application_config.root_directory, 'files', 'data', 'kira_bedel.csv')
        df = pandas.read_csv(path, sep=';', decimal=',')
        df = df.query(f"CITY.str.contains({cities_str})")
        df = df.reset_index(drop=True)
        return df

    def delete_prediction_table_data(self, schema_and_table, delete_query):
        self.one_map_database_context.execute(
            f'delete from {schema_and_table} where {delete_query}')

    def write_all_predictions(self, default_message, df_cell, file_name, schema_and_table, delete_query, geoloc_query):
        try:
            if schema_and_table is None or schema_and_table == '':
                schema_and_table = self.prediction_data_config.prediction_data_table_name
            self.logger.info(f"{default_message}write to db started")
            self.time_calculate(f"{default_message}01-delete_prediction_table_data")(
                self.delete_prediction_table_data)(schema_and_table=schema_and_table, delete_query=delete_query)
            data = self.time_calculate(f"{default_message}02-prepare_data")(
                self.prepare_data)(df_cell=df_cell, file_name=file_name)
            self.time_calculate(f"{default_message}03-insert_to_db_with_paging")(
                self.insert_to_db_with_paging)(default_message=default_message, data=data,
                                               table_and_schema=schema_and_table,
                                               page=0,
                                               limit=100000)
            self.time_calculate(f"{default_message}04-update_geoloc_on_prediction_table")(
                self.update_geoloc_on_prediction_table)(geoloc_query=geoloc_query)
            self.logger.info(f"{default_message} write to db finished")
            del data
        except Exception as ex:
            self.logger.exception(ex, f'{default_message} Write all prediction data getting error')

    def prepare_data(self, df_cell, file_name):
        df = self.read_file(file_name=file_name)
        merged_data = pandas.merge(df, df_cell, how="inner", left_on="SERVICED_BS_SITE_NO", right_on="SITE_NO")
        merged_data.drop(['LONGITUDE_y', 'LATITUDE_y'], errors='ignore')
        merged_data = merged_data.rename(
            columns={'SIRANO': 'ORDER_NO', 'TIP_ADI': 'TYPE_NAME', 'KAT': 'FLOOR', 'YUKSEKLIK': 'HEIGHT',
                     'ALANM2': 'AREAM2', 'LONGITUDE_x': 'BUILDING_LONGITUDE', 'LATITUDE_x': 'BUILDING_LATITUDE',
                     'BASARID': 'BASAR_ID', 'MAHALLE_ADI': 'NEIGHBORHOOD_NAME', 'IL_ADI': 'CITY_NAME',
                     'ILCE_ADI': 'COUNTY_NAME', 'MI_STYLE': 'MI_STYLE', 'MI_PRINX': 'MI_PRINX',
                     'TOTAL_ALAN': 'TOTAL_AREA', 'REGION_x': 'BUILDING_REGION',
                     'KIRA_TAHMIN': 'RENT_PREDICTION', 'REGION_y': 'REGION', 'max_year': 'MAX_YEAR'})
        data = merged_data[['ORDER_NO', 'TYPE_NAME', 'FLOOR', 'HEIGHT', 'AREAM2',
                            'BUILDING_LONGITUDE', 'BUILDING_LATITUDE', 'BASAR_ID',
                            'NEIGHBORHOOD_NAME', 'CITY_NAME', 'COUNTY_NAME', 'MI_STYLE', 'MI_PRINX',
                            'TOTAL_AREA', 'BS_ON_BUILDING', 'SERVICED_BASE_STATION',
                            'SERVICED_BS_SITE_NO', 'SERVICED_BS_LATITUDE', 'SERVICED_BS_LONGITUDE',
                            'SERVICED_BS_YEARLY_TL', 'SERVICED_BS_DISTANCE', 'R150',
                            'SCORE', 'RENT_PREDICTION', 'SITE_NO', 'SITE_CODE',
                            'NEIGHGBOUR_CALCULATION_AMOUNT', 'REGION', 'SITE_STATE', 'SITE_NAME',
                            'SITE_TYPE', 'OLD_CONTRACT_ID', 'CONTRACT_NO', 'CONTRACT_TYPE',
                            'CONTRACT_STATUS', 'CONTRACT_SIGN_DATE', 'CONTRACT_BEGIN_DATE',
                            'CONTRACT_END_DATE', 'CONTRACT_AMOUNT', 'CONTRACT_CURRENCY',
                            'SITE_OWNER_MAIN_TYPE_NAME', 'SITE_OWNER_TYPE_NAME', 'MONTAGE_TYPE',
                            'MAIN_REGION_NAME', 'CITY', 'COUNTY', 'CONTRACT_PAYMENT_PERIOD',
                            'PAYMENT_TYPE', 'PAYMENT_RANGE', 'PAYMENT_BEGIN_YEAR',
                            'SUM_OF_STOPPAGE_TOTAL_AMOUNT', 'ERP_KUR', 'YEARLY_AMOUNT',
                            'CURRENCY_ID', 'MAX_YEAR']]
        return data

    def insert_to_db_with_paging(self, default_message, data, table_and_schema, page, limit):
        columns = data.columns
        column_indexers = [f':{i + 1}' for i, col in enumerate(columns)]
        column_names = '"' + '","'.join(columns) + '"'
        query = f'insert into {table_and_schema} ({column_names}) values({",".join(column_indexers)})'
        test = data.replace({pandas.NaT: None})
        test = test.where(notnull(test), None)
        new_data = test[columns].values.tolist()
        del test
        data_length = len(new_data)
        self.logger.debug(
            f"{default_message}1- Operation started. data_length :{data_length} page :{page} limit :{limit}")

        total_fragment_count = int(data_length / limit) + 1
        fragment_count = total_fragment_count - page
        self.one_map_database_context.connector.connect()
        try:
            for rec in range(fragment_count):
                processing_page = page + rec
                # preparing data
                start = processing_page * limit
                end = start + limit
                fragmented_data = new_data[start:end]
                result = self.one_map_database_context.execute_many(
                    query=query,
                    data=fragmented_data)
                self.logger.debug(
                    f'{default_message}{processing_page},{start}-{end} finished. {result} data writed to db')
            # finish operation
            remaining_data_count = data_length - (total_fragment_count * limit)
            # preparing data
            if remaining_data_count > 0:
                self.logger.debug(f'{default_message}{remaining_data_count}  remaining data founded')
                start = total_fragment_count * limit
                end = start + remaining_data_count
                fragmented_data = new_data[start:end]

                result = self.one_map_database_context.connector.execute_many(
                    query=query,
                    data=fragmented_data)
                self.logger.debug(
                    f'{default_message}{processing_page + 1},{start}-{end} finished. {result} data writed to db')
        finally:
            self.one_map_database_context.connector.disconnect()

    def truncate_base_station_table(self):
        truncate_building_data_script = f'delete from {self.one_map_database_config.base_station_table}'
        self.one_map_database_context.execute(truncate_building_data_script)

    def write_base_station_to_db(self, default_message, df_cell, file_name, schema_and_table, delete_query):
        try:

            path = os.path.join(self.application_config.root_directory, 'files', 'predictions', f'{file_name}.csv')
            df_all = pandas.read_csv(path, sep=';', decimal=',')
            df_all.drop(['LONGITUDE', 'LATITUDE', 'REGION'], axis=1, inplace=True, errors='ignore')
            df_result = df_all.query(f"BS_ON_BUILDING!='NOT_LOCATED'")
            del df_all

            merged_data = pandas.merge(df_result, df_cell, how="left", left_on="BS_ON_BUILDING",
                                       right_on="SITE_CODE")
            del df_result
            columns = df_cell.columns.tolist()
            columns.append('SCORE')
            columns.append('KIRA_TAHMIN')
            data = merged_data[columns].query(f"SCORE==SCORE")
            del df_cell
            del merged_data
            self.one_map_database_context.execute(
                f'delete from {schema_and_table} where {delete_query}')
            data.drop(['YEARLY_AMOUNT'], axis=1, inplace=True, errors='ignore')
            data = data.rename(
                columns={'KIRA_TAHMIN': 'RENT_PREDICTION', 'max_year': 'MAX_YEAR','YEARLY_AMOUNT_NOSTOPPAGE':'YEARLY_AMOUNT'})
            data = data[
                ["SITE_NO", "SITE_CODE", "NEIGHGBOUR_CALCULATION_AMOUNT", "REGION", "SITE_STATE", "SITE_NAME",
                 "SITE_TYPE", "LATITUDE", "LONGITUDE", "OLD_CONTRACT_ID", "CONTRACT_NO", "CONTRACT_TYPE",
                 "CONTRACT_STATUS", "CONTRACT_SIGN_DATE", "CONTRACT_BEGIN_DATE", "CONTRACT_END_DATE",
                 "CONTRACT_AMOUNT", "CONTRACT_CURRENCY", "SITE_OWNER_MAIN_TYPE_NAME", "SITE_OWNER_TYPE_NAME",
                 "MONTAGE_TYPE", "MAIN_REGION_NAME", "CITY", "COUNTY", "CONTRACT_PAYMENT_PERIOD",
                 "PAYMENT_TYPE", "PAYMENT_RANGE", "PAYMENT_BEGIN_YEAR", "SUM_OF_STOPPAGE_TOTAL_AMOUNT",
                 "ERP_KUR", "YEARLY_AMOUNT", "CURRENCY_ID", "MAX_YEAR", "SCORE", "RENT_PREDICTION"]]
            self.insert_to_db_with_paging(default_message=default_message, data=data,
                                          table_and_schema=schema_and_table, page=0,
                                          limit=100000)
            del data
        except Exception as ex:
            self.logger.exception(ex, f'{default_message} Write Base Station Operation getting error')

    def update_geoloc_on_prediction_table(self, geoloc_query):
        create_building_detail_data_script = f'''
MERGE INTO {self.prediction_data_config.prediction_data_table_name} d
  USING (
  SELECT
  tt.mi_prinx, tt.geoloc
FROM
  {self.building_config.base_table_name} tt
  where {geoloc_query}
  ) i
--#161452
  ON
(d.mi_prinx = i.mi_prinx)
WHEN  MATCHED THEN
    UPDATE SET d.geoloc =i.geoloc
        '''
        self.one_map_database_context.execute(create_building_detail_data_script)

    def truncate_prediction_sdo_data_table(self):
        truncate_building_data_script = f'delete from {self.prediction_data_config.prediction_sdo_data_table_name}'
        self.one_map_database_context.execute(truncate_building_data_script)

    def get_prediction_data_table_count(self):
        count = self.one_map_database_context.get_table_count(
            query=f"SELECT * FROM {self.prediction_data_config.prediction_data_table_name}")
        return count

    def insert_sdo_table_data(self):
        count = self.get_prediction_data_table_count()
        process_count = int(self.prediction_data_config.prediction_data_process_count)
        limit = (int(count / process_count) + 10000)
        data_kwargs = {
            "limit": limit
        }
        process_manager = ProcessManager()
        try:
            process_manager.start_processes(
                process_count=process_count,
                target_method=self.start_insert_sdo_table_data_parallel,
                kwargs=data_kwargs)
            results = process_manager.get_results()

            for result in results:
                assert result.State == 3
        finally:
            if process_manager is not None:
                del process_manager

    @classmethod
    def start_insert_sdo_table_data_parallel(cls, sub_process_id, limit):
        return DependencyContainer \
            .Instance \
            .get(PredictionDataService) \
            .insert_sdo_table_data_parallel(sub_process_id, limit)

    def insert_sdo_table_data_parallel(self, page, limit):
        v_start = (page - 1) * limit
        v_end = page * limit
        insert_prediction_sdo_data_table_script = f'''
declare
  v_start number := {v_start};
  v_end   number := {v_end};
  v_count number;
  cursor cur is
    with temp_integration as
     (select ordered_query.*, row_number() over(order by mi_prinx) row_number from (
     select * from {self.prediction_data_config.prediction_data_table_name}
     ) ordered_query)
    select *
      from temp_integration
     where row_number >= v_start
       and row_number < v_end;

  --select tt.sirano,tt.tip_adi,tt.kat,tt.yukseklik,tt.alanm2,tt.longitude,tt.latitude, tt.basarid,tt.mahalle_adi,tt.il_adi,tt.ilce_adi,tt.mi_style,tt.mi_prinx from {self.one_map_database_config.building_detail} tt;
begin
  v_count := 0;
  for rec in cur
  loop
    v_count := v_count + 1;
  
    insert into {self.prediction_data_config.prediction_sdo_data_table_name}
    values
      (rec.order_no, rec.type_name, rec.floor, rec.height, rec.aream2, rec.building_longitude, rec.building_latitude, rec.basar_id,
       rec.neighborhood_name, rec.city_name, rec.county_name, rec.mi_style, rec.total_area, rec.bs_on_building, rec.serviced_base_station,
       rec.serviced_bs_site_no, rec.serviced_bs_latitude, rec.serviced_bs_longitude, rec.serviced_bs_yearly_tl, rec.serviced_bs_distance, rec.r150,
       rec.score, rec.rent_prediction, rec.site_no, rec.site_code, rec.neighgbour_calculation_amount, rec.region, rec.site_state, rec.site_name,
       rec.site_type, rec.old_contract_id, rec.contract_no, rec.contract_type, rec.contract_status, rec.contract_sign_date, rec.contract_begin_date,
       rec.contract_end_date, rec.contract_amount, rec.contract_currency, rec.site_owner_main_type_name, rec.site_owner_type_name, rec.montage_type,
       rec.main_region_name, rec.city, rec.county, rec.contract_payment_period, rec.payment_type, rec.payment_range, rec.payment_begin_year,
       rec.sum_of_stoppage_total_amount, rec.erp_kur, rec.yearly_amount, rec.currency_id, rec.max_year, rec.mi_prinx, rec.geoloc);
    if v_count > 20000 then
      commit;
      v_count := 0;
    end if;
  end loop;
  commit;
end;
            '''
        self.logger.debug(f'query:{insert_prediction_sdo_data_table_script}')
        self.logger.debug(f'{page} started with {limit}.')

        row_count = self.one_map_database_context.execute(insert_prediction_sdo_data_table_script)
        self.logger.debug(f'{page} finisihed with {limit}. Affected Row Count : {row_count}')
