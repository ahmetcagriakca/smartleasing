import functools
import os
import time

import pandas
from injector import inject
from pdip.configuration.models.application import ApplicationConfig
from pdip.cqrs import Dispatcher
from pdip.cqrs import ICommandHandler
from pdip.logging.loggers.database import SqlLogger

from smartleasing.application.WriteBaseStationToDatabase.WriteBaseStationToDatabaseCommand import \
    WriteBaseStationToDatabaseCommand
from smartleasing.application.services.PredictionDataService import PredictionDataService
from smartleasing.domain.utils import Utils


class WriteBaseStationToDatabaseCommandHandler(ICommandHandler[WriteBaseStationToDatabaseCommand]):
    @inject
    def __init__(self,
                 dispatcher: Dispatcher,
                 prediction_data_service: PredictionDataService,
                 logger: SqlLogger,
                 application_config: ApplicationConfig,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.application_config = application_config
        self.prediction_data_service = prediction_data_service
        self.dispatcher = dispatcher
        self.logger = logger

    def time_calculate(self, message=None):
        def decorate(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                import time
                start = time.time()
                try:
                    return func(*args, **kwargs)
                finally:
                    end = time.time()
                    elapsed_time = end - start
                    self.logger.debug(f'{message}, elapsed time: {elapsed_time}')

            return wrapper

        return decorate

    def handle(self, command: WriteBaseStationToDatabaseCommand):
        start = time.time()
        self.logger.debug(f"Write Base Station to database operation started")

        if command.request.TruncateTable:
            self.time_calculate(f"1-truncate_base_station_table")(
                self.prediction_data_service.truncate_base_station_table)()

        path = os.path.join(self.application_config.root_directory, 'files', 'data', 'kira_bedel.csv')
        df = pandas.read_csv(path, sep=';', decimal=',')
        df = df.where(pandas.notnull(df), None)

        for region_info in command.request.Regions:
            for city_info in region_info.Cities:
                unicode_name = Utils.translate(region_info.Name)
                unicode_region_name = Utils.translate(region_info.Region)
                unicode_city_name = Utils.translate(city_info.City)
                if city_info.Counties is not None and len(city_info.Counties) > 0:

                    for county in city_info.Counties:
                        unicode_county_name = Utils.translate(county)
                        default_message = f'name:{unicode_name},region:{unicode_region_name},city:{unicode_city_name},county:{unicode_county_name},operation:'
                        df_cell = df.query(f"CITY.str.contains('{city_info.City}') and COUNTY.str.contains('{county}')")
                        df_cell = df_cell.reset_index(drop=True)
                        file_name = f'{unicode_name}-{unicode_region_name}-{unicode_city_name}-{unicode_county_name}'
                        delete_query = f"CITY='{city_info.City}' and COUNTY='{county}'"
                        self.time_calculate(f"{default_message}2-write_base_station_to_db")(
                            self.prediction_data_service.write_base_station_to_db)(
                            default_message=default_message,
                            df_cell=df_cell,
                            file_name=file_name,
                            schema_and_table=command.request.SchemaAndTable,
                            delete_query=delete_query)

                else:
                    default_message = f'name:{unicode_name},region:{unicode_region_name},city:{unicode_city_name},county:All,operation:'
                    file_name = f'{unicode_name}-{unicode_region_name}-{unicode_city_name}-All'
                    df_cell = df.query(f"CITY.str.contains('{city_info.City}')")
                    delete_query = f"CITY='{city_info.City}'"
                    self.time_calculate(f"{default_message}2-write_base_station_to_db")(
                        self.prediction_data_service.write_base_station_to_db)(
                        default_message=default_message,
                        df_cell=df_cell,
                        file_name=file_name,
                        schema_and_table=command.request.SchemaAndTable,
                        delete_query=delete_query)

        end = time.time()
        elapsed_time = end - start
        # self.logger.info(f'Operation Completed for region:{command.request.Region}')
        # self.logger.info(f'{command.request.Region}-elapsed time: {elapsed_time}')
        self.logger.debug(f"Write Base Station to database operation finished-elapsed time: {elapsed_time}")
