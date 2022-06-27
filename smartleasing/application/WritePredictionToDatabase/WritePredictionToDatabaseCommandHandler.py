import functools
import os
import time

import pandas
from injector import inject
from pdip.configuration.models.application import ApplicationConfig
from pdip.cqrs import Dispatcher
from pdip.cqrs import ICommandHandler
from pdip.logging.loggers.database import SqlLogger

from smartleasing.application.WritePredictionToDatabase.WritePredictionToDatabaseCommand import \
    WritePredictionToDatabaseCommand
from smartleasing.application.services.PredictionDataService import PredictionDataService
from smartleasing.domain.utils import Utils


class WritePredictionToDatabaseCommandHandler(ICommandHandler[WritePredictionToDatabaseCommand]):
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

    def handle(self, command: WritePredictionToDatabaseCommand):
        start = time.time()
        self.logger.debug(f"Write Prediction to database operation started")

        if command.request.TruncateTable:
            self.time_calculate(f"1-truncate_prediction_data_table")(
                self.prediction_data_service.truncate_prediction_data_table)()

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
                        geoloc_query = f"IL_ADI = '{city_info.City}' and ILCE_ADI='{county}'"
                        self.time_calculate(f"{default_message}2-write_all_predictions")(
                            self.prediction_data_service.write_all_predictions)(
                            default_message=default_message,
                            df_cell=df_cell,
                            file_name=file_name,
                            schema_and_table=command.request.SchemaAndTable,
                            delete_query=delete_query,
                            geoloc_query=geoloc_query)

                else:
                    default_message = f'name:{unicode_name},region:{unicode_region_name},city:{unicode_city_name},county:All,operation:'
                    file_name = f'{unicode_name}-{unicode_region_name}-{unicode_city_name}-All'
                    delete_query = f"CITY='{city_info.City}'"
                    geoloc_query = f"IL_ADI = '{city_info.City}'"
                    df_cell = df.query(f"CITY.str.contains('{city_info.City}')")
                    self.time_calculate(f"{default_message}2-write_all_predictions")(
                        self.prediction_data_service.write_all_predictions)(
                        default_message=default_message,
                        df_cell=df_cell,
                        file_name=file_name,
                        schema_and_table=command.request.SchemaAndTable,
                        delete_query=delete_query,
                        geoloc_query=geoloc_query)

        end = time.time()
        elapsed_time = end - start
        self.logger.debug(f"Write Prediction to database operation finished - elapsed time: {elapsed_time}")
