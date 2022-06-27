import os

import pandas
from injector import inject
from pdip.configuration.models.application import ApplicationConfig
from pdip.connection.database.base import DatabaseProvider
from pdip.dependency import IScoped
from pdip.io import FolderManager
from pdip.logging.loggers.database import SqlLogger

from smartleasing.domain.configs.OneNtDatabaseConfig import OneNtDatabaseConfig


class LeasingService(IScoped):

    @inject
    def __init__(self,
                 application_config: ApplicationConfig,
                 sql_logger: SqlLogger,
                 one_nt_database_config: OneNtDatabaseConfig,
                 database_provider: DatabaseProvider,
                 folder_manager: FolderManager,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.folder_manager = folder_manager
        self.application_config = application_config
        self.sql_logger = sql_logger
        self.database_provider = database_provider
        self.one_nt_database_config = one_nt_database_config
        self.one_nt_database_context = self.database_provider.get_context_by_config(config=self.one_nt_database_config)

    def write_leasing_data_to_csv(self, recreate=False):
        folder = os.path.join(self.application_config.root_directory, 'files', 'data')
        self.folder_manager.create_folder_if_not_exist(folder)
        path = os.path.join(folder, 'kira_bedel.csv')
        if not os.path.exists(path) or recreate:
            leasing_data_query = 'select * from [NEMS_KYS].[V_LEASING_RENT_ESTIMATION]'
            self.one_nt_database_context.connector.connect()
            result_leasing = pandas.read_sql(leasing_data_query, con=self.one_nt_database_context.connector.connection)

            result_leasing.to_csv(path, index=False, sep=';', decimal=',')
            self.one_nt_database_context.connector.disconnect()
