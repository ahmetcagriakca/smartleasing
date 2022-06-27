import os

import pandas as pd
from injector import inject
from pandas import DataFrame
from pdip.configuration.models.application import ApplicationConfig
from pdip.dependency import IScoped
from pdip.io import FolderManager

from smartleasing.domain.utils import Utils


class DataService(IScoped):
    @inject
    def __init__(self,
                 application_config: ApplicationConfig,
                 folder_manager: FolderManager,
                 ):
        self.folder_manager = folder_manager
        self.application_config = application_config

    def get_cell_data(self, city):
        folder = os.path.join(self.application_config.root_directory, 'files', 'data')
        path = os.path.join(folder, 'kira_bedel.csv')
        df = pd.read_csv(path, sep=';', decimal=',')
        df = df.query(f"CITY.str.contains('{city}')")
        df = df.reset_index(drop=True)
        return df

    def get_all_data(self, region, city):
        folder = os.path.join(self.application_config.root_directory, 'files', 'data')
        unicode_city_name = Utils.translate(city)
        path = os.path.join(folder, f'building-{region}-{unicode_city_name}.csv')
        df = pd.read_csv(path, sep=';', decimal=',')
        df = df.query(f"IL_ADI.str.contains('{city}')")
        df = df.reset_index(drop=True)
        return df

    def write_prediction_to_csv(self, df: DataFrame, file_name, recreate):
        folder = os.path.join(self.application_config.root_directory, 'files', 'predictions')
        self.folder_manager.create_folder_if_not_exist(folder)
        mode = 'w' if recreate else 'a'
        path = os.path.join(self.application_config.root_directory, 'files', 'predictions',
                            f"{file_name}.csv")
        write_df = df
        write_df.to_csv(path, mode=mode, header=recreate, index=False, sep=';', decimal=',')
        del write_df
