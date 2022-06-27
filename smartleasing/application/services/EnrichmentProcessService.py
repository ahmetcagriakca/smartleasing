import numpy as np
from injector import inject
from pdip.logging.loggers.console import ConsoleLogger
from sklearn.preprocessing import MinMaxScaler


class EnrichmentProcessService:
    @inject
    def __init__(self, logger: ConsoleLogger):
        self.logger = logger

    def prepare_data(self, source_column, target_column, source_df, target_df):
        groups = source_df.groupby(source_column)

        for state, frame in groups:
            target_df.loc[target_df[source_column] == state, f'NEW_{source_column}_{target_column}_MEDIAN'] = \
                groups[target_column].median()[state]
            target_df.loc[target_df[source_column] == state, f'NEW_{source_column}_{target_column}_MEAN'] = \
                groups[target_column].mean()[state]
            target_df.loc[target_df[source_column] == state, f'NEW_{source_column}_{target_column}_MIN'] = \
                groups[target_column].min()[state]
            target_df.loc[target_df[source_column] == state, f'NEW_{source_column}_{target_column}_MAX'] = \
                groups[target_column].max()[state]

    def prepare_other_data_stats(self, main_df):
        self.prepare_data('REGION', 'ALANM2', main_df, main_df)
        self.prepare_data('REGION', 'KAT', main_df, main_df)
        self.prepare_data('REGION', 'TOTAL_ALAN', main_df, main_df)
        self.prepare_data('REGION', 'R150', main_df, main_df)
        self.prepare_data('REGION', 'SCORE', main_df, main_df)
        self.prepare_data('REGION', 'SERVICED_BS_DISTANCE', main_df, main_df)
        self.prepare_data('BASARID', 'ALANM2', main_df, main_df)
        self.prepare_data('BASARID', 'KAT', main_df, main_df)
        self.prepare_data('BASARID', 'TOTAL_ALAN', main_df, main_df)
        self.prepare_data('BASARID', 'R150', main_df, main_df)
        self.prepare_data('BASARID', 'SCORE', main_df, main_df)
        self.prepare_data('BASARID', 'SERVICED_BS_DISTANCE', main_df, main_df)

    def col_normalization(self, main_df, target_column):
        amount_cols = [col for col in main_df.columns if (
                '_MEDIAN' in col or '_MEAN' in col or '_MIN' in col or '_MAX' in col) and (
                                   '_' + target_column + '_' in col)]
        result_columns = main_df.columns.tolist()
        result_df = main_df[amount_cols]
        scaler = MinMaxScaler()
        scaled_values = scaler.fit_transform(result_df)
        result_df.loc[:, :] = scaled_values

        test_df = main_df[result_columns]
        test_df[amount_cols] = result_df[amount_cols]
        return test_df

    def start(self, data):
        main_df = data[data.columns.tolist()]
        self.prepare_other_data_stats(main_df)
        # self.prepare_amount_data(main_df)
        # main_df = self.col_normalization(main_df, 'SERVICED_BS_YEARLY_TL')
        main_df = self.col_normalization(main_df, 'ALANM2')
        main_df = self.col_normalization(main_df, 'KAT')
        main_df = self.col_normalization(main_df, 'TOTAL_ALAN')
        main_df = self.col_normalization(main_df, 'R150')
        main_df = self.col_normalization(main_df, 'SCORE')
        main_df = self.col_normalization(main_df, 'SERVICED_BS_DISTANCE')
        cor_matrix = main_df.corr().abs()
        upper_tri = cor_matrix.where(np.triu(np.ones(cor_matrix.shape), k=1).astype(np.bool))
        to_drop = [column for column in upper_tri.columns if any(upper_tri[column] > 0.95) and column.startswith('NEW_')]
        self.logger.info(to_drop)
        main_df = main_df.drop(to_drop, axis=1)
        return main_df
