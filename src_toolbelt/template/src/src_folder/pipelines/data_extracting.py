"""
Load data and generate CSV files.
"""

import datetime
import logging

import pandas as pd
from databases.interfaces.pipeline_step import PipelineStep
from unidecode import unidecode

from $PROJECT_NAME$.interfaces.pipeline_step import PipelineStep


class DataExtracting(PipelineStep):

    def __init__(self, params_path: str = "params/extracting.yaml"):
        super().__init__(input_path=None, input_specs=None, output_path=None)
        self.di.config.load('extracting', params_path)
        self.params = self.di.config.params['extracting']
        self.env = self.params['env']
        self.files = self.params['files']
        assert self.env == 'local' or self.env == 'prod', "Only local or prod environments supported"

    def load(self) -> None:
        datasets = {}
        for var, meta in self.files.items():
            if self.env == 'prod':
                if 'sheet_id' in meta:
                    datasets[meta['input_path']] = self.di.sheets.load(
                        sheet_id=meta['sheet_id'],
                        sheet_range=meta['sheet_range'],
                        sheet_name=meta['sheet_name'])
                else:
                    datasets[
                        meta['input_path']] = self.di.redshift.run_sql_query(
                            meta['query'])
            else:
                datasets[meta['input_path']] = self.di.static.load(
                    path=meta['input_path'], specs=meta['specs'])
        self.datasets = datasets

    def _transform(self, X: pd.DataFrame, db_table: str) -> pd.DataFrame:

        print('Transforming {}...'.format(db_table))

        X_new = X.copy()
        X_new.columns = pd.Series(X_new.columns).apply(lambda x: '_'.join(
            unidecode(x.lower().strip().replace('%', 'pct').replace('-', '').
                      replace('/', '')).split()))

        if db_table == 'table_name':
            pass

        print('Transformation not found for {}!'.format(db_table))
        return X_new

    def transform(self) -> None:
        if hasattr(self, 'datasets'):
            for var, meta in self.files.items():
                self.datasets[meta['output_path']] = self._transform(
                    self.datasets[meta['input_path']], var)
        else:
            logging.error("Data not found! Load it first.")

    def save(self) -> None:
        if hasattr(self, 'datasets'):
            for path, dataset in self.datasets.items():
                self.di.static.write(df=dataset, path=path)
        else:
            logging.error("Data not found! Load it first.")
