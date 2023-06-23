"""
Load data and generate CSV files.
"""

import logging

import pandas as pd

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

    def _transform_something(self, X: pd.DataFrame) -> pd.DataFrame:
        return None

    def transform(self) -> None:
        if hasattr(self, 'datasets'):
            for var, meta in self.files.items():
                if '_transform_{}'.format(var) in dir(self):
                    print('Transforming... {}'.format(var))
                    transformation = getattr(self, '_transform_{}'.format(var))
                    self.datasets[meta['output_path']] = transformation(
                        self.datasets[meta['input_path']])
                else:
                    self.datasets[meta['output_path']] = self.datasets[
                        meta['input_path']]

        else:
            logging.error("Data not found! Load it first.")

    def save(self) -> None:
        if hasattr(self, 'datasets'):
            for path, dataset in self.datasets.items():
                self.di.static.write(df=dataset, path=path)
        else:
            logging.error("Data not found! Load it first.")
