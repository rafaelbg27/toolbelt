"""
Clean data and apply basic transformations.
"""

import logging

from $PROJECT_NAME$.interfaces.pipeline_step import PipelineStep
from $PROJECT_NAME$.transformers.cleaners import Cleaner, Filter


class DataCleaning(PipelineStep):

    def __init__(
            self,
            input_path="interim/file_name.csv",
            input_specs={
                'low_memory': False,
                'encoding': 'utf-8'
            },
            output_path='interim/file_name_clean.csv',
            params_path="params/cleaning.yaml"):
        super().__init__(input_path=input_path,
                         input_specs=input_specs,
                         output_path=output_path)
        self.di.config.load('cleaning', params_path)
        self.params = self.di.config.params['cleaning']
        self.cleaner = Cleaner(
            variable_columns=self.params['variable_columns'],
            duplicate_columns=self.params['duplicate_columns'])
        self.filter = Filter(
            filter_notnull_columns=self.params['filter_notnull_columns'],
            filter_other_columns=self.params['filter_other_columns'])

    def transform(self) -> None:
        if hasattr(self, 'data'):
            self.data = self.cleaner.transform(self.data)
            self.data = self.filter.transform(self.data)

        else:
            logging.error("Data not found! Load it first.")
