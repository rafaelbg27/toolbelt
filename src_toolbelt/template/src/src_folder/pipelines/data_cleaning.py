"""
Clean data and apply basic transformations.
"""

import logging

from $PROJECT_NAME$.interfaces.pipeline_step import PipelineStep
from $PROJECT_NAME$.transformers.cleaners import Cleaner, Filter


class DataCleaning(PipelineStep):

    def __init__(
            self,
            input_path="interim/cuidarme_medical_loss.csv",
            input_specs={
                'low_memory': False,
                'encoding': 'utf-8',
                'dtype': {
                    'carteirinha': str,
                    'cps_num_aj': str
                }
            },
            output_path='interim/cuidarme_medical_loss_clean.csv',
            params_path="params/cuidarme_medical_loss/cleaning.yaml"):
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
            filter_other_meta=self.params['filter_other_meta'],
            filter_custom_query_columns=self.
            params['filter_custom_query_columns'])

    def transform(self) -> None:
        if hasattr(self, 'data'):
            self.data = self.cleaner.transform(self.data)
            self.data = self.filter.transform(self.data)

        else:
            logging.error("Data not found! Load it first.")
