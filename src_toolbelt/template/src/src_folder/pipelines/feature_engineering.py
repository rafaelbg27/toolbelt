"""
Feature engineering for the model.
"""

import logging

from $PROJECT_NAME$.interfaces.pipeline_step import PipelineStep
from $PROJECT_NAME$.transformers.feature_engineers import (Dumminizer, Scaler,
                                                           Selector)


class FeatureEngineering(PipelineStep):

    def __init__(
            self,
            input_path="interim/analysis_lead_member_funnel_preprocessed.csv",
            input_specs={
                'low_memory': False,
                'encoding': 'utf-8',
                'dtype': {
                    'member_postal_code_start': str
                }
            },
            output_path="interim/analysis_lead_member_funnel_feature_engineered.csv",
            params_path="params/analysis/feature_engineering.yaml"):
        super().__init__(input_path=input_path,
                         input_specs=input_specs,
                         output_path=output_path)
        self.di.config.load('feature_engineering', params_path)
        self.params = self.di.config.params['feature_engineering']
        self.selector = Selector(
            selection_drop_columns=self.params['selection_drop_columns'])
        self.dumminizer = Dumminizer(
            dummies_columns=self.params['dummies_columns'])
        self.scaler = Scaler(scale_meta=self.params['scale_meta'])

    def transform(self):
        if hasattr(self, 'data'):
            self.data = self.selector.transform(self.data)
            self.data = self.dumminizer.transform(self.data)
            self.data = self.scaler.transform(self.data)
            return self.data
        else:
            logging.error("Data not found! Load it first.")
