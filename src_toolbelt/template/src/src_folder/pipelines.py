import logging

# from $PROJECT_NAME$.protocols import PipelineStep
# from $PROJECT_NAME$.transformers import (
#     Binarizer,
#     Binner,
#     Cleaner,
#     CustomTransformer,
#     Dropper,
#     Dumminizer,
#     Encoder,
#     FeatureTransformer,
#     Filter,
#     MinimumPercentageFilter,
#     MissingInputer,
#     Renamer,
#     Scaler,
#     Selector,
# )


class DataCleaning(PipelineStep):

    def __init__(
        self,
        output_path="",
        params_path="",
        input_path="",
        input_specs={
            "low_memory": False,
            "encoding": "utf-8",
        },
        input_data=None,
    ):
        super().__init__(
            output_path=output_path,
            input_path=input_path,
            input_specs=input_specs,
            input_data=input_data,
        )
        self.di.config = self.di.config()
        self.di.config.load("cleaning", params_path)
        self.params = self.di.config.params["cleaning"]
        self.cleaner = Cleaner(
            variable_columns=self.params["variable_columns"],
            duplicate_columns=self.params["duplicate_columns"],
        )
        self.filter = Filter(
            filter_notnull_columns=self.params["filter_notnull_columns"],
            filter_other_meta=self.params["filter_other_meta"],
            filter_custom_query_columns=self.params["filter_custom_query_columns"],
            filter_week_difference_meta=self.params["filter_week_difference_meta"],
        )

    def transform(self) -> None:
        if hasattr(self, "data"):
            self.data = self.cleaner.transform(self.data)
            self.data = self.filter.transform(self.data)

        else:
            logging.error("Data not found! Load it first.")


class DataPreprocessing(PipelineStep):

    def __init__(
        self,
        output_path="",
        params_path="",
        input_path="",
        input_specs={
            "low_memory": False,
            "encoding": "utf-8",
        },
        input_data=None,
    ):
        super().__init__(
            output_path=output_path,
            input_path=input_path,
            input_specs=input_specs,
            input_data=input_data,
        )

        self.di.config = self.di.config()
        self.di.config.load("preprocessing", params_path)
        self.params = self.di.config.params["preprocessing"]

        self.dropper = Dropper(drop_columns=self.params["drop_columns"])
        self.renamer = Renamer(rename_meta=self.params["rename_meta"])
        self.binner = Binner(
            bins_cut_meta=self.params["bins_cut_meta"],
            bins_qcut_meta=self.params["bins_qcut_meta"],
            bins_other_meta=self.params["bins_other_meta"],
        )
        self.mininum_percentage_filter = MinimumPercentageFilter(
            minimum_percentage_meta=self.params["minimum_percentage_meta"]
        )
        self.binarizer = Binarizer(binarizer_meta=self.params["binarizer_meta"])
        self.encoder = Encoder(encoder_meta=self.params["encoder_meta"])
        self.feature_transformer = FeatureTransformer(
            transformer_meta=self.params["transformer_meta"]
        )
        self.custom_transformer = CustomTransformer(
            custom_transformer_name=self.params["custom_transformer_name"]
        )
        self.missing_inputer = MissingInputer(
            default_numerical_missing_columns=self.params[
                "default_numerical_missing_columns"
            ],
            default_categorical_missing_columns=self.params[
                "default_categorical_missing_columns"
            ],
            other_missing_meta=self.params["other_missing_meta"],
            default_fill_meta=self.params["default_fill_meta"],
        )

    def transform(self) -> None:
        if hasattr(self, "data"):
            self.data = self.dropper.transform(self.data)
            self.data = self.renamer.transform(self.data)
            self.data = self.binner.transform(self.data)
            self.data = self.mininum_percentage_filter.transform(self.data)
            self.data = self.binarizer.transform(self.data)
            self.data = self.encoder.transform(self.data)
            self.data = self.feature_transformer.transform(self.data)
            self.data = self.custom_transformer.transform(self.data)
            self.data = self.missing_inputer.transform(self.data)
        else:
            logging.error("Data not found! Load it first.")


class FeatureEngineering(PipelineStep):

    def __init__(
        self,
        output_path="",
        params_path="",
        input_path="",
        input_specs={
            "low_memory": False,
            "encoding": "utf-8",
        },
        input_data=None,
    ):
        super().__init__(
            output_path=output_path,
            input_path=input_path,
            input_specs=input_specs,
            input_data=input_data,
        )
        self.di.config = self.di.config()
        self.di.config.load("feature_engineering", params_path)
        self.params = self.di.config.params["feature_engineering"]
        self.selector = Selector(
            selection_drop_columns=self.params["selection_drop_columns"]
        )
        self.dumminizer = Dumminizer(dummies_columns=self.params["dummies_columns"])
        self.scaler = Scaler(scale_meta=self.params["scale_meta"])

    def transform(self):
        if hasattr(self, "data"):
            self.data = self.selector.transform(self.data)
            self.data = self.dumminizer.transform(self.data)
            self.data = self.scaler.transform(self.data)
            return self.data
        else:
            logging.error("Data not found! Load it first.")
