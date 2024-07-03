from typing import List, Tuple

import numpy as np
import pandas as pd

from $PROJECT_NAME$.data_interactor import DataInteractor
from $PROJECT_NAME$.metrics import generate_metrics
from $PROJECT_NAME$.models import ModelClassLookupCallback
from $PROJECT_NAME$.splitters import SplitterClassLookupCallback


class SplitterInteractor:

    def __init__(self, splitter_name: str) -> None:

        self.di = DataInteractor()
        self.di.config = self.di.config()

        self._splitter_name = splitter_name
        self._splitter_class = SplitterClassLookupCallback(splitter_name)

    def load_params(self, params_path: str) -> None:

        self.di.config.load("splitting", params_path)
        params = self.di.config.params["splitting"]
        self.splitter = self._splitter_class(**params)

    def split(self, dataset: pd.DataFrame, target_col: str, drop_cols: list) -> None:

        splits = self.splitter.split(dataset=dataset)

        X = dataset.drop(drop_cols + [target_col], axis=1)
        y = dataset[target_col]
        trains, tests = [], []
        for train_idx, test_idx in splits:
            trains.append((X.iloc[train_idx], y.iloc[train_idx]))
            tests.append((X.iloc[test_idx], y.iloc[test_idx]))

        self.trains = trains
        self.tests = tests


class ModelInteractor:

    def __init__(self, model_name: str) -> None:

        self.di = DataInteractor()

        self.model_name = model_name
        self.model_class = ModelClassLookupCallback(model_name)

    def _instantiate_model(self, model_parameters: dict = {}) -> None:

        if not model_parameters:
            self.model = self.model_class()
        else:
            self.model = self.model_class(**model_parameters)

    def load_data(
        self,
        raw_data: pd.DataFrame,
        trains: List[Tuple[pd.DataFrame, pd.Series]],
        tests: List[Tuple[pd.DataFrame, pd.Series]],
    ) -> None:

        self.raw_data = raw_data
        self.trains = trains
        self.tests = tests

    def fit_predict(
        self,
        model_parameters: dict = {},
    ) -> None:

        self._instantiate_model(model_parameters)

        if not hasattr(self, "trains"):
            raise ValueError("Data not found. Please run load_data first.")

        preds = []
        for (X_train, y_train), (X_test, y_test) in zip(self.trains, self.tests):
            X_train_ = X_train.copy()
            X_test_ = X_test.copy()
            categorical_features = list(
                X_train_.dtypes[
                    (X_train_.dtypes == "object") | (X_train_.dtypes == "category")
                ].index
            )
            for feat in categorical_features:
                encoder = {k: i for i, k in enumerate(X_train_[feat].unique())}
                X_train_[feat] = X_train_[feat].map(encoder).astype("category")
                X_test_[feat] = X_test_[feat].map(encoder).astype("category")
            self.model.fit(X=X_train_, y=y_train)
            preds.append(self.model.predict(X=X_test_))

        self.predictions = preds

    def calculate_metrics(self) -> None:

        metrics = []
        for (X_test, y_test), preds in zip(self.tests, self.predictions):

            aux = self.raw_data.loc[X_test.index].copy()
            aux = aux.merge(
                generate_metrics(
                    y_true=y_test,
                    y_pred=preds,
                    model_type=self.model_class.instance.model_type_,
                ),
                left_index=True,
                right_index=True,
            )
            metrics.append(aux)

        df_metrics = pd.concat(metrics)
        df_metrics["split"] = [
            i for i in range(len(metrics)) for _ in range(len(metrics[i]))
        ]
        self.metrics = df_metrics

    def evaluate(self, metric: str) -> None:

        if not hasattr(self, "predictions"):
            raise ValueError("Predictions not found. Please run fit_predict first.")

        self.calculate_metrics()
        metrics = {
            "avg": np.mean(self.metrics[metric]),
            "std": np.std(self.metrics[metric]),
        }
        self.evaluated_metrics = metrics

    def generate_metrics_report(self, metric: str, split: int = 0) -> None:

        if not hasattr(self, "metrics"):
            raise ValueError("Metrics not found. Please run calculate_metrics first.")

    def generate_shap_values_analysis(
        self,
    ) -> None: ...
