from abc import ABC, abstractmethod
from typing import List, Literal, Tuple, Type, Union

import joblib
import numpy as np
import pandas as pd

# from $PROJECT_NAME$.data_interactor import DataInteractor
# from $PROJECT_NAME$.env_functions import get_models_path
# from $PROJECT_NAME$.metrics import generate_metrics


class Model(ABC):

    is_tunable_: bool
    model_type_: Literal["regression", "classification"]

    def __init__(self) -> None:

        if not hasattr(self, "is_tunable_"):
            raise AttributeError("is_tunable_ attribute is missing, please define it.")

        if not hasattr(self, "model_type_"):
            raise AttributeError("model_type_ attribute is missing, please define it.")

        self.is_fitted_: bool = False
        self.prior_prediction_: bool = False

    def fit(self, X, y) -> None:
        X_ = X.copy()
        X_ = X_.drop(columns=["scale_id"])
        categorical_features = list(
            X_.dtypes[(X_.dtypes == "object") | (X_.dtypes == "category")].index
        )
        for feat in categorical_features:
            encoder = {k: i for i, k in enumerate(X_[feat].unique())}
            X_[feat] = X_[feat].map(encoder).astype("category")
        self.model.fit(X=X_, y=y)
        self.is_fitted_ = True

        if "prior_prediction" in X.columns:
            self.prior_prediction_ = True

    def _predict(self, X) -> np.array:
        X_ = X.copy()
        X_ = X_.drop(columns=["scale_id"])
        return self.model.predict(X_)

    def predict(self, X) -> np.array:

        X_ = X.copy()

        # predict for each scale id
        scale_ids = X_["scale_id"].unique()
        for scale_id in scale_ids:
            X_.loc[X_["scale_id"] == scale_id, "prediction"] = self.predict_scale(
                X, scale_id
            )

        return X_["prediction"].values

    def predict_scale(self, X: pd.DataFrame, scale_id: int) -> np.array:

        X_ = X[X["scale_id"] == scale_id].copy()

        preds = []
        max_days_diff = X_["days_diff_event_to_scale_limited"].max()
        scale_order = [
            max_days_diff - i for i in X_["days_diff_event_to_scale_limited"].values
        ]

        if self.prior_prediction_:

            for i in range(max_days_diff + 1):
                aux = X_[
                    X_["days_diff_event_to_scale_limited"] == (max_days_diff - i)
                ].copy()
                aux["prior_prediction"] = preds[-1] if preds else 0
                preds.append(self._predict(aux))

        else:
            preds = self._predict(X_)
            return preds

        return np.array(preds).flatten()[scale_order]

    def evaluate(self, y_pred: pd.Series, y_true: pd.Series, metric: str) -> float:
        return np.mean(generate_metrics(y_true, y_pred, self.model_type_)[metric])

    def save(self) -> None:
        joblib.dump(self, get_models_path("{}.joblib".format(self.__class__.__name__)))

    def load(self) -> Type["Model"]:
        return joblib.load(get_models_path("{}.joblib".format(self.__class__.__name__)))


class Splitter(ABC):

    def __init__(self):
        self.di = DataInteractor()

    @abstractmethod
    def split(
        self, df: Union[pd.DataFrame, pd.Series]
    ) -> List[Tuple[pd.Index, pd.Index]]: ...


class PipelineStep(ABC):

    def __init__(
        self,
        output_path: str,
        input_path: str,
        input_specs: dict,
        input_data: pd.DataFrame,
    ):
        self.di = DataInteractor()

        self.output_path = output_path
        self.input_path = input_path
        self.input_specs = input_specs
        self.input_data = input_data

    def load(self) -> None:
        if self.input_data is not None:
            self.data = self.input_data
        else:
            self.data = self.di.static().load(self.input_path, self.input_specs)

    @abstractmethod
    def transform(self) -> None:
        pass

    def save(self) -> None:
        self.di.static().write(self.data, self.output_path)
        return self.output_path

    def execute(self) -> None:
        self.load()
        self.transform()
        self.save()


class Transformer(ABC):

    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    @abstractmethod
    def transform(self, X):
        pass

    def inverse_transform(self, X):
        pass

    def fit_transform(self, X, y=None):
        return self.fit(X, y).transform(X)

    def save(self):
        joblib.dump(self, get_models_path("{}.joblib".format(self.__class__.__name__)))

    def load(self):
        return joblib.load(get_models_path("{}.joblib".format(self.__class__.__name__)))
