from typing import Literal

import numpy as np
import pandas as pd


def mse(a: float, b: float) -> float:
    return (a - b) ** 2


def rmse(a: float, b: float) -> float:
    return np.sqrt(mse(a, b))


def smape(a: float, b: float) -> float:
    return np.abs(a - b) / (np.abs(a) + np.abs(b))


def mae(a: float, b: float) -> float:
    return np.abs(a - b)


def mape(a: float, b: float) -> float:
    return np.abs(a - b) / b


def generate_metrics(
    y_true: pd.Series,
    y_pred: pd.Series,
    model_type: Literal["regression", "classification"],
) -> pd.DataFrame:

    if model_type == "classification":
        raise NotImplementedError("Classification metrics are not implemented yet.")

    if model_type == "regression":
        dataset = pd.DataFrame({"y_true": y_true, "y_pred": y_pred})
        dataset["mse"] = dataset.apply(lambda x: mse(x["y_true"], x["y_pred"]), axis=1)
        dataset["rmse"] = dataset.apply(
            lambda x: rmse(x["y_true"], x["y_pred"]), axis=1
        )
        dataset["smape"] = dataset.apply(
            lambda x: smape(x["y_true"], x["y_pred"]), axis=1
        )
        dataset["mae"] = dataset.apply(lambda x: mae(x["y_true"], x["y_pred"]), axis=1)
        dataset["mape"] = dataset.apply(
            lambda x: mape(x["y_true"], x["y_pred"]), axis=1
        )

        return dataset
