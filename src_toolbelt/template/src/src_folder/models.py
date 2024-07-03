from typing import Dict, Type

from catboost import CatBoostRegressor

from $PROJECT_NAME$.protocols import Model


class CatBoostModel(Model):

    model_type_ = "regression"
    is_tunable_ = True

    def __init__(self, learning_rate=0.1, depth=6, l2_leaf_reg=3, random_strength=1):

        super().__init__()

        self.model = CatBoostRegressor(
            learning_rate=learning_rate,
            depth=depth,
            l2_leaf_reg=l2_leaf_reg,
            random_strength=random_strength,
            verbose=0,
        )

    def fit(self, X, y) -> None:
        X_ = X.copy()
        X_ = X_.drop(columns=["scale_id"])
        self.model.fit(
            X_,
            y,
            cat_features=[col for col in X_.dtypes[X_.dtypes == "category"].index],
        )
        self.is_fitted_ = True

        if "prior_prediction" in X.columns:
            self.prior_prediction_ = True

    def suggest_params(trial):
        return {
            "learning_rate": trial.suggest_float("learning_rate", 0.001, 0.1),
            "depth": trial.suggest_int("depth", 4, 10),
            "l2_leaf_reg": trial.suggest_int("l2_leaf_reg", 1, 10),
            "random_strength": trial.suggest_int("random_strength", 0, 10),
        }


_MODEL_CLASS_LOOKUP: Dict[str, Type[Model]] = {
    "catboost": CatBoostModel,
}


class ModelClassLookupCallback:

    def __init__(self, model_name: str) -> None:

        if model_name not in _MODEL_CLASS_LOOKUP.keys():
            raise KeyError(
                "Model {} invalid or not implemented. The available models are: {}".format(
                    model_name, ", ".join(_MODEL_CLASS_LOOKUP.keys())
                )
            )

        self.model_name = model_name
        self.instance = _MODEL_CLASS_LOOKUP[model_name]

    def __call__(self, **kwargs) -> Model:
        return self.instance(**kwargs)
