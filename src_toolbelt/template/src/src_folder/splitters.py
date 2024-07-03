from typing import Dict, List, Tuple, Type

import pandas as pd

from $PROJECT_NAME$.protocols import Splitter


class WindowedWeeklySplitter(Splitter):
    def __init__(
        self,
        date_week_col: str,
        past_periods: int,
        future_periods: int,
        max_splits: int,
    ) -> None:

        super().__init__()

        self.date_week_col = date_week_col
        self.past_periods = past_periods
        self.future_periods = future_periods
        self.max_splits = max_splits

    def split(self, dataset: pd.DataFrame) -> List[Tuple[pd.Index, pd.Index]]:

        df = dataset.copy()
        df[self.date_week_col] = pd.to_datetime(df[self.date_week_col]).dt.date

        splits = []
        for i in range(self.max_splits):

            df_aux = df[
                df[self.date_week_col]
                <= (df[self.date_week_col].max() - pd.DateOffset(weeks=i)).date()
            ]

            ref_max_date = (
                df_aux[self.date_week_col]
                .sort_values(ascending=False)
                .drop_duplicates()
                .iloc[self.future_periods - 1]
            )
            ref_min_date = (
                ref_max_date - pd.DateOffset(weeks=self.past_periods)
            ).date()

            train_idx = df_aux[
                (df_aux[self.date_week_col] >= ref_min_date)
                & (df_aux[self.date_week_col] < ref_max_date)
            ].index
            test_idx = df_aux[df_aux[self.date_week_col] >= ref_max_date].index

            splits.append((train_idx, test_idx))

        return splits


_SPLITTER_CLASS_LOOKUP: Dict[str, Type[Splitter]] = {
    "windowed_weekly": WindowedWeeklySplitter,
}


class SplitterClassLookupCallback:

    def __init__(self, splitter_name: str) -> None:

        if splitter_name not in _SPLITTER_CLASS_LOOKUP.keys():
            raise KeyError(
                "Splitter {} invalid or not implemented. The available splitters are: {}".format(
                    splitter_name, ", ".join(_SPLITTER_CLASS_LOOKUP.keys())
                )
            )

        self.splitter_name = splitter_name
        self.instance = _SPLITTER_CLASS_LOOKUP[splitter_name]

    def __call__(self, **kwargs) -> Splitter:
        return self.instance(**kwargs)
