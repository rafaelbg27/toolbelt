import os
from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd
import pandas_gbq
import yaml

from $PROJECT_NAME$ import get_data_path, get_queries_path
from $PROJECT_NAME$.config import Config


class StaticDataInteractor(ABC):
    """Abstract class to interact with static data."""

    @abstractmethod
    def load(self, args, kwargs):
        """Load data from a file."""
        raise NotImplementedError(
            "StaticDataInteractor subclass must implement load method"
        )

    @abstractmethod
    def write(self, args, kwargs):
        """Write data to a file."""
        raise NotImplementedError(
            "StaticDataInteractor subclass must implement write method"
        )


class WarehouseDataInteractor(ABC):
    """Abstract class to interact with a data warehouse."""

    @abstractmethod
    def run_str_query(self, query: str) -> pd.DataFrame:
        """Runs an SQL query on the database and returns the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            pd.DataFrame: The result of the query execution as a DataFrame.
        """
        raise NotImplementedError(
            "WarehouseDataInteractor subclass must implement run_str_query method"
        )


class YAMLDataInteractor(StaticDataInteractor):

    def __new__(cls, *args, **kwargs):
        """Create a new instance of the class if it doesn't exist."""
        if not hasattr(cls, "_instance"):
            cls._instance = super(YAMLDataInteractor, cls).__new__(cls)
        return cls._instance

    def load(self, name, path):
        with open(get_data_path(path), "r") as f:
            params = yaml.load(f, Loader=yaml.FullLoader)
            self.params[name] = params
        return params

    def write(self, data, path):
        with open(get_data_path(path), "w") as f:
            yaml.dump(data, f)


class CSVDataInteractor(StaticDataInteractor):

    def __init__(self, base_path=get_data_path("")):
        self.cache_dict = {}
        self.base_path = base_path

    def load(self, path, specs={}, refresh=False):
        cache_id = path + str(specs)
        if cache_id in self.cache_dict.keys() and not refresh:
            return self.cache_dict[cache_id]
        else:
            if path.endswith(".csv"):
                df = pd.read_csv(os.path.join(self.base_path, path), **specs)
            self.cache_dict[cache_id] = df
            print("File {} loaded successfully".format(path))
            return df

    def write(self, df, path, specs={"index": False, "encoding": "utf-8"}):
        if path.endswith(".csv"):
            df.to_csv(os.path.join(self.base_path, path), **specs)
        print("File {} written successfully".format(path))


class BigQueryDataInteractor(WarehouseDataInteractor):

    def run_str_query(
        self, query: str, progress_bar_type: Optional[str] = "tqdm"
    ) -> pd.DataFrame:
        """Runs an SQL query on the database and returns the result as a pandas DataFrame.

        Args:
            query (str): The SQL query to be executed.

        Returns:
            pd.DataFrame: The result of the query execution as a DataFrame.
        """
        df = pandas_gbq.read_gbq(
            query,
            project_id=Config().GCP_PROJECT,
            dialect="standard",
            progress_bar_type=progress_bar_type,
        )
        return df

    def insert_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        dataset_id: str,
        project_id: str,
        table_schema: dict = None,
        if_exists: str = "replace",
    ):
        """
        Insert a pandas dataframe into a table
        """
        table_id = f"{dataset_id}.{table_name}"
        pandas_gbq.to_gbq(
            df,
            table_id,
            project_id=project_id,
            table_schema=table_schema,
            if_exists=if_exists,
        )


class DataInteractor:

    def __init__(self):
        self.yaml = YAMLDataInteractor()
        self.csv = CSVDataInteractor()
        self.bigquery = BigQueryDataInteractor()
