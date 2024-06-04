import logging
import os
import urllib

import pandas as pd
import pandas.io.sql as sqlio
import pandas_gbq
import sqlalchemy
import yaml
from google.cloud import bigquery
from google.oauth2 import service_account
from googleapiclient.discovery import build

# from $PROJECT_NAME$ import get_data_path, get_queries_path
# from $PROJECT_NAME$.config import Credentials


class ConfigDataInteractor:

    def __init__(self):
        self.params = {}

    def load(self, name, path):
        with open(get_data_path(path), "r") as f:
            params = yaml.load(f, Loader=yaml.FullLoader)
            self.params[name] = params
        return params

    def write(self, data, path):
        with open(get_data_path(path), "w") as f:
            yaml.dump(data, f)


class StaticDataInteractor:

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
            elif path.endswith(".xlsx"):
                df = pd.read_excel(os.path.join(self.base_path, path), **specs)
            self.cache_dict[cache_id] = df
            print("File {} loaded successfully".format(path))
            return df

    def write(self, df, path, specs={"index": False, "encoding": "utf-8"}):
        if path.endswith(".csv"):
            df.to_csv(os.path.join(self.base_path, path), **specs)
        elif path.endswith(".xlsx"):
            df.to_excel(os.path.join(self.base_path, path), **specs)
        print("File {} written successfully".format(path))


class GoogleDataInteractor:

    def __init__(
        self,
        credential_name: str,
        credential_type: str,
        credential_extension: str,
        scopes: list,
    ):
        """
        Interface to interact with Google APIs
        """

        self.credentials = service_account.Credentials.from_service_account_file(
            Credentials().get_credential(
                credential_name=credential_name,
                credential_type=credential_type,
                credential_extension=credential_extension,
            ),
            scopes=scopes,
        )


class GoogleSheetsDataInteractor(GoogleDataInteractor):

    def __init__(
        self,
        credential_name: str = "google_sheets",
        credential_type: str = "file",
        credential_extension: str = "json",
        scopes: list = ["https://www.googleapis.com/auth/spreadsheets"],
    ):
        super().__init__(credential_name, credential_type, credential_extension, scopes)
        if self.credentials is None:
            raise Exception(
                "Google credentials not found. Please check your credentials."
            )

    def load(self, sheet_id, sheet_range, sheet_name=None) -> pd.DataFrame:
        sheet_range = (
            "{}!{}".format(sheet_name, sheet_range) if sheet_name else sheet_range
        )
        service = build("sheets", "v4", credentials=self.credentials)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=sheet_range).execute()

        values = result.get("values", [])
        df_results = pd.DataFrame(values[1:], columns=values[0])

        return df_results

    def write(self, sheet_id, sheet_range, values, sheet_name=None) -> None:
        sheet_range = (
            "{}!{}".format(sheet_name, sheet_range) if sheet_name else sheet_range
        )
        service = build("sheets", "v4", credentials=self.credentials)
        body = {"values": values}
        result = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=sheet_id,
                range=sheet_range,
                valueInputOption="RAW",
                body=body,
            )
            .execute()
        )
        print("{0} cells updated.".format(result.get("updatedCells")))


class BigQueryDataInteractor(GoogleDataInteractor):

    def __init__(
        self,
        credential_name: str = "bigquery-data-science-295215",
        credential_type: str = "file_path",
        credential_extension: str = "json",
        scopes: list = [
            "https://www.googleapis.com/auth/cloud-platform",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/bigquery",
        ],
    ):
        super().__init__(credential_name, credential_type, credential_extension, scopes)
        if self.credentials is None:
            raise Exception(
                "Google credentials not found. Please check your credentials."
            )
        self.client = bigquery.Client(credentials=self.credentials)
        pandas_gbq.context.credentials = self.credentials

    def run_sql_query(self, query_name: str, query_params: dict = {}) -> pd.DataFrame:
        """
        Run a query from a file and return a pandas dataframe
        """
        with open(get_queries_path(query_name), "r") as f:
            query = f.read()
        query = query.format(**query_params)
        query_job = self.client.query(query)
        df = query_job.to_dataframe()
        return df

    def run_str_query(self, query: str) -> pd.DataFrame:
        """
        Run a query from a string and return a pandas dataframe
        """
        query_job = self.client.query(query)
        df = query_job.to_dataframe()
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


class WarehouseDataInteractor:

    def __init__(
        self,
        user: str,
        password: str,
        host: str,
        port: str,
        database: str,
        verbose: bool = False,
    ):

        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = None

        if verbose:
            logging.basicConfig()
            logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    def run_sql_query(self, query_name, query_params={}):
        """
        Run a query from a file and return a pandas dataframe
        """
        with open(get_queries_path(query_name), "r") as f:
            query = f.read()
        query = query.format(**query_params)
        conn = self.engine.connect()
        df = sqlio.read_sql_query(query, conn)
        conn.close()
        return df

    def run_str_query(self, query):
        """
        Run a query from a string and return a pandas dataframe
        """
        conn = self.engine.connect()
        query = sqlalchemy.text(query)
        df = sqlio.read_sql_query(query, conn)
        conn.close()
        return df

    def insert_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        schema: str,
        if_exists: str = "replace",
        index: bool = False,
        method: str = "multi",
    ):
        """
        Insert a pandas dataframe into a table
        """
        conn = self.engine.connect()
        df.to_sql(
            table_name,
            conn,
            schema=schema,
            if_exists=if_exists,
            index=index,
            method=method,
        )
        print(f"Table {table_name} inserted successfully on schema {schema}")
        conn.close()

    def delete_table(self, table_name: str, schema: str):
        """
        Delete a table
        """
        query = sqlalchemy.text(f"DROP TABLE IF EXISTS {schema}.{table_name}")
        conn = self.engine.connect()
        conn.execute(query)

        try:
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(str(e))

        print(f"Table {table_name} deleted successfully on schema {schema}")
        conn.close()

    def create_schema(self, schema: str):
        """
        Create a schema
        """
        query = sqlalchemy.text(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        conn = self.engine.connect()
        conn.execute(query)

        try:
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(str(e))

        print(f"Schema {schema} created successfully")
        conn.close()

    def delete_schema(self, schema: str):
        """
        Delete a schema
        """
        query = sqlalchemy.text(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        conn = self.engine.connect()
        conn.execute(query)

        try:
            conn.commit()
        except Exception as e:
            conn.rollback()
            print(str(e))

        print(f"Schema {schema} deleted successfully")
        conn.close()


class PostgresDataInteractor(WarehouseDataInteractor):

    def __init__(
        self,
        user: str = Credentials().get_credential("POSTGRES_USER"),
        password: str = (
            urllib.parse.quote_plus(Credentials().get_credential("POSTGRES_PASSWORD"))
            if Credentials().get_credential("POSTGRES_PASSWORD")
            else ""
        ),
        host: str = Credentials().get_credential("POSTGRES_HOST"),
        port: str = Credentials().get_credential("POSTGRES_PORT"),
        database: str = Credentials().get_credential("POSTGRES_DATABASE"),
    ):
        super().__init__(user, password, host, port, database)
        try:
            self.engine = sqlalchemy.create_engine(
                f"postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=prefer"
            )
        except:
            print("Could not connect to the database. Invalid Postgres credentials.")
            self.engine = None


class DataInteractor:

    def __init__(self):
        self.config = ConfigDataInteractor
        self.static = StaticDataInteractor
        self.sheets = GoogleSheetsDataInteractor
        self.bigquery = BigQueryDataInteractor
