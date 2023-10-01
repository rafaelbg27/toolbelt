import urllib
from zipfile import ZipFile

import pandas as pd
import pandas.io.sql as sqlio
import sqlalchemy
import yaml
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

from $PROJECT_NAME$ import get_data_path, get_queries_path, get_kmp_onedrive_data_path
from $PROJECT_NAME$.interfaces.config import Credentials


class StaticDataInteractor:

    def __init__(self, base_path=get_data_path('')):
        self.cache_dict = {}
        self.base_path = base_path

    def load(self, path, specs={}, refresh=False):
        cache_id = path + str(specs)
        if cache_id in self.cache_dict.keys() and not refresh:
            return self.cache_dict[cache_id]
        else:
            if path.endswith('.csv'):
                df = pd.read_csv(os.path.join(self.base_path, path), **specs)
            elif path.endswith('.xlsx'):
                df = pd.read_excel(os.path.join(self.base_path, path), **specs)
            self.cache_dict[cache_id] = df
            print("File {} loaded successfully".format(path))
            return df

    def write(self, df, path, sep=','):
        df.to_csv(os.path.join(self.base_path, path),
                  index=False,
                  encoding='utf-8',
                  sep=sep)
        print("File {} written successfully".format(path))


class ZipDataInteractor:

    def __init__(self):
        ...

    def extract(self, zip_path: str, extract_path: str):
        with ZipFile(get_data_path(zip_path), 'r') as zip_obj:
            zip_obj.extractall(get_data_path(extract_path))


class GoogleSheetsDataInteractor:

    def __init__(self,
                 GOOGLE_CERTIFICATE=Credentials().get_credential(
                     credential_name='google_sheets',
                     credential_type='file',
                     credential_extension='json')):
        """
        Create the object with a sheet_id and the path to the json file containing the key
        """
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        if GOOGLE_CERTIFICATE is not None:
            self.credentials = ServiceAccountCredentials.from_json_keyfile_dict(
                GOOGLE_CERTIFICATE, scopes)
        else:
            self.credentials = None

    def load(self, sheet_id, sheet_range, sheet_name=None):
        if self.credentials is None:
            raise Exception(
                "Google credentials not found. Please check your credentials.")
        sheet_range = '{}!{}'.format(
            sheet_name, sheet_range) if sheet_name else sheet_range
        service = build('sheets', 'v4', credentials=self.credentials)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
                                    range=sheet_range).execute()

        values = result.get('values', [])
        df_results = pd.DataFrame(values[1:], columns=values[0])

        return df_results


class ConfigDataInteractor:

    def __init__(self):
        self.params = {}

    def load(self, name, path):
        with open(get_data_path(path), 'r') as f:
            params = yaml.load(f, Loader=yaml.FullLoader)
            self.params[name] = params
        return params

    def write(self, data, path):
        with open(get_data_path(path), 'w') as f:
            yaml.dump(data, f)


class WarehouseDataInteractor:

    def __init__(self, user: str, password: str, host: str, port: str,
                 database: str):

        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.engine = None

    def run_sql_query(self, query_name, query_params={}):
        """
        Run a query from a file and return a pandas dataframe
        """
        with open(get_queries_path(query_name), 'r') as f:
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
        df = sqlio.read_sql_query(query, conn)
        conn.close()
        return df

    def insert_table(self,
                     df: pd.DataFrame,
                     table_name: str,
                     schema: str,
                     if_exists: str = 'append',
                     index: bool = False,
                     method: str = 'multi'):
        """
        Insert a pandas dataframe into a table
        """
        conn = self.engine.connect()
        df.to_sql(table_name,
                  conn,
                  schema=schema,
                  if_exists=if_exists,
                  index=index,
                  method=method)
        print(f'Table {table_name} inserted successfully on schema {schema}')
        conn.close()

    def delete_table(self, table_name: str, schema: str):
        """
        Delete a table
        """
        query = f'DROP TABLE IF EXISTS {schema}.{table_name}'
        conn = self.engine.connect()
        conn.execute(query)
        print(f'Table {table_name} deleted successfully on schema {schema}')
        conn.close()

    def create_schema(self, schema: str):
        """
        Create a schema
        """
        query = sqlalchemy.text(f'CREATE SCHEMA IF NOT EXISTS {schema}')
        conn = self.engine.connect()
        conn.execute(query)
        print(f'Schema {schema} created successfully')
        conn.close()


class PostgresDataInteractor(WarehouseDataInteractor):

    def __init__(
        self,
        user: str = Credentials().get_credential('POSTGRES_USER'),
        password: str = urllib.parse.quote_plus(
            Credentials().get_credential('POSTGRES_PASSWORD'))
        if Credentials().get_credential('POSTGRES_PASSWORD') else '',
        host: str = Credentials().get_credential('POSTGRES_HOST'),
        port: str = Credentials().get_credential('POSTGRES_PORT'),
        database: str = Credentials().get_credential('POSTGRES_DATABASE')):
        super().__init__(user, password, host, port, database)
        try:
            self.engine = sqlalchemy.create_engine(
                f'postgresql://{user}:{password}@{host}:{port}/{database}?sslmode=prefer'
            )
        except:
            print(
                "Could not connect to the database. Invalid Postgres credentials."
            )
            self.engine = None


class KMPOneDriveDataInteractor(StaticDataInteractor):

    def __init__(self):
        super().__init__(base_path=get_kmp_onedrive_data_path(''))


class DataInteractor:

    def __init__(self):
        self.static = StaticDataInteractor()
        self.zip = ZipDataInteractor()
        self.sheets = GoogleSheetsDataInteractor()
        self.config = ConfigDataInteractor()
        self.postgres = PostgresDataInteractor()
        self.kmp_onedrive = KMPOneDriveDataInteractor()
