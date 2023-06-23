import json
import os

from dotenv import load_dotenv
from $PROJECT_NAME$ import get_lib_path



load_dotenv(get_lib_path('.env'))


class Credentials:

    def __init__(self):
        pass

    def get_credential(self, credential_name: str):
        if not hasattr(self, credential_name):
            print('Credential {} not found'.format(credential_name))
            return None
        return getattr(self, credential_name)

    POSTGRES_USER = os.getenv('POSTGRES_USER')
    POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
    POSTGRES_HOST = os.getenv('POSTGRES_HOST')
    POSTGRES_PORT = os.getenv('POSTGRES_PORT')
    POSTGRES_DATABASE = os.getenv('POSTGRES_DATABASE')

    try:
        with open(get_lib_path('google_sheets_credentials.json')) as read_file:
            GOOGLE_CERTIFICATE = json.load(read_file)
    except FileNotFoundError:
        GOOGLE_CERTIFICATE = None
