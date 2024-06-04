import json
import os
from typing import Literal

from dotenv import load_dotenv

# from $PROJECT_NAME$ import get_lib_path

load_dotenv(get_lib_path(".env"))


class Credentials:

    def __init__(self):
        pass

    def get_credential(
        self,
        credential_name: str,
        credential_type: Literal["str", "file", "file_path"] = "str",
        credential_extension: str = "json",
    ):
        """
        Get a credential from the environment
        """
        if credential_type == "str":
            return os.getenv(credential_name)

        if credential_type == "file":
            try:
                with open(
                    get_lib_path(credential_name + "." + credential_extension), "r"
                ) as read_file:
                    return json.load(read_file)
            except FileNotFoundError:
                raise FileNotFoundError(
                    f"File {credential_name}.{credential_extension} not found"
                )

        if credential_type == "file_path":
            return get_lib_path(credential_name + "." + credential_extension)
