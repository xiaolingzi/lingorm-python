import random
import os
import sys
from ..common.file_helper import FileHelper


class DatabaseConfig:
    __database_config = {}

    @staticmethod
    def get_config_by_key(key):
        config_dict = DatabaseConfig.get_config()
        if key not in config_dict.keys():
            raise Exception("Database not found")
        return config_dict[key]

    @staticmethod
    def get_config():
        if not DatabaseConfig.__database_config:
            config_file = os.environ.get("LINGORM_CONFIG")
            if config_file is None or config_file == "":
                raise Exception("Database config file must specified")
            config_file = config_file.strip()
            if not config_file.startswith("/") and not config_file.startswith("\\") and ":" not in config_file:
                process_file_path = os.path.abspath(sys.argv[0])
                dir = os.path.dirname(process_file_path)
                config_file = os.path.join(dir, config_file)

            if not os.path.exists(config_file):
                raise Exception(
                    "Database config file not found in "+config_file+".")

            DatabaseConfig.__database_config = FileHelper.get_dict_from_json_file(
                config_file)
            if not DatabaseConfig.__database_config:
                raise Exception("Database config error.")
        return DatabaseConfig.__database_config
