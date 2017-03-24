from ..common.filehelper import FileHelper
from ..config import Config


class DatabaseConfig:
    __database_config = None

    @staticmethod
    def get_config_by_database(database):
        config_dict = DatabaseConfig.__get_config()
        for config_item in config_dict:
            if config_item["database"] == database:
                return config_item
        return None

    @staticmethod
    def get_config_by_key(key):
        config_dict = DatabaseConfig.__get_config()
        return config_dict[key]

    @staticmethod
    def __get_config():
        if DatabaseConfig.__database_config is None:
            DatabaseConfig.__database_config = FileHelper.get_dict_from_json_file(Config.database_config_file)
        return DatabaseConfig.__database_config
