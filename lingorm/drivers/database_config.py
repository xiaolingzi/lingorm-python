import random
import os
import sys
from ..common.file_helper import FileHelper


class DatabaseConfig:
    __database_config = {}
    MODE_READ = "r"
    MODE_WRITE = "w"

    @staticmethod
    def get_config_by_database(database):
        config_dict = DatabaseConfig.__get_config()
        
        for config_item in config_dict:
            item = config_dict[config_item]
            if "database" in item and item["database"] == database:
                return item
        return None

    @staticmethod
    def get_config_by_key(key):
        config_dict = DatabaseConfig.__get_config()
        return config_dict[key]

    @staticmethod
    def get_read_write_database(database_dict, mode):
        if "servers" not in database_dict.keys():
            return database_dict
        server_list = database_dict["servers"]

        target_database_list = []
        for item in server_list:
            if mode == DatabaseConfig.MODE_READ:
                item["weight"] = 0
                if "rweight" in item.keys():
                    item["weight"] = int(item["rweight"])
            elif mode == DatabaseConfig.MODE_WRITE:
                item["weight"] = 0
                if "wweight" in item.keys():
                    item["weight"] = int(item["wweight"])

            if "weight" not in item.keys() or int(item["weight"]) <= 0:
                continue

            if "mode" in item.keys():
                configMode = item["mode"].lower()
                if configMode == "" and mode == DatabaseConfig.MODE_READ:
                    target_database_list.append(item)
                elif mode in configMode:
                    target_database_list.append(item)
            elif mode == DatabaseConfig.MODE_READ:
                target_database_list.append(item)

        if len(target_database_list) <= 0:
            raise Exception("Database config error")

        result = DatabaseConfig.__get_random_database(target_database_list)

        if "database" not in result.keys():
            result["database"] = database_dict["database"]

        if "user" not in result.keys():
            result["user"] = database_dict["user"]

        if "password" not in result.keys():
            result["password"] = database_dict["password"]

        if "charset" not in result.keys():
            result["charset"] = database_dict["charset"]

        return result

    @staticmethod
    def __get_random_database(database_list):
        db_count = len(database_list)
        if db_count == 1:
            return database_list[0]
        weight_sum = 0
        for item in database_list:
            weight = int(item["weight"])
            weight_sum += weight
            item["weight"] = weight_sum

        random_number = random.randint(1, weight_sum)
        result = database_list[0]
        for item in database_list:
            if random_number <= item["weight"]:
                result = item
                break

        return result

    @staticmethod
    def __get_config():
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
                raise Exception("Database config file not found.")
            
            DatabaseConfig.__database_config = FileHelper.get_dict_from_json_file(
                config_file)
            if not DatabaseConfig.__database_config:
                raise Exception("Database config error.")
        return DatabaseConfig.__database_config
