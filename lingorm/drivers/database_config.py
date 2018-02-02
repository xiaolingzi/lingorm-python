import random
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
    def get_read_write_database(database_dict, mode):
        if "servers" not in database_dict.keys():
            return database_dict
        server_list = database_dict["servers"]
        read_database_list = []
        write_database_list = []
        for item in server_list:
            if "mode" in item.keys():
                config_mode = item["mode"]
                is_read = "r" in config_mode
                is_write = "w" in config_mode
                if not is_read and not is_write:
                    is_read = True
                if is_read:
                    read_database_list.append(item)
                if is_write:
                    write_database_list.append(item)
            else:
                read_database_list.append(item)
        config_dict = DatabaseConfig.__get_config()
        result = {}
        mode = mode.lower()
        if mode == "w":
            if not write_database_list:
                raise Exception("No database for writing")
            result = DatabaseConfig.__get_random_database(write_database_list, "w_weight")
        elif mode == "r":
            if not read_database_list:
                raise Exception("No database for reading")
            result = DatabaseConfig.__get_random_database(read_database_list, "weight")

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
    def __get_random_database(database_list, weight_key):
        db_count = len(database_list)
        if db_count == 1:
            return database_list[0]
        weight_sum = 0
        for item in database_list:
            if weight_key in item.keys():
                weight = int(item[weight_key])
                if weight < 0:
                    weight = 0
                weight_sum += weight
            else:
                weight_sum += 1
            item["sum_weight"] = weight_sum

        random_number = random.randint(1, weight_sum)
        result = database_list[0]
        for item in database_list:
            if random_number <= item["sum_weight"]:
                result = item
                break

        return result

    @staticmethod
    def __get_config():
        if DatabaseConfig.__database_config is None:
            DatabaseConfig.__database_config = FileHelper.get_dict_from_json_file(Config.database_config_file)
        return DatabaseConfig.__database_config
