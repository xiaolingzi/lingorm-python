import os
import json


class FileHelper:
    @staticmethod
    def get_file_content(filename):
        if not os.path.exists(filename):
            return ""
        fp = open(filename, "r")
        result = fp.read()
        fp.close()
        return result;

    @staticmethod
    def write_file_content(filename, content):
        fp = open(filename, "w")
        fp.write(content)
        fp.close()

    @staticmethod
    def get_dict_from_json_file(filename):
        content = FileHelper.get_file_content(filename)
        if content == "":
            content = "{}"
        result = json.loads(content)
        return result

    @staticmethod
    def write_dict_to_json_file(filename, dict_obj):
        content = json.dumps(dict_obj)
        FileHelper.write_file_content(filename, content)
