import json
import logging
from rocksdict import Rdict


class ScalarStorage:
    def __init__(self, db_path: str):
        """
        初始化 ScalarStorage
        :param db_path: RocksDB 数据库路径
        """
        try:
            self.db = Rdict(db_path)
        except Exception as e:
            raise RuntimeError(f"Failed to open RocksDB: {str(e)}")

    def __del__(self):
        """析构函数，确保数据库正确关闭"""
        if hasattr(self, 'db'):
            self.db.close()

    def insert_scalar(self, id: int, data: dict):
        """
        插入标量数据
        :param id: 数据ID
        :param data: 要存储的字典数据
        """
        try:
            value = json.dumps(data).encode('utf-8')
            key = str(id).encode('utf-8')
            self.db[key] = value
        except Exception as e:
            logging.error(f"Failed to insert scalar: {str(e)}")

    def get_scalar(self, id: int) -> dict:
        """
        获取标量数据
        :param id: 数据ID
        :return: 存储的字典数据，如果不存在则返回空字典
        """
        try:
            key = str(id).encode('utf-8')
            value = self.db.get(key)
            
            if value is None:
                return {}

            data = json.loads(value.decode('utf-8'))
            logging.debug(f"Data retrieved from ScalarStorage: {data}")
            
            return data
        except Exception as e:
            logging.error(f"Failed to get scalar: {str(e)}")
            return {}

    def put(self, key: str, value: str) -> None:
        """
        存储键值对
        :param key: 键
        :param value: 值
        """
        try:
            self.db[key.encode('utf-8')] = value.encode('utf-8')
        except Exception as e:
            logging.error(f"Failed to put key-value pair: {str(e)}")

    def get(self, key: str) -> str:
        """
        获取键对应的值
        :param key: 键
        :return: 值，如果不存在则返回空字符串
        """
        try:
            value = self.db.get(key.encode('utf-8'))
            return value.decode('utf-8') if value is not None else ""
        except Exception as e:
            logging.error(f"Failed to get value for key {key}: {str(e)}")
            return ""