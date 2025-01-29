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