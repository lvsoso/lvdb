import json
import logging as logger
from typing import Dict, Any, Optional, Tuple
from constants import SNAPSHOTS_MAX_LOG_ID

class Persistence:

    def __init__(self):
        """
        初始化
        """
        self.increase_id = 1
        self.last_snapshot_id = 0
        self.wal_log_file = None
        self.snapshot_path = ".snapshots"
        self.index_factory = None

    def __del__(self):
        if self.wal_log_file:
            self.wal_log_file.close()

    def init(self, index_factory, wal_log_file_path: str, snapshot_folder_path: str) -> None:
        """
        初始化WAL日志文件
        :param index_factory: 索引工厂对象
        :param wal_log_file_path: 日志文件路径
        :param snapshot_folder_path: 快照文件夹路径
        """
        self.index_factory = index_factory
        self.snapshot_path = snapshot_folder_path
        try:
            self.wal_log_file = open(wal_log_file_path, 'a+')
            self.wal_log_file.seek(0)
            self.load_last_snapshot_id()
        except Exception as e:
            raise RuntimeError(f"Failed to open WAL log file at path: {wal_log_file_path}")

    def increased_id(self) -> int:
        """
        增加并返回ID
        :return: 新的ID
        """
        self.increase_id += 1
        return self.increase_id

    def get_id(self) -> int:
        """
        获取当前ID
        :return: 当前ID
        """
        return self.increase_id

    def write_wal_log(self, 
                    operation_type: str, 
                    json_data: Dict[str, Any],
                    version: str) -> None:
        """
        写入WAL日志
        :param operation_type: 操作类型
        :param json_data: JSON数据
        :param version: 版本信息
        """
        log_id = self.increased_id()
        json_str = json.dumps(json_data)
        log_entry = f"{log_id}|{version}|{operation_type}|{json_str}\n"

        try:
            self.wal_log_file.write(log_entry)
            self.wal_log_file.flush()
            logger.debug(
                f"Wrote WAL log entry: log_id={log_id}, version={version}, "
                f"operation_type={operation_type}, json_data_str={json_str}"
            )
        except Exception as e:
            logger.error(f"An error occurred while writing the WAL log entry. Reason: {str(e)}")
            raise

    def read_next_wal_log(self) -> Optional[Tuple[str, Dict[str, Any]]]:
        """
        读取下一条WAL日志
        :return: (operation_type, json_data) 元组，如果没有更多日志则返回 None
        """
        logger.debug("Reading next WAL log entry")

        try:
            while True:
                line = self.wal_log_file.readline()
                if not line:
                    self.wal_log_file.seek(0)
                    logger.debug("No more WAL log entries to read")
                    return None

                log_id_str, version, operation_type, json_data_str = line.strip().split('|')
                log_id = int(log_id_str)
                
                if log_id > self.increase_id:
                    self.increase_id = log_id

                # 只处理比最后快照ID更新的日志
                if log_id > self.last_snapshot_id:
                    json_data = json.loads(json_data_str)
                    logger.debug(
                        f"Read WAL log entry: log_id={log_id_str}, "
                        f"operation_type={operation_type}, json_data_str={json_data_str}"
                    )
                    return operation_type, json_data
                else:
                    logger.debug(
                        f"Skip Read WAL log entry: log_id={log_id_str}, "
                        f"operation_type={operation_type}, json_data_str={json_data_str}"
                    )

        except Exception as e:
            logger.error(f"Error reading WAL log: {str(e)}")
            return None

    def take_snapshot(self, scalar_storage) -> None:
        """
        创建快照
        :param scalar_storage: 标量存储对象
        """
        logger.debug("Taking snapshot")
        
        self.last_snapshot_id = self.increase_id        
        
        self.index_factory.save_index(self.snapshot_path, scalar_storage)
        self.save_last_snapshot_id()

    def load_snapshot(self, scalar_storage) -> None:
        """
        加载快照
        :param scalar_storage: 标量存储对象
        """
        logger.debug("Loading snapshot")
        
        self.index_factory.load_index(self.snapshot_path, scalar_storage)

    def save_last_snapshot_id(self) -> None:
        """保存最后快照ID到文件"""
        try:
            with open(SNAPSHOTS_MAX_LOG_ID, "w") as f:
                f.write(str(self.last_snapshot_id))
            logger.debug(f"Save snapshot Max log ID {self.last_snapshot_id}")
        except Exception as e:
            logger.error(f"Failed to open file snapshots_MaxID for writing: {str(e)}")

    def load_last_snapshot_id(self) -> None:
        """从文件加载最后快照ID"""
        try:
            with open(SNAPSHOTS_MAX_LOG_ID, "r") as f:
                self.last_snapshot_id = int(f.read().strip())
            logger.debug(f"Loading snapshot Max log ID {self.last_snapshot_id}")
        except FileNotFoundError:
            logger.warning("Failed to open file snapshots_MaxID for reading")
        except Exception as e:
            logger.error(f"Error loading last snapshot ID: {str(e)}")