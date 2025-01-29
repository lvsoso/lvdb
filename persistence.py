import json
import logging as logger
from typing import Dict, Any, Optional, Tuple

class Persistence:

    def __init__(self):
        """
        初始化
        """
        self.increase_id = 1
        self.wal_log_file = None

    def __del__(self):
        if self.wal_log_file:
            self.wal_log_file.close()

    def init(self, local_path: str) -> None:
        """
        初始化WAL日志文件
        :param local_path: 日志文件路径
        """
        try:
            self.wal_log_file = open(local_path, 'a+')
            self.wal_log_file.seek(0)
        except Exception as e:
            raise RuntimeError(f"Failed to open WAL log file at path: {local_path}")

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
            line = self.wal_log_file.readline()
            if not line:
                self.wal_log_file.seek(0)
                logger.debug("No more WAL log entries to read")
                return None

            log_id_str, version, operation_type, json_data_str = line.strip().split('|')
            
            log_id = int(log_id_str)
            if log_id > self.increase_id:
                self.increase_id = log_id

            json_data = json.loads(json_data_str)

            logger.debug(
                f"Read WAL log entry: log_id={log_id_str}, "
                f"operation_type={operation_type}, json_data_str={json_data_str}"
            )

            return operation_type, json_data

        except Exception as e:
            logger.error(f"Error reading WAL log: {str(e)}")
            return None