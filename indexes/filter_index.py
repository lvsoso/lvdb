import logging
from typing import Dict, Optional
from pyroaring import BitMap
from collections import defaultdict

from constants import Operation


class FilterIndex:
    def __init__(self):
        """初始化过滤器索引"""
        # 使用嵌套的字典存储字段->值->位图的映射
        self.int_field_filter: Dict[str, Dict[int, BitMap]] = defaultdict(dict)
        self.logger = logging.getLogger(__name__)

    def add_int_field_filter(self, fieldname: str, value: int, id: int) -> None:
        """
        添加整数字段过滤器
        :param fieldname: 字段名
        :param value: 字段值
        :param id: 文档ID
        """
        if value not in self.int_field_filter[fieldname]:
            self.int_field_filter[fieldname][value] = BitMap()
        self.int_field_filter[fieldname][value].add(id)
        
        self.logger.debug(
            f"Added int field filter: fieldname={fieldname}, value={value}, id={id}"
        )

    def update_int_field_filter(
        self, 
        field_name: str, 
        old_value: Optional[int], 
        new_value: int, 
        id: int
    ) -> None:
        """
        更新整数字段过滤器
        :param field_name: 字段名
        :param old_value: 旧值（可能为None）
        :param new_value: 新值
        :param id: 文档ID
        """
        if old_value is not None:
            self.logger.debug(
                f"Updated int field filter: field_name={field_name}, "
                f"old_value={old_value}, new_value={new_value}, id={id}"
            )
        else:
            self.logger.debug(
                f"Updated int field filter: field_name={field_name}, "
                f"old_value=None, new_value={new_value}, id={id}"
            )

        # 如果字段存在
        if field_name in self.int_field_filter:
            value_map = self.int_field_filter[field_name]
            
            # 处理旧值
            if old_value is not None and old_value in value_map:
                value_map[old_value].remove(id)
                
                # 如果位图为空，删除该值的映射
                if len(value_map[old_value]) == 0:
                    del value_map[old_value]

            # 处理新值
            if new_value not in value_map:
                value_map[new_value] = BitMap()
            value_map[new_value].add(id)
        else:
            # 如果字段不存在，直接添加新值
            self.add_int_field_filter(field_name, new_value, id)

    def get_int_field_filter_bitmap(
        self, 
        field_name: str, 
        op: Operation, 
        value: int
    ) -> BitMap:
        """
        获取满足条件的位图
        :param field_name: 字段名
        :param op: 操作类型
        :param value: 比较值
        :return: 结果位图
        """
        result_bitmap = BitMap()
        
        if field_name in self.int_field_filter:
            value_map = self.int_field_filter[field_name]

            if op == Operation.EQUAL:
                if value in value_map:
                    result_bitmap |= value_map[value]
                    self.logger.debug(
                        f"Retrieved EQUAL bitmap for field_name={field_name}, value={value}"
                    )
            
            elif op == Operation.NOT_EQUAL:
                for val, bitmap in value_map.items():
                    if val != value:
                        result_bitmap |= bitmap
                self.logger.debug(
                    f"Retrieved NOT_EQUAL bitmap for field_name={field_name}, value={value}"
                )

        return result_bitmap
