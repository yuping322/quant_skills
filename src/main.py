#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
量化技能项目主入口
提供统一的命令行界面
"""

import argparse
import sys
import os
import json
import re
from typing import Dict, Any, Optional, List

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from parsers.md_parser import DataInterfaceParser
from mappers.field_mapper import FieldMapper
from formatters.field_formatter import FieldFormatter
from testers.interface_tester import InterfaceTester


class InterfaceExecutor:
    """接口执行器，处理参数统一化和执行"""
    
    def __init__(self):
        self.mapping_data = self._load_mapping_data()
        self.field_mapper = FieldMapper()
    
    def _load_mapping_data(self) -> Dict[str, Any]:
        """加载接口映射数据"""
        mapping_file = "result/complete_field_mapping.json"
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def get_interface_info(self, api_name: str) -> Optional[Dict[str, Any]]:
        """获取接口信息"""
        return self.mapping_data.get('interfaces', {}).get(api_name)
    
    def normalize_params(self, api_name: str, user_params: Dict[str, Any]) -> Dict[str, Any]:
        """标准化参数"""
        interface_info = self.get_interface_info(api_name)
        if not interface_info:
            raise ValueError(f"接口 {api_name} 不存在")
        
        input_params = interface_info.get('input_params', [])
        normalized = {}
        
        # 构建参数映射
        param_map = {}
        for param in input_params:
            param_name = param.get('名称')
            param_map[param_name.lower()] = param_name
        
        # 处理用户参数
        for key, value in user_params.items():
            key_lower = key.lower()
            
            # 查找匹配的参数
            matched = False
            for param_name, original_name in param_map.items():
                if param_name == key_lower:
                    normalized[original_name] = self._normalize_value(original_name, value)
                    matched = True
                    break
            
            # 尝试字段等价关系
            if not matched:
                for standard_field, equivalents in self.field_mapper.field_equivalents.items():
                    if key in equivalents or key_lower == standard_field.lower():
                        for param_name, original_name in param_map.items():
                            if param_name in equivalents or param_name == standard_field.lower():
                                normalized[original_name] = self._normalize_value(original_name, value)
                                matched = True
                                break
                    if matched:
                        break
        
        return normalized
    
    def _normalize_value(self, param_name: str, value: Any) -> Any:
        """标准化值"""
        param_name_lower = param_name.lower()
        
        # 股票代码标准化
        if any(keyword in param_name_lower for keyword in ['symbol', 'code', 'stock', 'bond', 'fund', 'index']):
            return FieldFormatter.normalize_stock_code(value)
        
        # 日期标准化
        elif any(keyword in param_name_lower for keyword in ['date', 'time', 'year', 'month']):
            return FieldFormatter.normalize_date(value)
        
        # 数值标准化
        elif any(keyword in param_name_lower for keyword in ['price', 'amount', 'value', 'rate', 'percent']):
            if isinstance(value, str):
                return FieldFormatter.normalize_float(value)
        
        return value
    
    def execute(self, api_name: str, user_params: Dict[str, Any]) -> Any:
        """执行接口"""
        import akshare as ak
        
        # 检查接口是否存在
        if not hasattr(ak, api_name):
            raise ValueError(f"akshare 中没有接口: {api_name}")
        
        # 标准化参数
        normalized_params = self.normalize_params(api_name, user_params)
        
        # 执行接口
        func = getattr(ak, api_name)
        result = func(**normalized_params)
        
        return result


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='量化技能项目主入口')
    
    subparsers = parser.add_subparsers(dest='command', help='可用命令')
    
    # 解析接口文档
    parse_parser = subparsers.add_parser('parse', help='解析接口文档')
    parse_parser.add_argument('--data-dir', default='data', help='数据目录')
    
    # 生成字段映射
    map_parser = subparsers.add_parser('map', help='生成字段映射')
    map_parser.add_argument('--data-dir', default='data', help='数据目录')
    map_parser.add_argument('--output-dir', default='result', help='输出目录')
    
    # 测试接口
    test_parser = subparsers.add_parser('test', help='测试接口')
    test_parser.add_argument('--limit', type=int, help='限制测试数量')
    test_parser.add_argument('--timeout', type=int, default=10, help='超时时间（秒）')
    
    # 格式化测试
    format_parser = subparsers.add_parser('format', help='测试字段格式化')
    
    # 执行接口
    exec_parser = subparsers.add_parser('exec', help='执行接口')
    exec_parser.add_argument('api_name', help='接口名称')
    exec_parser.add_argument('--params', help='参数（JSON格式）')
    
    # 列出接口
    list_parser = subparsers.add_parser('list', help='列出接口')
    list_parser.add_argument('--type', help='数据类型筛选')
    list_parser.add_argument('--search', help='搜索接口名称')
    
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()
    
    if args.command == 'parse':
        print("解析接口文档...")
        parser = DataInterfaceParser()
        df = parser.parse_all_files()
        print(f"成功解析 {len(df)} 个接口")
        
    elif args.command == 'map':
        print("生成字段映射...")
        mapper = FieldMapper(data_dir=args.data_dir, output_dir=args.output_dir)
        mapper.build_complete_mapping()
        print("字段映射生成完成")
        
    elif args.command == 'test':
        print("测试接口...")
        tester = InterfaceTester()
        tester.test_all_interfaces(limit=args.limit, timeout=args.timeout)
        
    elif args.command == 'format':
        print("测试字段格式化...")
        # 测试股票代码转换
        test_codes = ["000001", "000001.SZ", "sz000001", "600000", "600000.SH", "sh600000"]
        print("\n股票代码转换测试:")
        for code in test_codes:
            pure = FieldFormatter.normalize_stock_code(code)
            with_suffix = FieldFormatter.normalize_stock_code(code, target_format='with_suffix')
            with_prefix = FieldFormatter.normalize_stock_code(code, target_format='with_prefix')
            print(f"  {code} → {pure}, {with_suffix}, {with_prefix}")
        
        # 测试日期转换
        test_dates = ["20240101", "2024-01-01", "2024/01/01", "2024年1月1日"]
        print("\n日期转换测试:")
        for date in test_dates:
            ymd = FieldFormatter.normalize_date(date)
            ymd_num = FieldFormatter.normalize_date(date, target_format='yyyymmdd')
            print(f"  {date} → {ymd}, {ymd_num}")
        
    elif args.command == 'exec':
        print(f"执行接口: {args.api_name}")
        
        # 解析参数
        user_params = {}
        if args.params:
            try:
                user_params = json.loads(args.params)
            except json.JSONDecodeError:
                print("错误: 参数格式不正确，应为JSON格式")
                return
        
        try:
            executor = InterfaceExecutor()
            result = executor.execute(args.api_name, user_params)
            print("执行成功!")
            print(f"返回类型: {type(result).__name__}")
            if hasattr(result, 'head'):
                print("返回数据预览:")
                print(result.head())
            else:
                print("返回数据:")
                print(result)
        except Exception as e:
            print(f"执行失败: {e}")
            import traceback
            traceback.print_exc()
        
    elif args.command == 'list':
        print("列出接口...")
        executor = InterfaceExecutor()
        interfaces = executor.mapping_data.get('interfaces', {})
        
        # 筛选
        filtered = []
        for api_name, info in interfaces.items():
            # 类型筛选
            if args.type and info.get('data_type') != args.type:
                continue
            # 搜索筛选
            if args.search and args.search not in api_name:
                continue
            filtered.append((api_name, info.get('description', ''), info.get('data_type', 'unknown')))
        
        # 排序并显示
        filtered.sort()
        print(f"共找到 {len(filtered)} 个接口:")
        for api_name, desc, data_type in filtered[:50]:  # 限制显示数量
            print(f"  {api_name:30} | {data_type:10} | {desc}")
        if len(filtered) > 50:
            print(f"  ... 还有 {len(filtered) - 50} 个接口未显示")
        
    else:
        print("请指定命令: parse, map, test, format, exec, list")
        print("使用 --help 查看帮助")


if __name__ == "__main__":
    main()
