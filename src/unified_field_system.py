#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一字段系统
完整的输入输出字段统一化解决方案
"""

import json
import os
import sys
import time
from typing import Dict, Any, List, Optional
import pandas as pd

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from mappers.field_mapper import FieldMapper
from formatters.field_formatter import FieldFormatter
from testers.interface_tester import InterfaceTester


class UnifiedFieldSystem:
    """统一字段系统"""
    
    def __init__(self, data_dir: str = "data", output_dir: str = "result", config_path: str = None):
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.config_path = config_path
        self.field_formatter = FieldFormatter()
        self.interface_tester = InterfaceTester()
        
        # 配置数据
        self.config = {}
        self.field_equivalents = {}
        self.api_mappings = {}
        self.value_normalization = {}
        
        # 初始化字段映射器
        self.field_mapper = FieldMapper(data_dir, output_dir)
        
        # 加载配置
        self._load_config()
        
        # 统一字段映射缓存
        self.unified_mapping = {}
        self.input_mapping = {}
        self.output_mapping = {}
    
    def _load_config(self):
        """加载配置文件"""
        if self.config_path and os.path.exists(self.config_path):
            print(f"加载配置文件: {self.config_path}")
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self.config = json.load(f)
            
            self.field_equivalents = self.config.get('field_equivalents', {})
            self.api_mappings = self.config.get('api_mappings', {})
            self.value_normalization = self.config.get('value_normalization', {})
            
            print(f"  ✓ 已加载 {len(self.field_equivalents)} 个标准字段")
        else:
            print("使用默认字段等价关系（从FieldMapper）")
            self.field_equivalents = self.field_mapper.field_equivalents
    
    def build_unified_system(self):
        """构建完整的统一字段系统"""
        print("构建统一字段系统...")
        
        # 1. 构建字段映射
        print("1. 构建字段映射...")
        self.field_mapper.build_complete_mapping()
        
        # 2. 分析接口返回值
        print("2. 分析接口返回值...")
        from scripts.interface_response_analyzer import InterfaceResponseAnalyzer
        analyzer = InterfaceResponseAnalyzer(output_dir=os.path.join(self.output_dir, "unified_system"))
        analyzer.analyze_all_interfaces(limit=50)  # 分析前50个接口作为示例
        
        # 3. 构建统一映射
        print("3. 构建统一映射...")
        self._build_unified_mapping()
        
        # 4. 生成统一模式
        print("4. 生成统一模式...")
        unified_schema = analyzer.generate_unified_schema()
        
        # 5. 保存统一系统配置
        print("5. 保存统一系统配置...")
        self._save_unified_system()
        
        print("统一字段系统构建完成!")
    
    def _build_unified_mapping(self):
        """构建统一映射"""
        # 加载接口映射数据
        mapping_file = os.path.join(self.output_dir, "complete_field_mapping.json")
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
        else:
            mapping_data = {}
        
        interfaces = mapping_data.get('interfaces', {})
        
        # 构建输入和输出映射
        for api_name, info in interfaces.items():
            # 输入参数映射
            input_params = info.get('input_params', [])
            self.input_mapping[api_name] = {
                'original_params': input_params,
                'unified_params': self._unify_input_params(input_params)
            }
            
            # 输出参数映射
            output_params = info.get('output_params', [])
            self.output_mapping[api_name] = {
                'original_params': output_params,
                'unified_params': self._unify_output_params(output_params)
            }
    
    def _unify_input_params(self, input_params: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """统一输入参数"""
        unified = []
        for param in input_params:
            param_name = param.get('名称')
            unified_param = {
                'original_name': param_name,
                'unified_name': self._map_to_standard_field(param_name),
                'type': param.get('类型'),
                'description': param.get('描述')
            }
            unified.append(unified_param)
        return unified
    
    def _unify_output_params(self, output_params: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """统一输出参数"""
        unified = []
        for param in output_params:
            param_name = param.get('名称')
            unified_param = {
                'original_name': param_name,
                'unified_name': self._map_to_standard_field(param_name),
                'type': param.get('类型'),
                'description': param.get('描述')
            }
            unified.append(unified_param)
        return unified
    
    def _map_to_standard_field(self, field: str) -> str:
        """映射到标准字段"""
        # 从字段等价关系中查找
        for standard_field, equivalents in self.field_equivalents.items():
            if field in equivalents or field.lower() == standard_field.lower():
                return standard_field
        # 如果找不到，返回原字段
        return field
    
    def unify_input(self, api_name: str, user_input: Dict[str, Any]) -> Dict[str, Any]:
        """统一输入参数"""
        # 直接构建参数映射，不依赖缓存
        # 为测试目的，我们直接处理输入
        unified_input = {}
        
        for key, value in user_input.items():
            # 映射到标准字段
            unified_key = self._map_to_standard_field(key)
            # 标准化值
            normalized_value = self._normalize_value(unified_key, value)
            # 直接返回统一后的参数
            unified_input[unified_key] = normalized_value
        
        return unified_input
    
    def unify_output(self, api_name: str, api_output: Any) -> Dict[str, Any]:
        """统一输出结果"""
        unified = {
            'data': [],
            'type': 'unknown'
        }
        
        # 检查是否是DataFrame或类似DataFrame的对象
        if hasattr(api_output, 'to_dict'):
            # 转换DataFrame为字典列表
            try:
                records = api_output.to_dict('records')
                unified_records = []
                
                for record in records:
                    unified_record = {}
                    for field, value in record.items():
                        unified_field = self._map_to_standard_field(field)
                        normalized_value = self._normalize_value(unified_field, value)
                        unified_record[unified_field] = normalized_value
                    unified_records.append(unified_record)
                
                unified['data'] = unified_records
                unified['type'] = 'DataFrame'
            except Exception as e:
                print(f"处理DataFrame失败: {e}")
        
        elif isinstance(api_output, dict):
            unified_record = {}
            for field, value in api_output.items():
                unified_field = self._map_to_standard_field(field)
                normalized_value = self._normalize_value(unified_field, value)
                unified_record[unified_field] = normalized_value
            unified['data'] = unified_record
            unified['type'] = 'dict'
        
        # 处理模拟的DataFrame
        elif hasattr(api_output, 'data') and hasattr(api_output, 'columns'):
            # 模拟DataFrame处理
            records = []
            if api_output.columns:
                for i in range(len(api_output.data[api_output.columns[0]])):
                    record = {}
                    for col in api_output.columns:
                        record[col] = api_output.data[col][i]
                    records.append(record)
            
            unified_records = []
            for record in records:
                unified_record = {}
                for field, value in record.items():
                    unified_field = self._map_to_standard_field(field)
                    normalized_value = self._normalize_value(unified_field, value)
                    unified_record[unified_field] = normalized_value
                unified_records.append(unified_record)
            
            unified['data'] = unified_records
            unified['type'] = 'MockDataFrame'
        
        return unified
    
    def _normalize_value(self, field: str, value: Any) -> Any:
        """标准化值"""
        field_lower = field.lower()
        
        # 股票代码标准化
        if any(keyword in field_lower for keyword in ['symbol', 'code', 'stock', 'bond', 'fund', 'index']):
            return FieldFormatter.normalize_stock_code(value)
        
        # 日期标准化
        elif any(keyword in field_lower for keyword in ['date', 'time', 'year', 'month']):
            return FieldFormatter.normalize_date(value)
        
        # 数值标准化
        elif any(keyword in field_lower for keyword in ['price', 'amount', 'value', 'rate', 'percent']):
            if isinstance(value, str):
                return FieldFormatter.normalize_float(value)
        
        return value
    
    def execute_with_unification(self, api_name: str, user_input: Dict[str, Any]) -> Dict[str, Any]:
        """执行接口并统一处理"""
        import akshare as ak
        
        # 检查接口是否存在
        if not hasattr(ak, api_name):
            raise ValueError(f"接口 {api_name} 不存在")
        
        # 统一输入
        unified_input = self.unify_input(api_name, user_input)
        print(f"统一后的输入参数: {unified_input}")
        
        # 执行接口
        func = getattr(ak, api_name)
        try:
            output = func(**unified_input)
            print("接口执行成功!")
        except Exception as e:
            print(f"接口执行失败: {e}")
            raise
        
        # 统一输出
        unified_output = self.unify_output(api_name, output)
        print("输出已统一处理")
        
        return unified_output
    
    def _save_unified_system(self):
        """保存统一系统配置"""
        unified_dir = os.path.join(self.output_dir, "unified_system")
        os.makedirs(unified_dir, exist_ok=True)
        
        # 保存输入映射
        with open(os.path.join(unified_dir, 'input_mapping.json'), 'w', encoding='utf-8') as f:
            json.dump(self.input_mapping, f, ensure_ascii=False, indent=2)
        
        # 保存输出映射
        with open(os.path.join(unified_dir, 'output_mapping.json'), 'w', encoding='utf-8') as f:
            json.dump(self.output_mapping, f, ensure_ascii=False, indent=2)
        
        # 保存统一系统配置
        config = {
            'input_mapping': self.input_mapping,
            'output_mapping': self.output_mapping,
            'field_equivalents': self.field_mapper.field_equivalents
        }
        
        with open(os.path.join(unified_dir, 'unified_system_config.json'), 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
    
    def test_unified_system(self, test_cases: List[Dict[str, Any]]):
        """测试统一系统"""
        print("测试统一字段系统...")
        
        test_results = []
        for test_case in test_cases:
            api_name = test_case.get('api_name')
            user_input = test_case.get('input', {})
            
            print(f"测试接口: {api_name}")
            print(f"输入: {user_input}")
            
            try:
                result = self.execute_with_unification(api_name, user_input)
                test_results.append({
                    'api_name': api_name,
                    'success': True,
                    'input': user_input,
                    'output': result,
                    'error': None
                })
                print("测试成功!")
            except Exception as e:
                test_results.append({
                    'api_name': api_name,
                    'success': False,
                    'input': user_input,
                    'output': None,
                    'error': str(e)
                })
                print(f"测试失败: {e}")
            
            print()
        
        # 保存测试结果
        unified_dir = os.path.join(self.output_dir, "unified_system")
        with open(os.path.join(unified_dir, 'test_results.json'), 'w', encoding='utf-8') as f:
            json.dump(test_results, f, ensure_ascii=False, indent=2)
        
        # 统计测试结果
        success_count = sum(1 for r in test_results if r['success'])
        total_count = len(test_results)
        
        print(f"测试完成! 成功: {success_count}/{total_count}")
        return test_results


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='统一字段系统')
    parser.add_argument('--build', action='store_true', help='构建统一字段系统')
    parser.add_argument('--test', action='store_true', help='测试统一字段系统')
    parser.add_argument('--data-dir', default='data', help='数据目录')
    parser.add_argument('--output-dir', default='result', help='输出目录')
    
    args = parser.parse_args()
    
    system = UnifiedFieldSystem(data_dir=args.data_dir, output_dir=args.output_dir)
    
    if args.build:
        system.build_unified_system()
    elif args.test:
        # 测试用例
        test_cases = [
            {
                'api_name': 'stock_zh_a_hist',
                'input': {
                    'symbol': '000001',
                    'start_date': '2024-01-01',
                    'end_date': '2024-01-10'
                }
            },
            {
                'api_name': 'macro_china_cpi_monthly',
                'input': {}
            }
        ]
        system.test_unified_system(test_cases)
    else:
        print("统一字段系统")
        print("使用方式:")
        print("  构建系统: python unified_field_system.py --build")
        print("  测试系统: python unified_field_system.py --test")


if __name__ == "__main__":
    main()
