#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一字段系统测试框架
全面测试输入输出字段统一化功能
"""

import json
import os
import sys
import unittest
from typing import Dict, Any, List

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from unified_field_system import UnifiedFieldSystem
from mappers.field_mapper import FieldMapper
from formatters.field_formatter import FieldFormatter


class TestUnifiedFieldSystem(unittest.TestCase):
    """统一字段系统测试"""
    
    def setUp(self):
        """设置测试环境"""
        self.system = UnifiedFieldSystem()
        self.field_mapper = FieldMapper()
        self.field_formatter = FieldFormatter()
    
    def test_field_mapping(self):
        """测试字段映射功能"""
        print("测试字段映射功能...")
        
        # 测试标准字段映射
        test_fields = [
            ("symbol", "代码"),
            ("code", "代码"),
            ("股票代码", "symbol"),
            ("date", "日期"),
            ("日期", "date"),
            ("name", "名称"),
            ("名称", "name"),
        ]
        
        for input_field, expected in test_fields:
            result = self.system._map_to_standard_field(input_field)
            print(f"映射 '{input_field}' → '{result}' (期望: '{expected}')")
            # 检查是否映射到了预期的标准字段或其等价字段
            mapped_correctly = False
            for standard, equivalents in self.field_mapper.field_equivalents.items():
                if result == standard and (expected == standard or expected in equivalents):
                    mapped_correctly = True
                    break
            self.assertTrue(mapped_correctly, f"字段 '{input_field}' 映射不正确: {result}")
    
    def test_input_unification(self):
        """测试输入参数统一化"""
        print("测试输入参数统一化...")
        
        # 测试用例
        test_cases = [
            {
                'api_name': 'stock_zh_a_hist',
                'input': {
                    'symbol': 'sz000001',
                    'start_date': '2024年1月1日',
                    'end_date': '2024-01-10'
                },
                'expected_fields': ['symbol', 'start_date', 'end_date']
            },
            {
                'api_name': 'macro_china_cpi_monthly',
                'input': {},
                'expected_fields': []
            }
        ]
        
        for test_case in test_cases:
            api_name = test_case['api_name']
            user_input = test_case['input']
            expected_fields = test_case['expected_fields']
            
            print(f"测试接口: {api_name}")
            print(f"输入: {user_input}")
            
            unified_input = self.system.unify_input(api_name, user_input)
            print(f"统一后: {unified_input}")
            
            # 检查统一后的参数是否有效
            self.assertIsInstance(unified_input, dict)
            # 检查是否包含预期字段
            for field in expected_fields:
                self.assertIn(field, unified_input)
    
    def test_output_unification(self):
        """测试输出结果统一化"""
        print("测试输出结果统一化...")
        
        # 测试用例 - 模拟DataFrame输出
        test_dataframe = {
            '日期': ['2024-01-01', '2024-01-02'],
            '股票代码': ['000001', '000002'],
            '收盘价': [10.0, 20.0],
            '涨跌幅': ['1.0%', '2.0%']
        }
        
        # 转换为模拟的DataFrame结构
        class MockDataFrame:
            def __init__(self, data):
                self.data = data
                self.columns = list(data.keys())
            def to_dict(self, format):
                records = []
                for i in range(len(self.data[self.columns[0]])):
                    record = {}
                    for col in self.columns:
                        record[col] = self.data[col][i]
                    records.append(record)
                return records
        
        mock_df = MockDataFrame(test_dataframe)
        unified_output = self.system.unify_output('test_api', mock_df)
        
        print(f"原始输出: {test_dataframe}")
        print(f"统一后: {unified_output}")
        
        # 检查统一结果
        self.assertIsInstance(unified_output, dict)
        self.assertIn('data', unified_output)
        self.assertIsInstance(unified_output['data'], list)
        
        # 检查字段是否统一
        for record in unified_output['data']:
            self.assertIn('date', record)  # 日期应映射到date
            self.assertIn('symbol', record)  # 股票代码应映射到symbol
            self.assertIn('close', record)  # 收盘价应映射到close
            self.assertIn('涨跌幅', record)  # 涨跌幅映射
    
    def test_value_normalization(self):
        """测试值标准化功能"""
        print("测试值标准化功能...")
        
        # 测试股票代码标准化
        stock_codes = [
            ('000001.SZ', '000001'),
            ('sh600000', '600000'),
            ('600000', '600000')
        ]
        
        for input_code, expected in stock_codes:
            result = self.field_formatter.normalize_stock_code(input_code)
            print(f"股票代码标准化 '{input_code}' → '{result}'")
            self.assertEqual(result, expected)
        
        # 测试日期标准化
        dates = [
            ('20240101', '2024-01-01'),
            ('2024-01-01', '2024-01-01'),
            ('2024年1月1日', '2024-01-01')
        ]
        
        for input_date, expected in dates:
            result = self.field_formatter.normalize_date(input_date)
            print(f"日期标准化 '{input_date}' → '{result}'")
            self.assertEqual(result, expected)
        
        # 测试数值标准化
        values = [
            ('1,234.56', 1234.56),
            ('100%', 1.0),
            ('-78.9', -78.9)
        ]
        
        for input_value, expected in values:
            result = self.field_formatter.normalize_float(input_value)
            print(f"数值标准化 '{input_value}' → '{result}'")
            self.assertEqual(result, expected)
    
    def test_integration(self):
        """测试集成功能"""
        print("测试集成功能...")
        
        # 测试完整的统一流程
        test_input = {
            'symbol': 'sz000001',
            'start_date': '2024年1月1日',
            'end_date': '2024-01-10'
        }
        
        print(f"原始输入: {test_input}")
        unified_input = self.system.unify_input('stock_zh_a_hist', test_input)
        print(f"统一输入: {unified_input}")
        
        # 检查统一后的输入
        self.assertIsInstance(unified_input, dict)
        # 检查股票代码是否标准化
        self.assertEqual(unified_input.get('symbol'), '000001')


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("统一字段系统测试")
    print("=" * 60)
    
    # 运行单元测试
    unittest.main(argv=['first-arg-is-ignored'], exit=False)
    
    # 运行集成测试
    print("\n" + "=" * 60)
    print("集成测试")
    print("=" * 60)
    
    # 测试统一系统构建
    print("测试统一系统构建...")
    system = UnifiedFieldSystem()
    try:
        # 构建系统（限制接口数量以加快测试速度）
        print("构建统一字段系统...")
        # 注意：这里不实际构建，因为会调用外部接口
        print("统一字段系统构建测试跳过（会调用外部接口）")
    except Exception as e:
        print(f"构建测试失败: {e}")
    
    print("\n" + "=" * 60)
    print("测试完成!")
    print("=" * 60)


def main():
    """主函数"""
    run_all_tests()


if __name__ == "__main__":
    main()
