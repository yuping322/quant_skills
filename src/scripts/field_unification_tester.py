#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段统一化测试器
测试不同接口返回字段的统一化映射
"""

import json
import os
import sys
from typing import Dict, Any, List
import pandas as pd

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mappers.field_mapper import FieldMapper
from formatters.field_formatter import FieldFormatter
from interface_response_analyzer import InterfaceResponseAnalyzer


class FieldUnificationTester:
    """字段统一化测试器"""
    
    def __init__(self, analysis_dir: str = "result/response_analysis"):
        self.analysis_dir = analysis_dir
        self.field_mapper = FieldMapper()
        self.unified_schema = None
    
    def load_unified_schema(self) -> Dict[str, Any]:
        """加载统一模式"""
        schema_file = os.path.join(self.analysis_dir, 'unified_schema.json')
        if os.path.exists(schema_file):
            with open(schema_file, 'r', encoding='utf-8') as f:
                self.unified_schema = json.load(f)
                return self.unified_schema
        return {}
    
    def unify_interface_response(self, api_name: str, response: Any) -> Dict[str, Any]:
        """统一接口返回值"""
        unified = {}
        
        if isinstance(response, pd.DataFrame):
            # 转换DataFrame为字典列表
            records = response.to_dict('records')
            unified_records = []
            
            for record in records:
                unified_record = self._unify_record(record)
                unified_records.append(unified_record)
            
            unified['data'] = unified_records
            unified['type'] = 'DataFrame'
        
        elif isinstance(response, dict):
            unified['data'] = self._unify_record(response)
            unified['type'] = 'dict'
        
        return unified
    
    def _unify_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """统一单条记录"""
        unified = {}
        
        for field, value in record.items():
            # 映射到标准字段
            standard_field = self._map_to_standard_field(field)
            if standard_field:
                # 标准化值
                normalized_value = self._normalize_value(standard_field, value)
                unified[standard_field] = normalized_value
        
        return unified
    
    def _map_to_standard_field(self, field: str) -> str:
        """映射到标准字段"""
        # 先从统一模式中查找
        if self.unified_schema and 'field_mappings' in self.unified_schema:
            return self.unified_schema['field_mappings'].get(field, field)
        
        # 再从字段等价关系中查找
        for standard_field, equivalents in self.field_mapper.field_equivalents.items():
            if field in equivalents or field.lower() == standard_field.lower():
                return standard_field
        
        # 如果找不到，返回原字段
        return field
    
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
    
    def test_field_unification(self, limit: int = 10):
        """测试字段统一化"""
        # 先加载或生成分析结果
        if not os.path.exists(os.path.join(self.analysis_dir, 'unified_schema.json')):
            print("正在分析接口返回值...")
            analyzer = InterfaceResponseAnalyzer(output_dir=self.analysis_dir)
            analyzer.analyze_all_interfaces(limit=limit)
            analyzer.generate_unified_schema()
        
        # 加载统一模式
        self.load_unified_schema()
        
        # 加载响应样例
        samples_file = os.path.join(self.analysis_dir, 'response_samples.json')
        if not os.path.exists(samples_file):
            print("响应样例文件不存在")
            return
        
        with open(samples_file, 'r', encoding='utf-8') as f:
            samples = json.load(f)
        
        print(f"开始测试字段统一化，共 {len(samples)} 个接口...")
        
        # 测试统一化
        test_results = {}
        field_statistics = {}
        
        for api_name, sample in samples.items():
            if not sample['success']:
                continue
            
            print(f"测试接口: {api_name}")
            
            # 模拟统一化处理
            if sample['sample_data']:
                unified_data = []
                for record in sample['sample_data']:
                    unified_record = self._unify_record(record)
                    unified_data.append(unified_record)
                
                test_results[api_name] = {
                    'original_fields': sample['fields'],
                    'unified_fields': list(set(k for record in unified_data for k in record.keys())),
                    'sample_count': len(unified_data)
                }
                
                # 统计字段
                for field in test_results[api_name]['unified_fields']:
                    if field not in field_statistics:
                        field_statistics[field] = 0
                    field_statistics[field] += 1
        
        # 保存测试结果
        self._save_test_results(test_results, field_statistics)
        
        # 生成统一字段报告
        self._generate_unified_report(field_statistics)
        
        print("字段统一化测试完成!")
        print(f"测试了 {len(test_results)} 个接口")
        print(f"识别了 {len(field_statistics)} 个统一字段")
    
    def _save_test_results(self, test_results: Dict[str, Any], field_statistics: Dict[str, int]):
        """保存测试结果"""
        # 保存测试结果
        with open(os.path.join(self.analysis_dir, 'field_unification_test.json'), 'w', encoding='utf-8') as f:
            json.dump(test_results, f, ensure_ascii=False, indent=2)
        
        # 保存字段统计
        field_stats_df = pd.DataFrame([
            {'field': field, 'count': count}
            for field, count in field_statistics.items()
        ]).sort_values('count', ascending=False)
        
        field_stats_df.to_csv(
            os.path.join(self.analysis_dir, 'field_statistics.csv'), 
            index=False, 
            encoding='utf-8-sig'
        )
        
        # 保存接口统一化结果
        test_results_df = pd.DataFrame([
            {
                'api_name': api_name,
                'original_field_count': len(info['original_fields']),
                'unified_field_count': len(info['unified_fields']),
                'sample_count': info['sample_count'],
                'unified_fields': ', '.join(info['unified_fields'][:10])
            }
            for api_name, info in test_results.items()
        ])
        
        test_results_df.to_csv(
            os.path.join(self.analysis_dir, 'unification_test_results.csv'), 
            index=False, 
            encoding='utf-8-sig'
        )
    
    def _generate_unified_report(self, field_statistics: Dict[str, int]):
        """生成统一字段报告"""
        # 统计最常见的统一字段
        common_fields = sorted(field_statistics.items(), key=lambda x: x[1], reverse=True)[:20]
        
        print("\n最常见的统一字段:")
        print("=" * 60)
        for field, count in common_fields:
            print(f"{field:30} | 出现次数: {count}")
        print("=" * 60)
        
        # 生成报告
        report = {
            'common_fields': common_fields,
            'total_fields': len(field_statistics),
            'summary': f"识别了 {len(field_statistics)} 个统一字段，其中前20个最常见字段覆盖了大部分接口"
        }
        
        with open(os.path.join(self.analysis_dir, 'unified_field_report.json'), 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
    
    def verify_field_consistency(self):
        """验证字段一致性"""
        # 加载测试结果
        test_file = os.path.join(self.analysis_dir, 'field_unification_test.json')
        if not os.path.exists(test_file):
            print("测试结果文件不存在，请先运行测试")
            return
        
        with open(test_file, 'r', encoding='utf-8') as f:
            test_results = json.load(f)
        
        # 分析字段一致性
        all_fields = set()
        field_consistency = {}
        
        for api_name, info in test_results.items():
            fields = set(info['unified_fields'])
            all_fields.update(fields)
            
            for field in fields:
                if field not in field_consistency:
                    field_consistency[field] = []
                field_consistency[field].append(api_name)
        
        # 计算一致性指标
        consistency_report = {
            'total_unified_fields': len(all_fields),
            'field_coverage': {},
            'consistency_score': {}
        }
        
        total_apis = len(test_results)
        for field, apis in field_consistency.items():
            coverage = len(apis) / total_apis
            consistency_report['field_coverage'][field] = {
                'coverage': coverage,
                'apis': apis
            }
            consistency_report['consistency_score'][field] = coverage
        
        # 保存一致性报告
        with open(os.path.join(self.analysis_dir, 'field_consistency_report.json'), 'w', encoding='utf-8') as f:
            json.dump(consistency_report, f, ensure_ascii=False, indent=2)
        
        # 输出一致性分析
        print("\n字段一致性分析:")
        print("=" * 60)
        print(f"总统一字段数: {len(all_fields)}")
        print(f"测试接口数: {total_apis}")
        print()
        
        # 显示覆盖率最高的字段
        top_fields = sorted(
            consistency_report['field_coverage'].items(),
            key=lambda x: x[1]['coverage'],
            reverse=True
        )[:10]
        
        print("覆盖率最高的字段:")
        for field, info in top_fields:
            print(f"{field:30} | 覆盖率: {info['coverage']:.2f} | 接口数: {len(info['apis'])}")
        
        print("=" * 60)

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='字段统一化测试器')
    parser.add_argument('--limit', type=int, default=20, help='限制测试接口数量')
    parser.add_argument('--analysis-dir', default='result/response_analysis', help='分析结果目录')
    parser.add_argument('--verify', action='store_true', help='验证字段一致性')
    
    args = parser.parse_args()
    
    tester = FieldUnificationTester(analysis_dir=args.analysis_dir)
    
    if args.verify:
        tester.verify_field_consistency()
    else:
        tester.test_field_unification(limit=args.limit)


if __name__ == "__main__":
    main()
