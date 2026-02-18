#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段等价关系验证工具
验证field_mapper.py中写死的字段等价关系是否正确
"""

import json
import os
import sys
from collections import Counter
from typing import Dict, List, Set, Tuple

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mappers.field_mapper import FieldMapper


class FieldEquivalentsValidator:
    """字段等价关系验证器"""
    
    def __init__(self):
        self.mapper = FieldMapper()
        self.field_equivalents = self.mapper.field_equivalents
        self.interface_data = {}
        self.field_statistics = Counter()
        self.actual_field_groups = {}
        
    def load_interface_data(self):
        """加载接口数据"""
        print("加载接口数据...")
        
        # 尝试从生成的映射文件中加载
        mapping_file = "result/complete_field_mapping.json"
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
                self.interface_data = mapping_data.get('interfaces', {})
        else:
            # 如果没有映射文件，重新解析
            self.interface_data = self.mapper.parse_all_interfaces()
            self.mapper.build_mapping(self.interface_data)
        
        print(f"加载了 {len(self.interface_data)} 个接口")
        return self.interface_data
    
    def collect_all_fields(self) -> Counter:
        """收集所有出现的字段"""
        print("收集字段统计...")
        
        all_fields = []
        
        for api_name, interface in self.interface_data.items():
            # 收集输入参数
            for param in interface.get('input_params', []):
                field_name = param.get('名称')
                if field_name and field_name != '-' and field_name != '无':
                    all_fields.append(field_name)
            
            # 收集输出参数
            for param in interface.get('output_params', []):
                field_name = param.get('名称')
                if field_name and field_name != '-' and field_name != '无':
                    all_fields.append(field_name)
        
        self.field_statistics = Counter(all_fields)
        print(f"收集了 {len(self.field_statistics)} 个不同的字段")
        return self.field_statistics
    
    def analyze_equivalents_coverage(self):
        """分析等价关系的覆盖情况"""
        print("\n分析等价关系覆盖...")
        
        covered_fields = set()
        uncovered_fields = []
        
        # 计算已覆盖的字段
        for canonical, equivalents in self.field_equivalents.items():
            covered_fields.add(canonical)
            covered_fields.update(equivalents)
        
        # 找出未覆盖的字段
        for field, count in self.field_statistics.most_common():
            if field not in covered_fields:
                uncovered_fields.append((field, count))
        
        print(f"\n等价关系覆盖分析:")
        print(f"  总字段数: {len(self.field_statistics)}")
        print(f"  已覆盖字段数: {len(covered_fields)}")
        print(f"  未覆盖字段数: {len(uncovered_fields)}")
        
        if uncovered_fields:
            print(f"\n  最常见的未覆盖字段 (前20):")
            for i, (field, count) in enumerate(uncovered_fields[:20], 1):
                print(f"    {i:2d}. {field:30} (出现 {count} 次)")
        
        return {
            'covered_fields': covered_fields,
            'uncovered_fields': uncovered_fields
        }
    
    def validate_equivalents_consistency(self):
        """验证等价关系的一致性"""
        print("\n验证等价关系一致性...")
        
        issues = []
        field_to_standard = {}
        
        # 构建字段到标准字段的映射
        for canonical, equivalents in self.field_equivalents.items():
            if canonical in field_to_standard and field_to_standard[canonical] != canonical:
                issues.append(f"字段 '{canonical}' 同时被定义为标准字段和 '{field_to_standard[canonical]}' 的别名")
            field_to_standard[canonical] = canonical
            
            for alias in equivalents:
                if alias in field_to_standard:
                    if field_to_standard[alias] != canonical:
                        issues.append(f"字段 '{alias}' 同时映射到 '{field_to_standard[alias]}' 和 '{canonical}'")
                else:
                    field_to_standard[alias] = canonical
        
        # 检查循环引用
        for canonical, equivalents in self.field_equivalents.items():
            for alias in equivalents:
                if alias in self.field_equivalents:
                    if canonical in self.field_equivalents[alias]:
                        issues.append(f"循环等价关系: '{canonical}' ↔ '{alias}'")
        
        if issues:
            print(f"\n发现 {len(issues)} 个一致性问题:")
            for i, issue in enumerate(issues, 1):
                print(f"  {i}. {issue}")
        else:
            print("  ✓ 没有发现一致性问题")
        
        return issues
    
    def suggest_missing_equivalents(self):
        """建议缺失的等价关系"""
        print("\n分析可能缺失的等价关系...")
        
        suggestions = []
        
        # 找出相似的字段名（基于相似度）
        field_list = list(self.field_statistics.keys())
        
        # 简单的相似度规则：去掉标点符号，检查是否相似
        for i in range(len(field_list)):
            for j in range(i + 1, len(field_list)):
                field1 = field_list[i]
                field2 = field_list[j]
                
                # 检查是否应该是等价关系
                if self._should_be_equivalent(field1, field2):
                    # 检查是否已经在等价关系中
                    already_equivalent = False
                    for canonical, equivalents in self.field_equivalents.items():
                        if (field1 == canonical or field1 in equivalents) and \
                           (field2 == canonical or field2 in equivalents):
                            already_equivalent = True
                            break
                    
                    if not already_equivalent:
                        suggestions.append((field1, field2, 
                                            self.field_statistics[field1], 
                                            self.field_statistics[field2]))
        
        if suggestions:
            print(f"\n发现 {len(suggestions)} 对可能缺失的等价关系:")
            for i, (field1, field2, count1, count2) in enumerate(suggestions[:20], 1):
                print(f"  {i:2d}. '{field1}' ({count1}次) ↔ '{field2}' ({count2}次)")
        else:
            print("  ✓ 没有发现明显缺失的等价关系")
        
        return suggestions
    
    def _should_be_equivalent(self, field1: str, field2: str) -> bool:
        """判断两个字段是否应该是等价关系"""
        # 去掉空格和标点符号
        def normalize(s):
            return ''.join(c for c in s.lower() if c.isalnum())
        
        norm1 = normalize(field1)
        norm2 = normalize(field2)
        
        # 完全相同的标准化形式
        if norm1 == norm2:
            return True
        
        # 包含关系（一个是另一个的子集）
        if norm1 in norm2 or norm2 in norm1:
            # 但不能太短，避免误判
            if len(norm1) >= 3 and len(norm2) >= 3:
                return True
        
        return False
    
    def generate_validation_report(self):
        """生成验证报告"""
        print("\n" + "="*80)
        print("字段等价关系验证报告")
        print("="*80)
        
        # 1. 加载数据
        self.load_interface_data()
        self.collect_all_fields()
        
        # 2. 分析覆盖情况
        coverage = self.analyze_equivalents_coverage()
        
        # 3. 验证一致性
        consistency_issues = self.validate_equivalents_consistency()
        
        # 4. 建议缺失的等价关系
        suggestions = self.suggest_missing_equivalents()
        
        # 5. 保存报告
        report = {
            'field_statistics': dict(self.field_statistics.most_common()),
            'coverage': {
                'total_fields': len(self.field_statistics),
                'covered_fields': len(coverage['covered_fields']),
                'uncovered_fields': coverage['uncovered_fields'][:50]  # 只保存前50个
            },
            'consistency_issues': consistency_issues,
            'suggestions': suggestions[:50]  # 只保存前50个
        }
        
        report_file = "result/field_equivalents_validation.json"
        os.makedirs(os.path.dirname(report_file), exist_ok=True)
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        
        print(f"\n验证报告已保存到: {report_file}")
        
        return report


def main():
    """主函数"""
    validator = FieldEquivalentsValidator()
    validator.generate_validation_report()


if __name__ == "__main__":
    main()
