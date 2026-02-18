#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
接口返回值分析器
获取每个接口的返回样例，分析字段结构，构建字段映射
"""

import json
import os
import sys
import time
from typing import Dict, Any, List
import pandas as pd

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mappers.field_mapper import FieldMapper
from formatters.field_formatter import FieldFormatter


class InterfaceResponseAnalyzer:
    """接口返回值分析器"""
    
    def __init__(self, output_dir: str = "result/response_analysis"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.field_mapper = FieldMapper()
        self.response_samples = {}
        self.field_analysis = {}
    
    def _load_interface_mapping(self) -> Dict[str, Any]:
        """加载接口映射数据"""
        mapping_file = "result/complete_field_mapping.json"
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}
    
    def get_interface_sample(self, api_name: str, timeout: int = 10) -> Dict[str, Any]:
        """获取接口返回样例"""
        import akshare as ak
        import signal
        
        class TimeoutException(Exception):
            pass
        
        def timeout_handler(signum, frame):
            raise TimeoutException("执行超时")
        
        result = {
            'api_name': api_name,
            'success': False,
            'error': None,
            'response_type': None,
            'fields': [],
            'sample_data': None
        }
        
        try:
            # 设置超时
            if hasattr(signal, 'SIGALRM'):
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(timeout)
            
            # 检查接口是否存在
            if not hasattr(ak, api_name):
                result['error'] = "接口不存在"
                return result
            
            # 尝试调用接口（使用默认参数）
            func = getattr(ak, api_name)
            response = func()
            
            # 关闭超时
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
            
            result['success'] = True
            result['response_type'] = type(response).__name__
            
            # 提取字段信息
            if isinstance(response, pd.DataFrame):
                result['fields'] = list(response.columns)
                # 保存前几行作为样例
                if len(response) > 0:
                    sample = response.head(3).to_dict('records')
                    result['sample_data'] = sample
            elif isinstance(response, dict):
                result['fields'] = list(response.keys())
                # 保存字典前几个键值对作为样例
                sample = {k: v for k, v in list(response.items())[:5]}
                result['sample_data'] = sample
            
        except TimeoutException:
            result['error'] = "执行超时"
        except Exception as e:
            result['error'] = str(e)
        finally:
            # 确保关闭超时
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
        
        return result
    
    def analyze_all_interfaces(self, limit: int = None, delay: float = 0.5):
        """分析所有接口"""
        mapping_data = self._load_interface_mapping()
        interfaces = mapping_data.get('interfaces', {})
        
        print(f"开始分析 {len(interfaces)} 个接口...")
        
        count = 0
        for api_name in interfaces.keys():
            if limit and count >= limit:
                break
            
            print(f"[{count+1}/{len(interfaces)}] 分析接口: {api_name}")
            sample = self.get_interface_sample(api_name)
            self.response_samples[api_name] = sample
            
            # 分析字段
            if sample['success']:
                self._analyze_fields(api_name, sample['fields'])
            
            count += 1
            time.sleep(delay)
        
        # 保存结果
        self._save_results()
        
        print(f"分析完成! 成功: {sum(1 for s in self.response_samples.values() if s['success'])}")
        print(f"失败: {sum(1 for s in self.response_samples.values() if not s['success'])}")
    
    def _analyze_fields(self, api_name: str, fields: List[str]):
        """分析字段"""
        for field in fields:
            if field not in self.field_analysis:
                self.field_analysis[field] = {
                    'interfaces': [],
                    'standard_field': None
                }
            self.field_analysis[field]['interfaces'].append(api_name)
            
            # 尝试匹配标准字段
            if not self.field_analysis[field]['standard_field']:
                for standard_field, equivalents in self.field_mapper.field_equivalents.items():
                    if field in equivalents or field.lower() == standard_field.lower():
                        self.field_analysis[field]['standard_field'] = standard_field
                        break
    
    def _save_results(self):
        """保存分析结果"""
        # 准备可序列化的响应样例
        serializable_samples = {}
        for api_name, sample in self.response_samples.items():
            serializable_sample = sample.copy()
            # 处理不可序列化的数据
            if 'sample_data' in serializable_sample and serializable_sample['sample_data']:
                if isinstance(serializable_sample['sample_data'], list):
                    serializable_sample['sample_data'] = [
                        self._make_serializable(record)
                        for record in serializable_sample['sample_data']
                    ]
                elif isinstance(serializable_sample['sample_data'], dict):
                    serializable_sample['sample_data'] = self._make_serializable(serializable_sample['sample_data'])
            serializable_samples[api_name] = serializable_sample
        
        # 保存响应样例
        with open(os.path.join(self.output_dir, 'response_samples.json'), 'w', encoding='utf-8') as f:
            json.dump(serializable_samples, f, ensure_ascii=False, indent=2)
        
        # 保存字段分析
        with open(os.path.join(self.output_dir, 'field_analysis.json'), 'w', encoding='utf-8') as f:
            json.dump(self.field_analysis, f, ensure_ascii=False, indent=2)
        
        # 保存字段映射表（CSV）
        field_mapping_data = []
        for field, info in self.field_analysis.items():
            field_mapping_data.append({
                'original_field': field,
                'standard_field': info['standard_field'],
                'interfaces': ', '.join(info['interfaces'][:10]),  # 只保留前10个接口
                'interface_count': len(info['interfaces'])
            })
        
        df = pd.DataFrame(field_mapping_data)
        df.to_csv(os.path.join(self.output_dir, 'field_mapping.csv'), index=False, encoding='utf-8-sig')
        
        # 保存接口字段表
        interface_field_data = []
        for api_name, sample in self.response_samples.items():
            if sample['success']:
                interface_field_data.append({
                    'api_name': api_name,
                    'fields': ', '.join(sample['fields']),
                    'field_count': len(sample['fields']),
                    'success': True
                })
            else:
                interface_field_data.append({
                    'api_name': api_name,
                    'fields': '',
                    'field_count': 0,
                    'success': False,
                    'error': sample['error']
                })
        
        df = pd.DataFrame(interface_field_data)
        df.to_csv(os.path.join(self.output_dir, 'interface_fields.csv'), index=False, encoding='utf-8-sig')
    
    def _make_serializable(self, data):
        """将数据转换为可序列化的格式"""
        import datetime
        
        if isinstance(data, dict):
            return {k: self._make_serializable(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._make_serializable(item) for item in data]
        elif isinstance(data, (datetime.datetime, datetime.date)):
            return str(data)
        elif isinstance(data, (pd.Timestamp, pd.Timedelta)):
            return str(data)
        elif pd.isna(data):
            return None
        else:
            return data
    
    def generate_unified_schema(self):
        """生成统一的字段模式"""
        unified_schema = {
            'standard_fields': {},
            'field_mappings': {}
        }
        
        # 构建标准字段
        for field, info in self.field_analysis.items():
            standard_field = info['standard_field'] or field
            if standard_field not in unified_schema['standard_fields']:
                unified_schema['standard_fields'][standard_field] = {
                    'original_fields': [],
                    'interfaces': []
                }
            unified_schema['standard_fields'][standard_field]['original_fields'].append(field)
            unified_schema['standard_fields'][standard_field]['interfaces'].extend(info['interfaces'])
        
        # 去重
        for standard_field, info in unified_schema['standard_fields'].items():
            info['original_fields'] = list(set(info['original_fields']))
            info['interfaces'] = list(set(info['interfaces']))
        
        # 构建字段映射
        for field, info in self.field_analysis.items():
            standard_field = info['standard_field'] or field
            unified_schema['field_mappings'][field] = standard_field
        
        # 保存统一模式
        with open(os.path.join(self.output_dir, 'unified_schema.json'), 'w', encoding='utf-8') as f:
            json.dump(unified_schema, f, ensure_ascii=False, indent=2)
        
        return unified_schema

def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='接口返回值分析器')
    parser.add_argument('--limit', type=int, help='限制分析接口数量')
    parser.add_argument('--delay', type=float, default=0.5, help='接口调用延迟（秒）')
    parser.add_argument('--output-dir', default='result/response_analysis', help='输出目录')
    
    args = parser.parse_args()
    
    analyzer = InterfaceResponseAnalyzer(output_dir=args.output_dir)
    analyzer.analyze_all_interfaces(limit=args.limit, delay=args.delay)
    analyzer.generate_unified_schema()
    
    print(f"分析结果已保存到: {args.output_dir}")


if __name__ == "__main__":
    main()
