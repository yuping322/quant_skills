#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
接口测试系统
测试所有数据接口是否可以正常工作
"""

import sys
import json
import os
import time
import traceback
import signal
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict
import pandas as pd

# 超时异常
class TimeoutException(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutException("函数执行超时")

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))


class InterfaceTester:
    """接口测试器"""
    
    def __init__(self, mapping_file: str = "result/complete_field_mapping.json", 
                 output_dir: str = "result"):
        self.mapping_file = mapping_file
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        self.mapping_data = None
        self._load_mapping()
        
        # 测试结果
        self.test_results = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'details': []
        }
        
        # 用于测试的示例参数
        self.test_params = self._get_default_test_params()
    
    def _load_mapping(self):
        """加载映射数据"""
        if os.path.exists(self.mapping_file):
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                self.mapping_data = json.load(f)
            print(f"✓ 已加载映射数据: {self.mapping_file}")
    
    def _get_default_test_params(self) -> Dict[str, Dict[str, Any]]:
        """获取默认测试参数"""
        return {
            'symbol': '000001',
            'date': '20240101',
            'start_date': '20240101',
            'end_date': '20241231',
            'adjust': 'qfq',
            'period': 'daily',
            'market': 'sh',
            'timeout': 10,
        }
    
    def test_single_interface(self, api_name: str, 
                            custom_params: Optional[Dict[str, Any]] = None,
                            timeout: int = 10) -> Dict[str, Any]:
        """
        测试单个接口
        
        Args:
            api_name: 接口名称
            custom_params: 自定义参数
            timeout: 超时时间（秒）
            
        Returns:
            测试结果
        """
        result = {
            'api_name': api_name,
            'success': False,
            'error': None,
            'duration': 0,
            'data_shape': None,
            'fields': [],
        }
        
        if not self.mapping_data:
            result['error'] = '映射数据未加载'
            return result
        
        interfaces = self.mapping_data.get('interfaces', {})
        if api_name not in interfaces:
            result['error'] = f'接口 {api_name} 不存在'
            return result
        
        # 获取接口信息
        interface = interfaces[api_name]
        interface_fields = self.mapping_data.get('interface_to_fields', {}).get(api_name, {})
        input_params = interface_fields.get('input', {})
        
        # 准备参数 - 更智能地选择参数，避免参数太多
        params = {}
        
        # 先尝试没有参数的情况
        try_without_params = True
        
        # 只添加最必要的参数
        required_params = ['symbol', 'date', 'period']
        for param_name in input_params.keys():
            if param_name == '-':
                continue
            # 只添加几个关键参数，避免接口需要太多参数
            if param_name in required_params and param_name in self.test_params:
                params[param_name] = self.test_params[param_name]
        
        # 应用自定义参数
        if custom_params:
            params.update(custom_params)
        
        # 尝试调用接口
        start_time = time.time()
        
        try:
            import akshare as ak
            
            # 设置超时信号（仅Unix可用）
            if hasattr(signal, 'SIGALRM'):
                signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(timeout)
            
            # 获取函数
            if not hasattr(ak, api_name):
                result['error'] = f'akshare 中没有 {api_name} 函数'
                return result
            
            func = getattr(ak, api_name)
            
            # 调用函数
            if params:
                df = func(**params)
            else:
                df = func()
            
            # 关闭超时
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
            
            # 检查返回值
            if df is None:
                result['error'] = '返回值为 None'
                return result
            
            if isinstance(df, pd.DataFrame):
                result['data_shape'] = df.shape
                result['fields'] = list(df.columns)
            elif isinstance(df, dict):
                result['data_shape'] = {'type': 'dict', 'keys': list(df.keys())}
                result['fields'] = list(df.keys())
            else:
                result['data_shape'] = {'type': str(type(df))}
            
            result['success'] = True
            
        except TimeoutException:
            result['error'] = f'执行超时({timeout}秒)'
        except Exception as e:
            result['error'] = str(e)
            result['traceback'] = traceback.format_exc()
        finally:
            # 确保关闭超时
            if hasattr(signal, 'SIGALRM'):
                signal.alarm(0)
            result['duration'] = round(time.time() - start_time, 3)
        
        return result
    
    def test_all_interfaces(self, limit: Optional[int] = None, 
                          data_types: Optional[List[str]] = None,
                          delay: float = 0.5,
                          timeout: int = 10) -> Dict[str, Any]:
        """
        测试所有接口
        
        Args:
            limit: 限制测试数量
            data_types: 限制数据类型
            delay: 每次测试之间的延迟（秒）
            timeout: 单个接口超时时间（秒）
            
        Returns:
            测试结果汇总
        """
        if not self.mapping_data:
            return {'error': '映射数据未加载'}
        
        interfaces = self.mapping_data.get('interfaces', {})
        
        # 筛选接口并优先选择简单的接口（名字短的，不包含 info, spot, em 等可能慢的词）
        test_list = []
        for name, info in interfaces.items():
            if data_types and info.get('data_type') not in data_types:
                continue
            test_list.append(name)
        
        # 优先选择简单的接口（名字不包含 info, spot, bond_info 等）
        def priority_score(name):
            # 优先选择常见的股票、指数接口
            score = 0
            if 'stock' in name and 'hist' in name:
                score += 100
            elif 'index' in name and 'hist' in name:
                score += 90
            elif 'macro' in name:
                score += 80
            # 排除可能很慢的接口
            if 'info' in name and 'bond' in name:
                score -= 100
            elif 'spot' in name:
                score -= 50
            return score
        
        test_list.sort(key=lambda x: (-priority_score(x), x))
        
        if limit:
            test_list = test_list[:limit]
        
        print(f"=" * 80)
        print(f"开始测试接口，共 {len(test_list)} 个")
        print(f"=" * 80)
        
        # 重置结果
        self.test_results = {
            'total': len(test_list),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'details': [],
            'by_data_type': defaultdict(lambda: {'total': 0, 'success': 0, 'failed': 0})
        }
        
        # 逐个测试
        for idx, api_name in enumerate(test_list, 1):
            print(f"\n[{idx}/{len(test_list)}] 测试: {api_name}")
            
            result = self.test_single_interface(api_name, timeout=timeout)
            self.test_results['details'].append(result)
            
            # 统计
            data_type = interfaces.get(api_name, {}).get('data_type', 'unknown')
            self.test_results['by_data_type'][data_type]['total'] += 1
            
            if result['success']:
                self.test_results['success'] += 1
                self.test_results['by_data_type'][data_type]['success'] += 1
                print(f"  ✓ 成功! 耗时: {result['duration']}s, 数据形状: {result['data_shape']}")
            else:
                self.test_results['failed'] += 1
                self.test_results['by_data_type'][data_type]['failed'] += 1
                print(f"  ✗ 失败: {result['error']}")
            
            # 延迟
            if delay > 0 and idx < len(test_list):
                time.sleep(delay)
        
        # 保存结果
        self._save_results()
        
        # 打印汇总
        self._print_summary()
        
        return self.test_results
    
    def _save_results(self):
        """保存测试结果"""
        # 1. 保存详细结果（JSON）
        json_path = os.path.join(self.output_dir, 'interface_test_results.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(self.test_results, f, ensure_ascii=False, indent=2)
        print(f"\n✓ 详细结果已保存到: {json_path}")
        
        # 2. 保存汇总表（CSV）
        summary_data = []
        for detail in self.test_results['details']:
            summary_data.append({
                '接口名': detail['api_name'],
                '是否成功': '成功' if detail['success'] else '失败',
                '耗时(s)': detail['duration'],
                '数据形状': str(detail['data_shape']),
                '返回字段数': len(detail.get('fields', [])),
                '错误信息': detail.get('error', ''),
            })
        
        df = pd.DataFrame(summary_data)
        csv_path = os.path.join(self.output_dir, 'interface_test_summary.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"✓ 汇总表已保存到: {csv_path}")
    
    def _print_summary(self):
        """打印测试汇总"""
        print("\n" + "=" * 80)
        print("测试汇总")
        print("=" * 80)
        print(f"  总接口数: {self.test_results['total']}")
        print(f"  成功: {self.test_results['success']}")
        print(f"  失败: {self.test_results['failed']}")
        print(f"  成功率: {self.test_results['success']/self.test_results['total']*100:.1f}%")
        
        print("\n按数据类型统计:")
        for data_type, stats in sorted(self.test_results['by_data_type'].items()):
            success_rate = stats['success'] / stats['total'] * 100 if stats['total'] > 0 else 0
            print(f"  {data_type:15} - 总数: {stats['total']:3}, 成功: {stats['success']:3}, 失败: {stats['failed']:3}, 成功率: {success_rate:5.1f}%")
        
        print("\n" + "=" * 80)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='接口测试系统')
    parser.add_argument('--test-all', action='store_true', help='测试所有接口')
    parser.add_argument('--test-single', help='测试单个接口')
    parser.add_argument('--limit', type=int, help='限制测试数量')
    parser.add_argument('--data-types', help='指定数据类型（逗号分隔）')
    parser.add_argument('--delay', type=float, default=0.3, help='测试延迟（秒）')
    parser.add_argument('--timeout', type=int, default=10, help='单个接口超时时间（秒）')
    
    args = parser.parse_args()
    
    tester = InterfaceTester()
    
    if args.test_all:
        data_types = args.data_types.split(',') if args.data_types else None
        tester.test_all_interfaces(
            limit=args.limit,
            data_types=data_types,
            delay=args.delay,
            timeout=args.timeout
        )
    elif args.test_single:
        result = tester.test_single_interface(args.test_single, timeout=args.timeout)
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        # 默认测试几个示例
        print("=" * 80)
        print("接口测试系统")
        print("=" * 80)
        print("\n使用方式:")
        print("  测试单个接口: python3 interface_tester.py --test-single <接口名>")
        print("  测试所有接口: python3 interface_tester.py --test-all")
        print("  限制数量:   python3 interface_tester.py --test-all --limit 10")
        print("  指定类型:   python3 interface_tester.py --test-all --data-types stock,index")
        print("\n先测试几个示例接口...")
        
        # 测试几个示例
        example_apis = ['stock_zh_a_hist', 'index_zh_a_hist', 'macro_china_cpi_monthly']
        for api in example_apis:
            print(f"\n--- 测试: {api} ---")
            result = tester.test_single_interface(api)
            if result['success']:
                print(f"✓ 成功! 耗时: {result['duration']}s, 数据形状: {result['data_shape']}")
                print(f"  返回字段: {result['fields'][:10]}")
            else:
                print(f"✗ 失败: {result['error']}")


if __name__ == "__main__":
    main()
