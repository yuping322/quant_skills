#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import akshare as ak
import pandas as pd
import sys
from datetime import datetime
from typing import Dict, Any, List, Optional
import time

class MacroDataValidator:
    def __init__(self, config_file: str = "macro_tasks.json"):
        self.config_file = config_file
        self.results = []
        
    def load_config(self) -> List[Dict]:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"错误: 找不到配置文件 {self.config_file}")
            sys.exit(1)
        except json.JSONDecodeError as e:
            print(f"错误: 配置文件格式错误 - {e}")
            sys.exit(1)
    
    def validate_interface(self, interface_config: Dict) -> Dict[str, Any]:
        """验证单个接口"""
        name = interface_config['name']
        func_name = interface_config['func_name']
        description = interface_config['description']
        
        print(f"\n正在验证接口: {name} ({description})")
        
        result = {
            'name': name,
            'func_name': func_name,
            'description': description,
            'status': 'pending',
            'data_size': 0,
            'columns': [],
            'error': None,
            'execution_time': 0
        }
        
        try:
            # 获取akshare函数
            ak_func = getattr(ak, func_name, None)
            if ak_func is None:
                raise AttributeError(f"akshare中没有找到函数: {func_name}")
            
            start_time = time.time()
            
            # 调用akshare接口
            if 'func_params' in interface_config and interface_config['func_params']:
                # 处理需要参数的接口
                params = {}
                for param_name, param_type in interface_config['func_params'].items():
                    if param_type == 'string':
                        params[param_name] = "北京"  # 默认值
                df = ak_func(**params)
            else:
                df = ak_func()
            
            execution_time = time.time() - start_time
            
            # 验证数据
            if df is None:
                raise ValueError("返回数据为None")
            
            if not isinstance(df, pd.DataFrame):
                raise ValueError(f"返回数据类型错误: {type(df)}，期望DataFrame")
            
            if df.empty:
                raise ValueError("返回数据为空")
            
            # 记录结果
            result.update({
                'status': 'success',
                'data_size': len(df),
                'columns': list(df.columns),
                'execution_time': round(execution_time, 3)
            })
            
            print(f"  ✓ 成功获取 {len(df)} 行数据, {len(df.columns)} 列")
            print(f"  ⏱️ 执行时间: {execution_time:.3f}秒")
            
        except Exception as e:
            execution_time = time.time() - start_time if 'start_time' in locals() else 0
            result.update({
                'status': 'error',
                'error': str(e),
                'execution_time': round(execution_time, 3)
            })
            print(f"  ✗ 错误: {e}")
        
        return result
    
    def generate_report(self, results: List[Dict]) -> None:
        """生成验证报告"""
        total = len(results)
        success = sum(1 for r in results if r['status'] == 'success')
        failed = total - success
        
        print(f"\n{'='*60}")
        print("宏 观 数 据 接 口 验 证 报 告")
        print(f"{'='*60}")
        print(f"验证时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总接口数: {total}")
        print(f"成功: {success}")
        print(f"失败: {failed}")
        print(f"成功率: {success/total*100:.1f}%")
        print(f"{'='*60}")
        
        # 失败接口详情
        if failed > 0:
            print("\n失败接口详情:")
            for result in results:
                if result['status'] == 'error':
                    print(f"  {result['name']}: {result['error']}")
        
        # 保存详细报告
        report_data = {
            'validation_time': datetime.now().isoformat(),
            'total_interfaces': total,
            'successful': success,
            'failed': failed,
            'success_rate': success/total,
            'details': results
        }
        
        with open('macro_validation_report.json', 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        print(f"\n详细报告已保存至: macro_validation_report.json")
    
    def run_validation(self) -> None:
        """运行完整的验证流程"""
        print("开始验证宏观数据接口...")
        
        # 加载配置
        configs = self.load_config()
        print(f"加载了 {len(configs)} 个接口配置")
        
        # 逐个验证接口
        for i, config in enumerate(configs, 1):
            print(f"\n[{i}/{len(configs)}] ", end="")
            result = self.validate_interface(config)
            self.results.append(result)
            
            # 添加短暂延迟，避免请求过于频繁
            time.sleep(0.5)
        
        # 生成报告
        self.generate_report(self.results)

def main():
    """主函数"""
    validator = MacroDataValidator()
    
    try:
        validator.run_validation()
    except KeyboardInterrupt:
        print("\n用户中断验证")
        sys.exit(1)
    except Exception as e:
        print(f"验证过程发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()