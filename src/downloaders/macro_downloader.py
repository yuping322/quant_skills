#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import akshare as ak
import pandas as pd
import os
import sys
from datetime import datetime
import time
from typing import Dict, Any, List
import logging

class MacroDataDownloader:
    def __init__(self, config_file: str = "result/macro_tasks.json", data_dir: str = "result/data"):
        self.config_file = config_file
        self.data_dir = data_dir
        self.setup_logging()
        self.create_data_directory()
        
    def setup_logging(self):
        """设置日志"""
        # 创建日志目录
        os.makedirs('result/logs', exist_ok=True)
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('result/logs/macro_download.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_data_directory(self):
        """创建数据目录"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            self.logger.info(f"创建数据目录: {self.data_dir}")
    
    def load_config(self) -> List[Dict]:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            sys.exit(1)
    
    def download_interface_data(self, interface_config: Dict) -> Dict[str, Any]:
        """下载单个接口数据"""
        name = interface_config['name']
        func_name = interface_config['func_name']
        
        result = {
            'name': name,
            'func_name': func_name,
            'status': 'pending',
            'file_path': None,
            'data_size': 0,
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
            
            # 保存数据到文件 (使用CSV格式)
            file_name = f"{name}.csv"
            file_path = os.path.join(self.data_dir, file_name)
            
            # 保存为CSV文件
            df.to_csv(file_path, index=False)
            
            # 对于已知有问题的数据源添加标记
            problem_sources = [
                'macro_china_shrzgm', 'macro_china_central_bank_balance',
                'macro_china_insurance', 'macro_china_supply_of_money',
                'macro_china_swap_rate', 'macro_china_foreign_exchange_gold',
                'macro_china_retail_price_index', 'macro_china_nbs_nation',
                'macro_china_nbs_region'
            ]
            
            if name in problem_sources:
                with open(os.path.join(self.data_dir, f"{name}_NOTE.txt"), 'w') as f:
                    f.write("此数据源存在问题，需要手动处理\n\n错误详情:\n")
                    f.write(result.get('error', '无详细错误信息'))
            
            result.update({
                'status': 'success',
                'file_path': file_path,
                'data_size': len(df),
                'execution_time': round(execution_time, 3)
            })
            
            self.logger.info(f"✓ {name}: 成功下载 {len(df)} 行数据 -> {file_path}")
            
        except Exception as e:
            execution_time = time.time() - start_time if 'start_time' in locals() else 0
            result.update({
                'status': 'error',
                'error': str(e),
                'execution_time': round(execution_time, 3)
            })
            self.logger.error(f"✗ {name}: 下载失败 - {e}")
        
        return result
    
    def download_all_data(self, max_retries: int = 3, delay: float = 1.0):
        """下载所有数据"""
        configs = self.load_config()
        total = len(configs)
        results = []
        
        self.logger.info(f"开始下载 {total} 个宏观数据接口...")
        
        for i, config in enumerate(configs, 1):
            self.logger.info(f"[{i}/{total}] 处理接口: {config['name']}")
            
            for attempt in range(max_retries):
                try:
                    result = self.download_interface_data(config)
                    results.append(result)
                    
                    if result['status'] == 'success':
                        break
                    else:
                        if attempt < max_retries - 1:
                            self.logger.warning(f"第 {attempt + 1} 次尝试失败，等待重试...")
                            time.sleep(delay * (attempt + 1))  # 指数退避
                        else:
                            self.logger.error(f"接口 {config['name']} 经过 {max_retries} 次尝试仍然失败")
                            
                except Exception as e:
                    self.logger.error(f"处理接口 {config['name']} 时发生意外错误: {e}")
                    if attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))
            
            # 接口间延迟，避免请求过于频繁
            time.sleep(0.5)
        
        return results
    
    def generate_summary_report(self, results: List[Dict]):
        """生成摘要报告"""
        total = len(results)
        success = sum(1 for r in results if r['status'] == 'success')
        failed = total - success
        
        summary = {
            'download_time': datetime.now().isoformat(),
            'total_interfaces': total,
            'successful_downloads': success,
            'failed_downloads': failed,
            'success_rate': success / total if total > 0 else 0,
            'total_data_rows': sum(r.get('data_size', 0) for r in results if r['status'] == 'success'),
            'details': results
        }
        
        # 保存摘要报告
        report_file = os.path.join(self.data_dir, 'download_summary.json')
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"下载摘要报告已保存: {report_file}")
        
        # 打印统计信息
        print(f"\n{'='*60}")
        print("宏 观 数 据 下 载 总 结")
        print(f"{'='*60}")
        print(f"下载时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"总接口数: {total}")
        print(f"成功下载: {success}")
        print(f"失败: {failed}")
        print(f"成功率: {success/total*100:.1f}%")
        print(f"总数据行数: {summary['total_data_rows']:,}")
        print(f"{'='*60}")
        
        if failed > 0:
            print("\n失败接口详情:")
            for result in results:
                if result['status'] == 'error':
                    print(f"  {result['name']}: {result['error']}")

def main():
    """主函数"""
    downloader = MacroDataDownloader()
    
    try:
        results = downloader.download_all_data(max_retries=3, delay=2.0)
        downloader.generate_summary_report(results)
        
    except KeyboardInterrupt:
        print("\n用户中断下载")
    except Exception as e:
        print(f"下载过程发生错误: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()