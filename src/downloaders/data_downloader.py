#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
通用数据下载程序 - 支持所有AKShare数据类型
"""

import json
import akshare as ak
import pandas as pd
import os
import sys
from datetime import datetime
import time
import logging

class DataDownloader:
    def __init__(self, config_file: str = "result/all_data_tasks.json", 
                 data_dir: str = "result/data"):
        self.config_file = config_file
        self.data_dir = data_dir
        self.setup_logging()
        self.create_data_directory()
        
    def setup_logging(self):
        """设置日志系统"""
        os.makedirs('result/logs', exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('result/logs/data_download.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def create_data_directory(self):
        """创建按类型分类的数据目录"""
        for data_type in ['macro', 'stock', 'bond', 'futures', 'fund_public', 'fund_private', 'index', 'qdii']:
            os.makedirs(os.path.join(self.data_dir, data_type), exist_ok=True)
    
    def load_config(self) -> list:
        """加载任务配置"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            self.logger.error(f"加载配置文件失败: {e}")
            sys.exit(1)
    
    def download_data(self, task: dict, max_retries: int = 3) -> dict:
        """下载单个接口数据"""
        result = task.copy()
        result.update({
            'status': 'pending',
            'file_path': None,
            'error': None,
            'execution_time': 0
        })
        
        try:
            ak_func = getattr(ak, task['func_name'])
            start_time = time.time()
            
            # 处理特殊参数
            params = {}
            for param in task.get('func_params', {}):
                if task['func_params'][param] == 'string':
                    params[param] = "北京"  # 默认值
            
            df = ak_func(**params) if params else ak_func()
            
            # 验证数据
            if not isinstance(df, pd.DataFrame) or df.empty:
                raise ValueError("无效数据格式")
            
            # 按数据类型存储
            data_type_dir = os.path.join(self.data_dir, task['data_type'])
            csv_path = os.path.join(data_type_dir, f"{task['func_name']}.csv")
            df.to_csv(csv_path, index=False)
            
            result.update({
                'status': 'success',
                'file_path': csv_path,
                'data_size': len(df),
                'execution_time': round(time.time() - start_time, 3)
            })
            self.logger.info(f"✓ {task['func_name']} 下载成功")
            
        except Exception as e:
            result.update({
                'status': 'error',
                'error': str(e)
            })
            self.logger.error(f"✗ {task['func_name']} 下载失败: {e}")
        
        return result
    
    def run(self):
        """执行下载任务"""
        tasks = self.load_config()
        total = len(tasks)
        results = []
        
        self.logger.info(f"开始下载 {total} 个数据接口...")
        
        for i, task in enumerate(tasks, 1):
            self.logger.info(f"[{i}/{total}] 处理: {task['func_name']}")
            result = self.download_data(task)
            results.append(result)
            time.sleep(0.5)  # 请求间隔
            
        self.generate_report(results)
    
    def generate_report(self, results: list):
        """生成下载报告"""
        stats = {
            'timestamp': datetime.now().isoformat(),
            'total': len(results),
            'success': sum(1 for r in results if r['status'] == 'success'),
            'failed': sum(1 for r in results if r['status'] == 'error'),
            'details': results
        }
        
        report_path = os.path.join('result', 'download_report.json')
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(stats, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"报告已保存: {report_path}")
        print(f"\n下载完成! 成功: {stats['success']}, 失败: {stats['failed']}")

if __name__ == "__main__":
    downloader = DataDownloader()
    try:
        downloader.run()
    except KeyboardInterrupt:
        print("\n下载被中断")
        sys.exit(1)