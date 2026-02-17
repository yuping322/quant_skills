#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
从所有数据接口文档生成任务配置
"""

import json
import os
from code.md_parser import DataDictParser

def generate_tasks_for_all_data():
    """为所有数据类型生成任务配置"""
    parser = DataDictParser()
    df = parser.parse_all()
    
    tasks = []
    for _, row in df.iterrows():
        task = {
            "name": row['func_name'],
            "table_name": row['func_name'],
            "func_name": row['func_name'],
            "description": row['description'],
            "data_type": row['data_type'],
            "frequency": "daily",  # 默认每天更新
            "primary_keys": ["日期"],
            "func_params": {},
            "iterator_sql": None,
            "iterator_param": None,
            "date_params": None
        }
        tasks.append(task)
    
    # 保存到result目录
    os.makedirs("result", exist_ok=True)
    with open("result/all_data_tasks.json", 'w', encoding='utf-8') as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)
    
    print(f"生成 {len(tasks)} 个数据接口任务配置")
    print(f"已保存到 result/all_data_tasks.json")

if __name__ == "__main__":
    generate_tasks_for_all_data()