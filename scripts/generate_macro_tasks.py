#!/usr/bin/env python3
"""
从macro.md.txt生成macro_tasks.json配置 - 修正版
"""

import re
import json

def parse_macro_md(filename):
    """解析markdown文件，提取所有接口信息"""
    with open(filename, 'r', encoding='utf-8') as f:
        content = f.read()
    
    interfaces = []
    
    # 使用更精确的正则表达式
    pattern = r'接口:\s*(macro_[a-zA-Z0-9_]+)\s*\n\n目标地址:[^\n]*\n\n描述:\s*([^\n]+)'
    matches = re.findall(pattern, content)
    
    for func_name, description in matches:
        # 清理描述
        description = description.strip()
        if description.endswith(' '):
            description = description[:-1]
        
        # 提取分类
        category = "其他"
        desc_lower = description.lower()
        
        if '美国' in description or 'usa' in desc_lower or 'united states' in desc_lower:
            category = "美国宏观"
        elif '中国' in description:
            category = "中国宏观"
        elif '欧洲' in description or 'euro' in desc_lower:
            category = "欧洲宏观"
        elif '德国' in description or 'germany' in desc_lower:
            category = "德国宏观"
        elif '英国' in description or 'uk' in desc_lower or 'britain' in desc_lower:
            category = "英国宏观"
        elif '日本' in description or 'japan' in desc_lower:
            category = "日本宏观"
        elif '加拿大' in description or 'canada' in desc_lower:
            category = "加拿大宏观"
        elif '澳大利亚' in description or 'australia' in desc_lower:
            category = "澳大利亚宏观"
        elif '瑞士' in description or 'swiss' in desc_lower or 'switzerland' in desc_lower:
            category = "瑞士宏观"
        
        interface = {
            "name": func_name,
            "table_name": func_name,
            "func_name": func_name,
            "description": description,
            "frequency": "monthly",
            "primary_keys": ["日期"],
            "func_params": {},
            "iterator_sql": None,
            "iterator_param": None,
            "date_params": None,
            "category": category
        }
        interfaces.append(interface)
    
    return interfaces

def main():
    # 从data目录读取macro.md.txt
    interfaces = parse_macro_md('data/macro.md.txt')
    
    # 写入JSON文件
    with open('macro_tasks_generated.json', 'w', encoding='utf-8') as f:
        json.dump(interfaces, f, ensure_ascii=False, indent=2)
    
    print(f"生成了 {len(interfaces)} 个接口配置")
    print(f"已保存到 macro_tasks_generated.json")

if __name__ == '__main__':
    main()