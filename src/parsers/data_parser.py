#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AKShare 数据接口文档解析器
用于解析 data/ 目录下的所有 .md.txt 文件
"""

import re
import os
import pandas as pd
from typing import List, Dict, Tuple

class DataInterfaceParser:
    def __init__(self, data_dir: str = "data", output_dir: str = "result/data_dictionaries"):
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def parse_single_file(self, file_path: str) -> pd.DataFrame:
        """解析单个Markdown数据文档"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            interfaces = []
            
            # 为了准确识别API接口部分，我们特别查找包含"接口:"的段落
            api_pattern = r'接口:\s*(.+?)\n目标地址:\s*(.+?)\n描述:\s*(.+?)\n'
            api_matches = re.finditer(api_pattern, content, re.DOTALL)
            
            for match in api_matches:
                # 找到API接口的位置
                api_start = match.start()
                api_end = match.end()
                
                # 找到这个API接口所在的完整段落
                section_start = content.rfind('\n', 0, api_start)
                if section_start == -1:
                    section_start = 0
                else:
                    section_start += 1
                    
                # 找到下一个API接口或文件结尾
                section_end = len(content)
                next_api_match = re.search(api_pattern, content[api_end:], re.DOTALL)
                if next_api_match:
                    section_end = api_end + next_api_match.start()
                
                # 截取这一段内容
                section_content = content[section_start:section_end]
                
                # 提取接口描述信息
                func_info = self._extract_interface_info(section_content)
                if not func_info:
                    continue
                    
                # 提取输入参数表
                input_params = self._parse_parameter_table(section_content, "输入参数")
                # 提取输出参数表
                output_params = self._parse_parameter_table(section_content, "输出参数")
                # 提取数据示例
                data_example, total_lines = self._extract_data_example(section_content)
                
                # 尝试找到标题
                title = "未知接口"
                # 查找最近的标题（从当前段落开始向上查找）
                title_pattern = r'(#+\s+)(.+?)(?=\n|$)'
                title_match = re.search(title_pattern, section_content)
                if title_match:
                    title = title_match.group(2).strip()
                
                interface = {
                    **func_info,
                    "title": title,
                    "input_params_count": len(input_params),
                    "output_params_count": len(output_params),
                    "data_example_lines": total_lines,
                    "data_type": os.path.basename(file_path).replace('.md.txt', '')
                }
                interfaces.append(interface)
            
            return pd.DataFrame(interfaces)
        except Exception as e:
            print(f"解析文件 {file_path} 时出错: {str(e)}")
            return pd.DataFrame()
    
    def _extract_interface_info(self, section: str) -> Dict:
        """提取接口基本信息"""
        pattern = (
            r"接口:\s*(.+?)\s*\n"
            r"目标地址:\s*(.*?)\s*\n"
            r"描述:\s*(.+?)\s*\n"
            r"(?:限量:\s*(.*?)\s*\n)?"
        )
        match = re.search(pattern, section)
        if not match:
            return None
            
        info = {
            "func_name": match.group(1).strip(),
            "target_url": match.group(2).strip(),
            "description": match.group(3).strip()
        }
        if match.group(4):  # 限量信息
            info['limit'] = match.group(4).strip()
        return info
    
    def _parse_parameter_table(self, section: str, table_name: str) -> List[Dict]:
        """解析参数表格，适配AKShare文档格式"""
        # 匹配表格标题、表头和数据行
        pattern = (
            rf"{table_name}\s*\n"
            r"\|(.+)\|\s*\n"   # 表头行
            r"\|([\-:]+\|)+\s*\n"  # 分隔行
            r"((?:\|.+\|\s*\n)+)"  # 数据行
        )
        match = re.search(pattern, section)
        if not match:
            return []
            
        # 解析表头
        headers = [h.strip() for h in match.group(1).split('|') if h.strip()]
        
        # 解析数据行
        rows = []
        for row in match.group(3).strip().split('\n'):
            if not row.strip() or '|---' in row:
                continue
            cells = [c.strip() for c in row.split('|')[1:-1]]
            if len(cells) == len(headers):
                rows.append(dict(zip(headers, cells)))
                
        return rows
    
    def _extract_data_example(self, section: str) -> Tuple[str, int]:
        """提取数据示例并计算行数"""
        pattern = r"数据示例\n```(.+?)```"
        match = re.search(pattern, section, re.DOTALL)
        if not match:
            return None, 0
            
        data_str = match.group(1).strip()
        lines = [line.strip() for line in data_str.split("\n") if line.strip()]
        return data_str, len(lines)
    
    def parse_all_files(self) -> pd.DataFrame:
        """解析所有数据文件"""
        all_data = []
        total_interfaces = 0
        
        print("开始解析所有数据文件...")
        
        for fname in sorted(os.listdir(self.data_dir)):
            if fname.endswith('.md.txt'):
                file_path = os.path.join(self.data_dir, fname)
                try:
                    df = self.parse_single_file(file_path)
                    if not df.empty:
                        all_data.append(df)
                        total_interfaces += len(df)
                        print(f"✓ 成功解析: {fname} ({len(df)}个接口)")
                    else:
                        print(f"○ 未发现接口: {fname}")
                except Exception as e:
                    print(f"✗ 解析 {fname} 失败: {str(e)}")
                    continue
        
        if all_data:
            result_df = pd.concat(all_data, ignore_index=True)
            print(f"\n总计: 成功解析 {len(all_data)} 个文件，共 {total_interfaces} 个接口")
            return result_df
        else:
            print("警告: 没有成功解析任何文件")
            return pd.DataFrame()
    
    def save_to_csv(self, filename: str = "all_data_dictionary.csv") -> str:
        """将解析结果保存为CSV文件"""
        df = self.parse_all_files()
        if df.empty:
            print("没有数据可保存")
            return ""
            
        output_path = os.path.join(self.output_dir, filename)
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f"数据字典已保存到: {output_path}")
        return output_path
    
    def get_statistics(self) -> Dict:
        """获取解析统计信息"""
        df = self.parse_all_files()
        if df.empty:
            return {}
            
        stats = {
            "total_interfaces": len(df),
            "files_parsed": len(df['data_type'].unique()),
            "data_types": df['data_type'].value_counts().to_dict()
        }
        return stats

def main():
    """主函数"""
    parser = DataInterfaceParser()
    
    # 显示统计信息
    stats = parser.get_statistics()
    if stats:
        print("\n=== 解析统计 ===")
        print(f"总接口数: {stats['total_interfaces']}")
        print(f"处理文件数: {stats['files_parsed']}")
        print("各数据类型分布:")
        for data_type, count in stats['data_types'].items():
            print(f"  {data_type}: {count} 个接口")
    
    # 保存结果
    output_file = parser.save_to_csv()
    if output_file:
        print(f"\n✓ 成功生成数据字典: {output_file}")
    else:
        print("\n✗ 未能生成数据字典")

if __name__ == "__main__":
    main()