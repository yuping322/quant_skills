#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成统一的字段字典配置文件
将可以互相转换的字段识别为同一个字段，消除逻辑上重复的字段
"""

import pandas as pd
import json
import os
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple

class UnifiedDictionaryGenerator:
    def __init__(self, data_dir: str = "result/data", output_dir: str = "result"):
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 预定义的常见字段等价关系
        self.field_equivalents = {
            # 日期相关
            "日期": ["date", "TRADE_DATE", "年份", "月份", "年月", "发布时间", "时间", "datetime"],
            "date": ["日期", "TRADE_DATE", "年份", "月份", "年月", "发布时间", "时间", "datetime"],
            
            # 代码相关
            "symbol": ["代码", "股票代码", "基金代码", "指数代码", "债券代码", "期货代码", "合约代码", "code", "stock_code", "fund_code"],
            "代码": ["symbol", "股票代码", "基金代码", "指数代码", "债券代码", "期货代码", "合约代码", "code", "stock_code", "fund_code"],
            
            # 名称相关
            "名称": ["name", "股票名称", "基金名称", "指数名称", "债券名称", "期货名称", "商品", "title"],
            "name": ["名称", "股票名称", "基金名称", "指数名称", "债券名称", "期货名称", "商品", "title"],
            
            # 价格相关
            "收盘价": ["close", "最新价", "现价", "Close"],
            "开盘价": ["open", "Open"],
            "最高价": ["high", "High"],
            "最低价": ["low", "Low"],
            "成交量": ["volume", "成交量(手)", "Volume"],
            "成交额": ["amount", "成交额(万)", "Amount"],
            
            # 涨跌幅相关
            "涨跌幅": ["change", "涨跌", "涨跌幅(%)", "change_pct", "变动"],
            "涨跌额": ["change_amount", "涨跌额(元)"],
            
            # 金融指标
            "今值": ["actual", "当前值", "现值"],
            "前值": ["previous", "上期值"],
            "预测值": ["forecast", "预期值", "预测"],
            
            # 利率相关
            "LPR1Y": ["lpr_1y", "1年期LPR"],
            "LPR5Y": ["lpr_5y", "5年期LPR"],
            
            # 市场类型
            "市场": ["market", "exchange", "交易所"],
            
            # 复权类型
            "复权类型": ["adjust", "复权"],
            
            # 周期相关
            "周期": ["period", "frequency"],
        }
        
        # 字段类型映射
        self.type_mapping = {
            "object": "string",
            "str": "string",
            "int64": "integer",
            "int": "integer",
            "float64": "float",
            "float": "float",
            "bool": "boolean",
            "datetime": "datetime",
            "list": "array",
            "dict": "object",
        }
    
    def load_all_data(self) -> Dict[str, pd.DataFrame]:
        """加载所有数据字典文件"""
        data_files = {
            "normalized": os.path.join(self.data_dir, "normalized_data_dictionary.csv"),
            "all_with_examples": os.path.join(self.data_dir, "all_data_dictionary_with_examples.csv"),
            "macro": os.path.join(self.data_dir, "macro_data_dictionary.csv"),
            "macro_with_examples": os.path.join(self.data_dir, "macro_data_dictionary_with_examples.csv"),
        }
        
        dfs = {}
        for name, path in data_files.items():
            if os.path.exists(path):
                dfs[name] = pd.read_csv(path)
                print(f"✓ 加载 {name}: {len(dfs[name])} 条记录")
        
        return dfs
    
    def get_canonical_name(self, field_name: str) -> str:
        """获取字段的标准名称"""
        field_name_lower = field_name.lower().strip()
        
        # 检查预定义的等价关系
        for canonical, equivalents in self.field_equivalents.items():
            if field_name == canonical or field_name in equivalents:
                return canonical
            if field_name_lower in [e.lower() for e in equivalents]:
                return canonical
        
        # 检查是否为英文和中文的对应关系
        # 简单的启发式规则：如果字段名只有英文，尝试找中文对应
        # 如果字段名只有中文，尝试找英文对应
        return field_name
    
    def build_unified_dictionary(self, dfs: Dict[str, pd.DataFrame]) -> Dict:
        """构建统一的字段字典"""
        unified_dict = {
            "version": "1.0",
            "generated_at": pd.Timestamp.now().isoformat(),
            "fields": {},
            "interfaces": {},
            "statistics": {}
        }
        
        # 字段收集
        field_occurrences = defaultdict(list)
        field_types = defaultdict(Counter)
        field_interfaces = defaultdict(set)
        field_samples = defaultdict(list)
        
        # 处理 normalized_data_dictionary.csv
        if "normalized" in dfs:
            df = dfs["normalized"]
            for _, row in df.iterrows():
                orig_name = str(row["参数名"]) if pd.notna(row["参数名"]) else ""
                canonical_name = str(row["标准化参数名"]) if pd.notna(row["标准化参数名"]) else orig_name
                eng_name = str(row["参数英文名"]) if pd.notna(row["参数英文名"]) else ""
                field_type = str(row["类型"]) if pd.notna(row["类型"]) else "string"
                param_type = str(row["参数类型"]) if pd.notna(row["参数类型"]) else "输出参数"
                interface_name = str(row["接口名"]) if pd.notna(row["接口名"]) else ""
                interface_type = str(row["接口类型"]) if pd.notna(row["接口类型"]) else ""
                sample = str(row["数据样例"]) if pd.notna(row["数据样例"]) else ""
                frequency = int(row["出现频次"]) if pd.notna(row["出现频次"]) else 1
                
                # 确定标准名称
                final_canonical = self.get_canonical_name(canonical_name)
                
                # 记录字段信息
                field_occurrences[final_canonical].append({
                    "original_name": orig_name,
                    "english_name": eng_name,
                    "interface": interface_name,
                    "interface_type": interface_type,
                    "parameter_type": param_type,
                })
                
                # 标准化类型
                normalized_type = self.type_mapping.get(field_type.lower(), field_type.lower())
                field_types[final_canonical][normalized_type] += 1
                
                # 记录出现的接口
                if interface_name:
                    field_interfaces[final_canonical].add(interface_name)
                
                # 记录数据样例
                if sample and sample != "nan" and sample != "":
                    field_samples[final_canonical].append(sample)
        
        # 处理 all_data_dictionary_with_examples.csv
        if "all_with_examples" in dfs:
            df = dfs["all_with_examples"]
            for _, row in df.iterrows():
                orig_name = str(row["参数名"]) if pd.notna(row["参数名"]) else ""
                eng_name = str(row["参数英文名"]) if pd.notna(row["参数英文名"]) else ""
                field_type = str(row["类型"]) if pd.notna(row["类型"]) else "string"
                param_type = str(row["参数类型"]) if pd.notna(row["参数类型"]) else "输出参数"
                interface_name = str(row["接口名"]) if pd.notna(row["接口名"]) else ""
                interface_type = str(row["接口类型"]) if pd.notna(row["接口类型"]) else ""
                sample = str(row["数据样例"]) if pd.notna(row["数据样例"]) else ""
                
                # 跳过无效字段
                if orig_name in ["-", "--------", "------:", "---:"] or orig_name.startswith("..."):
                    continue
                
                # 确定标准名称
                final_canonical = self.get_canonical_name(orig_name)
                
                # 记录字段信息
                field_occurrences[final_canonical].append({
                    "original_name": orig_name,
                    "english_name": eng_name,
                    "interface": interface_name,
                    "interface_type": interface_type,
                    "parameter_type": param_type,
                })
                
                # 标准化类型
                normalized_type = self.type_mapping.get(field_type.lower(), field_type.lower())
                field_types[final_canonical][normalized_type] += 1
                
                # 记录出现的接口
                if interface_name:
                    field_interfaces[final_canonical].add(interface_name)
                
                # 记录数据样例
                if sample and sample != "nan" and sample != "":
                    field_samples[final_canonical].append(sample)
        
        # 构建统一字段字典
        for canonical_name in field_occurrences.keys():
            # 收集所有别名
            aliases = set()
            english_names = set()
            for occ in field_occurrences[canonical_name]:
                aliases.add(occ["original_name"])
                if occ["english_name"]:
                    english_names.add(occ["english_name"])
            
            # 确定最常见的类型
            type_counter = field_types[canonical_name]
            most_common_type = type_counter.most_common(1)[0][0] if type_counter else "string"
            
            # 收集数据样例（去重，最多保留10个）
            unique_samples = list(set(field_samples[canonical_name]))[:10]
            
            # 收集接口信息
            interfaces = list(field_interfaces[canonical_name])
            
            unified_dict["fields"][canonical_name] = {
                "canonical_name": canonical_name,
                "aliases": sorted(list(aliases - {canonical_name})),
                "english_names": sorted(list(english_names)),
                "common_type": most_common_type,
                "all_types": dict(type_counter),
                "sample_values": unique_samples,
                "interfaces": interfaces,
                "occurrence_count": len(field_occurrences[canonical_name]),
                "interface_count": len(interfaces),
            }
        
        # 收集接口信息
        if "normalized" in dfs:
            df = dfs["normalized"]
            for interface_name in df["接口名"].unique():
                if pd.notna(interface_name):
                    interface_rows = df[df["接口名"] == interface_name]
                    interface_type = interface_rows["接口类型"].iloc[0] if len(interface_rows) > 0 else ""
                    
                    unified_dict["interfaces"][interface_name] = {
                        "name": interface_name,
                        "type": interface_type,
                        "fields": [
                            {
                                "name": str(row["参数名"]),
                                "canonical_name": self.get_canonical_name(str(row["标准化参数名"]) if pd.notna(row["标准化参数名"]) else str(row["参数名"])),
                                "type": self.type_mapping.get(str(row["类型"]).lower(), str(row["类型"]).lower()),
                                "parameter_type": str(row["参数类型"]),
                            }
                            for _, row in interface_rows.iterrows()
                        ]
                    }
        
        # 添加统计信息
        unified_dict["statistics"] = {
            "total_fields": len(unified_dict["fields"]),
            "total_interfaces": len(unified_dict["interfaces"]),
            "fields_by_occurrence": sorted(
                [(name, info["occurrence_count"]) for name, info in unified_dict["fields"].items()],
                key=lambda x: x[1],
                reverse=True
            )[:50],
            "fields_by_interface_count": sorted(
                [(name, info["interface_count"]) for name, info in unified_dict["fields"].items()],
                key=lambda x: x[1],
                reverse=True
            )[:50],
        }
        
        return unified_dict
    
    def save_unified_dictionary(self, unified_dict: Dict, output_format: str = "both"):
        """保存统一字典"""
        # 保存 JSON 格式
        json_path = os.path.join(self.output_dir, "unified_field_dictionary.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(unified_dict, f, ensure_ascii=False, indent=2)
        print(f"✓ 统一字段字典已保存到: {json_path}")
        
        # 保存 CSV 格式的字段列表
        field_list = []
        for canonical_name, info in unified_dict["fields"].items():
            field_list.append({
                "标准字段名": canonical_name,
                "别名": "|".join(info["aliases"]),
                "英文名": "|".join(info["english_names"]),
                "常见类型": info["common_type"],
                "所有类型": json.dumps(info["all_types"], ensure_ascii=False),
                "出现次数": info["occurrence_count"],
                "涉及接口数": info["interface_count"],
                "样例值": "|".join(info["sample_values"])[:500],
            })
        
        field_df = pd.DataFrame(field_list)
        field_df = field_df.sort_values("出现次数", ascending=False)
        csv_path = os.path.join(self.output_dir, "unified_field_dictionary.csv")
        field_df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        print(f"✓ 统一字段字典已保存到: {csv_path}")
        
        # 保存字段映射表
        field_mapping = []
        for canonical_name, info in unified_dict["fields"].items():
            # 添加标准字段到自身的映射
            field_mapping.append({
                "原始字段名": canonical_name,
                "标准字段名": canonical_name,
            })
            # 添加别名映射
            for alias in info["aliases"]:
                field_mapping.append({
                    "原始字段名": alias,
                    "标准字段名": canonical_name,
                })
            # 添加英文名映射
            for eng_name in info["english_names"]:
                field_mapping.append({
                    "原始字段名": eng_name,
                    "标准字段名": canonical_name,
                })
        
        mapping_df = pd.DataFrame(field_mapping)
        mapping_df = mapping_df.drop_duplicates().sort_values("标准字段名")
        mapping_path = os.path.join(self.output_dir, "field_mapping.csv")
        mapping_df.to_csv(mapping_path, index=False, encoding='utf-8-sig')
        print(f"✓ 字段映射表已保存到: {mapping_path}")
    
    def generate(self):
        """主生成函数"""
        print("=" * 80)
        print("统一字段字典生成器")
        print("=" * 80)
        
        # 1. 加载数据
        print("\n[1/4] 加载数据字典文件...")
        dfs = self.load_all_data()
        
        if not dfs:
            print("错误: 未找到任何数据字典文件")
            return
        
        # 2. 构建统一字典
        print("\n[2/4] 构建统一字段字典...")
        unified_dict = self.build_unified_dictionary(dfs)
        
        # 3. 显示统计信息
        print("\n[3/4] 统计信息:")
        print(f"  - 总字段数: {unified_dict['statistics']['total_fields']}")
        print(f"  - 总接口数: {unified_dict['statistics']['total_interfaces']}")
        print("\n  出现次数最多的字段:")
        for i, (name, count) in enumerate(unified_dict["statistics"]["fields_by_occurrence"][:10], 1):
            print(f"    {i}. {name}: {count} 次")
        
        print("\n  涉及接口最多的字段:")
        for i, (name, count) in enumerate(unified_dict["statistics"]["fields_by_interface_count"][:10], 1):
            print(f"    {i}. {name}: {count} 个接口")
        
        # 4. 保存结果
        print("\n[4/4] 保存结果...")
        self.save_unified_dictionary(unified_dict)
        
        print("\n" + "=" * 80)
        print("✓ 统一字段字典生成完成!")
        print("=" * 80)

def main():
    generator = UnifiedDictionaryGenerator()
    generator.generate()

if __name__ == "__main__":
    main()
