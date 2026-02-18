#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段映射与转换系统
提供完整的字段查询和转换功能
"""

import pandas as pd
import json
import os
import re
from collections import defaultdict, Counter
from typing import Dict, List, Set, Tuple, Any
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

class FieldMapper:
    def __init__(self, data_dir: str = "data", output_dir: str = "result"):
        self.data_dir = data_dir
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # 预定义的字段等价关系（英文作为标准字段）
        self.field_equivalents = {
            # 日期相关
            "date": ["日期", "DATE", "TRADE_DATE", "年月", "发布时间", "时间", "datetime", "交易日期", "更新日期", "公告日期", "上市日期", "成立日期", "报告期", "变动日期", "净值日期", "统计时间", "start_date", "end_date", "tradedate", "report_date", "REPORT_DATE", "发布日期", "DATE_TYPE_CODE", "STD_REPORT_DATE", "START_DATE", "NOTICE_DATE", "FINANCIAL_DATE", "DATE_TYPE", "update_time", "月份", "当月"],
            
            # 代码相关
            "symbol": ["代码", "股票代码", "基金代码", "指数代码", "债券代码", "期货代码", "合约代码", "code", "stock_code", "fund_code", "证券代码", "stock", "bond_code", "bond_name", "bond_code"],
            
            # 名称相关
            "name": ["名称", "股票名称", "基金名称", "指数名称", "债券名称", "期货名称", "商品", "title", "股票简称", "基金简称", "债券简称", "证券简称", "简称", "report_name", "quarter_name", "metric_name", "SECURITY_NAME_ABBR", "STD_ITEM_NAME", "ITEM_NAME", "REPORT_DATE_NAME"],
            
            # 价格相关
            "close": ["收盘价", "CLOSE", "最新价", "现价", "收盘"],
            "open": ["开盘价", "OPEN", "今开", "开盘"],
            "high": ["最高价", "HIGH", "最高"],
            "low": ["最低价", "LOW", "最低"],
            "volume": ["成交量", "VOLUME", "成交量(手)"],
            "amount": ["成交额", "AMOUNT", "成交额(万)"],
            
            # 涨跌幅相关
            "change_pct": ["涨跌幅", "涨跌", "涨跌幅(%)", "change", "变动"],
            "change_amount": ["涨跌额", "变化值"],
            
            # 金融指标
            "actual": ["今值", "现值", "最新值"],
            "previous": ["前值", "昨收"],
            "forecast": ["预测值", "预期值", "预测"],
            
            # 市场类型
            "market": ["市场", "exchange", "交易所", "交易市场"],
            
            # 复权类型
            "adjust": ["复权类型", "复权"],
            
            # 周期相关
            "period": ["周期", "frequency"],
            
            # 市值相关
            "market_cap": ["总市值", "市值"],
            "float_market_cap": ["流通市值"],
            
            # 估值指标
            "pe_ratio": ["市盈率", "市盈率-动态"],
            "pb_ratio": ["市净率"],
            
            # 换手率
            "turnover_rate": ["换手率"],
            
            # 基金相关
            "net_value": ["单位净值"],
            "daily_growth": ["日增长率"],
            "fund_manager": ["基金经理"],
            "fund_company": ["基金公司"],
            
            # 其他常见字段
            "value": ["数值", "值", "mid_convert_value"],
            "year": ["年"],
            "month": ["月"],
            "issue_year": ["发行年份"],
            
            # 新增常见字段
            "indicator": ["指标", "item"],
            "amplitude": ["振幅"],
            "change_pct_1y": ["近1年涨跌幅"],
            "change_pct_3m": ["近3月涨跌幅"],
            "change_pct_6m": ["近6月涨跌幅"],
            "change_pct_2y": ["近2年涨跌幅"],
            "change_pct_3y": ["近3年涨跌幅"],
            "industry": ["所属行业", "行业"],
            "shareholder_name": ["股东名称"],
            "shareholder_type": ["股东类型"],
            "volume_ratio": ["量比"],
            "fee": ["手续费"],
            "change_speed": ["涨速"],
            "location": ["注册地"],
            "status": ["申购状态", "赎回状态"],
            "type": ["类型", "品种"],
            "index": ["指数"],
            "total_shares": ["总股本"],
            "issue_price": ["发行价格"],
            "institution_name": ["机构名称", "营业部名称"],
            "avg_price": ["均价"],
            "change_pct_5m": ["5分钟涨跌"],
            "change_pct_60d": ["60日涨跌幅"],
            "change_pct_ytd": ["年初至今涨跌幅"],
            "holding_value": ["持股市值"],
            "qvix": []
        }
        
        # 字段类型标准化
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
            "int32": "integer",
            "datetime64": "datetime",
        }
        
        # 数据存储
        self.interfaces = {}  # 接口信息
        self.fields = {}  # 字段信息
        self.field_to_interfaces = defaultdict(set)  # 字段 → 接口映射
        self.interface_to_fields = defaultdict(dict)  # 接口 → 字段映射
    
    def get_canonical_name(self, field_name: str) -> str:
        """获取字段的标准名称"""
        if pd.isna(field_name) or field_name == "nan" or field_name == "" or field_name == "-":
            return field_name
            
        field_name_str = str(field_name).strip()
        field_name_lower = field_name_str.lower()
        
        # 检查预定义的等价关系
        for canonical, equivalents in self.field_equivalents.items():
            if field_name_str == canonical:
                return canonical
            if field_name_str in equivalents:
                return canonical
            if field_name_lower in [e.lower() for e in equivalents]:
                return canonical
        
        return field_name_str
    
    def parse_all_interfaces(self) -> Dict[str, Any]:
        """解析所有接口，获取详细的输入输出参数"""
        print("正在解析所有接口...")
        
        all_interfaces = {}
        
        for fname in sorted(os.listdir(self.data_dir)):
            if fname.endswith('.md.txt'):
                file_path = os.path.join(self.data_dir, fname)
                data_type = fname.replace('.md.txt', '')
                
                interfaces = self._parse_file_with_detail(file_path, data_type)
                for iface in interfaces:
                    all_interfaces[iface['func_name']] = iface
        
        print(f"✓ 解析完成，共 {len(all_interfaces)} 个接口")
        return all_interfaces
    
    def _parse_file_with_detail(self, file_path: str, data_type: str) -> List[Dict]:
        """解析单个文件并获取详细参数"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
        except Exception as e:
            print(f"✗ 读取文件 {file_path} 失败: {e}")
            return []
        
        interfaces = []
        
        # 匹配接口信息
        api_pattern = r'接口:\s*(.+?)\n目标地址:\s*(.+?)\n描述:\s*(.+?)\n'
        api_matches = list(re.finditer(api_pattern, content, re.DOTALL))
        
        for idx, match in enumerate(api_matches):
            func_name = match.group(1).strip()
            target_url = match.group(2).strip()
            description = match.group(3).strip()
            
            # 找到这个接口所在的段落
            api_start = match.start()
            api_end = match.end()
            
            # 确定段落边界
            section_start = content.rfind('\n', 0, api_start)
            if section_start == -1:
                section_start = 0
            else:
                section_start += 1
            
            section_end = len(content)
            if idx < len(api_matches) - 1:
                section_end = api_matches[idx + 1].start()
            
            section_content = content[section_start:section_end]
            
            # 解析输入参数
            input_params = self._parse_table_with_prefix(section_content, "输入参数")
            
            # 解析输出参数
            output_params = self._parse_table_with_prefix(section_content, "输出参数")
            
            interface = {
                'func_name': func_name,
                'target_url': target_url,
                'description': description,
                'data_type': data_type,
                'input_params': input_params,
                'output_params': output_params,
            }
            
            interfaces.append(interface)
        
        return interfaces
    
    def _parse_table_with_prefix(self, section: str, prefix: str) -> List[Dict]:
        """解析带有前缀的表格（如 '输出参数'、'输出参数-实时行情数据'）"""
        # 先尝试精确匹配
        rows = self._parse_table(section, prefix)
        if rows:
            return rows
        
        # 尝试带后缀的匹配
        pattern = rf"({prefix}[^\n]*)\s*\n\|(.+)\|\s*\n\|([\-:]+\|)+\s*\n((?:\|.+\|\s*\n)+)"
        match = re.search(pattern, section)
        if not match:
            return []
        
        headers = [h.strip() for h in match.group(2).split('|') if h.strip()]
        rows = []
        
        for row in match.group(4).strip().split('\n'):
            if not row.strip() or '|---' in row:
                continue
            cells = [c.strip() for c in row.split('|')[1:-1]]
            if len(cells) == len(headers):
                rows.append(dict(zip(headers, cells)))
        
        return rows
    
    def _parse_table(self, section: str, table_name: str) -> List[Dict]:
        """解析表格"""
        pattern = (
            rf"{table_name}\s*\n"
            r"\|(.+)\|\s*\n"
            r"\|([\-:]+\|)+\s*\n"
            r"((?:\|.+\|\s*\n)+)"
        )
        match = re.search(pattern, section)
        if not match:
            return []
        
        headers = [h.strip() for h in match.group(1).split('|') if h.strip()]
        rows = []
        
        for row in match.group(3).strip().split('\n'):
            if not row.strip() or '|---' in row:
                continue
            cells = [c.strip() for c in row.split('|')[1:-1]]
            if len(cells) == len(headers):
                rows.append(dict(zip(headers, cells)))
        
        return rows
    
    def build_mapping(self, interfaces: Dict[str, Any]):
        """构建字段映射关系"""
        print("\n正在构建字段映射关系...")
        
        for func_name, iface in interfaces.items():
            self.interfaces[func_name] = iface
            
            self.interface_to_fields[func_name] = {
                'input': {},
                'output': {},
            }
            
            # 处理输入参数
            for param in iface.get('input_params', []):
                # 查找名称字段
                param_name = None
                for key in ['名称', 'name', 'Name', '字段名', '字段']:
                    if key in param:
                        param_name = param[key]
                        break
                
                if param_name and param_name != '-' and param_name != '无':
                    canonical = self.get_canonical_name(param_name)
                    self._add_field_mapping(canonical, param_name, func_name, 'input', param)
            
            # 处理输出参数
            for param in iface.get('output_params', []):
                # 查找名称字段
                param_name = None
                for key in ['名称', 'name', 'Name', '字段名', '字段']:
                    if key in param:
                        param_name = param[key]
                        break
                
                if param_name and param_name != '-':
                    canonical = self.get_canonical_name(param_name)
                    self._add_field_mapping(canonical, param_name, func_name, 'output', param)
        
        print(f"✓ 构建完成，共 {len(self.fields)} 个标准字段")
    
    def _add_field_mapping(self, canonical: str, original: str, func_name: str, param_type: str, param: Dict):
        """添加字段映射"""
        self.field_to_interfaces[canonical].add(func_name)
        self.interface_to_fields[func_name][param_type][original] = canonical
        
        if canonical not in self.fields:
            self.fields[canonical] = {
                'canonical_name': canonical,
                'aliases': set(),
                'interfaces': [],
                'input_interfaces': [],
                'output_interfaces': [],
                'types': Counter(),
                'descriptions': [],
            }
        
        self.fields[canonical]['aliases'].add(original)
        
        if param_type == 'input':
            if func_name not in self.fields[canonical]['input_interfaces']:
                self.fields[canonical]['input_interfaces'].append(func_name)
        else:
            if func_name not in self.fields[canonical]['output_interfaces']:
                self.fields[canonical]['output_interfaces'].append(func_name)
        
        # 记录类型
        for key in ['类型', 'type', 'Type']:
            if key in param:
                field_type = param[key]
                normalized_type = self.type_mapping.get(field_type.lower(), field_type.lower())
                self.fields[canonical]['types'][normalized_type] += 1
                break
        
        # 记录描述
        for key in ['描述', 'description', 'Description', '说明']:
            if key in param:
                desc = param[key]
                if desc and desc != '-':
                    self.fields[canonical]['descriptions'].append(desc)
                break
    
    def save_mapping(self):
        """保存映射关系"""
        # 1. 保存完整的 JSON 映射
        mapping_data = {
            'version': '2.0',
            'generated_at': pd.Timestamp.now().isoformat(),
            'interfaces': self.interfaces,
            'fields': {},
            'field_to_interfaces': {k: list(v) for k, v in self.field_to_interfaces.items()},
            'interface_to_fields': self.interface_to_fields,
        }
        
        for canonical, info in self.fields.items():
            mapping_data['fields'][canonical] = {
                'canonical_name': canonical,
                'aliases': sorted(list(info['aliases'] - {canonical})),
                'interfaces': sorted(list(info['interfaces'])),
                'input_interfaces': sorted(info['input_interfaces']),
                'output_interfaces': sorted(info['output_interfaces']),
                'common_type': info['types'].most_common(1)[0][0] if info['types'] else 'string',
                'all_types': dict(info['types']),
                'descriptions': list(set(info['descriptions']))[:10],
            }
        
        json_path = os.path.join(self.output_dir, 'complete_field_mapping.json')
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        print(f"✓ 完整映射已保存到: {json_path}")
        
        # 2. 保存字段查询表（CSV）
        field_list = []
        for canonical, info in self.fields.items():
            field_list.append({
                '标准字段名': canonical,
                '别名': '|'.join(sorted(list(info['aliases'] - {canonical}))),
                '常见类型': info['types'].most_common(1)[0][0] if info['types'] else 'string',
                '可从接口获取': len(info['output_interfaces']),
                '输出接口示例': '|'.join(sorted(info['output_interfaces'][:10])),
                '可用于接口参数': len(info['input_interfaces']),
                '输入接口示例': '|'.join(sorted(info['input_interfaces'][:10])),
            })
        
        field_df = pd.DataFrame(field_list)
        field_df = field_df.sort_values('可从接口获取', ascending=False)
        field_csv = os.path.join(self.output_dir, 'field_query_table.csv')
        field_df.to_csv(field_csv, index=False, encoding='utf-8-sig')
        print(f"✓ 字段查询表已保存到: {field_csv}")
        
        # 3. 保存接口查询表（CSV）
        interface_list = []
        for func_name, iface in self.interfaces.items():
            input_fields = list(self.interface_to_fields.get(func_name, {}).get('input', {}).keys())
            output_fields = list(self.interface_to_fields.get(func_name, {}).get('output', {}).keys())
            
            interface_list.append({
                '接口名': func_name,
                '数据类型': iface.get('data_type', ''),
                '描述': iface.get('description', ''),
                '输入参数数量': len(input_fields),
                '输入参数': '|'.join(input_fields[:20]),
                '输出字段数量': len(output_fields),
                '输出字段': '|'.join(output_fields[:20]),
            })
        
        interface_df = pd.DataFrame(interface_list)
        interface_df = interface_df.sort_values(['数据类型', '接口名'])
        interface_csv = os.path.join(self.output_dir, 'interface_query_table.csv')
        interface_df.to_csv(interface_csv, index=False, encoding='utf-8-sig')
        print(f"✓ 接口查询表已保存到: {interface_csv}")
        
        return mapping_data
    
    def generate(self):
        """主生成函数"""
        print("=" * 80)
        print("完整字段映射系统")
        print("=" * 80)
        
        # 1. 解析所有接口
        interfaces = self.parse_all_interfaces()
        
        # 2. 构建映射
        self.build_mapping(interfaces)
        
        # 3. 显示统计
        print("\n统计信息:")
        print(f"  - 总接口数: {len(self.interfaces)}")
        print(f"  - 总标准字段数: {len(self.fields)}")
        
        print("\n  输出接口最多的字段:")
        top_fields = sorted(
            self.fields.items(),
            key=lambda x: len(x[1]['output_interfaces']),
            reverse=True
        )[:10]
        
        for i, (name, info) in enumerate(top_fields, 1):
            print(f"    {i}. {name}: {len(info['output_interfaces'])} 个接口")
        
        # 4. 保存
        print("\n正在保存映射...")
        mapping_data = self.save_mapping()
        
        print("\n" + "=" * 80)
        print("✓ 字段映射系统生成完成!")
        print("=" * 80)
        
        return mapping_data

class FieldQueryTool:
    """字段查询工具类"""
    def __init__(self, mapping_file: str = "result/complete_field_mapping.json"):
        self.mapping_file = mapping_file
        self.mapping_data = None
        self._load_mapping()
    
    def _load_mapping(self):
        """加载映射数据"""
        if os.path.exists(self.mapping_file):
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                self.mapping_data = json.load(f)
            print(f"✓ 已加载映射数据: {self.mapping_file}")
        else:
            print(f"✗ 映射文件不存在: {self.mapping_file}")
    
    def find_interfaces_for_field(self, field_name: str, show_detail: bool = True):
        """查找可以提供指定字段的接口"""
        if not self.mapping_data:
            print("请先生成映射数据")
            return
        
        fields = self.mapping_data.get('fields', {})
        
        # 查找匹配的字段
        matched_canonical = None
        for canonical, info in fields.items():
            if field_name == canonical or field_name in info.get('aliases', []):
                matched_canonical = canonical
                break
        
        if not matched_canonical:
            print(f"未找到字段: {field_name}")
            print("\n相似字段:")
            for canonical in sorted(fields.keys()):
                if field_name.lower() in canonical.lower():
                    print(f"  - {canonical}")
            return
        
        info = fields[matched_canonical]
        
        print(f"\n{'='*80}")
        print(f"字段: {field_name}")
        print(f"标准名: {matched_canonical}")
        print(f"别名: {', '.join(info.get('aliases', []))}")
        print(f"常见类型: {info.get('common_type', 'string')}")
        print(f"{'='*80}")
        
        output_interfaces = info.get('output_interfaces', [])
        print(f"\n可以从 {len(output_interfaces)} 个接口获取此字段:")
        
        if show_detail:
            interfaces = self.mapping_data.get('interfaces', {})
            for i, iface_name in enumerate(sorted(output_interfaces), 1):
                iface = interfaces.get(iface_name, {})
                print(f"\n  {i}. {iface_name}")
                print(f"     类型: {iface.get('data_type', '')}")
                desc = iface.get('description', '')
                if len(desc) > 80:
                    desc = desc[:80] + "..."
                print(f"     描述: {desc}")
        else:
            print(f"\n  {', '.join(sorted(output_interfaces[:20]))}")
            if len(output_interfaces) > 20:
                print(f"  ... 还有 {len(output_interfaces) - 20} 个接口")
        
        input_interfaces = info.get('input_interfaces', [])
        if input_interfaces:
            print(f"\n\n此字段可用于 {len(input_interfaces)} 个接口的输入参数:")
            print(f"  {', '.join(sorted(input_interfaces[:20]))}")
            if len(input_interfaces) > 20:
                print(f"  ... 还有 {len(input_interfaces) - 20} 个接口")
    
    def find_fields_for_interface(self, func_name: str, show_detail: bool = True):
        """查找指定接口的输入/输出字段"""
        if not self.mapping_data:
            print("请先生成映射数据")
            return
        
        interfaces = self.mapping_data.get('interfaces', {})
        if func_name not in interfaces:
            print(f"未找到接口: {func_name}")
            print("\n相似接口:")
            for name in sorted(interfaces.keys()):
                if func_name.lower() in name.lower():
                    print(f"  - {name}")
            return
        
        iface = interfaces[func_name]
        interface_fields = self.mapping_data.get('interface_to_fields', {}).get(func_name, {'input': {}, 'output': {}})
        
        print(f"\n{'='*80}")
        print(f"接口: {func_name}")
        print(f"类型: {iface.get('data_type', '')}")
        print(f"描述: {iface.get('description', '')}")
        print(f"{'='*80}")
        
        input_fields = interface_fields.get('input', {})
        print(f"\n输入参数 ({len(input_fields)} 个):")
        for orig, canonical in sorted(input_fields.items()):
            if orig == canonical:
                print(f"  - {orig}")
            else:
                print(f"  - {orig} → {canonical}")
        
        output_fields = interface_fields.get('output', {})
        print(f"\n输出字段 ({len(output_fields)} 个):")
        for orig, canonical in sorted(output_fields.items()):
            if orig == canonical:
                print(f"  - {orig}")
            else:
                print(f"  - {orig} → {canonical}")
    
    def interactive_mode(self):
        """交互模式"""
        print("\n" + "="*80)
        print("字段查询工具 - 交互模式")
        print("="*80)
        print("\n命令:")
        print("  field <字段名>  - 查询可以提供某字段的接口")
        print("  iface <接口名>  - 查询某接口的输入/输出字段")
        print("  quit            - 退出")
        
        while True:
            try:
                cmd = input("\n> ").strip()
                
                if cmd.lower() in ['quit', 'exit', 'q']:
                    break
                
                if cmd.startswith('field '):
                    field_name = cmd[6:].strip()
                    self.find_interfaces_for_field(field_name)
                elif cmd.startswith('iface '):
                    iface_name = cmd[6:].strip()
                    self.find_fields_for_interface(iface_name)
                elif cmd:
                    print("未知命令，请使用: field <字段名>, iface <接口名>, 或 quit")
            
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"错误: {e}")

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='字段映射系统')
    parser.add_argument('--generate', action='store_true', help='生成映射数据')
    parser.add_argument('--field', help='查询字段')
    parser.add_argument('--iface', help='查询接口')
    parser.add_argument('--interactive', action='store_true', help='交互模式')
    
    args = parser.parse_args()
    
    if args.generate:
        mapper = FieldMapper()
        mapper.generate()
    elif args.field:
        tool = FieldQueryTool()
        tool.find_interfaces_for_field(args.field)
    elif args.iface:
        tool = FieldQueryTool()
        tool.find_fields_for_interface(args.iface)
    elif args.interactive:
        tool = FieldQueryTool()
        tool.interactive_mode()
    else:
        # 默认生成映射
        mapper = FieldMapper()
        mapper.generate()

if __name__ == "__main__":
    main()
