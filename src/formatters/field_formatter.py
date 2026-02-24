#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
字段格式转换系统
提供各种字段格式的标准化转换功能
"""

import re
from typing import Optional, Union, Dict, Any, Tuple
from enum import Enum


class StockCodeFormat(Enum):
    """股票代码格式"""
    PURE_NUMERIC = "pure_numeric"  # 纯数字，如 "000001"
    WITH_SUFFIX = "with_suffix"  # 带后缀，如 "000001.SZ"
    WITH_PREFIX = "with_prefix"  # 带前缀，如 "sz000001"


class DateFormat(Enum):
    """日期格式"""
    YYYYMMDD = "YYYYMMDD"  # "20240101"
    YYYY_MM_DD = "YYYY-MM-DD"  # "2024-01-01"
    YYYY_MM_DD_HH_MM_SS = "YYYY-MM-DD HH:MM:SS"  # "2024-01-01 12:34:56"
    YYYYMM = "YYYYMM"  # "202401"


class FieldFormatter:
    """字段格式化器"""
    
    # 股票代码后缀映射
    MARKET_SUFFIX = {
        "sh": ["SH", "sh", ".SH", ".sh"],
        "sz": ["SZ", "sz", ".SZ", ".sz"],
        "bj": ["BJ", "bj", ".BJ", ".bj"],
    }
    
    # 股票代码前缀映射
    MARKET_PREFIX = {
        "sh": ["sh", "SH"],
        "sz": ["sz", "SZ"],
        "bj": ["bj", "BJ"],
    }
    
    @staticmethod
    def normalize_stock_code(code: Optional[Union[str, int]], 
                           target_format: StockCodeFormat = StockCodeFormat.PURE_NUMERIC,
                           default_market: str = "auto") -> Optional[str]:
        """
        标准化股票代码格式
        
        Args:
            code: 股票代码
            target_format: 目标格式
            default_market: 默认市场（当无法推断时使用）
            
        Returns:
            标准化后的股票代码
        """
        if code is None:
            return None
        
        code_str = str(code).strip()
        if not code_str:
            return None
        
        # 1. 提取纯数字部分和市场标识
        numeric_part = ""
        market = None
        
        # 先清理一下字符串
        code_str_clean = code_str
        
        # 检查是否有后缀（按长度排序，先匹配长的）
        all_suffixes = []
        for mkt, suffixes in FieldFormatter.MARKET_SUFFIX.items():
            for suffix in suffixes:
                all_suffixes.append((mkt, suffix))
        
        # 按后缀长度降序排序
        all_suffixes.sort(key=lambda x: len(x[1]), reverse=True)
        
        for mkt, suffix in all_suffixes:
            if code_str_clean.endswith(suffix):
                numeric_part = code_str_clean[:-len(suffix)]
                market = mkt
                break
        
        # 检查是否有前缀
        if not market:
            all_prefixes = []
            for mkt, prefixes in FieldFormatter.MARKET_PREFIX.items():
                for prefix in prefixes:
                    all_prefixes.append((mkt, prefix))
            
            all_prefixes.sort(key=lambda x: len(x[1]), reverse=True)
            
            for mkt, prefix in all_prefixes:
                if code_str_clean.startswith(prefix):
                    numeric_part = code_str_clean[len(prefix):]
                    market = mkt
                    break
        
        # 如果都没有，提取数字部分
        if not market:
            numeric_match = re.search(r'(\d+)', code_str_clean)
            if numeric_match:
                numeric_part = numeric_match.group(1)
                # 尝试根据代码长度推断市场
                if default_market == "auto":
                    if len(numeric_part) == 6:
                        if numeric_part.startswith('6'):
                            market = "sh"
                        elif numeric_part.startswith('0') or numeric_part.startswith('3'):
                            market = "sz"
                        elif numeric_part.startswith('8') or numeric_part.startswith('4') or numeric_part.startswith('9'):
                            market = "bj"
                else:
                    market = default_market
        
        if not numeric_part:
            return code_str
        
        # 只保留数字
        numeric_part = re.sub(r'[^\d]', '', numeric_part)
        
        # 补全到6位
        numeric_part = numeric_part.zfill(6)
        
        # 2. 按目标格式输出
        if target_format == StockCodeFormat.PURE_NUMERIC:
            return numeric_part
        elif target_format == StockCodeFormat.WITH_SUFFIX:
            if market:
                suffix = f".{market.upper()}"
                return f"{numeric_part}{suffix}"
            return numeric_part
        elif target_format == StockCodeFormat.WITH_PREFIX:
            if market:
                return f"{market.lower()}{numeric_part}"
            return numeric_part
        
        return numeric_part
    
    @staticmethod
    def normalize_date(date_str: Optional[str], 
                      target_format: DateFormat = DateFormat.YYYY_MM_DD) -> Optional[str]:
        """
        标准化日期格式
        
        Args:
            date_str: 日期字符串
            target_format: 目标格式
            
        Returns:
            标准化后的日期字符串
        """
        if date_str is None:
            return None
        
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() in ['nan', 'nat', 'none']:
            return None
        
        # 处理常见格式
        year = month = day = hour = minute = second = None
        
        # 优先手动解析中文日期格式（避免 pandas 错误解析）
        # 格式: 2024年1月1日, 2024年01月01日
        cn_date_match = re.match(r'(\d{4})年(\d{1,2})月(\d{1,2})日', date_str)
        if cn_date_match:
            year = int(cn_date_match.group(1))
            month = int(cn_date_match.group(2))
            day = int(cn_date_match.group(3))
        else:
            # 尝试用 pandas 解析（如果可用）
            try:
                import pandas as pd
                dt = pd.to_datetime(date_str, errors='coerce', dayfirst=False)
                if pd.notna(dt):
                    year = dt.year
                    month = dt.month
                    day = dt.day
                    hour = dt.hour
                    minute = dt.minute
                    second = dt.second
            except Exception:
                pass
        
        # 如果都解析失败，手动解析
        if year is None:
            # 提取数字部分
            digits = re.findall(r'\d+', date_str)
            if not digits:
                return date_str
            
            # 组合数字
            combined = ''.join(digits)
            
            # 根据长度解析
            if len(combined) >= 8:
                year = int(combined[0:4])
                month = int(combined[4:6])
                day = int(combined[6:8])
                if len(combined) >= 14:
                    hour = int(combined[8:10])
                    minute = int(combined[10:12])
                    second = int(combined[12:14])
            elif len(combined) == 6:
                year = int(combined[0:4])
                month = int(combined[4:6])
            elif len(combined) == 4:
                year = int(combined)
                month = 1
        
        if year is None:
            return date_str
        
        # 验证日期合理性
        if month is None or not (1 <= month <= 12):
            month = 1
        if day is None or not (1 <= day <= 31):
            day = 1
        
        # 生成目标格式
        if target_format == DateFormat.YYYYMMDD and year and month and day:
            return f"{year:04d}{month:02d}{day:02d}"
        elif target_format == DateFormat.YYYY_MM_DD and year and month and day:
            return f"{year:04d}-{month:02d}-{day:02d}"
        elif target_format == DateFormat.YYYY_MM_DD_HH_MM_SS and year and month and day:
            if hour is not None:
                return f"{year:04d}-{month:02d}-{day:02d} {hour:02d}:{minute or 0:02d}:{second or 0:02d}"
            return f"{year:04d}-{month:02d}-{day:02d} 00:00:00"
        elif target_format == DateFormat.YYYYMM and year and month:
            return f"{year:04d}{month:02d}"
        
        return date_str
    
    @staticmethod
    def normalize_float(value: Optional[Union[str, float, int]]) -> Optional[float]:
        """标准化浮点数"""
        if value is None:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        value_str = str(value).strip()
        if not value_str or value_str == '-' or value_str.lower() in ['nan', 'none']:
            return None
        
        # 处理百分比
        is_percent = False
        if '%' in value_str:
            is_percent = True
            value_str = value_str.replace('%', '')
        
        # 清理字符串，保留数字、小数点、负号、逗号
        value_str = re.sub(r'[^\d\.\-\,]', '', value_str)
        
        # 处理千位分隔符 - 先移除所有逗号（假设都是千位分隔符）
        # 这样可以确保 1,234.56 变成 1234.56
        if ',' in value_str and '.' in value_str:
            # 同时有逗号和小数点，逗号应该是千位分隔符
            value_str = value_str.replace(',', '')
        elif ',' in value_str:
            # 只有逗号，看后面的长度
            parts = value_str.split(',')
            if len(parts) > 2:
                # 多个逗号，千位分隔符
                value_str = value_str.replace(',', '')
            elif len(parts) == 2:
                # 一个逗号
                if len(parts[1]) == 3:
                    # 千位分隔符
                    value_str = value_str.replace(',', '')
                else:
                    # 小数点
                    value_str = value_str.replace(',', '.')
        
        try:
            result = float(value_str)
            if is_percent:
                result = result / 100.0
            return result
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def normalize_int(value: Optional[Union[str, float, int]]) -> Optional[int]:
        """标准化整数"""
        if value is None:
            return None
        
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(round(value))
        
        value_str = str(value).strip()
        if not value_str or value_str == '-' or value_str.lower() in ['nan', 'none']:
            return None
        
        # 先尝试作为浮点数解析，再取整
        float_val = FieldFormatter.normalize_float(value_str)
        if float_val is not None:
            return int(round(float_val))
        
        return None


class InterfaceFieldConverter:
    """接口字段转换器，根据接口配置进行字段转换"""
    
    def __init__(self, mapping_file: str = "result/complete_field_mapping.json"):
        self.mapping_file = mapping_file
        self.mapping_data = None
        self._load_mapping()
    
    def _load_mapping(self):
        """加载映射数据"""
        import os
        import json
        if os.path.exists(self.mapping_file):
            with open(self.mapping_file, 'r', encoding='utf-8') as f:
                self.mapping_data = json.load(f)
    
    def convert_field_from_api(self, field_name: str, value: Any, 
                             api_name: str) -> Tuple[str, Any]:
        """
        将API返回的字段转换为标准字段
        
        Args:
            field_name: API返回的原始字段名
            value: 字段值
            api_name: API名称
            
        Returns:
            (标准字段名, 转换后的值)
        """
        if not self.mapping_data:
            return field_name, value
        
        # 查找字段映射
        interface_fields = self.mapping_data.get('interface_to_fields', {}).get(api_name, {})
        output_mapping = interface_fields.get('output', {})
        
        canonical_name = output_mapping.get(field_name, field_name)
        
        # 根据字段类型进行值转换
        converted_value = self._convert_value(canonical_name, value)
        
        return canonical_name, converted_value
    
    def convert_field_to_api(self, field_name: str, value: Any,
                           api_name: str) -> Tuple[str, Any]:
        """
        将标准字段转换为API需要的格式
        
        Args:
            field_name: 标准字段名
            value: 字段值
            api_name: API名称
            
        Returns:
            (API字段名, 转换后的值)
        """
        if not self.mapping_data:
            return field_name, value
        
        # 查找字段映射（反向查找）
        interface_fields = self.mapping_data.get('interface_to_fields', {}).get(api_name, {})
        input_mapping = interface_fields.get('input', {})
        
        # 找到对应的原始字段名
        api_field_name = field_name
        for orig, canonical in input_mapping.items():
            if canonical == field_name:
                api_field_name = orig
                break
        
        # 根据字段类型进行值转换
        converted_value = self._convert_value_for_api(api_field_name, value, api_name)
        
        return api_field_name, converted_value
    
    def _convert_value(self, field_name: str, value: Any) -> Any:
        """根据标准字段名转换值"""
        # 日期字段
        if any(keyword in field_name.lower() for keyword in ['date', '日期', '时间', 'time']):
            if isinstance(value, str):
                return FieldFormatter.normalize_date(value, DateFormat.YYYY_MM_DD)
        
        # 股票代码字段
        if any(keyword in field_name.lower() for keyword in ['symbol', 'code', '代码']):
            if isinstance(value, str):
                return FieldFormatter.normalize_stock_code(value, StockCodeFormat.PURE_NUMERIC)
        
        # 数值字段
        if any(keyword in field_name.lower() for keyword in ['price', 'close', 'open', 'high', 'low', 'volume', 'amount', '涨', '跌', '价', '额', '量']):
            return FieldFormatter.normalize_float(value)
        
        return value
    
    def _convert_value_for_api(self, field_name: str, value: Any, api_name: str) -> Any:
        """根据API需要转换值格式"""
        # 这里可以根据不同API的要求进行特殊处理
        # 暂时使用默认转换
        return self._convert_value(field_name, value)


def main():
    """测试字段格式化器"""
    print("=" * 80)
    print("字段格式转换系统 - 测试")
    print("=" * 80)
    
    # 测试股票代码转换
    print("\n1. 股票代码转换测试:")
    test_codes = [
        "000001",
        "000001.SZ",
        "sz000001",
        "600000",
        "600000.SH",
        "sh600000",
        "888888",
    ]
    
    for code in test_codes:
        pure = FieldFormatter.normalize_stock_code(code, StockCodeFormat.PURE_NUMERIC)
        with_suffix = FieldFormatter.normalize_stock_code(code, StockCodeFormat.WITH_SUFFIX)
        with_prefix = FieldFormatter.normalize_stock_code(code, StockCodeFormat.WITH_PREFIX)
        print(f"  {code:15} → 纯数字: {pure or '-':10} 后缀: {with_suffix or '-':12} 前缀: {with_prefix or '-':12}")
    
    # 测试日期转换
    print("\n2. 日期格式转换测试:")
    test_dates = [
        "20240101",
        "2024-01-01",
        "2024-01-01 12:34:56",
        "2024/01/01",
        "2024年1月1日",
    ]
    
    for date in test_dates:
        ymd = FieldFormatter.normalize_date(date, DateFormat.YYYY_MM_DD)
        ymd_num = FieldFormatter.normalize_date(date, DateFormat.YYYYMMDD)
        print(f"  {date:25} → {ymd or '-':12} | {ymd_num or '-':8}")
    
    # 测试数值转换
    print("\n3. 数值转换测试:")
    test_values = [
        "123.45",
        "1,234.56",
        "-78.9",
        "0.00",
        "100%",
    ]
    
    for val in test_values:
        f_val = FieldFormatter.normalize_float(val)
        i_val = FieldFormatter.normalize_int(val)
        f_str = f"{f_val:10.2f}" if f_val is not None else "      None"
        i_str = f"{i_val:10d}" if i_val is not None else "      None"
        print(f"  {val:15} → float: {f_str} | int: {i_str}")
    
    print("\n" + "=" * 80)
    print("测试完成!")
    print("=" * 80)


if __name__ == "__main__":
    main()
