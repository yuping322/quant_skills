#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
分析AKShare宏观数据接口的返回值范围
"""

import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

def analyze_column_values(df, column_name):
    """分析列的取值范围"""
    if column_name not in df.columns:
        return None
    
    col_data = df[column_name].dropna()
    if len(col_data) == 0:
        return {"total": 0, "unique_count": 0}
    
    unique_values = col_data.unique()
    
    result = {
        "total": len(col_data),
        "unique_count": len(unique_values),
        "dtype": str(col_data.dtype),
    }
    
    # 对于不同类型的列分析取值
    if col_data.dtype == 'object':
        # 字符串类型
        result["type"] = "string"
        if len(unique_values) <= 20:
            result["sample_values"] = list(unique_values[:10])
        else:
            result["sample_values"] = list(unique_values[:5])
    elif col_data.dtype in ['int64', 'float64']:
        # 数值类型
        result["type"] = "numeric"
        result["min"] = float(col_data.min()) if pd.notna(col_data.min()) else None
        result["max"] = float(col_data.max()) if pd.notna(col_data.max()) else None
        result["mean"] = float(col_data.mean()) if pd.notna(col_data.mean()) else None
        
        # 检查是否为日期格式
        if 'date' in column_name.lower() or 'time' in column_name.lower() or '月' in column_name:
            result["is_date_like"] = True
    
    return result

def get_date_range_samples():
    """测试日期范围参数"""
    print("=" * 60)
    print("测试日期范围参数 (start_date, end_date)")
    print("=" * 60)
    
    # 测试股票历史数据接口
    try:
        df = ak.stock_zh_a_hist(symbol="000001", start_date="20230101", end_date="20231231")
        print(f"\n[stock_zh_a_hist] 返回数据形状: {df.shape}")
        print(f"列名: {list(df.columns)}")
        if '日期' in df.columns:
            print(f"日期范围: {df['日期'].min()} 至 {df['日期'].max()}")
    except Exception as e:
        print(f"Error: {e}")

def test_macro_interfaces():
    """测试宏观数据接口"""
    interfaces_to_test = [
        # 中国宏观数据
        ("macro_china_gdp_yearly", lambda: ak.macro_china_gdp_yearly()),
        ("macro_china_cpi_yearly", lambda: ak.macro_china_cpi_yearly()),
        ("macro_china_ppi_yearly", lambda: ak.macro_china_ppi_yearly()),
        ("macro_china_m2_yearly", lambda: ak.macro_china_m2_yearly()),
        ("macro_china_pmi_yearly", lambda: ak.macro_china_pmi_yearly()),
        ("macro_china_urban_unemployment", lambda: ak.macro_china_urban_unemployment()),
        ("macro_china_fx_reserves_yearly", lambda: ak.macro_china_fx_reserves_yearly()),
        
        # 中国金融指标
        ("macro_china_shibor_all", lambda: ak.macro_china_shibor_all()),
        ("macro_china_lpr", lambda: ak.macro_china_lpr()),
        
        # 物价水平
        ("macro_china_cpi", lambda: ak.macro_china_cpi()),
        ("macro_china_ppi", lambda: ak.macro_china_ppi()),
        
        # 贸易数据
        ("macro_china_trade_balance", lambda: ak.macro_china_trade_balance()),
        ("macro_china_exports_yoy", lambda: ak.macro_china_exports_yoy()),
        
        # 金融数据
        ("macro_china_money_supply", lambda: ak.macro_china_money_supply()),
        
        # 美国宏观数据
        ("macro_usa_non_farm", lambda: ak.macro_usa_non_farm()),
        ("macro_usa_cpi_monthly", lambda: ak.macro_usa_cpi_monthly()),
        ("macro_usa_unemployment_rate", lambda: ak.macro_usa_unemployment_rate()),
    ]
    
    results = {}
    
    for name, func in interfaces_to_test:
        try:
            df = func()
            print(f"\n{'='*60}")
            print(f"接口: {name}")
            print(f"数据形状: {df.shape}")
            print(f"列名: {list(df.columns)}")
            
            results[name] = {
                "shape": df.shape,
                "columns": list(df.columns),
                "column_analysis": {}
            }
            
            # 分析每列的取值
            for col in df.columns:
                analysis = analyze_column_values(df, col)
                if analysis:
                    results[name]["column_analysis"][col] = analysis
                    print(f"\n  {col}:")
                    print(f"    - 类型: {analysis.get('type', 'unknown')}")
                    if analysis.get('type') == 'numeric':
                        print(f"    - 范围: {analysis.get('min')} 至 {analysis.get('max')}")
                    elif analysis.get('type') == 'string':
                        print(f"    - 唯一值数量: {analysis.get('unique_count')}")
                        print(f"    - 示例值: {analysis.get('sample_values', [])[:5]}")
                        
        except Exception as e:
            print(f"\n接口 {name} 出错: {e}")
    
    return results

def test_input_params():
    """测试输入参数的取值范围"""
    print("\n" + "=" * 60)
    print("测试输入参数的可能取值")
    print("=" * 60)
    
    # 测试 symbol 参数 - 股票代码
    print("\n[测试 symbol 参数 - 股票代码]")
    try:
        # 获取A股列表
        df = ak.stock_zh_a_spot_em()
        print(f"A股数量: {len(df)}")
        print(f"代码示例: {df['代码'].head(10).tolist()}")
    except Exception as e:
        print(f"Error: {e}")
    
    # 测试 adjust 参数 - 复权类型
    print("\n[测试 adjust 参数 - 复权类型]")
    print("可能的值: 'qfq' (前复权), 'hfq' (后复权), 'None' (不复权)")
    
    # 测试 period 参数 - 周期
    print("\n[测试 period 参数 - 周期]")
    print("可能的值: 'daily', 'weekly', 'monthly'")
    
    # 测试 date 参数 - 日期
    print("\n[测试 date 参数 - 日期格式]")
    print("格式: 'YYYYMMDD' 或 'YYYY-MM-DD'")
    
    # 测试 market 参数 - 市场
    print("\n[测试 market 参数 - 市场]")
    print("可能的值: 'sh', 'sz', 'hk', 'us'")

if __name__ == "__main__":
    print("开始分析AKShare接口参数取值范围...")
    print(f"时间: {datetime.now()}")
    
    # 测试输入参数
    test_input_params()
    
    # 测试宏观数据接口
    results = test_macro_interfaces()
    
    # 保存结果
    print("\n\n分析完成!")
