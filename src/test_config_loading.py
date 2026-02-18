#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试配置文件加载功能
"""

import os
import sys

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from unified_field_system import UnifiedFieldSystem


def test_config_loading():
    """测试配置文件加载"""
    print("=" * 80)
    print("测试配置文件加载")
    print("=" * 80)
    
    config_path = "config/unified_field_config.json"
    
    print(f"\n1. 检查配置文件: {config_path}")
    if os.path.exists(config_path):
        print(f"   ✓ 配置文件存在")
        size = os.path.getsize(config_path)
        print(f"   文件大小: {size} 字节")
    else:
        print(f"   ✗ 配置文件不存在")
        return False
    
    print(f"\n2. 初始化UnifiedFieldSystem...")
    try:
        system = UnifiedFieldSystem(config_path=config_path)
        print(f"   ✓ 系统初始化成功")
    except Exception as e:
        print(f"   ✗ 初始化失败: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print(f"\n3. 检查加载的配置...")
    print(f"   标准字段数: {len(system.field_equivalents)}")
    print(f"   API映射数: {len(system.api_mappings)}")
    
    print(f"\n4. 测试字段转换...")
    test_cases = [
        ("日期", "date"),
        ("代码", "symbol"),
        ("收盘价", "close"),
        ("开盘价", "open"),
        ("成交量", "volume"),
        ("成交额", "amount"),
        ("涨跌幅", "change_pct"),
    ]
    
    all_correct = True
    for original, expected in test_cases:
        result = system._map_to_standard_field(original)
        status = "✓" if result == expected else "✗"
        print(f"   {status} {original} → {result} (期望: {expected})")
        if result != expected:
            all_correct = False
    
    print(f"\n5. 列出所有标准字段...")
    for i, (canonical, equivalents) in enumerate(sorted(system.field_equivalents.items()), 1):
        print(f"   {i:2d}. {canonical} ({len(equivalents)} 个等价字段)")
    
    print("\n" + "=" * 80)
    if all_correct:
        print("测试通过！✓")
    else:
        print("部分测试失败！✗")
    print("=" * 80)
    
    return all_correct


def main():
    """主函数"""
    success = test_config_loading()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
