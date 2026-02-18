#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统一字段配置生成器
从LLM分析结果生成完整的统一字段配置文件
"""

import json
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class UnifiedConfigGenerator:
    """统一配置生成器"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = config_dir
        os.makedirs(config_dir, exist_ok=True)
    
    def load_llm_analysis(self) -> Dict[str, Any]:
        """加载LLM分析结果"""
        print("加载LLM分析结果...")
        
        equivalence_file = os.path.join(self.config_dir, "llm_equivalence_analysis.json")
        coverage_file = os.path.join(self.config_dir, "llm_coverage_analysis.json")
        
        result = {
            "equivalence": None,
            "coverage": None
        }
        
        if os.path.exists(equivalence_file):
            with open(equivalence_file, 'r', encoding='utf-8') as f:
                result["equivalence"] = json.load(f)
            print(f"  ✓ 已加载: {equivalence_file}")
        else:
            print(f"  ⚠  未找到: {equivalence_file}")
            print(f"     请使用 prompts/enhanced_field_equivalence_analysis.txt 进行LLM分析")
        
        if os.path.exists(coverage_file):
            with open(coverage_file, 'r', encoding='utf-8') as f:
                result["coverage"] = json.load(f)
            print(f"  ✓ 已加载: {coverage_file}")
        else:
            print(f"  ⚠  未找到: {coverage_file}")
            print(f"     请使用 prompts/enhanced_field_coverage_analysis.txt 进行LLM分析")
        
        return result
    
    def generate_field_equivalents(self, llm_data: Dict[str, Any]) -> Dict[str, List[str]]:
        """从LLM数据生成字段等价关系"""
        print("\n生成字段等价关系...")
        
        field_equivalents = {}
        
        # 从等价关系分析中提取
        if llm_data.get("equivalence"):
            field_groups = llm_data["equivalence"].get("field_groups", [])
            for group in field_groups:
                canonical = group.get("canonical")
                equivalents = group.get("equivalents", [])
                if canonical and equivalents:
                    field_equivalents[canonical] = equivalents
                    print(f"  ✓ {canonical}: {len(equivalents)} 个等价字段")
        
        # 从覆盖分析中补充
        if llm_data.get("coverage"):
            suggestions = llm_data["coverage"].get("suggestions", [])
            for suggestion in suggestions:
                field = suggestion.get("field")
                suggested_canonical = suggestion.get("suggested_canonical")
                action = suggestion.get("action")
                
                if field and suggested_canonical:
                    if action == "create_new":
                        # 创建新的标准字段
                        if suggested_canonical not in field_equivalents:
                            field_equivalents[suggested_canonical] = []
                        if field not in field_equivalents[suggested_canonical]:
                            field_equivalents[suggested_canonical].append(field)
                    elif action == "add_to_existing":
                        # 添加到现有标准字段
                        if suggested_canonical in field_equivalents:
                            if field not in field_equivalents[suggested_canonical]:
                                field_equivalents[suggested_canonical].append(field)
                        else:
                            field_equivalents[suggested_canonical] = [field]
        
        print(f"\n总计: {len(field_equivalents)} 个标准字段")
        return field_equivalents
    
    def generate_value_normalization(self) -> Dict[str, Any]:
        """生成值标准化配置"""
        print("生成值标准化配置...")
        
        normalization = {
            "date": {
                "formatter": "date",
                "format": "YYYY-MM-DD"
            },
            "symbol": {
                "formatter": "stock_code",
                "market_prefix": True
            },
            "numeric": {
                "formatter": "float",
                "decimal_places": 4
            }
        }
        
        return normalization
    
    def load_default_equivalents(self) -> Dict[str, List[str]]:
        """加载默认的字段等价关系（当没有LLM分析时使用）"""
        print("\n使用默认字段等价关系...")
        
        # 尝试从field_mapper加载
        try:
            from mappers.field_mapper import FieldMapper
            mapper = FieldMapper()
            print("  ✓ 从 FieldMapper 加载默认配置")
            return mapper.field_equivalents
        except Exception as e:
            print(f"  ⚠  无法加载 FieldMapper: {e}")
            return {}
    
    def generate_config(self) -> Dict[str, Any]:
        """生成完整配置"""
        print("=" * 80)
        print("统一字段配置生成器")
        print("=" * 80)
        
        # 加载LLM分析结果
        llm_data = self.load_llm_analysis()
        
        # 生成字段等价关系
        if llm_data.get("equivalence") or llm_data.get("coverage"):
            field_equivalents = self.generate_field_equivalents(llm_data)
        else:
            field_equivalents = self.load_default_equivalents()
        
        # 生成值标准化配置
        value_normalization = self.generate_value_normalization()
        
        # 构建完整配置
        config = {
            "version": "1.0",
            "generated_at": datetime.now().isoformat(),
            "description": "统一字段配置文件",
            "field_equivalents": field_equivalents,
            "api_mappings": {},
            "value_normalization": value_normalization
        }
        
        return config
    
    def save_config(self, config: Dict[str, Any], filename: str = "unified_field_config.json"):
        """保存配置文件"""
        output_path = os.path.join(self.config_dir, filename)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, ensure_ascii=False, indent=2)
        
        print(f"\n配置文件已保存到: {output_path}")
        print(f"  大小: {os.path.getsize(output_path)} 字节")
        
        # 打印配置摘要
        print(f"\n配置摘要:")
        print(f"  版本: {config['version']}")
        print(f"  生成时间: {config['generated_at']}")
        print(f"  标准字段数: {len(config['field_equivalents'])}")
        print(f"  API映射数: {len(config['api_mappings'])}")
        
        return output_path


def main():
    """主函数"""
    generator = UnifiedConfigGenerator()
    config = generator.generate_config()
    generator.save_config(config)
    
    print("\n" + "=" * 80)
    print("配置生成完成！")
    print("=" * 80)
    print("\n下一步:")
    print("  1. 查看 config/unified_field_config.json")
    print("  2. 修改 src/unified_field_system.py 使用配置文件")
    print("  3. 运行验证工具: python src/scripts/validate_field_equivalents.py")


if __name__ == "__main__":
    main()
