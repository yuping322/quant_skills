#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
完整工作流执行脚本
自动化执行字段统一化的完整流程
"""

import os
import sys
import time
from typing import Dict, Any

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class WorkflowExecutor:
    """工作流执行器"""
    
    def __init__(self):
        self.steps = [
            {
                "name": "数据收集与分析",
                "function": self.step_collect_data,
                "description": "从data目录和akshare接口收集字段信息"
            },
            {
                "name": "生成LLM提示词",
                "function": self.step_generate_prompts,
                "description": "生成增强版的LLM分析提示词"
            },
            {
                "name": "用户LLM分析",
                "function": self.step_user_llm_analysis,
                "description": "提示用户进行LLM分析"
            },
            {
                "name": "生成统一配置",
                "function": self.step_generate_config,
                "description": "从LLM分析结果生成统一配置"
            },
            {
                "name": "验证配置",
                "function": self.step_validate_config,
                "description": "验证生成的配置"
            },
            {
                "name": "测试系统",
                "function": self.step_test_system,
                "description": "测试统一字段系统"
            }
        ]
        
        self.results = {}
    
    def print_header(self, title: str):
        """打印标题"""
        print("\n" + "=" * 80)
        print(f"  {title}")
        print("=" * 80)
    
    def print_step(self, step_num: int, step_name: str, description: str):
        """打印步骤信息"""
        print(f"\n[{step_num}/{len(self.steps)}] {step_name}")
        print(f"    {description}")
        print("-" * 60)
    
    def step_collect_data(self) -> bool:
        """步骤1: 数据收集与分析"""
        print("数据收集与分析已经在之前完成！")
        print("检查已有的分析文件...")
        
        required_files = [
            "analysis/complete_field_statistics.json",
            "prompts/enhanced_field_equivalence_analysis.txt",
            "prompts/enhanced_field_coverage_analysis.txt"
        ]
        
        all_exist = True
        for file_path in required_files:
            if os.path.exists(file_path):
                print(f"  ✓ {file_path}")
            else:
                print(f"  ✗ {file_path} (未找到)")
                all_exist = False
        
        if not all_exist:
            print("\n需要先运行数据分析...")
            print("执行: python src/scripts/llm_field_analyzer_v2.py")
            
            try:
                from scripts.llm_field_analyzer_v2 import LLMFieldAnalyzerV2
                analyzer = LLMFieldAnalyzerV2()
                analyzer.generate_all_enhanced_prompts()
                return True
            except Exception as e:
                print(f"执行失败: {e}")
                return False
        
        return True
    
    def step_generate_prompts(self) -> bool:
        """步骤2: 生成LLM提示词"""
        print("提示词已经生成！")
        
        prompts_dir = "prompts"
        if os.path.exists(prompts_dir):
            files = os.listdir(prompts_dir)
            print(f"\n已生成的提示词:")
            for file in files:
                file_path = os.path.join(prompts_dir, file)
                size = os.path.getsize(file_path)
                print(f"  - {file} ({size} 字节)")
        
        return True
    
    def step_user_llm_analysis(self) -> bool:
        """步骤3: 用户LLM分析"""
        print("\n" + "*" * 80)
        print("  用户手动操作步骤")
        print("*" * 80)
        
        print("\n请执行以下操作:")
        print("\n1. 打开以下文件并进行LLM分析:")
        print("   - prompts/enhanced_field_equivalence_analysis.txt")
        print("   - prompts/enhanced_field_coverage_analysis.txt")
        
        print("\n2. 将LLM分析结果保存到:")
        print("   - config/llm_equivalence_analysis.json")
        print("   - config/llm_coverage_analysis.json")
        
        print("\n3. 保存完成后，按回车键继续...")
        
        try:
            input()
        except KeyboardInterrupt:
            print("\n\n用户中断，退出工作流")
            return False
        
        # 检查文件是否存在
        config_dir = "config"
        required_files = [
            "llm_equivalence_analysis.json",
            "llm_coverage_analysis.json"
        ]
        
        print("\n检查分析结果文件...")
        all_exist = True
        for file in required_files:
            file_path = os.path.join(config_dir, file)
            if os.path.exists(file_path):
                print(f"  ✓ {file}")
            else:
                print(f"  ✗ {file} (未找到)")
                all_exist = False
        
        if not all_exist:
            print("\n文件未找到，是否继续使用默认配置？(y/n)")
            try:
                answer = input().strip().lower()
                if answer != 'y':
                    return False
            except KeyboardInterrupt:
                return False
        
        return True
    
    def step_generate_config(self) -> bool:
        """步骤4: 生成统一配置"""
        print("\n生成统一配置...")
        
        try:
            from scripts.generate_unified_config import UnifiedConfigGenerator
            generator = UnifiedConfigGenerator()
            config = generator.generate_config()
            config_path = generator.save_config(config)
            
            self.results["config_path"] = config_path
            return True
        except Exception as e:
            print(f"生成配置失败: {e}")
            return False
    
    def step_validate_config(self) -> bool:
        """步骤5: 验证配置"""
        print("\n验证配置...")
        
        try:
            from scripts.validate_field_equivalents import FieldEquivalentsValidator
            validator = FieldEquivalentsValidator()
            validator.generate_validation_report()
            return True
        except Exception as e:
            print(f"验证失败: {e}")
            return False
    
    def step_test_system(self) -> bool:
        """步骤6: 测试系统"""
        print("\n测试统一字段系统...")
        
        config_path = self.results.get("config_path", "config/unified_field_config.json")
        
        try:
            from unified_field_system import UnifiedFieldSystem
            
            print(f"\n初始化系统，使用配置: {config_path}")
            system = UnifiedFieldSystem(config_path=config_path)
            
            print("\n系统初始化成功！")
            print("\n测试简单的字段转换:")
            
            test_fields = ["日期", "代码", "收盘价", "开盘价"]
            for field in test_fields:
                unified = system._map_to_standard_field(field)
                print(f"  {field} → {unified}")
            
            print("\n系统测试完成！")
            return True
        except Exception as e:
            print(f"测试失败: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def run(self):
        """运行完整工作流"""
        self.print_header("字段统一化完整工作流")
        
        success_count = 0
        failed_steps = []
        
        for i, step in enumerate(self.steps, 1):
            self.print_step(i, step["name"], step["description"])
            
            try:
                success = step["function"]()
                if success:
                    print(f"\n✓ 步骤 {i} 完成")
                    success_count += 1
                else:
                    print(f"\n✗ 步骤 {i} 失败")
                    failed_steps.append(step["name"])
            except Exception as e:
                print(f"\n✗ 步骤 {i} 执行出错: {e}")
                import traceback
                traceback.print_exc()
                failed_steps.append(step["name"])
        
        # 总结
        self.print_header("工作流总结")
        
        print(f"\n总步骤数: {len(self.steps)}")
        print(f"成功: {success_count}/{len(self.steps)}")
        
        if failed_steps:
            print(f"\n失败的步骤:")
            for step in failed_steps:
                print(f"  - {step}")
        else:
            print("\n所有步骤完成！")
        
        print("\n" + "=" * 80)
        print("下一步:")
        print("  1. 查看 config/unified_field_config.json")
        print("  2. 使用统一字段系统: python src/unified_field_system.py --test")
        print("  3. 根据需要迭代改进配置")
        print("=" * 80)


def main():
    """主函数"""
    executor = WorkflowExecutor()
    executor.run()


if __name__ == "__main__":
    main()
