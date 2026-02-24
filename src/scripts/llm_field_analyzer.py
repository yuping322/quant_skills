#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于大模型的字段等价关系分析工具
使用大模型进行语义判断，生成更准确的字段等价关系
"""

import json
import os
import sys
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mappers.field_mapper import FieldMapper


class LLMFieldAnalyzer:
    """基于大模型的字段分析器"""
    
    def __init__(self):
        self.mapper = FieldMapper()
        self.interface_data = {}
        self.field_statistics = Counter()
        self.field_contexts = defaultdict(list)
        self.prompt_templates = {
            "field_equivalence": "你是一个专业的金融数据字段分析专家，负责分析字段名称的语义等价关系。\n\n请分析以下字段列表，将语义相同或相似的字段分组：\n\n{fields}\n\n对于每个组，请：\n1. 选择一个标准字段名（优先使用英文，采用下划线命名法）\n2. 列出所有语义等价的字段\n3. 简要说明分组理由\n\n输出格式：\n{{\n  \"field_groups\": [\n    {{\n      \"canonical\": \"标准字段名\",\n      \"equivalents\": [\"等价字段1\", \"等价字段2\"],\n      \"reason\": \"分组理由\"\n    }}\n  ]\n}}\n",
            "field_coverage": "你是一个专业的金融数据字段分析专家。\n\n以下是当前已有的字段等价关系：\n{existing_equivalents}\n\n以下是未覆盖的常见字段：\n{uncovered_fields}\n\n请分析这些未覆盖的字段，为每个字段建议合适的等价关系：\n1. 如果与现有字段等价，请指定对应的标准字段\n2. 如果是新的字段类型，请创建新的标准字段\n3. 简要说明建议理由\n\n输出格式：\n{{\n  \"suggestions\": [\n    {{\n      \"field\": \"字段名\",\n      \"suggested_canonical\": \"建议的标准字段\",\n      \"reason\": \"建议理由\"\n    }}\n  ]\n}}\n",
            "field_standardization": "你是一个专业的金融数据字段标准化专家。\n\n请将以下字段等价关系配置转换为Python代码：\n{equivalence_config}\n\n代码应该：\n1. 定义在self.field_equivalents字典中\n2. 英文作为标准字段\n3. 格式清晰，有适当的注释\n\n输出格式：\n```python\nself.field_equivalents = {{\n    # 字段类型注释\n    \"标准字段\": [\"等价字段1\", \"等价字段2\"]\n}}\n```\n"
        }
    
    def load_interface_data(self):
        """加载接口数据"""
        print("加载接口数据...")
        
        # 尝试从生成的映射文件中加载
        mapping_file = "result/complete_field_mapping.json"
        if os.path.exists(mapping_file):
            with open(mapping_file, 'r', encoding='utf-8') as f:
                mapping_data = json.load(f)
                self.interface_data = mapping_data.get('interfaces', {})
        else:
            # 如果没有映射文件，重新解析
            self.interface_data = self.mapper.parse_all_interfaces()
            self.mapper.build_mapping(self.interface_data)
        
        print(f"加载了 {len(self.interface_data)} 个接口")
        return self.interface_data
    
    def collect_field_contexts(self) -> Dict[str, List[Dict]]:
        """收集字段上下文信息"""
        print("收集字段上下文...")
        
        field_contexts = defaultdict(list)
        
        for api_name, interface in self.interface_data.items():
            # 收集输入参数
            for param in interface.get('input_params', []):
                field_name = param.get('名称')
                if field_name and field_name != '-' and field_name != '无':
                    field_contexts[field_name].append({
                        "api": api_name,
                        "type": "input",
                        "description": param.get('描述', ''),
                        "required": param.get('必填', '否') == '是'
                    })
            
            # 收集输出参数
            for param in interface.get('output_params', []):
                field_name = param.get('名称')
                if field_name and field_name != '-' and field_name != '无':
                    field_contexts[field_name].append({
                        "api": api_name,
                        "type": "output",
                        "description": param.get('描述', ''),
                        "example": param.get('示例', '')
                    })
        
        self.field_contexts = field_contexts
        self.field_statistics = Counter({field: len(contexts) for field, contexts in field_contexts.items()})
        
        print(f"收集了 {len(field_contexts)} 个不同的字段")
        return field_contexts
    
    def generate_field_equivalence_prompt(self, top_n: int = 100) -> str:
        """生成字段等价关系分析提示词"""
        print(f"生成字段等价关系分析提示词（前{top_n}个字段）...")
        
        # 获取最常见的字段
        top_fields = self.field_statistics.most_common(top_n)
        fields_list = [field for field, _ in top_fields]
        
        template = self.prompt_templates["field_equivalence"]
        prompt = template.replace("{fields}", "\n".join(fields_list))
        
        return prompt
    
    def generate_coverage_analysis_prompt(self, existing_equivalents: Dict[str, List[str]], top_n: int = 50) -> str:
        """生成覆盖分析提示词"""
        print(f"生成覆盖分析提示词（前{top_n}个未覆盖字段）...")
        
        # 获取未覆盖的字段
        covered_fields = set()
        for canonical, equivalents in existing_equivalents.items():
            covered_fields.add(canonical)
            covered_fields.update(equivalents)
        
        uncovered_fields = [(field, count) for field, count in self.field_statistics.items() 
                           if field not in covered_fields]
        uncovered_fields.sort(key=lambda x: x[1], reverse=True)
        top_uncovered = uncovered_fields[:top_n]
        
        # 格式化现有等价关系
        existing_str = "\n".join([f"{canonical}: {equivalents}" for canonical, equivalents in existing_equivalents.items()])
        
        # 格式化未覆盖字段
        uncovered_str = "\n".join([f"{field} (出现 {count} 次)" for field, count in top_uncovered])
        
        template = self.prompt_templates["field_coverage"]
        prompt = template.replace("{existing_equivalents}", existing_str)
        prompt = prompt.replace("{uncovered_fields}", uncovered_str)
        
        return prompt
    
    def generate_standardization_prompt(self, equivalence_config: Dict) -> str:
        """生成字段标准化提示词"""
        print("生成字段标准化提示词...")
        
        template = self.prompt_templates["field_standardization"]
        prompt = template.replace("{equivalence_config}", json.dumps(equivalence_config, ensure_ascii=False, indent=2))
        
        return prompt
    
    def save_prompt_to_file(self, prompt: str, filename: str):
        """保存提示词到文件"""
        os.makedirs("prompts", exist_ok=True)
        with open(f"prompts/{filename}", 'w', encoding='utf-8') as f:
            f.write(prompt)
        print(f"提示词已保存到: prompts/{filename}")
    
    def load_existing_equivalents(self) -> Dict[str, List[str]]:
        """加载现有的字段等价关系"""
        return self.mapper.field_equivalents
    
    def generate_all_prompts(self):
        """生成所有提示词"""
        # 加载数据
        self.load_interface_data()
        self.collect_field_contexts()
        
        # 生成字段等价关系分析提示词
        equivalence_prompt = self.generate_field_equivalence_prompt(top_n=100)
        self.save_prompt_to_file(equivalence_prompt, "field_equivalence_analysis.txt")
        
        # 生成覆盖分析提示词
        existing_equivalents = self.load_existing_equivalents()
        coverage_prompt = self.generate_coverage_analysis_prompt(existing_equivalents, top_n=50)
        self.save_prompt_to_file(coverage_prompt, "field_coverage_analysis.txt")
        
        print("\n所有提示词生成完成！")
        print("请将这些提示词提交给大模型进行分析，然后使用分析结果更新字段等价关系。")
    
    def analyze_field_semantics(self):
        """分析字段语义"""
        print("分析字段语义...")
        
        # 加载数据
        self.load_interface_data()
        field_contexts = self.collect_field_contexts()
        
        # 按使用频率排序
        sorted_fields = sorted(field_contexts.items(), 
                             key=lambda x: len(x[1]), 
                             reverse=True)
        
        # 保存字段上下文分析
        field_analysis = {
            "total_fields": len(field_contexts),
            "field_details": {}
        }
        
        for field, contexts in sorted_fields:
            field_analysis["field_details"][field] = {
                "usage_count": len(contexts),
                "api_usage": list(set([ctx["api"] for ctx in contexts])),
                "input_usage": sum(1 for ctx in contexts if ctx["type"] == "input"),
                "output_usage": sum(1 for ctx in contexts if ctx["type"] == "output"),
                "sample_contexts": contexts[:3]  # 只保存前3个上下文
            }
        
        # 保存分析结果
        os.makedirs("analysis", exist_ok=True)
        with open("analysis/field_semantic_analysis.json", 'w', encoding='utf-8') as f:
            json.dump(field_analysis, f, ensure_ascii=False, indent=2)
        
        print(f"字段语义分析已保存到: analysis/field_semantic_analysis.json")
        return field_analysis


def main():
    """主函数"""
    analyzer = LLMFieldAnalyzer()
    
    # 生成所有提示词
    analyzer.generate_all_prompts()
    
    # 分析字段语义
    analyzer.analyze_field_semantics()


if __name__ == "__main__":
    main()
