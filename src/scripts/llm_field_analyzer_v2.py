#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于大模型的字段等价关系分析工具 v2
从实际数据中读取统计信息，生成更丰富的提示词
"""

import json
import os
import sys
from collections import Counter, defaultdict
from typing import Dict, List, Set, Tuple

# 添加src目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mappers.field_mapper import FieldMapper


class LLMFieldAnalyzerV2:
    """基于大模型的字段分析器 v2"""
    
    def __init__(self):
        self.mapper = FieldMapper()
        self.interface_data = {}
        self.field_statistics = Counter()
        self.field_contexts = defaultdict(list)
        self.field_api_usage = defaultdict(set)
        self.field_input_output = defaultdict(Counter)
    
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
    
    def collect_field_details(self) -> Dict:
        """收集字段详细信息"""
        print("收集字段详细信息...")
        
        field_contexts = defaultdict(list)
        field_api_usage = defaultdict(set)
        field_input_output = defaultdict(Counter)
        field_descriptions = defaultdict(list)
        
        for api_name, interface in self.interface_data.items():
            # 收集输入参数
            for param in interface.get('input_params', []):
                field_name = param.get('名称')
                if field_name and field_name != '-' and field_name != '无':
                    context = {
                        "api": api_name,
                        "type": "input",
                        "description": param.get('描述', ''),
                        "required": param.get('必填', '否') == '是'
                    }
                    field_contexts[field_name].append(context)
                    field_api_usage[field_name].add(api_name)
                    field_input_output[field_name]["input"] += 1
                    if param.get('描述'):
                        field_descriptions[field_name].append(param.get('描述'))
            
            # 收集输出参数
            for param in interface.get('output_params', []):
                field_name = param.get('名称')
                if field_name and field_name != '-' and field_name != '无':
                    context = {
                        "api": api_name,
                        "type": "output",
                        "description": param.get('描述', ''),
                        "example": param.get('示例', '')
                    }
                    field_contexts[field_name].append(context)
                    field_api_usage[field_name].add(api_name)
                    field_input_output[field_name]["output"] += 1
                    if param.get('描述'):
                        field_descriptions[field_name].append(param.get('描述'))
        
        self.field_contexts = field_contexts
        self.field_api_usage = field_api_usage
        self.field_input_output = field_input_output
        self.field_statistics = Counter({field: len(contexts) for field, contexts in field_contexts.items()})
        
        print(f"收集了 {len(field_contexts)} 个不同的字段")
        
        return {
            "field_contexts": field_contexts,
            "field_api_usage": field_api_usage,
            "field_input_output": field_input_output,
            "field_descriptions": field_descriptions
        }
    
    def generate_enhanced_equivalence_prompt(self, top_n: int = 150) -> str:
        """生成增强版的字段等价关系分析提示词"""
        print(f"生成增强版字段等价关系分析提示词（前{top_n}个字段）...")
        
        # 获取最常见的字段
        top_fields = self.field_statistics.most_common(top_n)
        
        # 构建详细的字段信息列表
        field_info_list = []
        for field, count in top_fields:
            input_count = self.field_input_output[field].get("input", 0)
            output_count = self.field_input_output[field].get("output", 0)
            api_count = len(self.field_api_usage[field])
            
            # 获取一些示例描述
            descriptions = list(set([ctx.get("description", "") 
                                     for ctx in self.field_contexts[field] 
                                     if ctx.get("description")]))
            sample_desc = descriptions[0][:50] if descriptions else ""
            
            # 获取一些使用的API示例
            apis = list(self.field_api_usage[field])[:3]
            
            field_info = {
                "field": field,
                "usage_count": count,
                "input_count": input_count,
                "output_count": output_count,
                "api_count": api_count,
                "sample_apis": apis,
                "sample_description": sample_desc
            }
            field_info_list.append(field_info)
        
        # 构建提示词
        prompt = self._build_equivalence_prompt(field_info_list)
        
        return prompt
    
    def _build_equivalence_prompt(self, field_info_list: List[Dict]) -> str:
        """构建字段等价关系分析提示词"""
        
        prompt = '''你是一个专业的金融数据字段分析专家，负责分析字段名称的语义等价关系。

请基于以下字段的详细使用信息，将语义相同或相似的字段分组：

---

字段列表（按使用频率排序，包含详细使用信息）：
'''
        
        for i, info in enumerate(field_info_list, 1):
            prompt += f'''
{i}. 字段: {info["field"]}
   - 使用次数: {info["usage_count"]}
   - 输入参数: {info["input_count"]}次
   - 输出参数: {info["output_count"]}次
   - 使用接口数: {info["api_count"]}个
   - 示例接口: {", ".join(info["sample_apis"])}'''
            if info["sample_description"]:
                prompt += f'''
   - 示例描述: {info["sample_description"]}'''
        
        prompt += '''

---

对于每个字段组，请：
1. 选择一个标准字段名（优先使用英文，采用下划线命名法）
2. 列出所有语义等价的字段
3. 简要说明分组理由
4. 考虑字段的使用场景（输入/输出）和描述信息

输出格式请使用JSON：
{
  "field_groups": [
    {
      "canonical": "标准字段名",
      "equivalents": ["等价字段1", "等价字段2"],
      "reason": "分组理由",
      "confidence": "high/medium/low"
    }
  ]
}

注意事项：
- 中英文但语义相同的字段应该归为一组
- 不同接口但含义相同的字段应该归为一组
- 优先使用英文作为标准字段名
- 考虑字段的使用上下文（输入还是输出）
- 参考字段的描述信息来判断语义
'''
        
        return prompt
    
    def generate_enhanced_coverage_prompt(self, existing_equivalents: Dict[str, List[str]], top_n: int = 60) -> str:
        """生成增强版的覆盖分析提示词"""
        print(f"生成增强版覆盖分析提示词（前{top_n}个未覆盖字段）...")
        
        # 获取未覆盖的字段
        covered_fields = set()
        for canonical, equivalents in existing_equivalents.items():
            covered_fields.add(canonical)
            covered_fields.update(equivalents)
        
        uncovered_fields = [(field, count) for field, count in self.field_statistics.items() 
                           if field not in covered_fields]
        uncovered_fields.sort(key=lambda x: x[1], reverse=True)
        top_uncovered = uncovered_fields[:top_n]
        
        # 构建详细的未覆盖字段信息
        field_info_list = []
        for field, count in top_uncovered:
            input_count = self.field_input_output[field].get("input", 0)
            output_count = self.field_input_output[field].get("output", 0)
            api_count = len(self.field_api_usage[field])
            
            descriptions = list(set([ctx.get("description", "") 
                                     for ctx in self.field_contexts[field] 
                                     if ctx.get("description")]))
            sample_desc = descriptions[0][:50] if descriptions else ""
            
            apis = list(self.field_api_usage[field])[:3]
            
            field_info = {
                "field": field,
                "usage_count": count,
                "input_count": input_count,
                "output_count": output_count,
                "api_count": api_count,
                "sample_apis": apis,
                "sample_description": sample_desc
            }
            field_info_list.append(field_info)
        
        # 构建提示词
        prompt = self._build_coverage_prompt(existing_equivalents, field_info_list)
        
        return prompt
    
    def _build_coverage_prompt(self, existing_equivalents: Dict[str, List[str]], field_info_list: List[Dict]) -> str:
        """构建覆盖分析提示词"""
        
        # 格式化现有等价关系
        existing_str = "\n".join([f"  {canonical}: {equivalents}" for canonical, equivalents in existing_equivalents.items()])
        
        prompt = '''你是一个专业的金融数据字段分析专家。

以下是当前已有的字段等价关系：
{existing_equivalents}

以下是未覆盖的常见字段（包含详细使用信息）：
'''
        
        for i, info in enumerate(field_info_list, 1):
            prompt += f'''
{i}. 字段: {info["field"]}
   - 使用次数: {info["usage_count"]}
   - 输入参数: {info["input_count"]}次
   - 输出参数: {info["output_count"]}次
   - 使用接口数: {info["api_count"]}个
   - 示例接口: {", ".join(info["sample_apis"])}'''
            if info["sample_description"]:
                prompt += f'''
   - 示例描述: {info["sample_description"]}'''
        
        prompt += '''

请分析这些未覆盖的字段，为每个字段建议合适的等价关系：
1. 如果与现有字段等价，请指定对应的标准字段
2. 如果是新的字段类型，请创建新的标准字段
3. 简要说明建议理由

输出格式请使用JSON：
{
  "suggestions": [
    {
      "field": "字段名",
      "suggested_canonical": "建议的标准字段",
      "action": "add_to_existing/create_new",
      "reason": "建议理由"
    }
  ]
}
'''.replace("{existing_equivalents}", existing_str)
        
        return prompt
    
    def save_prompt_to_file(self, prompt: str, filename: str):
        """保存提示词到文件"""
        os.makedirs("prompts", exist_ok=True)
        file_path = f"prompts/{filename}"
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(prompt)
        print(f"提示词已保存到: {file_path}")
        print(f"  大小: {os.path.getsize(file_path)} 字节")
        return file_path
    
    def load_existing_equivalents(self) -> Dict[str, List[str]]:
        """加载现有的字段等价关系"""
        return self.mapper.field_equivalents
    
    def save_field_statistics(self):
        """保存字段统计信息到JSON文件"""
        print("保存字段统计信息...")
        
        statistics = {
            "summary": {
                "total_fields": len(self.field_statistics),
                "total_interfaces": len(self.interface_data),
                "total_usage": sum(self.field_statistics.values())
            },
            "field_details": {}
        }
        
        # 按使用频率排序
        sorted_fields = sorted(self.field_statistics.items(), 
                             key=lambda x: x[1], 
                             reverse=True)
        
        for field, count in sorted_fields:
            statistics["field_details"][field] = {
                "usage_count": count,
                "input_count": self.field_input_output[field].get("input", 0),
                "output_count": self.field_input_output[field].get("output", 0),
                "api_count": len(self.field_api_usage[field]),
                "apis": list(self.field_api_usage[field]),
                "descriptions": list(set([ctx.get("description", "") 
                                         for ctx in self.field_contexts[field] 
                                         if ctx.get("description")]))
            }
        
        os.makedirs("analysis", exist_ok=True)
        with open("analysis/complete_field_statistics.json", 'w', encoding='utf-8') as f:
            json.dump(statistics, f, ensure_ascii=False, indent=2)
        
        print(f"字段统计信息已保存到: analysis/complete_field_statistics.json")
        return statistics
    
    def generate_all_enhanced_prompts(self):
        """生成所有增强版提示词"""
        # 加载数据
        self.load_interface_data()
        self.collect_field_details()
        
        # 保存完整的统计信息
        self.save_field_statistics()
        
        # 生成字段等价关系分析提示词
        equivalence_prompt = self.generate_enhanced_equivalence_prompt(top_n=150)
        self.save_prompt_to_file(equivalence_prompt, "enhanced_field_equivalence_analysis.txt")
        
        # 生成覆盖分析提示词
        existing_equivalents = self.load_existing_equivalents()
        coverage_prompt = self.generate_enhanced_coverage_prompt(existing_equivalents, top_n=60)
        self.save_prompt_to_file(coverage_prompt, "enhanced_field_coverage_analysis.txt")
        
        print("\n所有增强版提示词生成完成！")
        print("提示词包含：")
        print("  - 字段使用频率统计")
        print("  - 输入/输出参数统计")
        print("  - 使用接口列表")
        print("  - 字段描述示例")


def main():
    """主函数"""
    analyzer = LLMFieldAnalyzerV2()
    analyzer.generate_all_enhanced_prompts()


if __name__ == "__main__":
    main()
