# LLM字段分析系统使用指南

## 概述

本系统提供了一个完整的基于大模型的金融数据字段等价关系分析流程，通过从实际数据中提取丰富的统计信息，生成专业的提示词，然后利用大模型的语义理解能力来确定字段的等价关系。

## 系统架构

### 工具文件

1. **`src/scripts/validate_field_equivalents.py`** - 基础验证工具
   - 检查字段等价关系的逻辑一致性
   - 分析字段覆盖情况
   - 检测循环引用等问题

2. **`src/scripts/llm_field_analyzer_v2.py`** - 增强版LLM分析工具
   - 从实际数据中提取详细统计信息
   - 生成包含丰富上下文的提示词
   - 保存完整的字段统计报告

3. **`prompts/`** - 提示词输出目录
   - `enhanced_field_equivalence_analysis.txt` - 字段等价关系分析提示词
   - `enhanced_field_coverage_analysis.txt` - 字段覆盖分析提示词

4. **`analysis/`** - 分析报告输出目录
   - `complete_field_statistics.json` - 完整的字段统计信息

## 使用流程

### Step 1: 生成增强版提示词和统计数据

```bash
python src/scripts/llm_field_analyzer_v2.py
```

这个命令会：
1. 加载851个接口数据
2. 收集2936个不同字段的详细信息
3. 保存完整的字段统计到 `analysis/complete_field_statistics.json`
4. 生成两个增强版提示词到 `prompts/` 目录

### Step 2: 查看生成的提示词

提示词包含以下详细信息：

**字段详情包括：**
- 字段名称
- 使用次数统计
- 输入参数使用次数
- 输出参数使用次数
- 使用接口数量
- 示例接口列表
- 字段描述示例

**示例：**
```
1. 字段: symbol
   - 使用次数: 316
   - 输入参数: 308次
   - 输出参数: 8次
   - 使用接口数: 314个
   - 示例接口: stock_hk_valuation_baidu, sw_index_third_cons, index_analysis_weekly_sw
   - 示例描述: symbol="600030"
```

### Step 3: 使用大模型进行分析

将以下文件的内容提交给大模型进行分析：

1. **`prompts/enhanced_field_equivalence_analysis.txt`** 
   - 用于分析字段等价关系分组
   - 输出包含标准字段、等价字段列表、分组理由、置信度

2. **`prompts/enhanced_field_coverage_analysis.txt`**
   - 用于分析未覆盖的字段
   - 输出建议的等价关系、操作类型（添加到现有/创建新字段）、建议理由

### Step 4: 验证当前配置（可选）

在使用大模型分析之前，可以先验证当前配置的正确性：

```bash
python src/scripts/validate_field_equivalents.py
```

这会检查：
- 字段等价关系的一致性
- 是否有循环引用
- 字段覆盖情况
- 一致性问题

### Step 5: 根据大模型输出更新配置

根据大模型的分析结果，更新 `src/mappers/field_mapper.py` 中的 `field_equivalents` 字典。

**更新原则：**
1. 使用英文作为标准字段名
2. 采用下划线命名法
3. 按照语义类别组织注释
4. 保存大模型的分析理由作为参考

### Step 6: 重新验证配置

更新配置后，再次运行验证工具确保无问题：

```bash
python src/scripts/validate_field_equivalents.py
```

## 统计数据说明

### `analysis/complete_field_statistics.json`

这个文件包含了完整的字段统计信息：

```json
{
  "summary": {
    "total_fields": 2936,
    "total_interfaces": 851,
    "total_usage": 28749
  },
  "field_details": {
    "symbol": {
      "usage_count": 316,
      "input_count": 308,
      "output_count": 8,
      "api_count": 314,
      "apis": ["stock_hk_valuation_baidu", ...],
      "descriptions": ["symbol=\"600030\"", ...]
    },
    ...
  }
}
```

## 提示词模板说明

### 1. 字段等价关系分析提示词

**功能：** 将语义相同或相似的字段分组

**输入：** 前150个最常用字段的详细信息

**输出格式要求：**
```json
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
```

### 2. 字段覆盖分析提示词

**功能：** 分析未覆盖的字段，建议合适的等价关系

**输入：**
- 当前已有的字段等价关系
- 前60个未覆盖字段的详细信息

**输出格式要求：**
```json
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
```

## 最佳实践

### 1. 定期更新流程

建议按以下周期进行更新：
- **每周**：运行验证工具检查当前配置
- **每月**：生成新的提示词，分析新增的字段
- **每季度**：完整审查所有字段等价关系

### 2. 大模型选择

建议使用具备以下能力的大模型：
- 良好的中文理解能力
- 金融领域知识
- JSON格式输出能力
- 逻辑推理能力

### 3. 版本管理

建议保存每次大模型分析的结果：
- 保存分析结果到 `analysis/llm_analysis_YYYYMMDD.json`
- 记录使用的大模型版本
- 记录分析时间

### 4. 人工审核

重要的字段等价关系变更应该经过人工审核：
- 检查大模型的分组理由是否合理
- 验证实际使用场景
- 考虑后续可能的字段扩展

## 常见问题

### Q: 提示词太大怎么办？

A: 可以调整 `top_n` 参数，减少分析的字段数量。在 `llm_field_analyzer_v2.py` 中修改：
- `generate_enhanced_equivalence_prompt(top_n=150)` - 减少到100或更少
- `generate_enhanced_coverage_prompt(existing_equivalents, top_n=60)` - 减少到30或更少

### Q: 如何处理大模型输出的不一致性？

A: 
1. 多次运行分析，取一致的结果
2. 保留置信度高的分组
3. 人工审核有争议的分组

### Q: 新增字段如何处理？

A: 
1. 运行 `llm_field_analyzer_v2.py` 生成新的统计
2. 使用覆盖分析提示词分析未覆盖的字段
3. 根据建议更新配置
4. 运行验证工具确保正确性

## 完整示例流程

```bash
# 1. 生成提示词和统计数据
python src/scripts/llm_field_analyzer_v2.py

# 2. 查看生成的文件
ls -la prompts/
ls -la analysis/

# 3. （可选）先验证当前配置
python src/scripts/validate_field_equivalents.py

# 4. 将 prompts/ 中的文件提交给大模型
# 5. 根据大模型输出更新 field_mapper.py

# 6. 重新验证
python src/scripts/validate_field_equivalents.py

# 7. 完成！
```

## 总结

本系统提供了一个完整的、数据驱动的字段等价关系分析流程：
- ✅ 从实际数据中提取丰富的统计信息
- ✅ 生成专业的、上下文丰富的提示词
- ✅ 利用大模型的语义理解能力
- ✅ 提供验证工具确保配置正确性
- ✅ 保存完整的分析报告和统计数据

通过这个系统，您可以确保 `field_mapper.py` 中的字段等价关系既准确反映了字段的语义关系，又没有逻辑错误！
