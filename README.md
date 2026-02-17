# quant_skills

量化投资技能库 - AKShare 数据接口处理工具集

## 项目概述

这是一个通用的 AKShare 数据接口处理系统，能够解析和处理所有类型的数据接口文档，包括：
- 宏观经济数据
- 股票数据  
- 债券数据
- 期货数据
- 基金数据
- 指数数据
- QDII数据等

## 项目结构

```
quant_skills/
├── data/                      # 原始数据文档
│   ├── *.md.txt             # 接口文档（按数据类型分类）
│   │   ├── macro.md.txt        # 宏观数据
│   │   ├── stock.md.txt        # 股票数据
│   │   ├── bond.md.txt         # 债券数据
│   │   ├── futures.md.txt      # 期货数据
│   │   ├── fund_public.md.txt  # 公募基金
│   │   ├── fund_private.md.txt # 私募基金
│   │   ├── index.md.txt        # 指数数据
│   │   └── qdii.md.txt        # QDII数据
│   │
│   └── dictionary/           # 生成的数据字典
│       ├── all_data_dictionary_with_examples.csv    # 完整数据字典
│       └── normalized_data_dictionary_sorted.csv    # 归一化数据字典
│
├── code/                      # 核心代码
│   └── md_parser.py           # 统一解析器
│
├── scripts/                   # 工具脚本
│   ├── analyze_macro_params.py  # 参数分析
│   └── generate_macro_tasks.py # 任务生成
│
├── download_macro_data.py      # 数据下载主程序
├── validate_macro_data.py    # 数据验证工具
├── requirements.txt          # Python依赖
└── start_download.sh         # 启动脚本
```

## 快速开始

### 安装依赖
```bash
pip install -r requirements.txt
```

### 下载数据
```bash
chmod +x start_download.sh
./start_download.sh
```

### 使用解析器
```python
from code.md_parser import DataDictParser, FieldNormalizer

# 解析所有数据文档
parser = DataDictParser()
parser.parse_all().save_csv("data/dictionary/all_data_dictionary_with_examples.csv")

# 归一化处理
normalizer = FieldNormalizer("data/dictionary/all_data_dictionary_with_examples.csv")
normalizer.normalize("data/dictionary/normalized_data_dictionary_sorted.csv")
```

## 核心功能

| 模块 | 说明 |
|------|------|
| download_macro_data.py | 从AKShare批量下载所有类型数据 |
| validate_macro_data.py | 验证下载数据的完整性 |
| md_parser.py | 解析所有Markdown文档生成统一数据字典 |
| analyze_macro_params.py | 分析接口参数取值范围 |

## 依赖

- akshare
- pandas
- requests