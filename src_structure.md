# 新的src目录结构设计

## 目录结构

```
src/
├── __init__.py           # 包初始化文件
├── parsers/              # 解析器模块
│   ├── __init__.py
│   ├── md_parser.py      # Markdown解析器
│   ├── data_parser.py    # 数据解析器
├── formatters/           # 格式化器模块
│   ├── __init__.py
│   ├── field_formatter.py # 字段格式化器
├── mappers/              # 映射器模块
│   ├── __init__.py
│   ├── field_mapper.py   # 字段映射器
├── testers/              # 测试模块
│   ├── __init__.py
│   ├── interface_tester.py # 接口测试器
├── generators/           # 生成器模块
│   ├── __init__.py
│   ├── task_generator.py # 任务生成器
│   ├── dict_generator.py # 字典生成器
├── downloaders/          # 下载器模块
│   ├── __init__.py
│   ├── data_downloader.py # 数据下载器
│   ├── macro_downloader.py # 宏观数据下载器
├── utils/                # 工具模块
│   ├── __init__.py
│   ├── common.py         # 通用工具
├── scripts/              # 脚本模块（原scripts目录）
│   ├── __init__.py
│   ├── analyze_macro_params.py
│   ├── generate_all_data_tasks.py
│   ├── generate_macro_tasks.py
│   ├── generate_unified_dictionary.py
│   ├── start_download.sh
└── main.py               # 主入口文件
```

## 文件迁移计划

### 1. 从 code/ 目录迁移
- `code/md_parser.py` → `src/parsers/md_parser.py`
- `code/data_parser.py` → `src/parsers/data_parser.py`

### 2. 从 scripts/ 目录迁移
- `scripts/field_formatter.py` → `src/formatters/field_formatter.py`
- `scripts/field_mapper.py` → `src/mappers/field_mapper.py`
- `scripts/interface_tester.py` → `src/testers/interface_tester.py`
- `scripts/analyze_macro_params.py` → `src/scripts/analyze_macro_params.py`
- `scripts/generate_all_data_tasks.py` → `src/scripts/generate_all_data_tasks.py`
- `scripts/generate_macro_tasks.py` → `src/scripts/generate_macro_tasks.py`
- `scripts/generate_unified_dictionary.py` → `src/scripts/generate_unified_dictionary.py`

### 3. 从根目录迁移
- `download_data.py` → `src/downloaders/data_downloader.py`
- `download_macro_data.py` → `src/downloaders/macro_downloader.py`
- `validate_macro_data.py` → `src/validators/validate_macro_data.py`

## 导入路径更新

所有文件中的导入路径需要更新，例如：
- 从 `from code.md_parser import DataInterfaceParser` 改为 `from src.parsers.md_parser import DataInterfaceParser`
- 从 `from scripts.field_mapper import FieldMapper` 改为 `from src.mappers.field_mapper import FieldMapper`

## 执行脚本更新

- `start_download.sh` 需要更新路径
- 根目录可以保留一些快捷脚本，指向src目录中的对应文件
