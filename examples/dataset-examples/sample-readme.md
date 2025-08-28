# CyberBench Dataset

## 数据集描述

网络安全领域的综合基准测试数据集，包含多种网络安全相关的评测任务。该数据集专门设计用于评估大语言模型在网络安全代码分析、漏洞检测和安全评估方面的能力。

## 数据集来源

https://github.com/cybersec-research/CyberBench

## 数据集用途

模型评测

## 数据模态

代码

## 赋能专业方向

攻防

## 赋能业务方向

代码分析

## 赋能业务点

静态分析

## 专业评级

高级

## 格式

py,json,txt,md

## 样例

```python
# 示例：SQL注入检测代码
def check_sql_injection(user_input):
    dangerous_patterns = ['union', 'select', 'drop', 'delete']
    for pattern in dangerous_patterns:
        if pattern.lower() in user_input.lower():
            return True
    return False
```

## 数据集文件

```
CyberBench/
├── code_analysis/
│   ├── vulnerability_detection/
│   │   ├── sql_injection_samples.json
│   │   ├── xss_samples.json
│   │   └── buffer_overflow_samples.json
│   └── static_analysis/
│       ├── python_security_checks.py
│       └── cpp_memory_checks.cpp
├── benchmarks/
│   ├── evaluation_metrics.json
│   └── baseline_results.json
└── README.md
```

## 大小

15.2MB

## 统计信息

- **总文件数**: 124
- **代码文件**: 89 (.py, .cpp, .java, .js)
- **数据文件**: 29 (.json, .csv, .txt)  
- **文档文件**: 6 (.md, .rst)
- **估计记录数**: 5,000个代码样本