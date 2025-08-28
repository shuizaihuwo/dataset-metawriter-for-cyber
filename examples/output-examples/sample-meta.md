# CyberBench

网络安全领域的综合基准测试数据集，包含多种网络安全相关的评测任务。该数据集专门设计用于评估大语言模型在网络安全代码分析、漏洞检测和安全评估方面的能力。

## 基本信息

- **数据集ID**: `abc123def456`
- **数据集名称**: CyberBench
- **版本**: v1.0
- **来源**: GitHub
- **官方链接**: https://github.com/cybersec-research/CyberBench
- **总大小**: 15.2MB
- **文件数量**: 124
- **记录数量**: 5,000
- **支持语言**: 中文, 英文
- **创建者**: qiaoyu
- **创建日期**: 2025-04-14

## 技术属性

- **数据模态**: 代码
- **主要用途**: 模型评测
- **任务类型**: 代码分析, 漏洞检测, 安全评估, 静态分析, 动态分析
- **许可证**: MIT
- **访问级别**: 公开

### 文件格式统计

| 格式 | 文件数 | 大小 | 占比 |
|-----|-------|------|-----|
| .py | 45 | 8.1MB | 53.3% |
| .json | 29 | 2.1MB | 13.8% |
| .cpp | 18 | 3.2MB | 21.1% |
| .txt | 15 | 1.1MB | 7.2% |
| .md | 17 | 0.7MB | 4.6% |

### 数据模式

**输入模式**:
- `code_snippet`: string - 代码片段
- `language`: string - 编程语言
- `vulnerability_type`: string - 漏洞类型

**标签模式**:
- `is_vulnerable`: boolean - 是否存在漏洞
- `vulnerability_severity`: enum[low,medium,high,critical] - 漏洞严重程度
- `cwe_category`: string - CWE分类

## 业务属性

- **专业领域**: 网络攻防
- **业务方向**: 代码分析, 漏洞挖掘, 策略规划
- **业务场景**: 静态分析, 脆弱性分析, 代码辅助生成
- **专业评级**: 高级

## 质量与安全

- **隐私风险评估**: 低风险
- **质量说明**: 数据质量较高，适合研究使用。代码样本经过人工验证，涵盖常见漏洞类型
- **数据校验和**: `7ca0894dcde72cfd`

## 样例数据

```python
def check_sql_injection(user_input):
    dangerous_patterns = ['union', 'select', 'drop', 'delete']
    for pattern in dangerous_patterns:
        if pattern.lower() in user_input.lower():
            return True
    return False
```

## 备注

该数据集通过智能文档解析和扩展生成，基于原始README.md文件的信息进行了合理的业务标签扩展

## 引用格式

```
CyberBench: A Comprehensive Benchmark for Cybersecurity Code Analysis
```

---

*本文档由 Dataset Metadata Annotation Tool v1.2.0 自动生成*