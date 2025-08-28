"""数据集文档解析器 - 从内部文档提取真实的数据集信息

支持解析数据集目录中的README.md、同名.md文件等，
提取真实的来源、描述、用途等关键信息。
"""

import re
from pathlib import Path
from typing import Dict, Any, Optional, List
import structlog

logger = structlog.get_logger(__name__)


class DatasetDocumentParser:
    """数据集文档解析器"""
    
    def __init__(self):
        self.field_patterns = {
            # 基础信息
            "description": [r"## 数据集描述\s*\n\s*(.*?)(?=\n##|\n$)", r"## 描述\s*\n\s*(.*?)(?=\n##|\n$)"],
            "source_url": [r"## 数据集来源\s*\n\s*(.*?)(?=\n##|\n$)", r"## 来源\s*\n\s*(.*?)(?=\n##|\n$)"],
            "use_case": [r"## 数据集用途\s*\n\s*(.*?)(?=\n##|\n$)", r"## 用途\s*\n\s*(.*?)(?=\n##|\n$)"],
            "modality": [r"## 数据模态\s*\n\s*(.*?)(?=\n##|\n$)"],
            "size": [r"## 大小\s*\n\s*(.*?)(?=\n##|\n$)"],
            
            # 业务属性
            "domain": [r"## 赋能专业方向\s*\n\s*(.*?)(?=\n##|\n$)", r"## 专业方向\s*\n\s*(.*?)(?=\n##|\n$)"],
            "business_direction": [r"## 赋能业务方向\s*\n\s*(.*?)(?=\n##|\n$)", r"## 业务方向\s*\n\s*(.*?)(?=\n##|\n$)"],
            "business_point": [r"## 赋能业务点\s*\n\s*(.*?)(?=\n##|\n$)", r"## 业务点\s*\n\s*(.*?)(?=\n##|\n$)"],
            "rating": [r"## 专业评级\s*\n\s*(.*?)(?=\n##|\n$)"],
            
            # 其他信息
            "format": [r"## 格式\s*\n\s*(.*?)(?=\n##|\n$)"],
            "remarks": [r"## 备注\s*\n\s*(.*?)(?=\n##|\n$)"],
            "sequence": [r"## 序号\s*\n\s*(.*?)(?=\n##|\n$)"]
        }
        
        # 字段映射和标准化
        self.value_mappings = {
            "modality": {
                "自然语言文本": "自然语言文本",
                "代码": "代码",
                "结构化": "结构化/表格",
                "表格": "结构化/表格",
                "图像": "图像",
                "多模态": "多模态"
            },
            "use_case": {
                "微调问答": "模型微调",
                "强化学习": "强化学习",
                "模型评测": "模型评测",
                "数据分析": "数据分析"
            },
            "domain": {
                "基础通用": "基础通用",
                "攻防": "网络攻防",
                "网络攻防": "网络攻防",
                "安全认知": "安全认知",
                "体系化防御": "体系化防御"
            },
            "rating": {
                "基础": "基础",
                "进阶": "进阶",
                "高级": "高级",
                "专用私有": "专用私有"
            }
        }
    
    def find_dataset_docs(self, dataset_path: str) -> List[Path]:
        """查找数据集相关文档"""
        dataset_dir = Path(dataset_path)
        dataset_name = dataset_dir.name
        
        # 可能的文档文件名
        possible_files = [
            "README.md",
            "readme.md", 
            "README.MD",
            f"{dataset_name}.md",
            # 提取数据集名称（去除前缀）
        ]
        
        # 尝试提取纯数据集名称（去除qiaoyu-日期-前缀）
        if "-" in dataset_name:
            parts = dataset_name.split("-")
            if len(parts) >= 3:
                pure_name = "-".join(parts[2:])  # qiaoyu-20250414-CyberBench -> CyberBench
                possible_files.extend([
                    f"{pure_name}.md",
                    f"{pure_name}.MD"
                ])
                
                # 处理带括号的情况：CyberSecEval(CybersecurityBenchmarks) -> CyberSecEval
                if "(" in pure_name:
                    base_name = pure_name.split("(")[0]
                    possible_files.extend([
                        f"{base_name}.md",
                        f"{base_name}.MD"
                    ])
        
        found_docs = []
        for filename in possible_files:
            doc_path = dataset_dir / filename
            if doc_path.exists():
                found_docs.append(doc_path)
                logger.debug("找到数据集文档", doc_path=str(doc_path))
        
        return found_docs
    
    def parse_document(self, doc_path: Path) -> Dict[str, Any]:
        """解析单个文档文件"""
        try:
            with open(doc_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            extracted_info = {}
            
            for field, patterns in self.field_patterns.items():
                value = None
                for pattern in patterns:
                    match = re.search(pattern, content, re.DOTALL | re.MULTILINE)
                    if match:
                        value = match.group(1).strip()
                        break
                
                if value:
                    # 清理和标准化值
                    value = self._clean_value(value)
                    
                    # 应用映射
                    if field in self.value_mappings and value in self.value_mappings[field]:
                        value = self.value_mappings[field][value]
                    
                    extracted_info[field] = value
            
            logger.info("文档解析成功", 
                       doc_path=str(doc_path),
                       extracted_fields=list(extracted_info.keys()))
            
            return extracted_info
            
        except Exception as e:
            logger.error("文档解析失败", 
                        doc_path=str(doc_path), 
                        error=str(e))
            return {}
    
    def _clean_value(self, value: str) -> str:
        """清理提取的值"""
        if not value:
            return ""
        
        # 去除多余空白
        value = value.strip()
        
        # 去除可能的markdown格式
        value = re.sub(r'^\s*[-*+]\s*', '', value)  # 去除列表符号
        value = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', value)  # 将链接转为纯文本
        
        # 处理换行符
        value = value.replace('\n', ' ').replace('\r', ' ')
        value = re.sub(r'\s+', ' ', value)  # 合并多个空白为单个空格
        
        return value.strip()
    
    def parse_dataset_info(self, dataset_path: str) -> Dict[str, Any]:
        """解析数据集的完整信息"""
        docs = self.find_dataset_docs(dataset_path)
        
        if not docs:
            logger.warning("未找到数据集文档", dataset_path=dataset_path)
            return {}
        
        # 合并所有文档的信息
        combined_info = {}
        for doc_path in docs:
            doc_info = self.parse_document(doc_path)
            combined_info.update(doc_info)
        
        # 后处理
        combined_info = self._postprocess_info(combined_info)
        
        logger.info("数据集信息解析完成",
                   dataset_path=dataset_path,
                   found_docs=len(docs),
                   extracted_info=combined_info)
        
        return combined_info
    
    def _postprocess_info(self, info: Dict[str, Any]) -> Dict[str, Any]:
        """后处理提取的信息"""
        # 处理多值字段
        multi_value_fields = ["business_direction", "business_point"]
        for field in multi_value_fields:
            if field in info and isinstance(info[field], str):
                # 如果是单个值，转换为列表
                info[field] = [info[field]]
        
        # 处理URL
        if "source_url" in info:
            url = info["source_url"]
            # 确保URL格式正确
            if url and not url.startswith(("http://", "https://")):
                if url.startswith("github.com") or url.startswith("huggingface.co"):
                    info["source_url"] = f"https://{url}"
        
        # 推断source从source_url
        if "source_url" in info and "source" not in info:
            url = info["source_url"]
            if "github.com" in url:
                info["source"] = "GitHub"
            elif "huggingface.co" in url:
                info["source"] = "HuggingFace"
            elif "kaggle.com" in url:
                info["source"] = "Kaggle"
            else:
                info["source"] = "其他"
        
        return info


def parse_dataset_documents(dataset_path: str) -> Dict[str, Any]:
    """解析数据集文档的便捷函数"""
    parser = DatasetDocumentParser()
    return parser.parse_dataset_info(dataset_path)