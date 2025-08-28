"""CSV导出模块 - 汇总所有已处理数据集的标注信息

支持扫描指定目录下的所有数据集元数据文件，并导出为CSV格式，
便于批量分析和管理数据集信息。
"""

import csv
import json
import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import structlog

from .models import DatasetMetadata

logger = structlog.get_logger(__name__)


class DatasetExporter:
    """数据集元数据导出器"""
    
    def __init__(self):
        self.csv_headers = [
            # 基础信息
            "id", "name", "description", "source", "source_url", 
            "size", "num_files", "num_records", "languages",
            
            # 技术属性
            "modality", "use_case", "task_types", "file_formats_summary",
            
            # 业务属性
            "domain", "business_direction", "business_point", "rating",
            
            # 管理信息
            "creator", "creation_date", "version", "license", 
            "access_level", "citation",
            
            # 质量安全
            "pii_risk", "quality_notes", "checksum",
            
            # 元数据
            "metadata_file_path", "last_updated", "processing_status"
        ]
    
    def find_metadata_files(self, root_dir: str, patterns: List[str] = None) -> List[Path]:
        """查找所有元数据文件"""
        if patterns is None:
            patterns = ["**/meta.json", "**/meta.yaml"]
            
        root_path = Path(root_dir)
        metadata_files = []
        
        for pattern in patterns:
            found_files = list(root_path.glob(pattern))
            metadata_files.extend(found_files)
        
        logger.info("发现元数据文件", 
                   root_dir=root_dir, 
                   total_files=len(metadata_files),
                   patterns=patterns)
        
        return metadata_files
    
    def load_metadata_from_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """从文件加载元数据"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.suffix.lower() == '.json':
                    data = json.load(f)
                elif file_path.suffix.lower() in ['.yaml', '.yml']:
                    data = yaml.safe_load(f)
                else:
                    logger.warning("不支持的文件格式", file_path=str(file_path))
                    return None
            
            # 添加文件路径和最后更新时间
            data['metadata_file_path'] = str(file_path)
            data['last_updated'] = datetime.fromtimestamp(file_path.stat().st_mtime).isoformat()
            data['processing_status'] = 'completed'  # 有文件说明处理完成
            
            return data
            
        except Exception as e:
            logger.error("加载元数据文件失败", 
                        file_path=str(file_path), 
                        error=str(e))
            return None
    
    def format_list_field(self, field_value: Any) -> str:
        """格式化列表字段为CSV友好的字符串"""
        if field_value is None:
            return ""
        if isinstance(field_value, list):
            if not field_value:
                return ""
            # 对于枚举值列表，提取值
            formatted_items = []
            for item in field_value:
                if hasattr(item, 'value'):
                    formatted_items.append(item.value)
                elif isinstance(item, dict):
                    # 对于文件格式统计等复杂对象
                    formatted_items.append(f"{item.get('format', 'unknown')}({item.get('count', 0)})")
                else:
                    formatted_items.append(str(item))
            return "; ".join(formatted_items)
        return str(field_value)
    
    def format_file_formats(self, file_formats: List[Dict]) -> str:
        """格式化文件格式统计信息"""
        if not file_formats:
            return ""
        
        summaries = []
        for fmt in file_formats:
            if isinstance(fmt, dict):
                format_name = fmt.get('format', 'unknown')
                count = fmt.get('count', 0)
                size = fmt.get('size', '0B')
                summaries.append(f"{format_name}({count}files,{size})")
            else:
                summaries.append(str(fmt))
        
        return "; ".join(summaries)
    
    def convert_to_csv_row(self, metadata: Dict[str, Any]) -> Dict[str, str]:
        """将元数据转换为CSV行"""
        csv_row = {}
        
        for header in self.csv_headers:
            value = metadata.get(header, "")
            
            # 特殊处理某些字段
            if header == "file_formats_summary":
                value = self.format_file_formats(metadata.get('file_formats', []))
            elif header in ["languages", "task_types", "business_direction", "business_point"]:
                value = self.format_list_field(value)
            elif isinstance(value, (list, dict)):
                value = self.format_list_field(value)
            elif value is None:
                value = ""
            else:
                value = str(value)
            
            csv_row[header] = value
        
        return csv_row
    
    def export_to_csv(self, 
                      root_dir: str, 
                      output_file: str = "datasets_summary.csv",
                      patterns: List[str] = None) -> Dict[str, Any]:
        """导出数据集信息到CSV文件"""
        
        logger.info("开始导出数据集信息到CSV", 
                   root_dir=root_dir, 
                   output_file=output_file)
        
        # 查找所有元数据文件
        metadata_files = self.find_metadata_files(root_dir, patterns)
        
        if not metadata_files:
            logger.warning("未找到任何元数据文件", root_dir=root_dir)
            return {
                "success": False,
                "error": "未找到任何元数据文件",
                "total_files": 0,
                "exported_rows": 0
            }
        
        # 处理每个文件并收集数据
        csv_rows = []
        failed_files = []
        
        for file_path in metadata_files:
            metadata = self.load_metadata_from_file(file_path)
            if metadata:
                try:
                    csv_row = self.convert_to_csv_row(metadata)
                    csv_rows.append(csv_row)
                    logger.debug("成功处理元数据文件", file_path=str(file_path))
                except Exception as e:
                    logger.error("转换CSV行失败", 
                               file_path=str(file_path), 
                               error=str(e))
                    failed_files.append(str(file_path))
            else:
                failed_files.append(str(file_path))
        
        # 写入CSV文件
        output_path = Path(output_file)
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
                if csv_rows:
                    writer = csv.DictWriter(csvfile, fieldnames=self.csv_headers)
                    writer.writeheader()
                    writer.writerows(csv_rows)
                
            logger.info("CSV导出完成", 
                       output_file=output_file,
                       total_datasets=len(csv_rows),
                       failed_files=len(failed_files))
            
            return {
                "success": True,
                "output_file": str(output_path.absolute()),
                "total_files": len(metadata_files),
                "exported_rows": len(csv_rows),
                "failed_files": failed_files,
                "csv_headers": self.csv_headers
            }
            
        except Exception as e:
            logger.error("写入CSV文件失败", 
                        output_file=output_file, 
                        error=str(e))
            return {
                "success": False,
                "error": f"写入CSV文件失败: {e}",
                "total_files": len(metadata_files),
                "exported_rows": 0
            }
    
    def generate_summary_report(self, csv_file: str) -> Dict[str, Any]:
        """生成数据集汇总报告"""
        try:
            import pandas as pd
            
            df = pd.read_csv(csv_file)
            
            summary = {
                "total_datasets": len(df),
                "modality_distribution": df['modality'].value_counts().to_dict(),
                "domain_distribution": df['domain'].value_counts().to_dict(),
                "use_case_distribution": df['use_case'].value_counts().to_dict(),
                "rating_distribution": df['rating'].value_counts().to_dict(),
                "creators": df['creator'].value_counts().head(10).to_dict(),
                "file_count_stats": {
                    "mean": float(df['num_files'].mean()) if 'num_files' in df else 0,
                    "median": float(df['num_files'].median()) if 'num_files' in df else 0,
                    "max": int(df['num_files'].max()) if 'num_files' in df else 0,
                    "min": int(df['num_files'].min()) if 'num_files' in df else 0
                }
            }
            
            return summary
            
        except ImportError:
            logger.warning("pandas未安装，跳过统计报告生成")
            return {"error": "需要安装pandas来生成统计报告"}
        except Exception as e:
            logger.error("生成汇总报告失败", error=str(e))
            return {"error": f"生成报告失败: {e}"}


def export_datasets_csv(root_dir: str, 
                       output_file: str = "datasets_summary.csv",
                       include_summary: bool = False) -> Dict[str, Any]:
    """导出数据集信息的便捷函数"""
    
    exporter = DatasetExporter()
    result = exporter.export_to_csv(root_dir, output_file)
    
    if result["success"] and include_summary:
        summary = exporter.generate_summary_report(output_file)
        result["summary"] = summary
    
    return result