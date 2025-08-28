"""Generate markdown documentation node."""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
import structlog
from jinja2 import Environment, FileSystemLoader, TemplateNotFound

from ..models import (
    DatasetState, ProcessingStatus, DatasetMetadata, FileFormatStat,
    DataModality, UseCase, Domain, BusinessDirection, BusinessPoint, Rating, PIIRisk
)
from ..config import Config

logger = structlog.get_logger(__name__)


async def generate_markdown(state: DatasetState, config: Optional[Config] = None, template_name: str = "default.md.j2") -> Dict[str, Any]:
    """
    Generate markdown documentation from dataset metadata.
    
    Args:
        state: Current dataset processing state
        config: Configuration object
        template_name: Template file name
        
    Returns:
        Updated state with generated documentation
    """
    if config is None:
        from ..config import load_config
        config = load_config()
    
    try:
        processing_id = state.get("processing_id") if isinstance(state, dict) else getattr(state, "processing_id", "unknown")
        
        logger.info("Starting generate_markdown",
                   processing_id=processing_id,
                   template=template_name,
                   state_keys=list(state.keys()) if isinstance(state, dict) else "Not a dict",
                   has_preliminary=state.get("preliminary") if isinstance(state, dict) else getattr(state, "preliminary", None))
        
        # Use validated metadata from state if available, otherwise create from scratch
        validated_meta = _get_state_value(state, "meta")
        if validated_meta:
            # Use validated metadata and convert to DatasetMetadata object
            metadata = _create_metadata_from_dict(validated_meta)
        else:
            # Fallback: create metadata from preliminary analysis
            metadata = _create_complete_metadata(state)
        
        # Setup Jinja2 environment
        template_dir = Path(config.output.template_dir)
        if not template_dir.exists():
            # Fallback to package templates
            template_dir = Path(__file__).parent.parent.parent.parent / "templates"
        
        if not template_dir.exists():
            raise FileNotFoundError(f"Template directory not found: {template_dir}")
        
        env = Environment(
            loader=FileSystemLoader(str(template_dir)),
            autoescape=False,
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Load template
        try:
            template = env.get_template(template_name)
        except TemplateNotFound:
            logger.warning(f"Template {template_name} not found, using default template")
            template = env.get_template("default.md.j2")
        
        # Render markdown
        markdown_content = template.render(**metadata.dict())
        
        # Generate JSON metadata
        json_content = json.dumps(metadata.dict(), ensure_ascii=False, indent=2)
        
        # Prepare artifacts
        artifacts = {
            "markdown": {
                "filename": "meta.md",
                "content": markdown_content
            },
            "json": {
                "filename": "meta.json", 
                "content": json_content
            }
        }
        
        # Add YAML if requested
        if "yaml" in config.output.output_formats:
            import yaml
            yaml_content = yaml.dump(metadata.dict(), allow_unicode=True, default_flow_style=False)
            artifacts["yaml"] = {
                "filename": "meta.yaml",
                "content": yaml_content
            }
        
        logger.info("Documentation generated successfully",
                   processing_id=processing_id,
                   formats=list(artifacts.keys()),
                   markdown_length=len(markdown_content))
        
        return {
            "meta": metadata.dict(),
            "artifacts": artifacts,
            "status": ProcessingStatus.PROCESSING,
            "current_step": "generate_markdown"
        }
        
    except Exception as e:
        error_msg = f"Error in generate_markdown: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            "status": ProcessingStatus.FAILED,
            "error_message": error_msg,
            "current_step": "generate_markdown"
        }


def _get_state_value(state, key, default=None):
    """Safely get value from state (dict or object)."""
    if isinstance(state, dict):
        return state.get(key, default)
    else:
        return getattr(state, key, default)


def _create_metadata_from_dict(meta_dict: Dict[str, Any]) -> DatasetMetadata:
    """从验证后的字典创建DatasetMetadata对象."""
    try:
        # 直接使用Pydantic创建对象，它会处理验证
        return DatasetMetadata(**meta_dict)
    except Exception as e:
        logger.error("从字典创建DatasetMetadata失败", error=str(e), meta_keys=list(meta_dict.keys()))
        logger.info("使用回退策略补充缺失字段")
        
        # 补充缺失的必需字段
        supplemented_dict = meta_dict.copy()
        
        # 补充基础必需字段
        if "size" not in supplemented_dict:
            supplemented_dict["size"] = "0B"
        if "num_files" not in supplemented_dict:
            supplemented_dict["num_files"] = 0
        if "modality" not in supplemented_dict:
            supplemented_dict["modality"] = "代码"
        if "use_case" not in supplemented_dict:
            supplemented_dict["use_case"] = "模型评测"
        if "domain" not in supplemented_dict:
            supplemented_dict["domain"] = "网络攻防"
        if "description" not in supplemented_dict:
            supplemented_dict["description"] = f"{supplemented_dict.get('name', 'Unknown Dataset')} 数据集"
            
        # 再次尝试创建对象
        try:
            return DatasetMetadata(**supplemented_dict)
        except Exception as e2:
            logger.error("回退策略也失败了", error=str(e2))
            raise


def _create_complete_metadata(state) -> DatasetMetadata:
    """Create complete metadata from state information."""
    
    # Get preliminary analysis results
    preliminary = _get_state_value(state, "preliminary", {})
    
    # Calculate file format statistics
    files = _get_state_value(state, "files", [])
    total_size = _get_state_value(state, "total_size", 0)
    file_formats = _calculate_file_format_stats(files, total_size)
    
    # Detect languages from file samples (simplified)
    file_samples = _get_state_value(state, "file_samples", "")
    languages = _detect_languages(file_samples)
    
    # Estimate number of records
    num_records = _estimate_record_count(files, file_samples)
    
    # Format size  
    size = _format_size(total_size)
    
    # Extract sample content
    sample = _extract_sample(file_samples)
    
    metadata = DatasetMetadata(
        # Basic Info
        id=str(uuid.uuid4()),
        name=_get_state_value(state, "dataset_name", "Unknown Dataset"),
        description=preliminary.get("description", "No description available"),
        source=None,  # Will be filled by web search
        source_url=None,  # Will be filled by web search
        size=size,
        num_files=len(files),
        num_records=num_records,
        languages=languages,
        
        # Technical Attributes
        modality=preliminary.get("modality", DataModality.CODE.value),
        file_formats=file_formats,
        use_case=preliminary.get("use_case", UseCase.EVALUATION.value),
        task_types=preliminary.get("task_types", []),
        input_schema=preliminary.get("input_schema"),
        label_schema=preliminary.get("label_schema"),
        
        # Business Attributes  
        domain=preliminary.get("domain", Domain.GENERAL.value),
        business_direction=preliminary.get("business_direction"),
        business_point=preliminary.get("business_point"), 
        rating=Rating.BASIC.value,
        
        # Management Info
        creator=_get_state_value(state, "creator"),
        creation_date=_get_state_value(state, "creation_date"),
        version="v1.0",
        license=None,  # Will be filled by web search
        citation=None,  # Will be filled by web search
        
        # Quality & Security
        sample=sample,
        remarks=preliminary.get("reasoning"),
        pii_risk=PIIRisk.NONE.value,
        quality_notes=None,
        checksum=_get_state_value(state, "cache_key")  # Use cache key as simple checksum
    )
    
    return metadata


def _calculate_file_format_stats(files: List, total_size: int) -> List[FileFormatStat]:
    """Calculate file format statistics."""
    if not files:
        return []
    
    format_stats = {}
    
    # Aggregate by format
    for file_stat in files:
        fmt = file_stat.format
        if fmt not in format_stats:
            format_stats[fmt] = {"count": 0, "size": 0}
        
        format_stats[fmt]["count"] += 1
        format_stats[fmt]["size"] += file_stat.size_bytes
    
    # Convert to FileFormatStat objects
    result = []
    for fmt, stats in format_stats.items():
        size_str = _format_size(stats["size"])
        ratio = f"{stats['size'] / total_size * 100:.1f}%" if total_size > 0 else "0%"
        
        result.append(FileFormatStat(
            format=fmt,
            count=stats["count"],
            size=size_str,
            ratio=ratio
        ))
    
    # Sort by size descending
    result.sort(key=lambda x: int(x.count), reverse=True)
    
    # Merge small formats into "others"
    if len(result) > 10:
        others_count = sum(item.count for item in result[9:])
        others_size = sum(_parse_size(item.size) for item in result[9:])
        others_ratio = f"{others_size / total_size * 100:.1f}%" if total_size > 0 else "0%"
        
        result = result[:9]
        result.append(FileFormatStat(
            format="others",
            count=others_count,
            size=_format_size(others_size),
            ratio=others_ratio
        ))
    
    return result


def _detect_languages(file_samples: str) -> List[str]:
    """Detect languages from file samples (simplified)."""
    if not file_samples:
        return []
    
    # Simple heuristic based on character patterns
    languages = []
    
    # Chinese characters
    if any('\u4e00' <= char <= '\u9fff' for char in file_samples):
        languages.append("zh")
    
    # English (basic ASCII letters)
    if any('a' <= char.lower() <= 'z' for char in file_samples):
        languages.append("en")
    
    return languages if languages else ["unknown"]


def _estimate_record_count(files: List, file_samples: str) -> Optional[int]:
    """Estimate number of records in dataset."""
    if not files:
        return None
    
    # Simple heuristic based on file types and samples
    total_estimate = 0
    
    for file_stat in files:
        if file_stat.format in ['.json', '.jsonl']:
            # Estimate based on file size (rough)
            estimated_per_file = max(1, file_stat.size_bytes // 1000)  # Assume ~1KB per record
            total_estimate += estimated_per_file
        elif file_stat.format in ['.csv', '.tsv']:
            # Estimate based on file size
            estimated_per_file = max(1, file_stat.size_bytes // 500)  # Assume ~500B per record
            total_estimate += estimated_per_file
        elif file_stat.format in ['.txt']:
            # Estimate based on lines (rough)
            estimated_per_file = max(1, file_stat.size_bytes // 100)  # Assume ~100B per line
            total_estimate += estimated_per_file
    
    return total_estimate if total_estimate > 0 else None


def _extract_sample(file_samples: str) -> Optional[str]:
    """Extract a representative sample from file samples."""
    if not file_samples:
        return None
    
    try:
        samples_data = json.loads(file_samples)
        combined = samples_data.get("combined", "")
        
        if combined:
            # Return first 500 characters
            return combined[:500] + "..." if len(combined) > 500 else combined
        
        # Fallback to text samples
        text_samples = samples_data.get("text_samples", [])
        if text_samples:
            first_sample = text_samples[0].get("sample", "")
            return first_sample[:300] + "..." if len(first_sample) > 300 else first_sample
        
    except:
        # If JSON parsing fails, return raw sample
        return file_samples[:300] + "..." if len(file_samples) > 300 else file_samples
    
    return None


def _format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f}PB"


def _parse_size(size_str: str) -> int:
    """Parse human-readable size back to bytes."""
    size_str = size_str.upper().strip()
    
    units = {'B': 1, 'KB': 1024, 'MB': 1024**2, 'GB': 1024**3, 'TB': 1024**4}
    
    for unit, multiplier in units.items():
        if size_str.endswith(unit):
            try:
                value = float(size_str[:-len(unit)])
                return int(value * multiplier)
            except ValueError:
                return 0
    
    return 0