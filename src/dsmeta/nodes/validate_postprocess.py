"""校验与后处理节点 - 对生成的元数据进行验证、规范化和补充处理."""

import hashlib
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional
import structlog
from pydantic import ValidationError

from ..models import DatasetMetadata, DataModality, UseCase, Domain, Rating, PIIRisk, AccessLevel, BusinessDirection, BusinessPoint
from ..config import Config

logger = structlog.get_logger(__name__)


async def validate_and_postprocess(state, config: Optional[Config] = None) -> Dict[str, Any]:
    """
    对生成的元数据进行校验、规范化和后处理.
    
    参数:
        state: 当前数据集处理状态 (dict或DatasetState对象)
        config: 配置对象
        
    返回:
        包含验证和规范化后元数据的状态更新
    """
    if config is None:
        from ..config import load_config
        config = load_config()

    try:
        # 安全获取状态值
        processing_id = _get_state_value(state, "processing_id", "unknown")
        meta = _get_state_value(state, "meta", {})
        files = _get_state_value(state, "files", [])
        total_size = _get_state_value(state, "total_size", 0)
        
        logger.info("开始元数据校验与后处理",
                   processing_id=processing_id,
                   meta_fields=len(meta),
                   validation_enabled=config.quality_control.enum_validation)
        
        if not meta:
            error_msg = "没有元数据需要处理"
            logger.error(error_msg)
            return {
                "current_step": "validate_and_postprocess",
                "status": "failed", 
                "error_message": error_msg
            }
        
        # 第一步：基础数据清理和规范化
        cleaned_meta = _clean_and_normalize_meta(meta)
        
        # 第二步：补充缺失的必需字段
        complete_meta = _supplement_required_fields(cleaned_meta, state)
        
        # 第三步：计算文件格式统计
        complete_meta["file_formats"] = _calculate_file_format_stats(files, total_size)
        
        # 第四步：枚举值验证和修正
        if config.quality_control.enum_validation:
            validated_meta = _validate_and_fix_enum_values(complete_meta)
        else:
            validated_meta = complete_meta
        
        # 第五步：生成聚合校验和
        validated_meta["checksum"] = _generate_dataset_checksum(state, files)
        
        # 第六步：质量控制检查
        quality_issues = _perform_quality_checks(validated_meta, config)
        if quality_issues:
            validated_meta["quality_issues"] = quality_issues
            logger.warning("发现数据质量问题", issues=quality_issues)
        
        # 第七步：Pydantic模型验证（可选）
        validation_result = _validate_with_pydantic(validated_meta)
        
        if validation_result["valid"]:
            logger.info("元数据验证成功",
                       processing_id=processing_id,
                       total_fields=len(validated_meta),
                       quality_issues=len(quality_issues))
            
            return {
                "current_step": "validate_and_postprocess",
                "status": "processing",
                "meta": validated_meta,
                "validation_passed": True,
                "quality_score": _calculate_quality_score(validated_meta, quality_issues)
            }
        else:
            logger.warning("Pydantic验证失败，使用修正后的数据",
                          validation_errors=validation_result["errors"])
            
            # 使用修正策略处理验证失败
            fixed_meta = _fix_validation_errors(validated_meta, validation_result["errors"])
            
            return {
                "current_step": "validate_and_postprocess", 
                "status": "processing",
                "meta": fixed_meta,
                "validation_passed": False,
                "validation_errors": validation_result["errors"],
                "quality_score": _calculate_quality_score(fixed_meta, quality_issues)
            }
        
    except Exception as e:
        error_msg = f"元数据校验与后处理出错: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 降级处理：返回基础元数据
        fallback_meta = _create_fallback_metadata(state)
        
        return {
            "current_step": "validate_and_postprocess",
            "status": "processing",
            "meta": fallback_meta,
            "error_message": error_msg,
            "fallback_used": True
        }


def _get_state_value(state, key, default=None):
    """安全地从状态中获取值 (支持dict和对象)."""
    if isinstance(state, dict):
        return state.get(key, default)
    else:
        return getattr(state, key, default)


def _clean_and_normalize_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    """清理和规范化元数据."""
    cleaned = {}
    
    for key, value in meta.items():
        if value is None:
            continue
            
        # 字符串清理
        if isinstance(value, str):
            # 去除首尾空白
            value = value.strip()
            # 空字符串转为None
            if not value:
                continue
            # 统一换行符
            value = value.replace('\r\n', '\n').replace('\r', '\n')
            
        # 列表清理
        elif isinstance(value, list):
            # 移除空值和重复项
            value = [item for item in value if item is not None and str(item).strip()]
            value = list(dict.fromkeys(value))  # 去重但保持顺序
            if not value:
                continue
        
        cleaned[key] = value
    
    return cleaned


def _supplement_required_fields(meta: Dict[str, Any], state) -> Dict[str, Any]:
    """补充缺失的必需字段."""
    
    # 生成唯一ID
    if "id" not in meta:
        meta["id"] = str(uuid.uuid4())
    
    # 确保有基本名称
    if "name" not in meta:
        meta["name"] = _get_state_value(state, "dataset_name", "Unknown Dataset")
    
    # 确保有描述
    if "description" not in meta:
        meta["description"] = f"{meta['name']} 数据集的自动生成描述"
    
    # 补充管理信息
    if "creator" not in meta:
        meta["creator"] = _get_state_value(state, "creator")
    
    if "creation_date" not in meta:
        meta["creation_date"] = _get_state_value(state, "creation_date")
    
    # 设置默认版本
    if "version" not in meta:
        meta["version"] = "v1.0"
    
    # 设置默认访问级别
    if "access_level" not in meta:
        meta["access_level"] = AccessLevel.PUBLIC.value
    
    # 补充文件统计
    files = _get_state_value(state, "files", [])
    total_size = _get_state_value(state, "total_size", 0)
    
    meta["size"] = _format_size(total_size)
    meta["num_files"] = len(files)
    
    # 语言检测
    if "languages" not in meta:
        meta["languages"] = ["zh", "en"]  # 默认支持中英文
    
    return meta


def _calculate_file_format_stats(files: List, total_size: int) -> List[Dict[str, Any]]:
    """计算文件格式统计信息."""
    if not files:
        return []
    
    format_stats = {}
    
    # 统计各格式的文件数和大小
    for file_stat in files:
        if hasattr(file_stat, 'format'):
            fmt = file_stat.format
            size = getattr(file_stat, 'size_bytes', 0)
        else:
            # dict格式
            fmt = file_stat.get('format', '.unknown')
            size = file_stat.get('size_bytes', 0)
        
        if fmt not in format_stats:
            format_stats[fmt] = {"count": 0, "size": 0}
        
        format_stats[fmt]["count"] += 1
        format_stats[fmt]["size"] += size
    
    # 转换为列表格式
    result = []
    for fmt, stats in format_stats.items():
        ratio = (stats["size"] / total_size * 100) if total_size > 0 else 0
        
        result.append({
            "format": fmt,
            "count": stats["count"],
            "size": _format_size(stats["size"]),
            "ratio": f"{ratio:.1f}%"
        })
    
    # 按大小排序
    result.sort(key=lambda x: float(x["ratio"].rstrip('%')), reverse=True)
    
    return result


def _validate_and_fix_enum_values(meta: Dict[str, Any]) -> Dict[str, Any]:
    """验证和修正枚举字段的值."""
    
    # 处理多选字段（从字符串转为列表）
    multi_select_fields = ["business_direction", "business_point", "task_types"]
    for field in multi_select_fields:
        if field in meta and isinstance(meta[field], str):
            # 如果是字符串，尝试分割为列表
            if '|' in meta[field]:
                meta[field] = [item.strip() for item in meta[field].split('|') if item.strip()]
            else:
                meta[field] = [meta[field]] if meta[field].strip() else []
    
    # 枚举字段映射
    enum_fields = {
        "modality": DataModality,
        "use_case": UseCase,
        "domain": Domain,
        "rating": Rating,
        "pii_risk": PIIRisk,
        "access_level": AccessLevel
    }
    
    for field, enum_class in enum_fields.items():
        if field in meta and meta[field] is not None:
            current_value = meta[field]
            valid_values = [e.value for e in enum_class]
            
            if current_value not in valid_values:
                logger.warning(f"无效的{field}值: {current_value}，尝试修正")
                
                # 尝试模糊匹配
                fixed_value = _fuzzy_match_enum_value(current_value, valid_values)
                if fixed_value:
                    meta[field] = fixed_value
                    logger.info(f"已修正{field}: {current_value} -> {fixed_value}")
                else:
                    # 使用默认值
                    default_value = _get_default_enum_value(field, enum_class)
                    meta[field] = default_value
                    logger.info(f"使用{field}默认值: {default_value}")
    
    # 验证多选枚举字段
    multi_enum_fields = {
        "business_direction": BusinessDirection,
        "business_point": BusinessPoint
    }
    
    for field, enum_class in multi_enum_fields.items():
        if field in meta and meta[field] is not None:
            if isinstance(meta[field], list):
                # 验证列表中的每个值
                valid_values = [e.value for e in enum_class]
                corrected_list = []
                
                for item in meta[field]:
                    if item in valid_values:
                        corrected_list.append(item)
                    else:
                        # 尝试模糊匹配
                        fixed_item = _fuzzy_match_enum_value(item, valid_values)
                        if fixed_item:
                            corrected_list.append(fixed_item)
                            logger.info(f"已修正{field}项: {item} -> {fixed_item}")
                
                meta[field] = corrected_list
    
    return meta


def _fuzzy_match_enum_value(value: str, valid_values: List[str]) -> Optional[str]:
    """模糊匹配枚举值，支持管道分隔的值."""
    if not value:
        return None
    
    # 处理管道分隔的多值情况，取第一个匹配的值
    if '|' in value:
        parts = [part.strip() for part in value.split('|')]
        for part in parts:
            result = _fuzzy_match_enum_value(part, valid_values)
            if result:
                return result
    
    value_lower = value.lower()
    
    # 精确匹配
    for valid in valid_values:
        if valid.lower() == value_lower:
            return valid
    
    # 包含匹配
    for valid in valid_values:
        if value_lower in valid.lower() or valid.lower() in value_lower:
            return valid
    
    # 关键词匹配
    keyword_mapping = {
        "text": "自然语言文本",
        "code": "代码", 
        "image": "图像",
        "audio": "音频",
        "video": "视频",
        "table": "结构化/表格",
        "training": "模型预训练",
        "finetune": "模型微调",
        "eval": "模型评测",
        "basic": "基础",
        "advanced": "高级",
        "cyber": "网络攻防",
        "security": "安全认知"
    }
    
    for keyword, mapped_value in keyword_mapping.items():
        if keyword in value_lower and mapped_value in valid_values:
            return mapped_value
    
    return None


def _get_default_enum_value(field: str, enum_class) -> str:
    """获取枚举字段的默认值."""
    defaults = {
        "modality": DataModality.CODE.value,
        "use_case": UseCase.EVALUATION.value,
        "domain": Domain.GENERAL.value,
        "rating": Rating.BASIC.value,
        "pii_risk": PIIRisk.NONE.value,
        "access_level": AccessLevel.PUBLIC.value
    }
    
    return defaults.get(field, list(enum_class)[0].value)


def _generate_dataset_checksum(state, files: List) -> str:
    """生成数据集的聚合校验和."""
    
    # 收集用于校验和计算的数据
    checksum_data = []
    
    # 数据集路径
    dataset_path = _get_state_value(state, "dataset_path", "")
    checksum_data.append(dataset_path)
    
    # 总大小
    total_size = _get_state_value(state, "total_size", 0)
    checksum_data.append(str(total_size))
    
    # 文件列表信息
    for file_stat in files:
        if hasattr(file_stat, 'path'):
            path = file_stat.path
            size = getattr(file_stat, 'size_bytes', 0)
            sha256 = getattr(file_stat, 'sha256', '')
        else:
            # dict格式
            path = file_stat.get('path', '')
            size = file_stat.get('size_bytes', 0)
            sha256 = file_stat.get('sha256', '')
        
        checksum_data.append(f"{path}:{size}:{sha256}")
    
    # 计算SHA256
    combined_data = '|'.join(checksum_data)
    return hashlib.sha256(combined_data.encode('utf-8')).hexdigest()[:16]


def _perform_quality_checks(meta: Dict[str, Any], config: Config) -> List[str]:
    """执行质量控制检查."""
    issues = []
    
    # 检查必需字段
    for field in config.quality_control.required_fields:
        if field not in meta or not meta[field]:
            issues.append(f"缺少必需字段: {field}")
    
    # 检查描述长度
    description = meta.get("description", "")
    if len(description) < 10:
        issues.append("描述过短，建议至少10个字符")
    elif len(description) > 500:
        issues.append("描述过长，建议不超过500个字符")
    
    # 检查置信度
    confidence = meta.get("confidence_score", 1.0)
    if confidence < config.quality_control.min_confidence_score:
        issues.append(f"置信度过低: {confidence}")
    
    # 检查URL有效性
    source_url = meta.get("source_url")
    if source_url and not _is_valid_url(source_url):
        issues.append(f"无效的源URL: {source_url}")
    
    return issues


def _is_valid_url(url: str) -> bool:
    """检查URL是否有效."""
    import re
    url_pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    
    return bool(url_pattern.match(url))


def _validate_with_pydantic(meta: Dict[str, Any]) -> Dict[str, Any]:
    """使用Pydantic模型验证元数据."""
    try:
        # 尝试创建DatasetMetadata实例
        dataset_meta = DatasetMetadata(**meta)
        return {"valid": True, "errors": []}
    
    except ValidationError as e:
        errors = []
        for error in e.errors():
            field = ".".join(str(loc) for loc in error["loc"])
            msg = error["msg"]
            errors.append(f"{field}: {msg}")
        
        return {"valid": False, "errors": errors}
    
    except Exception as e:
        return {"valid": False, "errors": [f"验证错误: {str(e)}"]}


def _fix_validation_errors(meta: Dict[str, Any], errors: List[str]) -> Dict[str, Any]:
    """修正验证错误."""
    fixed_meta = meta.copy()
    
    # 基于错误信息进行修正
    for error in errors:
        if "modality" in error:
            fixed_meta["modality"] = DataModality.CODE.value
        elif "use_case" in error:
            fixed_meta["use_case"] = UseCase.EVALUATION.value
        elif "domain" in error:
            fixed_meta["domain"] = Domain.GENERAL.value
        elif "rating" in error:
            fixed_meta["rating"] = Rating.BASIC.value
        elif "pii_risk" in error:
            fixed_meta["pii_risk"] = PIIRisk.NONE.value
        elif "access_level" in error:
            fixed_meta["access_level"] = AccessLevel.PUBLIC.value
    
    return fixed_meta


def _calculate_quality_score(meta: Dict[str, Any], quality_issues: List[str]) -> float:
    """计算数据质量得分."""
    base_score = 1.0
    
    # 每个质量问题扣分
    penalty_per_issue = 0.1
    score = base_score - len(quality_issues) * penalty_per_issue
    
    # 奖励分数
    bonus_fields = ["source_url", "license", "citation", "sample"]
    bonus_per_field = 0.05
    
    for field in bonus_fields:
        if field in meta and meta[field]:
            score += bonus_per_field
    
    # 置信度影响
    confidence = meta.get("confidence_score", 0.5)
    score = score * confidence
    
    return max(0.0, min(1.0, score))


def _create_fallback_metadata(state) -> Dict[str, Any]:
    """创建降级元数据."""
    
    dataset_name = _get_state_value(state, "dataset_name", "Unknown Dataset")
    files = _get_state_value(state, "files", [])
    total_size = _get_state_value(state, "total_size", 0)
    
    return {
        "id": str(uuid.uuid4()),
        "name": dataset_name,
        "description": f"{dataset_name} 数据集",
        "size": _format_size(total_size),
        "num_files": len(files),
        "modality": DataModality.CODE.value,
        "use_case": UseCase.DATA_ANALYSIS.value,
        "domain": Domain.GENERAL.value,
        "rating": Rating.BASIC.value,
        "pii_risk": PIIRisk.NONE.value,
        "access_level": AccessLevel.PUBLIC.value,
        "version": "v1.0",
        "languages": ["en"],
        "creator": _get_state_value(state, "creator"),
        "creation_date": _get_state_value(state, "creation_date"),
        "checksum": _generate_dataset_checksum(state, files),
        "file_formats": _calculate_file_format_stats(files, total_size),
        "quality_notes": "使用降级策略生成的基础元数据"
    }


def _format_size(size_bytes: int) -> str:
    """格式化文件大小为人类可读形式."""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f}KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f}MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f}GB"