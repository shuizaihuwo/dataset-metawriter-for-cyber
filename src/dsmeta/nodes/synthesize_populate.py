"""综合分析与数据填充节点 - 整合本地分析和网页搜索结果生成完整元数据."""

import asyncio
import json
import re
from typing import Dict, Any, List, Optional
import structlog
from openai import AsyncOpenAI

from ..models import DatasetState
from ..config import Config

logger = structlog.get_logger(__name__)


async def synthesize_and_populate(state, config: Optional[Config] = None) -> Dict[str, Any]:
    """
    综合本地分析结果和网页搜索结果，生成完整的数据集元数据.
    
    参数:
        state: 当前数据集处理状态 (dict或DatasetState对象)
        config: 配置对象
        
    返回:
        包含完整元数据的状态更新
    """
    if config is None:
        from ..config import load_config
        config = load_config()

    try:
        # 安全获取状态值
        processing_id = _get_state_value(state, "processing_id", "unknown")
        dataset_name = _get_state_value(state, "dataset_name", "")
        preliminary = _get_state_value(state, "preliminary", {})
        web_search = _get_state_value(state, "web_search", [])
        files = _get_state_value(state, "files", [])
        total_size = _get_state_value(state, "total_size", 0)
        
        logger.info("开始综合分析与填充",
                   processing_id=processing_id,
                   dataset_name=dataset_name,
                   has_search_results=len(web_search) > 0,
                   search_results_count=len(web_search))
        
        # 检查是否需要使用LLM进行综合分析
        if config.llm.api_key in ["test-key-for-testing", "mock-api-key"]:
            logger.info("使用模拟综合分析")
            return await _generate_mock_synthesis(state)
        
        # 获取文档信息
        doc_info = _get_state_value(state, "doc_info", {})
        
        # 构建综合分析提示词
        synthesis_prompt = _build_synthesis_prompt(
            dataset_name, preliminary, web_search, files, total_size, doc_info
        )
        
        # 调用LLM进行综合分析
        synthesis_result = await _call_llm_for_synthesis(synthesis_prompt, config)
        
        if not synthesis_result:
            logger.warning("LLM综合分析失败，使用本地分析结果")
            synthesis_result = _fallback_to_preliminary(preliminary, web_search, dataset_name)
        
        # 补充本地统计数据
        complete_meta = _supplement_with_local_data(
            synthesis_result, files, total_size, state
        )
        
        logger.info("综合分析完成",
                   processing_id=processing_id,
                   filled_fields=len(complete_meta),
                   has_source_url=bool(complete_meta.get("source_url")),
                   has_license=bool(complete_meta.get("license")),
                   has_citation=bool(complete_meta.get("citation")))
        
        return {
            "current_step": "synthesize_and_populate",
            "status": "processing",
            "meta": complete_meta,
            "synthesis_confidence": synthesis_result.get("confidence_score", 0.8)
        }
        
    except Exception as e:
        error_msg = f"综合分析出错: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 降级到仅使用本地分析结果
        fallback_meta = _fallback_to_preliminary(
            _get_state_value(state, "preliminary", {}),
            _get_state_value(state, "web_search", []),
            _get_state_value(state, "dataset_name", "")
        )
        
        complete_meta = _supplement_with_local_data(
            fallback_meta, 
            _get_state_value(state, "files", []),
            _get_state_value(state, "total_size", 0),
            state
        )
        
        return {
            "current_step": "synthesize_and_populate",
            "status": "processing",
            "meta": complete_meta,
            "error_message": error_msg,
            "fallback_used": True
        }


def _get_state_value(state, key, default=None):
    """安全地从状态中获取值 (支持dict和对象)."""
    if isinstance(state, dict):
        return state.get(key, default)
    else:
        return getattr(state, key, default)


def _build_synthesis_prompt(
    dataset_name: str, 
    preliminary: Dict[str, Any], 
    web_search: List[Dict[str, Any]],
    files: List,
    total_size: int,
    doc_info: Dict[str, Any] = None
) -> str:
    """构建综合分析的提示词."""
    
    prompt = f"""你是一个专业的数据集分析专家，需要综合分析本地文件扫描结果和网页搜索结果，生成完整的数据集元数据。

## 数据集基本信息
- 数据集名称: {dataset_name}
- 文件总数: {len(files)}
- 总大小: {_format_size(total_size)}

## 本地分析结果
{json.dumps(preliminary, ensure_ascii=False, indent=2)}

## 数据集内部文档信息
{_format_doc_info_for_prompt(doc_info)}

## 网页搜索结果
"""
    
    if web_search:
        for i, result in enumerate(web_search[:5], 1):  # 限制搜索结果数量
            prompt += f"""
### 搜索结果 {i}
- 标题: {result.get('title', '')}
- URL: {result.get('url', '')}
- 摘要: {result.get('snippet', '')[:200]}...
- 相关性得分: {result.get('relevance_score', 0)}
"""
    else:
        prompt += "（未找到相关搜索结果）"
    
    prompt += """

## 任务要求
请基于上述信息，生成完整的数据集元数据。**特别注意**：

1. **优先使用内部文档信息**: 如果数据集内部文档提供了真实信息（如description、source_url等），必须以此为准
2. **智能扩展补充**: 对于文档中较简单的信息（如只有一个business_point），应基于数据集特性和领域知识进行合理扩展
3. **信息融合**: 综合本地分析、文档信息和搜索结果，生成完整准确的元数据
4. **质量保证**: 确保所有信息的一致性和准确性

**信息优先级**：内部文档 > 网页搜索结果 > 本地分析推测

## 输出格式
请严格按照以下JSON格式输出，不要包含其他内容：

```json
{
  "name": "数据集官方名称",
  "description": "数据集详细描述(100-200字)",
  "source": "来源平台名称(如GitHub/HuggingFace/Kaggle等)",
  "source_url": "官方仓库或下载链接",
  "modality": "数据模态(自然语言文本/代码/流量/日志/结构化/表格/二进制/图像/音频/视频/多模态)",
  "use_case": "主要用途(模型预训练/模型微调/模型微调(含思维链)/模型评测/强化学习/分类/回归/实体识别/数据分析/混合用途)",
  "domain": "专业领域(基础通用/网络攻防/安全认知/体系化防御)",
  "business_direction": ["业务方向数组(可多选，如：代码分析、工具生成、情报分析等)"],
  "business_point": ["业务场景数组(可多选，如：代码辅助生成、工具测试、目标分析等)"],
  "rating": "专业评级(基础/进阶/高级/专用私有)",
  "license": "许可证信息(如MIT/Apache-2.0/CC-BY-4.0等，如未知填null)",
  "citation": "引用格式(BibTeX或文本格式，如未知填null)",
  "task_types": ["具体任务类型列表"],
  "pii_risk": "隐私风险评估(none/low/medium/high)",
  "quality_notes": "数据质量说明(可选)",
  "confidence_score": 0.85,
  "reasoning": "分析推理过程简述"
}
```

请确保：
- 所有字段都要填写，不能遗漏
- 枚举字段必须使用指定的选项值
- business_direction和business_point是数组字段，可以包含多个值
- task_types是数组，可包含多个具体任务类型
- confidence_score为0-1之间的浮点数
- 如果搜索结果与本地分析冲突，优先采用搜索结果中的权威信息
"""
    
    return prompt


def _format_doc_info_for_prompt(doc_info: Dict[str, Any]) -> str:
    """格式化文档信息用于提示词."""
    if not doc_info:
        return "（未找到内部文档信息）"
    
    formatted = "从数据集内部文档（README.md、数据集名.md等）中提取的真实信息：\n"
    
    # 重要字段优先显示
    priority_fields = [
        ("description", "数据集描述"),
        ("source_url", "官方来源链接"),
        ("use_case", "用途"),
        ("modality", "数据模态"),
        ("domain", "专业领域"),
        ("business_direction", "业务方向"),
        ("business_point", "业务场景"),
        ("rating", "专业评级"),
        ("size", "数据集大小"),
        ("format", "文件格式"),
        ("remarks", "备注说明")
    ]
    
    for field, label in priority_fields:
        if field in doc_info and doc_info[field]:
            value = doc_info[field]
            if isinstance(value, list):
                value = "; ".join(value)
            formatted += f"- {label}: {value}\n"
    
    # 其他字段
    other_fields = set(doc_info.keys()) - set(f[0] for f in priority_fields)
    for field in sorted(other_fields):
        if doc_info[field]:
            value = doc_info[field]
            if isinstance(value, list):
                value = "; ".join(value)
            formatted += f"- {field}: {value}\n"
    
    formatted += "\n**注意**: 这些是从官方文档中提取的真实信息，应该作为元数据生成的权威参考。对于较简单的信息（如只有一个业务点），请基于数据集特性进行合理扩展。"
    
    return formatted


async def _call_llm_for_synthesis(prompt: str, config: Config) -> Optional[Dict[str, Any]]:
    """调用LLM进行综合分析."""
    client = AsyncOpenAI(
        api_key=config.llm.api_key,
        base_url=config.llm.base_url,
        timeout=config.llm.timeout
    )
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            logger.info("调用LLM进行综合分析", attempt=attempt + 1)
            
            response = await client.chat.completions.create(
                model=config.llm.model,
                messages=[
                    {
                        "role": "system", 
                        "content": "你是一个专业的数据集分析专家。请仔细分析提供的信息，生成准确完整的数据集元数据。输出必须是有效的JSON格式。"
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=config.llm.temperature,
                max_tokens=config.llm.max_tokens,
                response_format={"type": "json_object"}
            )
            
            content = response.choices[0].message.content.strip()
            logger.debug("LLM综合分析响应", content_length=len(content))
            
            # 解析JSON响应
            result = _parse_llm_synthesis_response(content)
            
            if result:
                logger.info("LLM综合分析成功")
                return result
            else:
                logger.warning("LLM响应解析失败", attempt=attempt + 1)
                
        except Exception as e:
            logger.warning("LLM综合分析失败", 
                          attempt=attempt + 1,
                          error=str(e))
            
            if attempt < max_retries - 1:
                # 指数退避重试
                await asyncio.sleep(2 ** attempt)
    
    logger.error("所有LLM综合分析重试均失败")
    return None


def _parse_llm_synthesis_response(content: str) -> Optional[Dict[str, Any]]:
    """解析LLM的综合分析响应."""
    try:
        # 清理响应内容
        content = content.strip()
        
        # 尝试提取JSON部分
        if "```json" in content:
            start = content.find("```json") + 7
            end = content.find("```", start)
            if end != -1:
                content = content[start:end].strip()
        
        # 解析JSON
        result = json.loads(content)
        
        # 验证必需字段
        required_fields = [
            "name", "description", "modality", "use_case", 
            "domain", "confidence_score"
        ]
        
        for field in required_fields:
            if field not in result:
                logger.warning(f"LLM响应缺少必需字段: {field}")
                return None
        
        # 验证枚举值
        if not _validate_enum_values(result):
            logger.warning("LLM响应包含无效的枚举值")
            return None
        
        return result
        
    except json.JSONDecodeError as e:
        logger.warning("JSON解析错误", error=str(e))
        return None
    except Exception as e:
        logger.warning("响应解析错误", error=str(e))
        return None


def _validate_enum_values(result: Dict[str, Any]) -> bool:
    """验证枚举字段的值是否有效."""
    from ..models import DataModality, UseCase, Domain, BusinessDirection, BusinessPoint, Rating, PIIRisk
    
    enum_validations = [
        ("modality", DataModality),
        ("use_case", UseCase), 
        ("domain", Domain),
        ("rating", Rating),
        ("pii_risk", PIIRisk)
    ]
    
    for field, enum_class in enum_validations:
        if field in result and result[field]:
            try:
                # 检查值是否在枚举中
                valid_values = [e.value for e in enum_class]
                if result[field] not in valid_values:
                    logger.warning(f"无效的{field}值: {result[field]}")
                    return False
            except Exception:
                return False
    
    # 验证可选枚举字段
    optional_enums = [
        ("business_direction", BusinessDirection),
        ("business_point", BusinessPoint)
    ]
    
    for field, enum_class in optional_enums:
        if field in result and result[field] is not None:
            try:
                valid_values = [e.value for e in enum_class]
                # 处理多选字段（列表类型）
                if isinstance(result[field], list):
                    valid_items = []
                    for item in result[field]:
                        if item in valid_values:
                            valid_items.append(item)
                        else:
                            logger.warning(f"无效的{field}项: {item}")
                    result[field] = valid_items if valid_items else None
                else:
                    # 处理单选字段
                    if result[field] not in valid_values:
                        logger.warning(f"无效的{field}值: {result[field]}")
                        result[field] = None
            except Exception:
                result[field] = None
    
    return True


def _simulate_intelligent_expansion(
    dataset_name: str,
    preliminary: Dict[str, Any],
    doc_info: Dict[str, Any],
    best_result: Dict[str, Any] = None
) -> Dict[str, Any]:
    """模拟LLM基于文档信息进行智能扩展."""
    
    # 基础结果，优先使用文档信息
    result = {
        "name": dataset_name,
        "description": doc_info.get("description") or preliminary.get("description") or f"{dataset_name} 是一个数据集，包含多种格式的数据文件。",
        "source_url": doc_info.get("source_url") or (best_result.get("url") if best_result else None),
        "modality": doc_info.get("modality") or preliminary.get("modality") or "代码",
        "use_case": doc_info.get("use_case") or preliminary.get("use_case") or "模型评测",
        "domain": doc_info.get("domain") or preliminary.get("domain") or "网络攻防",
        "rating": doc_info.get("rating") or "基础",
        "confidence_score": 0.85
    }
    
    # 推断source从source_url
    if result["source_url"]:
        if "github.com" in result["source_url"]:
            result["source"] = "GitHub"
        elif "huggingface.co" in result["source_url"]:
            result["source"] = "HuggingFace"
        elif "kaggle.com" in result["source_url"]:
            result["source"] = "Kaggle"
    
    # 智能扩展business_direction（模拟LLM基于领域知识的扩展）
    doc_business_direction = doc_info.get("business_direction", [])
    if isinstance(doc_business_direction, str):
        doc_business_direction = [doc_business_direction]
    
    expanded_directions = list(doc_business_direction) if doc_business_direction else []
    
    # 模拟LLM的智能推理：基于现有信息扩展相关业务方向
    if "代码分析" in expanded_directions and "网络攻防" in result.get("domain", ""):
        # 代码分析通常会伴随漏洞挖掘和安全评估
        if "漏洞挖掘" not in expanded_directions:
            expanded_directions.append("漏洞挖掘")
        if "策略规划" not in expanded_directions:  # 使用正确的枚举值
            expanded_directions.append("策略规划")
    
    result["business_direction"] = expanded_directions or ["代码分析"]
    
    # 智能扩展business_point
    doc_business_point = doc_info.get("business_point", [])
    if isinstance(doc_business_point, str):
        doc_business_point = [doc_business_point]
    
    expanded_points = list(doc_business_point) if doc_business_point else []
    
    # 模拟LLM的智能推理：基于业务方向扩展业务场景
    if "代码辅助生成" in expanded_points:
        # 代码辅助生成通常涉及多个相关场景
        if "静态分析" not in expanded_points:
            expanded_points.append("静态分析")
        if "脆弱性分析" not in expanded_points:
            expanded_points.append("脆弱性分析")
    
    result["business_point"] = expanded_points or ["静态分析"]
    
    # 其他字段
    result.update({
        "license": "MIT" if result.get("source") == "GitHub" else None,
        "citation": _generate_mock_citation(dataset_name, best_result) if best_result else None,
        "task_types": ["benchmark", "evaluation", "安全测试"],
        "pii_risk": "low",
        "quality_notes": "基于内部文档信息和智能扩展的分析结果"
    })
    
    return result


async def _generate_mock_synthesis(state) -> Dict[str, Any]:
    """生成模拟的综合分析结果，基于文档信息进行智能扩展."""
    
    dataset_name = _get_state_value(state, "dataset_name", "")
    preliminary = _get_state_value(state, "preliminary", {})
    web_search = _get_state_value(state, "web_search", [])
    files = _get_state_value(state, "files", [])
    total_size = _get_state_value(state, "total_size", 0)
    doc_info = _get_state_value(state, "doc_info", {})
    
    # 从搜索结果中提取信息
    best_result = web_search[0] if web_search else None
    
    logger.info("模拟LLM基于文档信息进行智能扩展", 
               has_doc_info=bool(doc_info),
               doc_fields=list(doc_info.keys()) if doc_info else [])
    
    # 模拟LLM的智能分析和扩展逻辑
    mock_synthesis = _simulate_intelligent_expansion(
        dataset_name, preliminary, doc_info, best_result
    )
    
    # 补充本地数据
    complete_meta = _supplement_with_local_data(mock_synthesis, files, total_size, state)
    
    logger.info("模拟综合分析完成",
               has_source_url=bool(complete_meta.get("source_url")),
               has_license=bool(complete_meta.get("license")))
    
    return {
        "current_step": "synthesize_and_populate", 
        "status": "processing",
        "meta": complete_meta,
        "synthesis_confidence": 0.85,
        "mock_mode": True
    }


def _generate_mock_citation(dataset_name: str, search_result: Dict[str, Any]) -> Optional[str]:
    """生成模拟的引用信息."""
    if not search_result:
        return None
    
    url = search_result.get("url", "")
    title = search_result.get("title", dataset_name)
    
    if "github.com" in url:
        # GitHub风格引用
        repo_match = re.search(r'github\.com/([^/]+/[^/]+)', url)
        if repo_match:
            repo_name = repo_match.group(1)
            return f"@misc{{{dataset_name.lower()},\n  author = {{Community}},\n  title = {{{title}}},\n  url = {{{url}}},\n  year = {{2024}}\n}}"
    
    # 通用引用格式
    return f"{title}. Available at: {url}"


def _fallback_to_preliminary(preliminary: Dict[str, Any], web_search: List[Dict[str, Any]], dataset_name: str = "") -> Dict[str, Any]:
    """降级到仅使用初步分析结果."""
    logger.info("使用降级分析策略")
    
    result = preliminary.copy()
    
    # 确保有name字段
    if not result.get("name") and dataset_name:
        result["name"] = dataset_name
    
    # 尝试从搜索结果中提取基础信息
    if web_search:
        best_result = web_search[0]
        url = best_result.get("url", "")
        
        if not result.get("source_url"):
            result["source_url"] = url
            
        if not result.get("source"):
            if "github.com" in url:
                result["source"] = "GitHub"
            elif "huggingface.co" in url:
                result["source"] = "HuggingFace"
            elif "kaggle.com" in url:
                result["source"] = "Kaggle"
    
    return result


def _supplement_with_local_data(
    meta: Dict[str, Any],
    files: List,
    total_size: int,
    state
) -> Dict[str, Any]:
    """用本地统计数据和文档信息补充元数据."""
    
    # 获取从文档中解析的真实信息（已在LLM提示词中作为上下文使用）
    doc_info = _get_state_value(state, "doc_info", {})
    
    logger.info("补充本地数据",
               doc_fields=list(doc_info.keys()) if doc_info else [],
               meta_fields_before=list(meta.keys()),
               has_doc_info=bool(doc_info))
    
    # 添加本地统计信息（如果文档中没有提供）
    if "size" not in meta or not meta["size"]:
        meta["size"] = _format_size(total_size)
    meta["num_files"] = len(files)
    
    # 添加状态信息
    meta["creator"] = _get_state_value(state, "creator")
    meta["creation_date"] = _get_state_value(state, "creation_date") 
    meta["checksum"] = _get_state_value(state, "cache_key")
    
    # 设置默认版本
    if "version" not in meta:
        meta["version"] = "v1.0"
    
    # 设置默认访问级别
    if "access_level" not in meta:
        meta["access_level"] = "public"
    
    # 估算记录数
    if "num_records" not in meta:
        meta["num_records"] = _estimate_record_count(files)
    
    # 语言检测 (简化版)
    if "languages" not in meta:
        meta["languages"] = _detect_languages_from_files(files)
    
    source_url = meta.get("source_url") or ""
    logger.info("本地数据补充完成",
               final_fields=list(meta.keys()),
               has_real_source_url=bool(source_url.startswith("http")))
    
    return meta


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


def _estimate_record_count(files: List) -> Optional[int]:
    """估算数据记录数."""
    record_count = 0
    
    for file_stat in files:
        if hasattr(file_stat, 'path'):
            path = file_stat.path
        else:
            path = file_stat.get('path', '') if isinstance(file_stat, dict) else ''
            
        # 根据文件类型估算记录数
        if path.endswith(('.json', '.jsonl')):
            # JSON文件按文件大小估算
            size = getattr(file_stat, 'size_bytes', 0) if hasattr(file_stat, 'size_bytes') else file_stat.get('size_bytes', 0)
            record_count += max(1, size // 500)  # 假设平均每条记录500字节
        elif path.endswith('.csv'):
            # CSV文件按行数估算
            lines = getattr(file_stat, 'lines', 0) if hasattr(file_stat, 'lines') else 0
            lines = lines if lines is not None else 0  # 确保lines不为None
            if lines > 0:
                record_count += max(0, lines - 1)  # 减去标题行
        elif path.endswith('.txt'):
            # 文本文件按行数估算
            lines = getattr(file_stat, 'lines', 0) if hasattr(file_stat, 'lines') else 0
            lines = lines if lines is not None else 0  # 确保lines不为None
            record_count += max(1, lines // 10)  # 假设10行为一条记录
    
    return record_count if record_count > 0 else None


def _detect_languages_from_files(files: List) -> List[str]:
    """从文件名中检测语言."""
    languages = set()
    
    # 检查是否有中文文件名或路径
    has_chinese = False
    has_english = False
    
    for file_stat in files:
        if hasattr(file_stat, 'path'):
            path = file_stat.path
        else:
            path = file_stat.get('path', '') if isinstance(file_stat, dict) else ''
        
        # 简单的语言检测
        if re.search(r'[\u4e00-\u9fff]', path):
            has_chinese = True
        if re.search(r'[a-zA-Z]', path):
            has_english = True
    
    if has_chinese:
        languages.add("zh")
    if has_english:
        languages.add("en")
    
    if not languages:
        languages.add("en")  # 默认英文
    
    return list(languages)