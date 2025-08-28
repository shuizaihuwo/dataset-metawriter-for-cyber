"""Preliminary analysis node using LLM."""

import json
import asyncio
from typing import Dict, Any, Optional
import structlog
from openai import AsyncOpenAI

from ..models import DatasetState, ProcessingStatus, LLMConfig
from ..config import Config

logger = structlog.get_logger(__name__)


async def preliminary_analysis(state: DatasetState, config: Optional[Config] = None) -> Dict[str, Any]:
    """
    Perform preliminary analysis using LLM.
    
    Args:
        state: Current dataset processing state  
        config: Configuration object
        
    Returns:
        Updated state with preliminary analysis results
    """
    if config is None:
        from ..config import load_config
        config = load_config()
    
    try:
        logger.info("Starting preliminary_analysis",
                   processing_id=state.processing_id,
                   dataset_name=state.dataset_name)
        
        # Check for test/mock mode
        if config.llm.api_key in ["test-key-for-testing", "mock-api-key"]:
            logger.info("Using mock analysis for testing")
            return await _generate_mock_analysis(state)
        
        # Create LLM client
        client = AsyncOpenAI(
            api_key=config.llm.api_key,
            base_url=config.llm.base_url
        )
        
        # Build analysis prompt
        prompt = _build_analysis_prompt(state)
        
        # Call LLM with retry logic
        analysis_result = await _call_llm_with_retry(
            client, 
            prompt, 
            config.llm,
            config.node_config.max_retries
        )
        
        if analysis_result is None:
            return {
                "status": ProcessingStatus.FAILED,
                "error_message": "Failed to get LLM response after retries",
                "current_step": "preliminary_analysis"
            }
        
        # Validate and parse response
        parsed_result = _parse_llm_response(analysis_result)
        
        if parsed_result is None:
            return {
                "status": ProcessingStatus.FAILED,
                "error_message": "Failed to parse LLM response",
                "current_step": "preliminary_analysis"
            }
        
        # Extract confidence score and need_search flag
        confidence_score = parsed_result.get("confidence_score", 0.5)
        need_search = parsed_result.get("needs_web_search", False)
        
        # Determine if search is needed based on missing info
        if not need_search:
            need_search = _should_search(parsed_result)
        
        logger.info("Preliminary analysis completed",
                   processing_id=state.processing_id,
                   confidence_score=confidence_score,
                   need_search=need_search)
        
        return {
            "preliminary": parsed_result,
            "need_search": need_search,
            "confidence_scores": {"preliminary_analysis": confidence_score},
            "status": ProcessingStatus.PROCESSING,
            "current_step": "preliminary_analysis"
        }
        
    except Exception as e:
        error_msg = f"Error in preliminary_analysis: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "status": ProcessingStatus.FAILED,
            "error_message": error_msg,
            "current_step": "preliminary_analysis",
            "errors": state.errors + [error_msg]
        }


def _build_analysis_prompt(state: DatasetState) -> str:
    """Build structured prompt for preliminary analysis."""
    
    # Calculate file statistics
    file_stats = _calculate_file_stats(state.files)
    
    prompt = f"""你是一个专业的数据集分析专家。请分析以下数据集并输出JSON格式的结果。

数据集基本信息：
- 名称：{state.dataset_name or "未知"}
- 创建者：{state.creator or "未知"}
- 创建日期：{state.creation_date or "未知"}
- 文件总数：{len(state.files)}
- 总大小：{_format_size(state.total_size)}

文件类型统计：
{file_stats}

文件内容样本：
{state.file_samples[:4000] if len(state.file_samples) > 4000 else state.file_samples}

请根据以上信息分析数据集，并按照以下JSON格式输出结果：
{{
  "description": "详细描述数据集内容、用途和特点（100-200字）",
  "modality": "选择一个：自然语言文本|代码|流量|日志|结构化/表格|二进制|图像|音频|视频|多模态",
  "use_case": "选择一个：模型预训练|模型微调|模型微调(含思维链)|模型评测|强化学习|分类/回归|实体识别|数据分析|混合用途",
  "domain": "选择一个：基础通用|网络攻防|安全认知|体系化防御",
  "business_direction": "如果是网安相关，选择一个：代码分析|工具生成|情报分析|日志分析|流量分析|漏洞挖掘|策略规划|目标检测|策略验证|高级关联威胁分析|诱捕，否则填null",
  "business_point": "如果是网安相关，选择具体业务点，否则填null",
  "task_types": "细粒度任务类型列表，如：['NER', '分类', '问答']",
  "confidence_score": 0.85,
  "needs_web_search": true,
  "reasoning": "详细说明分析推理过程和依据"
}}

注意：
1. 请严格按照上述JSON格式输出
2. 所有枚举值必须完全匹配给定选项
3. confidence_score范围为0.0-1.0
4. 如果内容显示这是已知的公开数据集，设置needs_web_search为true
5. 重点关注网络安全相关特征进行分类"""

    return prompt


def _calculate_file_stats(files) -> str:
    """Calculate and format file statistics."""
    if not files:
        return "无文件"
    
    # Count by format
    format_counts = {}
    format_sizes = {}
    
    for file_stat in files:
        fmt = file_stat.format
        format_counts[fmt] = format_counts.get(fmt, 0) + 1
        format_sizes[fmt] = format_sizes.get(fmt, 0) + file_stat.size_bytes
    
    # Sort by count
    sorted_formats = sorted(format_counts.items(), key=lambda x: x[1], reverse=True)
    
    stats_lines = []
    for fmt, count in sorted_formats[:10]:  # Top 10 formats
        size = _format_size(format_sizes[fmt])
        stats_lines.append(f"  {fmt}: {count}个文件, {size}")
    
    return "\n".join(stats_lines)


def _format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f}PB"


async def _call_llm_with_retry(
    client: AsyncOpenAI, 
    prompt: str, 
    llm_config: LLMConfig,
    max_retries: int = 3
) -> Optional[str]:
    """Call LLM with exponential backoff retry."""
    
    for attempt in range(max_retries):
        try:
            response = await client.chat.completions.create(
                model=llm_config.model,
                messages=[
                    {"role": "system", "content": "You are a professional dataset analysis expert. Always respond in valid JSON format as requested."},
                    {"role": "user", "content": prompt}
                ],
                temperature=llm_config.temperature,
                max_tokens=llm_config.max_tokens,
                timeout=llm_config.timeout
            )
            
            if response.choices and response.choices[0].message:
                content = response.choices[0].message.content.strip()
                logger.info("LLM response received",
                           attempt=attempt + 1,
                           response_length=len(content))
                return content
            else:
                logger.warning("Empty LLM response", attempt=attempt + 1)
                
        except Exception as e:
            logger.warning("LLM call failed",
                          attempt=attempt + 1,
                          error=str(e))
            
            if attempt < max_retries - 1:
                # Exponential backoff
                wait_time = 2 ** attempt
                logger.info(f"Retrying in {wait_time} seconds...")
                await asyncio.sleep(wait_time)
            else:
                logger.error("All LLM retry attempts failed")
    
    return None


def _parse_llm_response(response: str) -> Optional[Dict[str, Any]]:
    """Parse and validate LLM JSON response."""
    try:
        # Try to extract JSON from response
        response = response.strip()
        
        # Remove markdown code block markers if present
        if response.startswith("```json"):
            response = response[7:]
        if response.startswith("```"):
            response = response[3:]
        if response.endswith("```"):
            response = response[:-3]
        
        response = response.strip()
        
        # Parse JSON
        result = json.loads(response)
        
        # Validate required fields
        required_fields = ["description", "modality", "use_case", "domain", "confidence_score"]
        for field in required_fields:
            if field not in result:
                logger.error(f"Missing required field: {field}")
                return None
        
        # Validate confidence score
        confidence = result.get("confidence_score", 0)
        if not isinstance(confidence, (int, float)) or not 0 <= confidence <= 1:
            result["confidence_score"] = 0.5  # Default value
        
        # Ensure needs_web_search is boolean
        if "needs_web_search" not in result:
            result["needs_web_search"] = False
        
        return result
        
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON response", error=str(e), response=response[:500])
        return None
    except Exception as e:
        logger.error("Error parsing LLM response", error=str(e))
        return None


async def _generate_mock_analysis(state: DatasetState) -> Dict[str, Any]:
    """Generate mock analysis for testing purposes."""
    
    # Analyze file types to provide realistic mock data
    has_code = any(".py" in f.path or ".js" in f.path or ".cpp" in f.path or ".c" in f.path 
                  for f in state.files)
    has_data = any(".json" in f.path or ".csv" in f.path or ".txt" in f.path 
                  for f in state.files)
    has_docs = any(".md" in f.path or ".rst" in f.path or "README" in f.path 
                  for f in state.files)
    
    # Generate appropriate mock analysis based on content
    if "cyber" in state.dataset_name.lower():
        modality = "代码" if has_code else "自然语言文本"
        domain = "网络攻防"
        use_case = "模型评测"
        description = f"{state.dataset_name} 是一个网络安全相关的数据集，包含用于网络安全评测和训练的数据。"
    elif has_code:
        modality = "代码"
        domain = "基础通用"
        use_case = "模型评测"
        description = f"{state.dataset_name} 是一个代码相关的数据集，可用于代码分析和评测。"
    else:
        modality = "自然语言文本"
        domain = "基础通用" 
        use_case = "数据分析"
        description = f"{state.dataset_name} 是一个通用数据集，包含多种格式的数据文件。"
    
    mock_analysis = {
        "name": state.dataset_name,
        "description": description,
        "modality": modality,
        "domain": domain,
        "use_case": use_case,
        "task_types": ["benchmark", "evaluation"] if "bench" in state.dataset_name.lower() else ["analysis"],
        "reasoning": "基于文件扫描和命名模式的分析结果（测试模式）",
        "confidence_score": 0.75
    }
    
    # Update the state with mock analysis results
    updates = {
        "current_step": "preliminary_analysis", 
        "status": "processing",
        "preliminary": mock_analysis,
        "need_search": False  # Skip web search in mock mode
    }
    
    logger.info("Mock analysis completed", 
                processing_id=state.processing_id,
                analysis=mock_analysis)
    
    return updates


def _should_search(analysis_result: Dict[str, Any]) -> bool:
    """Determine if web search is needed based on analysis."""
    # Search if confidence is low
    confidence = analysis_result.get("confidence_score", 0)
    if confidence < 0.6:
        return True
    
    # Search if description suggests this is a known public dataset
    description = analysis_result.get("description", "").lower()
    public_indicators = ["github", "论文", "公开", "开源", "benchmark", "competition", "challenge"]
    
    if any(indicator in description for indicator in public_indicators):
        return True
    
    return False