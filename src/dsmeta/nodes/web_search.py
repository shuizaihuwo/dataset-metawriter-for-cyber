"""网页搜索节点 - 用于自动获取数据集的来源信息、许可证和引用信息."""

import asyncio
import json
from typing import Dict, Any, List, Optional
import structlog
import httpx
from urllib.parse import quote, urljoin

from ..models import DatasetState
from ..config import Config

logger = structlog.get_logger(__name__)


async def web_search(state, config: Optional[Config] = None) -> Dict[str, Any]:
    """
    执行网页搜索以获取数据集的额外信息.
    
    参数:
        state: 当前数据集处理状态 (dict或DatasetState对象)
        config: 配置对象
        
    返回:
        包含搜索结果的状态更新
    """
    if config is None:
        from ..config import load_config
        config = load_config()

    try:
        # 安全获取状态值
        processing_id = _get_state_value(state, "processing_id", "unknown")
        dataset_name = _get_state_value(state, "dataset_name", "")
        preliminary = _get_state_value(state, "preliminary", {})
        
        logger.info("开始网页搜索",
                   processing_id=processing_id,
                   dataset_name=dataset_name,
                   search_enabled=config.search.enabled)
        
        # 检查搜索是否被禁用或API密钥缺失
        if not config.search.enabled or not config.search.api_key:
            logger.info("跳过网页搜索 - 搜索被禁用或缺少API密钥")
            return {
                "current_step": "web_search",
                "status": "processing", 
                "web_search": [],
                "search_skipped": True
            }
        
        # 构建搜索查询
        search_queries = _build_search_queries(dataset_name, preliminary)
        
        if not search_queries:
            logger.info("没有可用的搜索查询")
            return {
                "current_step": "web_search",
                "status": "processing",
                "web_search": []
            }
        
        # 执行搜索
        all_results = []
        for query in search_queries:
            try:
                results = await _execute_search(query, config)
                all_results.extend(results)
                
                # 避免过于频繁的API调用
                if len(search_queries) > 1:
                    await asyncio.sleep(1)
                    
            except Exception as e:
                logger.warning("搜索查询失败", 
                              query=query, 
                              error=str(e))
                continue
        
        # 去重并过滤结果
        unique_results = _deduplicate_results(all_results)
        filtered_results = _filter_results(unique_results, dataset_name)
        
        logger.info("网页搜索完成",
                   processing_id=processing_id,
                   total_queries=len(search_queries),
                   total_results=len(all_results),
                   filtered_results=len(filtered_results))
        
        return {
            "current_step": "web_search", 
            "status": "processing",
            "web_search": filtered_results[:config.search.max_results]  # 限制结果数量
        }
        
    except Exception as e:
        error_msg = f"网页搜索出错: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        return {
            "current_step": "web_search",
            "status": "processing",  # 搜索失败不影响整体流程
            "web_search": [],
            "error_message": error_msg
        }


def _get_state_value(state, key, default=None):
    """安全地从状态中获取值 (支持dict和对象)."""
    if isinstance(state, dict):
        return state.get(key, default)
    else:
        return getattr(state, key, default)


def _build_search_queries(dataset_name: str, preliminary: Dict[str, Any]) -> List[str]:
    """构建搜索查询列表."""
    if not dataset_name:
        return []
    
    queries = []
    
    # 基础查询 - 数据集名称
    queries.append(f'"{dataset_name}" dataset')
    
    # 如果有描述，添加关键词查询
    description = preliminary.get("description", "")
    if description and len(description) > 10:
        # 提取关键词
        keywords = _extract_keywords_from_description(description)
        if keywords:
            queries.append(f'"{dataset_name}" {" ".join(keywords[:3])}')
    
    # GitHub专门搜索
    queries.append(f'"{dataset_name}" site:github.com')
    
    # 学术论文搜索
    queries.append(f'"{dataset_name}" dataset paper arxiv')
    
    # HuggingFace搜索
    queries.append(f'"{dataset_name}" site:huggingface.co')
    
    return queries[:3]  # 限制查询数量


def _extract_keywords_from_description(description: str) -> List[str]:
    """从描述中提取关键词."""
    import re
    
    # 移除常见停用词和标点
    stop_words = {
        "是", "一个", "的", "和", "或", "在", "用于", "包含", "提供", "支持", "可以", "能够",
        "数据集", "数据", "文件", "内容", "信息", "this", "is", "a", "an", "the", "and", "or", 
        "in", "for", "with", "to", "of", "that", "dataset", "data", "file", "files"
    }
    
    # 提取中英文词汇
    words = re.findall(r'[\u4e00-\u9fff]+|[a-zA-Z]{3,}', description.lower())
    
    # 过滤停用词和短词
    keywords = [w for w in words if w not in stop_words and len(w) > 2]
    
    return keywords[:5]


async def _execute_search(query: str, config: Config) -> List[Dict[str, Any]]:
    """执行单个搜索查询."""
    if config.search.provider == "tavily":
        return await _search_tavily(query, config)
    else:
        logger.warning("不支持的搜索提供商", provider=config.search.provider)
        return []


async def _search_tavily(query: str, config: Config) -> List[Dict[str, Any]]:
    """使用Tavily API执行搜索."""
    url = "https://api.tavily.com/search"
    
    payload = {
        "api_key": config.search.api_key,
        "query": query,
        "search_depth": "basic",
        "include_answer": False,
        "include_images": False,
        "include_raw_content": False,
        "max_results": min(config.search.max_results, 5),  # 单次查询限制
        "include_domains": ["github.com", "huggingface.co", "arxiv.org", "paperswithcode.com"]
    }
    
    timeout = httpx.Timeout(config.search.timeout)
    
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            
            data = response.json()
            results = data.get("results", [])
            
            # 转换为统一格式
            formatted_results = []
            for result in results:
                formatted_results.append({
                    "title": result.get("title", ""),
                    "url": result.get("url", ""),
                    "snippet": result.get("content", ""),
                    "source": "tavily"
                })
            
            return formatted_results
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 429:
                logger.warning("搜索API请求频率限制", query=query)
                await asyncio.sleep(5)  # 等待后重试
                raise
            else:
                logger.error("搜索API错误", 
                           status_code=e.response.status_code,
                           query=query)
                raise
        
        except Exception as e:
            logger.error("搜索请求失败", query=query, error=str(e))
            raise


def _deduplicate_results(results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """去重搜索结果."""
    seen_urls = set()
    unique_results = []
    
    for result in results:
        url = result.get("url", "")
        if url and url not in seen_urls:
            seen_urls.add(url)
            unique_results.append(result)
    
    return unique_results


def _filter_results(results: List[Dict[str, Any]], dataset_name: str) -> List[Dict[str, Any]]:
    """过滤和排序搜索结果."""
    if not results:
        return []
    
    # 计算相关性得分
    scored_results = []
    for result in results:
        score = _calculate_relevance_score(result, dataset_name)
        if score > 0:
            result["relevance_score"] = score
            scored_results.append(result)
    
    # 按得分排序
    scored_results.sort(key=lambda x: x["relevance_score"], reverse=True)
    
    return scored_results


def _calculate_relevance_score(result: Dict[str, Any], dataset_name: str) -> float:
    """计算搜索结果的相关性得分."""
    score = 0.0
    
    title = result.get("title", "").lower()
    url = result.get("url", "").lower()
    snippet = result.get("snippet", "").lower()
    dataset_name_lower = dataset_name.lower()
    
    # 标题中包含数据集名称 - 高分
    if dataset_name_lower in title:
        score += 10.0
    
    # URL中包含数据集名称 - 中等分数  
    if dataset_name_lower in url:
        score += 5.0
    
    # 摘要中包含数据集名称 - 低分
    if dataset_name_lower in snippet:
        score += 2.0
    
    # 优质来源加分
    domain_scores = {
        "github.com": 8.0,
        "huggingface.co": 7.0,
        "arxiv.org": 6.0,
        "paperswithcode.com": 5.0,
        "kaggle.com": 4.0
    }
    
    for domain, bonus in domain_scores.items():
        if domain in url:
            score += bonus
            break
    
    # 相关关键词加分
    relevant_keywords = [
        "dataset", "data", "repository", "repo", "license", "citation", 
        "paper", "benchmark", "collection", "corpus"
    ]
    
    text_to_check = f"{title} {snippet}".lower()
    for keyword in relevant_keywords:
        if keyword in text_to_check:
            score += 1.0
    
    return score


async def decide_need_search(state) -> Dict[str, Any]:
    """
    决定是否需要执行网页搜索.
    
    参数:
        state: 当前数据集处理状态
        
    返回:
        包含搜索决策的状态更新
    """
    try:
        # 获取初步分析结果
        preliminary = _get_state_value(state, "preliminary", {})
        dataset_name = _get_state_value(state, "dataset_name", "")
        processing_id = _get_state_value(state, "processing_id", "unknown")
        
        logger.info("评估是否需要搜索",
                   processing_id=processing_id,
                   dataset_name=dataset_name)
        
        # 检查关键信息是否缺失
        missing_info = []
        
        # 检查来源信息
        if not preliminary.get("source"):
            missing_info.append("source")
        
        if not preliminary.get("source_url"):
            missing_info.append("source_url")
            
        if not preliminary.get("license"):
            missing_info.append("license")
            
        if not preliminary.get("citation"):
            missing_info.append("citation")
        
        # 检查置信度
        confidence = preliminary.get("confidence_score", 0.0)
        low_confidence = confidence < 0.7
        
        # 检查是否为知名数据集 (通过名称判断)
        is_known_dataset = _is_likely_public_dataset(dataset_name, preliminary)
        
        # 决策逻辑
        need_search = (
            len(missing_info) >= 2 or  # 缺少2个或更多关键信息
            low_confidence or          # 置信度较低
            is_known_dataset          # 疑似公开数据集
        )
        
        logger.info("搜索决策完成",
                   processing_id=processing_id,
                   need_search=need_search,
                   missing_info=missing_info,
                   confidence=confidence,
                   is_known_dataset=is_known_dataset)
        
        return {
            "current_step": "decide_need_search",
            "status": "processing", 
            "need_search": need_search,
            "search_reasons": {
                "missing_info": missing_info,
                "low_confidence": low_confidence,
                "is_known_dataset": is_known_dataset
            }
        }
        
    except Exception as e:
        error_msg = f"搜索决策出错: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 出错时默认进行搜索
        return {
            "current_step": "decide_need_search",
            "status": "processing",
            "need_search": True,
            "error_message": error_msg
        }


def _is_likely_public_dataset(dataset_name: str, preliminary: Dict[str, Any]) -> bool:
    """判断是否为可能的公开数据集."""
    if not dataset_name:
        return False
    
    name_lower = dataset_name.lower()
    description = preliminary.get("description", "").lower()
    
    # 知名数据集名称模式
    public_indicators = [
        "benchmark", "bench", "eval", "test", "challenge", "competition",
        "coco", "imagenet", "bert", "glue", "squad", "wiki", "common",
        "open", "public", "arxiv", "paper", "official"
    ]
    
    # 检查名称中的指示词
    for indicator in public_indicators:
        if indicator in name_lower or indicator in description:
            return True
    
    # 检查是否包含版本号或日期(通常表示正式发布)
    import re
    if re.search(r'v\d+|version|20\d{2}', name_lower):
        return True
    
    return False