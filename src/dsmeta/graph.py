"""LangGraph工作流 - 用于数据集自动标注的完整工作流程."""

from typing import Dict, Any
import structlog
from langgraph.graph import StateGraph, START, END

from .models import DatasetState
from .config import Config
from .nodes import (
    scan_and_parse,
    read_and_sample,
    preliminary_analysis,
    generate_markdown,
    write_outputs
)
from .nodes.web_search import web_search, decide_need_search
from .nodes.synthesize_populate import synthesize_and_populate
from .nodes.validate_postprocess import validate_and_postprocess

logger = structlog.get_logger(__name__)


def create_annotation_graph(config: Config) -> StateGraph:
    """
    创建数据集标注工作流程图 - V1.1版本支持9个节点的完整流程.
    
    工作流程:
    1. scan_and_parse - 扫描和解析文件
    2. read_and_sample - 读取和采样内容
    3. preliminary_analysis - 初步分析（LLM）
    4. decide_need_search - 决定是否需要搜索
    5. web_search - 网页搜索（条件执行）
    6. synthesize_and_populate - 综合分析和填充
    7. validate_and_postprocess - 验证和后处理
    8. generate_markdown - 生成Markdown文档
    9. write_outputs - 写入输出文件
    
    参数:
        config: 配置对象
        
    返回:
        编译后的StateGraph工作流
    """
    
    # 创建状态图
    workflow = StateGraph(DatasetState)
    
    # 创建包含配置参数的异步封装函数
    async def read_and_sample_wrapper(state) -> Dict[str, Any]:
        """读取和采样节点封装器."""
        return await read_and_sample(state, config.file_processing)
    
    async def preliminary_analysis_wrapper(state) -> Dict[str, Any]:
        """初步分析节点封装器."""
        return await preliminary_analysis(state, config)
    
    async def web_search_wrapper(state) -> Dict[str, Any]:
        """网页搜索节点封装器."""
        return await web_search(state, config)
    
    async def synthesize_and_populate_wrapper(state) -> Dict[str, Any]:
        """综合分析节点封装器."""
        return await synthesize_and_populate(state, config)
    
    async def validate_and_postprocess_wrapper(state) -> Dict[str, Any]:
        """验证和后处理节点封装器."""
        return await validate_and_postprocess(state, config)
    
    async def generate_markdown_wrapper(state) -> Dict[str, Any]:
        """生成Markdown节点封装器."""
        return await generate_markdown(state, config)
    
    async def write_outputs_wrapper(state) -> Dict[str, Any]:
        """写入输出节点封装器."""
        return await write_outputs(state, config)
    
    # 添加所有工作流节点
    workflow.add_node("scan_and_parse", scan_and_parse)
    workflow.add_node("read_and_sample", read_and_sample_wrapper)
    workflow.add_node("preliminary_analysis", preliminary_analysis_wrapper)
    workflow.add_node("decide_need_search", decide_need_search)
    workflow.add_node("web_search", web_search_wrapper)
    workflow.add_node("synthesize_and_populate", synthesize_and_populate_wrapper)
    workflow.add_node("validate_and_postprocess", validate_and_postprocess_wrapper)
    workflow.add_node("generate_markdown", generate_markdown_wrapper)
    workflow.add_node("write_outputs", write_outputs_wrapper)
    
    # 定义线性工作流边
    workflow.add_edge(START, "scan_and_parse")
    workflow.add_edge("scan_and_parse", "read_and_sample")
    workflow.add_edge("read_and_sample", "preliminary_analysis")
    workflow.add_edge("preliminary_analysis", "decide_need_search")
    
    # 定义条件分支 - 根据是否需要搜索来决定路径
    def should_search(state) -> str:
        """决定是否进行网页搜索的路由函数."""
        need_search = state.get("need_search", False) if isinstance(state, dict) else getattr(state, "need_search", False)
        
        logger.debug("路由决策", need_search=need_search)
        return "search" if need_search else "skip_search"
    
    workflow.add_conditional_edges(
        "decide_need_search",
        should_search,
        {
            "search": "web_search",
            "skip_search": "synthesize_and_populate"
        }
    )
    
    # 搜索完成后继续到综合分析
    workflow.add_edge("web_search", "synthesize_and_populate")
    
    # 继续后续流程
    workflow.add_edge("synthesize_and_populate", "validate_and_postprocess")
    workflow.add_edge("validate_and_postprocess", "generate_markdown")
    workflow.add_edge("generate_markdown", "write_outputs")
    workflow.add_edge("write_outputs", END)
    
    # 编译工作流图
    app = workflow.compile()
    
    logger.info("数据集标注工作流创建完成 - V1.1版本",
               total_nodes=9,
               has_conditional_search=True)
    
    return app


async def process_dataset(dataset_path: str, config: Config) -> Dict[str, Any]:
    """
    通过标注工作流处理单个数据集.
    
    参数:
        dataset_path: 数据集目录路径
        config: 配置对象
        
    返回:
        处理结果字典
    """
    
    logger.info("开始数据集处理",
               dataset_path=dataset_path)
    
    try:
        # 创建初始状态
        initial_state = DatasetState(
            dataset_path=dataset_path
        )
        
        # 创建并运行工作流
        app = create_annotation_graph(config)
        
        # 执行工作流
        final_state = await app.ainvoke(initial_state)
        
        logger.info("数据集处理完成",
                   dataset_path=dataset_path,
                   status=final_state.get("status", "unknown"),
                   processing_id=final_state.get("processing_id", "unknown"))
        
        return {
            "success": final_state.get("status") == "success",
            "status": final_state.get("status", "failed"),
            "processing_id": final_state.get("processing_id"),
            "dataset_name": final_state.get("dataset_name"),
            "error_message": final_state.get("error_message"),
            "written_files": final_state.get("written_files", [])
        }
        
    except Exception as e:
        error_msg = f"Workflow execution failed: {str(e)}"
        logger.error(error_msg, exc_info=True, dataset_path=dataset_path)
        
        return {
            "success": False,
            "status": "failed",
            "error_message": error_msg,
            "dataset_path": dataset_path
        }