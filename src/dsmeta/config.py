"""数据集标注工具的配置管理模块."""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, validator

from .models import LLMConfig, FileProcessingConfig, NodeConfig


class SearchConfig(BaseModel):
    """网页搜索配置."""
    enabled: bool = Field(True, description="启用网页搜索")
    provider: str = Field("tavily", description="搜索服务提供商")
    api_key: Optional[str] = Field(None, description="搜索API密钥")
    max_results: int = Field(10, description="最大搜索结果数")
    timeout: int = Field(30, description="搜索超时时间（秒）")


class OutputConfig(BaseModel):
    """输出配置."""
    template_dir: str = Field("./templates", description="模板目录")
    default_template: str = Field("default.md.j2", description="默认模板")
    output_formats: List[str] = Field(["markdown", "json"], description="输出格式")
    backup_existing: bool = Field(True, description="备份已存在的文件")


class LoggingConfig(BaseModel):
    """日志配置."""
    level: str = Field("INFO", description="日志级别")
    format: str = Field("json", description="日志格式")
    file_path: Optional[str] = Field(None, description="日志文件路径")


class MonitoringConfig(BaseModel):
    """目录监控配置 - V1.2增强版."""
    directories: List[str] = Field(default_factory=list, description="监控目录列表")
    patterns: List[str] = Field(["**/qiaoyu-*"], description="目录模式匹配")
    recursive: bool = Field(True, description="递归监控")
    polling_interval: int = Field(30, description="轮询间隔（秒）")
    max_concurrent_tasks: int = Field(4, description="最大并发任务数")
    cooldown_seconds: int = Field(5, description="处理冷却时间（秒）")
    retry_attempts: int = Field(3, description="最大重试次数")
    queue_size: int = Field(1000, description="任务队列大小")


class QualityControlConfig(BaseModel):
    """质量控制配置."""
    min_confidence_score: float = Field(0.7, description="最小置信度分数")
    require_human_review_threshold: float = Field(0.5, description="需要人工审核的阈值")
    required_fields: List[str] = Field(["name", "description", "modality"], description="必需字段列表")
    enum_validation: bool = Field(True, description="启用枚举值验证")


class Config(BaseModel):
    """主配置模型 - V1.2版本."""
    # 应用程序设置
    app_name: str = Field("dataset-annotation-tool", description="应用程序名称")
    version: str = Field("1.2.0", description="应用程序版本")
    debug: bool = Field(False, description="调试模式")
    
    # 组件配置
    llm: LLMConfig = Field(..., description="大语言模型配置")
    file_processing: FileProcessingConfig = Field(default_factory=FileProcessingConfig, description="文件处理配置")
    search: SearchConfig = Field(default_factory=SearchConfig, description="网页搜索配置")
    output: OutputConfig = Field(default_factory=OutputConfig, description="输出配置")
    logging: LoggingConfig = Field(default_factory=LoggingConfig, description="日志配置")
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig, description="目录监控配置")
    quality_control: QualityControlConfig = Field(default_factory=QualityControlConfig, description="质量控制配置")
    node_config: NodeConfig = Field(default_factory=NodeConfig, description="节点配置")
    
    @classmethod
    def from_file(cls, config_path: str) -> "Config":
        """从YAML文件加载配置，支持环境变量替换."""
        path = Path(config_path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            config_data = yaml.safe_load(f)
        
        # 替换环境变量
        config_data = cls._substitute_env_vars(config_data)
        
        return cls(**config_data)
    
    @staticmethod
    def _substitute_env_vars(obj: Any) -> Any:
        """递归地在配置中替换环境变量."""
        if isinstance(obj, dict):
            return {k: Config._substitute_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [Config._substitute_env_vars(item) for item in obj]
        elif isinstance(obj, str) and obj.startswith("${") and obj.endswith("}"):
            env_var = obj[2:-1]
            default_value = None
            
            # 处理默认值: ${VAR:default_value}
            if ":" in env_var:
                env_var, default_value = env_var.split(":", 1)
            
            return os.getenv(env_var, default_value)
        else:
            return obj
    
    @classmethod
    def create_default(cls) -> "Config":
        """创建默认配置."""
        api_key = os.getenv("SILICONFLOW_API_KEY")
        if not api_key:
            raise ValueError("SILICONFLOW_API_KEY environment variable is required")
        
        return cls(
            llm=LLMConfig(api_key=api_key),
            search=SearchConfig(
                api_key=os.getenv("TAVILY_API_KEY")
            )
        )
    
    def save_to_file(self, config_path: str) -> None:
        """将配置保存到YAML文件."""
        path = Path(config_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(path, 'w', encoding='utf-8') as f:
            yaml.dump(self.dict(exclude_none=True), f, default_flow_style=False, allow_unicode=True)


def load_config(config_path: Optional[str] = None) -> Config:
    """从文件加载配置或创建默认配置."""
    if config_path and Path(config_path).exists():
        return Config.from_file(config_path)
    
    # 尝试在常见位置查找config.yaml文件
    for path in ["config.yaml", "config/config.yaml", "../config.yaml"]:
        if Path(path).exists():
            return Config.from_file(path)
    
    # 创建默认配置
    return Config.create_default()