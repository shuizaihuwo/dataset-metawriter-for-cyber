"""Pydantic models for dataset annotation workflow."""

import hashlib
import uuid
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class ProcessingStatus(str, Enum):
    """Processing status enumeration."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCESS = "success"
    FAILED = "failed"
    RETRY = "retry"


class DataModality(str, Enum):
    """Data modality enumeration."""
    NATURAL_LANGUAGE = "自然语言文本"
    CODE = "代码"
    TRAFFIC = "流量"
    LOG = "日志"
    STRUCTURED = "结构化/表格"
    BINARY = "二进制"
    IMAGE = "图像"
    AUDIO = "音频"
    VIDEO = "视频"
    MULTIMODAL = "多模态"


class UseCase(str, Enum):
    """Use case enumeration."""
    PRETRAINING = "模型预训练"
    FINETUNING = "模型微调"
    FINETUNING_COT = "模型微调(含思维链)"
    EVALUATION = "模型评测"
    REINFORCEMENT = "强化学习"
    CLASSIFICATION = "分类/回归"
    NER = "实体识别"
    DATA_ANALYSIS = "数据分析"
    MIXED = "混合用途"


class Domain(str, Enum):
    """Domain enumeration."""
    GENERAL = "基础通用"
    CYBER_ATTACK = "网络攻防"
    SECURITY_COGNITION = "安全认知"
    SYSTEMATIC_DEFENSE = "体系化防御"


class BusinessDirection(str, Enum):
    """Business direction enumeration."""
    CODE_ANALYSIS = "代码分析"
    TOOL_GENERATION = "工具生成"
    INTELLIGENCE_ANALYSIS = "情报分析"
    LOG_ANALYSIS = "日志分析"
    TRAFFIC_ANALYSIS = "流量分析"
    VULNERABILITY_MINING = "漏洞挖掘"
    STRATEGIC_PLANNING = "策略规划"
    TARGET_DETECTION = "目标检测"
    POLICY_VERIFICATION = "策略验证"
    ADVANCED_THREAT_ANALYSIS = "高级关联威胁分析"
    DECEPTION = "诱捕"


class BusinessPoint(str, Enum):
    """Business point enumeration."""
    CODE_GENERATION = "代码辅助生成"
    TOOL_TESTING = "工具测试"
    TACTICS_DESIGN = "技战法设计"
    TARGET_ANALYSIS = "目标分析"
    TARGET_DETECTION = "目标探测"
    VULNERABILITY_ANALYSIS = "脆弱性分析"
    STATIC_ANALYSIS = "静态分析"
    DYNAMIC_ANALYSIS = "动态分析"
    FUZZING = "模糊测试"
    VULNERABILITY_EXPLOITATION = "脆弱性利用"
    ADVANCED_CODE_DESIGN = "高级代码设计"
    VERIFICATION_ENV = "验证环境生成"


class Rating(str, Enum):
    """Rating enumeration."""
    BASIC = "基础"
    INTERMEDIATE = "进阶"
    ADVANCED = "高级"
    PRIVATE = "专用私有"


class PIIRisk(str, Enum):
    """PII risk enumeration."""
    NONE = "none"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class AccessLevel(str, Enum):
    """Access level enumeration."""
    PUBLIC = "public"
    INTERNAL = "internal"
    PRIVATE = "private"


class FileStat(BaseModel):
    """File statistics model."""
    path: str = Field(..., description="Relative file path")
    size_bytes: int = Field(..., description="File size in bytes")
    format: str = Field(..., description="File format/extension")
    sha256: Optional[str] = Field(None, description="SHA256 hash")
    encoding: Optional[str] = Field(None, description="File encoding")
    lines: Optional[int] = Field(None, description="Number of lines (for text files)")

    @validator('format')
    def format_with_dot(cls, v):
        """Ensure format starts with dot."""
        if not v.startswith('.'):
            return f'.{v}'
        return v


class FileFormatStat(BaseModel):
    """File format statistics model."""
    format: str = Field(..., description="File format")
    count: int = Field(..., description="Number of files")
    size: str = Field(..., description="Total size (human readable)")
    ratio: str = Field(..., description="Percentage of total")


class DatasetMetadata(BaseModel):
    """Complete dataset metadata model."""
    # Basic Info
    id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Unique identifier")
    name: str = Field(..., description="Dataset name")
    description: str = Field(..., description="Dataset description")
    source: Optional[str] = Field(None, description="Source platform/project")
    source_url: Optional[str] = Field(None, description="Official repository/download URL")
    size: str = Field(..., description="Total size (human readable)")
    num_files: int = Field(..., description="Number of files")
    num_records: Optional[int] = Field(None, description="Estimated number of records")
    languages: List[str] = Field(default_factory=list, description="Detected languages")
    
    # Technical Attributes
    modality: DataModality = Field(..., description="Data modality")
    file_formats: List[FileFormatStat] = Field(default_factory=list, description="File format statistics")
    use_case: UseCase = Field(..., description="Primary use case")
    task_types: List[str] = Field(default_factory=list, description="Fine-grained task types")
    input_schema: Optional[Dict[str, Any]] = Field(None, description="Input schema")
    label_schema: Optional[Dict[str, Any]] = Field(None, description="Label schema")
    
    # Business Attributes  
    domain: Domain = Field(..., description="Professional domain")
    business_direction: List[BusinessDirection] = Field(default_factory=list, description="Business directions (multi-select)")
    business_point: List[BusinessPoint] = Field(default_factory=list, description="Business points (multi-select)")
    rating: Rating = Field(Rating.BASIC, description="Professional rating")
    
    # Management Info
    creator: Optional[str] = Field(None, description="Creator/submitter")
    creation_date: Optional[str] = Field(None, description="Creation date")
    version: str = Field("v1.0", description="Version")
    license: Optional[str] = Field(None, description="License")
    access_level: AccessLevel = Field(AccessLevel.PUBLIC, description="Access level")
    citation: Optional[str] = Field(None, description="Citation text")
    
    # Quality & Security
    sample: Optional[str] = Field(None, description="Sample content")
    remarks: Optional[str] = Field(None, description="Additional remarks")
    pii_risk: PIIRisk = Field(PIIRisk.NONE, description="PII risk assessment")
    quality_notes: Optional[str] = Field(None, description="Quality notes")
    checksum: Optional[str] = Field(None, description="Dataset checksum")


class DatasetState(BaseModel):
    """LangGraph state model for dataset processing."""
    # Basic Info
    dataset_path: str = Field(..., description="Dataset directory path")
    dataset_name: Optional[str] = Field(None, description="Parsed dataset name")
    creator: Optional[str] = Field(None, description="Parsed creator")
    creation_date: Optional[str] = Field(None, description="Parsed creation date")
    processing_id: str = Field(default_factory=lambda: str(uuid.uuid4()), description="Processing ID")
    
    # File Analysis Results
    files: List[FileStat] = Field(default_factory=list, description="File statistics")
    total_size: int = Field(0, description="Total size in bytes")
    file_samples: str = Field("", description="Sampled file content")
    
    # LLM Analysis Results
    preliminary: Dict[str, Any] = Field(default_factory=dict, description="Preliminary analysis")
    web_search: List[Dict[str, Any]] = Field(default_factory=list, description="Web search results")
    meta: Dict[str, Any] = Field(default_factory=dict, description="Final metadata")
    artifacts: Dict[str, Any] = Field(default_factory=dict, description="Generated artifacts")
    
    # State Management
    current_step: str = Field("start", description="Current processing step")
    status: ProcessingStatus = Field(ProcessingStatus.PENDING, description="Processing status")
    error_message: Optional[str] = Field(None, description="Error message")
    retry_count: int = Field(0, description="Retry count")
    
    # Quality Control
    need_search: bool = Field(False, description="Whether web search is needed")
    cache_key: Optional[str] = Field(None, description="Cache key")
    errors: List[str] = Field(default_factory=list, description="Error list")
    confidence_scores: Dict[str, float] = Field(default_factory=dict, description="Confidence scores")
    
    def generate_cache_key(self) -> str:
        """Generate cache key based on dataset path and total size."""
        key_data = f"{self.dataset_path}:{self.total_size}"
        return hashlib.sha256(key_data.encode()).hexdigest()[:16]
    
    def to_metadata(self) -> DatasetMetadata:
        """Convert state to DatasetMetadata model."""
        return DatasetMetadata(**self.meta) if self.meta else None


class NodeConfig(BaseModel):
    """Configuration for individual nodes."""
    max_retries: int = Field(3, description="Maximum retry attempts")
    timeout: int = Field(60, description="Timeout in seconds")
    cache_enabled: bool = Field(True, description="Enable caching")


class LLMConfig(BaseModel):
    """LLM configuration."""
    provider: str = Field("siliconflow", description="LLM provider")
    model: str = Field("THUDM/GLM-4-9B-0414", description="Model name")
    base_url: str = Field("https://api.siliconflow.cn/v1", description="API base URL")
    api_key: str = Field(..., description="API key")
    temperature: float = Field(0.1, description="Temperature")
    max_tokens: int = Field(4000, description="Max tokens")
    timeout: int = Field(60, description="Request timeout")


class FileProcessingConfig(BaseModel):
    """File processing configuration."""
    max_file_size: str = Field("50MB", description="Maximum file size")
    sample_head_lines: int = Field(1000, description="Head lines to sample")
    sample_tail_lines: int = Field(100, description="Tail lines to sample")
    sample_random_size: int = Field(500, description="Random sample size")
    encoding_fallback: List[str] = Field(["utf-8", "gbk", "latin-1"], description="Encoding fallback order")