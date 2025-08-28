"""Read and sample node for dataset content analysis."""

import json
import random
import re
from pathlib import Path
from typing import Dict, Any, List, Optional
import chardet
import structlog
import aiofiles

from ..models import DatasetState, FileStat, ProcessingStatus
from ..config import FileProcessingConfig

logger = structlog.get_logger(__name__)


async def read_and_sample(state: DatasetState, config: Optional[FileProcessingConfig] = None) -> Dict[str, Any]:
    """
    Read and sample file contents for analysis.
    
    Args:
        state: Current dataset processing state
        config: File processing configuration
        
    Returns:
        Updated state with sampled content
    """
    if config is None:
        config = FileProcessingConfig()
    
    try:
        logger.info("Starting read_and_sample",
                   processing_id=state.processing_id,
                   num_files=len(state.files))
        
        dataset_path = Path(state.dataset_path)
        
        # Prioritize important files
        prioritized_files = _prioritize_files(state.files)
        
        # Sample files based on type and importance
        sampled_content = await _sample_files(
            dataset_path, 
            prioritized_files, 
            config
        )
        
        logger.info("Sampling completed",
                   processing_id=state.processing_id,
                   sampled_files=len(sampled_content),
                   total_content_length=len(sampled_content.get("combined", "")))
        
        return {
            "file_samples": json.dumps(sampled_content, ensure_ascii=False, indent=2),
            "status": ProcessingStatus.PROCESSING,
            "current_step": "read_and_sample"
        }
        
    except Exception as e:
        error_msg = f"Error in read_and_sample: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "status": ProcessingStatus.FAILED,
            "error_message": error_msg,
            "current_step": "read_and_sample",
            "errors": state.errors + [error_msg]
        }


def _prioritize_files(files: List[FileStat]) -> List[FileStat]:
    """
    Prioritize files by importance for analysis.
    
    Priority order:
    1. Documentation files (README, LICENSE, etc.)
    2. Configuration files
    3. Data files by size and type
    """
    def get_priority(file_stat: FileStat) -> int:
        name_lower = Path(file_stat.path).name.lower()
        ext = file_stat.format.lower()
        
        # Documentation files - highest priority
        if any(doc in name_lower for doc in ['readme', 'license', 'changelog', 'contributing']):
            return 1
        
        # Configuration and metadata files
        if any(config in name_lower for config in ['config', 'meta', 'info']) or ext in ['.yaml', '.yml', '.toml', '.ini']:
            return 2
        
        # Data files
        if ext in ['.json', '.csv', '.tsv', '.xml']:
            return 3
        
        # Text files  
        if ext in ['.txt', '.md', '.rst']:
            return 4
        
        # Code files
        if ext in ['.py', '.js', '.java', '.cpp', '.c', '.h']:
            return 5
        
        # Other files
        return 6
    
    return sorted(files, key=get_priority)


async def _sample_files(
    dataset_path: Path, 
    files: List[FileStat], 
    config: FileProcessingConfig
) -> Dict[str, Any]:
    """
    Sample content from files based on type and size.
    """
    sampled_content = {
        "file_summaries": [],
        "text_samples": [],
        "data_samples": [],
        "special_files": {},
        "combined": ""
    }
    
    # Limit number of files to process
    max_files = 50
    files_to_process = files[:max_files]
    
    for file_stat in files_to_process:
        file_path = dataset_path / file_stat.path
        
        try:
            # Add file summary
            sampled_content["file_summaries"].append({
                "path": file_stat.path,
                "size": file_stat.size_bytes,
                "format": file_stat.format
            })
            
            # Skip binary files and very large files
            if _is_binary_file(file_path) or file_stat.size_bytes > 10 * 1024 * 1024:  # 10MB limit
                continue
                
            # Detect encoding and read file
            content = await _read_file_with_encoding(file_path, config.encoding_fallback)
            
            if content is None:
                continue
            
            # Sample based on file type
            if file_stat.format in ['.json', '.csv', '.tsv', '.xml']:
                sample = _sample_structured_data(content, file_stat.format)
                if sample:
                    sampled_content["data_samples"].append({
                        "file": file_stat.path,
                        "type": file_stat.format,
                        "sample": sample
                    })
            else:
                # Text sampling
                sample = _sample_text_content(content, config)
                if sample:
                    sampled_content["text_samples"].append({
                        "file": file_stat.path,
                        "sample": sample
                    })
            
            # Special handling for important files
            name_lower = Path(file_stat.path).name.lower()
            if any(special in name_lower for special in ['readme', 'license', 'config']):
                sampled_content["special_files"][file_stat.path] = content[:2000]
                
        except Exception as e:
            logger.warning("Failed to sample file",
                          file_path=str(file_path),
                          error=str(e))
            continue
    
    # Create combined sample for LLM analysis
    combined_parts = []
    
    # Add special files first
    for path, content in sampled_content["special_files"].items():
        combined_parts.append(f"=== {path} ===\n{content}\n")
    
    # Add text samples
    for sample_info in sampled_content["text_samples"][:10]:  # Limit to 10 files
        combined_parts.append(f"=== {sample_info['file']} ===\n{sample_info['sample']}\n")
    
    # Add data samples
    for sample_info in sampled_content["data_samples"][:5]:  # Limit to 5 files  
        combined_parts.append(f"=== {sample_info['file']} ({sample_info['type']}) ===\n{sample_info['sample']}\n")
    
    sampled_content["combined"] = "\n".join(combined_parts)
    
    return sampled_content


def _is_binary_file(file_path: Path) -> bool:
    """Check if file is binary."""
    binary_extensions = {
        '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
        '.zip', '.tar', '.gz', '.rar', '.7z',
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.svg', '.ico',
        '.mp3', '.wav', '.mp4', '.avi', '.mov', '.mkv',
        '.exe', '.dll', '.so', '.dylib',
        '.bin', '.dat', '.db', '.sqlite'
    }
    
    return file_path.suffix.lower() in binary_extensions


async def _read_file_with_encoding(file_path: Path, encoding_fallback: List[str]) -> Optional[str]:
    """Read file with encoding detection and fallback."""
    # First try to detect encoding
    try:
        async with aiofiles.open(file_path, 'rb') as f:
            raw_data = await f.read(min(8192, file_path.stat().st_size))
            if raw_data:
                detected = chardet.detect(raw_data)
                if detected['encoding'] and detected['confidence'] > 0.7:
                    encoding = detected['encoding']
                    async with aiofiles.open(file_path, 'r', encoding=encoding) as f:
                        return await f.read()
    except:
        pass
    
    # Fallback to predefined encodings
    for encoding in encoding_fallback:
        try:
            async with aiofiles.open(file_path, 'r', encoding=encoding) as f:
                content = await f.read()
                # Apply PII masking
                return _mask_pii(content)
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logger.warning(f"Failed to read file with encoding {encoding}",
                          file_path=str(file_path),
                          error=str(e))
            continue
    
    return None


def _mask_pii(content: str) -> str:
    """Basic PII masking."""
    # Email addresses
    content = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[EMAIL]', content)
    
    # IP addresses
    content = re.sub(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', '[IP]', content)
    
    # Phone numbers (simple pattern)
    content = re.sub(r'\b\d{3}-\d{3}-\d{4}\b', '[PHONE]', content)
    
    # API keys and tokens (simple pattern)
    content = re.sub(r'\b[A-Za-z0-9]{32,}\b', '[TOKEN]', content)
    
    return content


def _sample_text_content(content: str, config: FileProcessingConfig) -> Optional[str]:
    """Sample text content using head/tail/random strategy."""
    lines = content.split('\n')
    total_lines = len(lines)
    
    if total_lines <= config.sample_head_lines + config.sample_tail_lines:
        return content
    
    # Head lines
    head_lines = lines[:config.sample_head_lines]
    
    # Tail lines
    tail_lines = lines[-config.sample_tail_lines:] if config.sample_tail_lines > 0 else []
    
    # Random middle lines
    middle_start = config.sample_head_lines
    middle_end = total_lines - config.sample_tail_lines
    
    if middle_end > middle_start:
        available_middle = middle_end - middle_start
        sample_size = min(config.sample_random_size, available_middle)
        middle_indices = sorted(random.sample(range(middle_start, middle_end), sample_size))
        middle_lines = [lines[i] for i in middle_indices]
    else:
        middle_lines = []
    
    # Combine samples
    sampled_lines = head_lines
    if middle_lines:
        sampled_lines.extend(["\n... [RANDOM MIDDLE SAMPLE] ...\n"] + middle_lines)
    if tail_lines:
        sampled_lines.extend(["\n... [TAIL SAMPLE] ...\n"] + tail_lines)
    
    return '\n'.join(sampled_lines)


def _sample_structured_data(content: str, file_format: str) -> Optional[str]:
    """Sample structured data files."""
    try:
        if file_format == '.json':
            data = json.loads(content)
            return _sample_json_data(data)
        elif file_format in ['.csv', '.tsv']:
            return _sample_csv_data(content)
        else:
            # For other formats, just return truncated content
            return content[:2000] if len(content) > 2000 else content
    except:
        return content[:1000] if len(content) > 1000 else content


def _sample_json_data(data: Any, max_items: int = 10) -> str:
    """Sample JSON data structure."""
    if isinstance(data, list):
        if len(data) <= max_items:
            return json.dumps(data, ensure_ascii=False, indent=2)
        else:
            sample = data[:max_items]
            return json.dumps({
                "sample_size": max_items,
                "total_items": len(data),
                "sample": sample
            }, ensure_ascii=False, indent=2)
    elif isinstance(data, dict):
        if len(data) <= max_items:
            return json.dumps(data, ensure_ascii=False, indent=2)
        else:
            sample_keys = list(data.keys())[:max_items]
            sample = {k: data[k] for k in sample_keys}
            return json.dumps({
                "sample_size": max_items,
                "total_keys": len(data),
                "sample": sample
            }, ensure_ascii=False, indent=2)
    else:
        return json.dumps(data, ensure_ascii=False, indent=2)


def _sample_csv_data(content: str, max_lines: int = 20) -> str:
    """Sample CSV data."""
    lines = content.split('\n')
    
    if len(lines) <= max_lines:
        return content
    
    # Keep header + sample lines
    header = lines[0] if lines else ""
    sample_lines = lines[1:max_lines]
    
    result_lines = [header] + sample_lines
    if len(lines) > max_lines:
        result_lines.append(f"... ({len(lines) - max_lines} more lines)")
    
    return '\n'.join(result_lines)