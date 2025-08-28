"""Scan and parse node for dataset analysis."""

import hashlib
import re
from pathlib import Path
from typing import Dict, Any
import structlog

from ..models import DatasetState, FileStat, ProcessingStatus
from ..parsers import parse_dataset_documents

logger = structlog.get_logger(__name__)


async def scan_and_parse(state: DatasetState) -> Dict[str, Any]:
    """
    Scan dataset directory and parse basic information.
    
    Args:
        state: Current dataset processing state
        
    Returns:
        Updated state with file statistics and parsed metadata
    """
    try:
        logger.info("Starting scan_and_parse", 
                   processing_id=state.processing_id,
                   dataset_path=state.dataset_path)
        
        dataset_path = Path(state.dataset_path)
        
        if not dataset_path.exists():
            error_msg = f"Dataset path does not exist: {state.dataset_path}"
            logger.error(error_msg)
            return {
                "status": ProcessingStatus.FAILED,
                "error_message": error_msg,
                "current_step": "scan_and_parse"
            }
        
        if not dataset_path.is_dir():
            error_msg = f"Dataset path is not a directory: {state.dataset_path}"
            logger.error(error_msg)
            return {
                "status": ProcessingStatus.FAILED,
                "error_message": error_msg,
                "current_step": "scan_and_parse"
            }
        
        # Parse dataset name, creator, and date from path
        parsed_info = _parse_dataset_info(dataset_path)
        
        # Scan files and calculate statistics
        files, total_size = await _scan_files(dataset_path)
        
        # Parse dataset documentation for real information
        logger.info("解析数据集文档", processing_id=state.processing_id)
        doc_info = parse_dataset_documents(state.dataset_path)
        
        # Generate cache key
        cache_key = _generate_cache_key(state.dataset_path, total_size)
        
        logger.info("Scan completed",
                   processing_id=state.processing_id,
                   num_files=len(files),
                   total_size=total_size,
                   dataset_name=parsed_info.get("name"),
                   doc_fields_found=list(doc_info.keys()) if doc_info else [])
        
        # Combine parsed info with document info (document info takes precedence)
        final_info = {**parsed_info, **doc_info}
        
        return {
            "dataset_name": final_info.get("name") or parsed_info.get("name"),
            "creator": final_info.get("creator") or parsed_info.get("creator"), 
            "creation_date": final_info.get("date") or parsed_info.get("date"),
            "files": files,
            "total_size": total_size,
            "cache_key": cache_key,
            "doc_info": doc_info,  # Store original document info
            "status": ProcessingStatus.PROCESSING,
            "current_step": "scan_and_parse"
        }
        
    except Exception as e:
        error_msg = f"Error in scan_and_parse: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "status": ProcessingStatus.FAILED,
            "error_message": error_msg,
            "current_step": "scan_and_parse",
            "errors": state.errors + [error_msg]
        }


def _parse_dataset_info(dataset_path: Path) -> Dict[str, str]:
    """
    Parse dataset information from directory path.
    
    Expected format: {creator}-{date}-{dataset_name}
    Example: qiaoyu-20250414-CyberBench
    """
    folder_name = dataset_path.name
    
    # Pattern for parsing: creator-YYYYMMDD-dataset_name
    pattern = r'^([^-]+)-(\d{8})-(.+)$'
    match = re.match(pattern, folder_name)
    
    if match:
        creator, date_str, dataset_name = match.groups()
        
        # Format date as YYYY-MM-DD
        try:
            formatted_date = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        except:
            formatted_date = date_str
            
        return {
            "creator": creator,
            "date": formatted_date,
            "name": dataset_name
        }
    else:
        # Fallback: use folder name as dataset name
        return {
            "creator": None,
            "date": None,
            "name": folder_name
        }


async def _scan_files(dataset_path: Path) -> tuple[list[FileStat], int]:
    """
    Recursively scan files in dataset directory.
    
    Returns:
        Tuple of (file_stats_list, total_size_bytes)
    """
    files = []
    total_size = 0
    
    # Patterns to ignore
    ignore_patterns = {
        '.git', '.svn', '__pycache__', '.pytest_cache', 
        'node_modules', '.DS_Store', 'Thumbs.db'
    }
    
    def should_ignore(path: Path) -> bool:
        """Check if path should be ignored."""
        return any(pattern in path.parts for pattern in ignore_patterns)
    
    try:
        for file_path in dataset_path.rglob('*'):
            if file_path.is_file() and not should_ignore(file_path):
                try:
                    stat = file_path.stat()
                    size_bytes = stat.st_size
                    
                    # Calculate relative path from dataset root
                    rel_path = file_path.relative_to(dataset_path)
                    
                    file_stat = FileStat(
                        path=str(rel_path),
                        size_bytes=size_bytes,
                        format=file_path.suffix.lower() or '.unknown'
                    )
                    
                    files.append(file_stat)
                    total_size += size_bytes
                    
                except (OSError, ValueError) as e:
                    logger.warning("Failed to stat file", 
                                 file_path=str(file_path), 
                                 error=str(e))
                    continue
                    
    except Exception as e:
        logger.error("Error scanning directory", 
                    dataset_path=str(dataset_path),
                    error=str(e))
        raise
    
    return files, total_size


def _generate_cache_key(dataset_path: str, total_size: int) -> str:
    """Generate cache key based on path and size."""
    key_data = f"{dataset_path}:{total_size}"
    return hashlib.sha256(key_data.encode()).hexdigest()[:16]


def _format_size(size_bytes: int) -> str:
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f}{unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f}PB"