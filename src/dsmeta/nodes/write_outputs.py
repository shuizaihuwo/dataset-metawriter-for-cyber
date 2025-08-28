"""Write outputs node for saving generated documentation."""

import hashlib
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import structlog
import aiofiles

from ..models import DatasetState, ProcessingStatus
from ..config import Config

logger = structlog.get_logger(__name__)


async def write_outputs(state: DatasetState, config: Optional[Config] = None) -> Dict[str, Any]:
    """
    Write generated artifacts to files.
    
    Args:
        state: Current dataset processing state
        config: Configuration object
        
    Returns:
        Updated state with write results
    """
    if config is None:
        from ..config import load_config
        config = load_config()
    
    try:
        # Handle both dict and DatasetState objects
        processing_id = state.get("processing_id") if isinstance(state, dict) else getattr(state, "processing_id", "unknown")
        dataset_path = state.get("dataset_path") if isinstance(state, dict) else getattr(state, "dataset_path", "unknown")
        
        logger.info("Starting write_outputs",
                   processing_id=processing_id,
                   dataset_path=dataset_path,
                   state_keys=list(state.keys()) if isinstance(state, dict) else "Not a dict",
                   state_type=type(state))
        
        # Check for artifacts
        artifacts = state.get("artifacts") if isinstance(state, dict) else getattr(state, "artifacts", None)
        logger.info("Checking artifacts", artifacts_found=bool(artifacts), artifacts_type=type(artifacts) if artifacts else "None")
        
        if not artifacts:
            error_msg = "No artifacts to write"
            logger.error(error_msg)
            return {
                "status": ProcessingStatus.FAILED,
                "error_message": error_msg,
                "current_step": "write_outputs"
            }
        
        dataset_path_obj = Path(dataset_path)
        
        if not dataset_path_obj.exists():
            error_msg = f"Dataset path does not exist: {dataset_path}"
            logger.error(error_msg)
            return {
                "status": ProcessingStatus.FAILED,
                "error_message": error_msg,
                "current_step": "write_outputs"
            }
        
        written_files = []
        
        # Process each artifact
        for format_name, artifact in artifacts.items():
            filename = artifact["filename"]
            content = artifact["content"]
            
            file_path = dataset_path_obj / filename
            
            # Check if file already exists and if backup is needed
            if file_path.exists():
                if config.output.backup_existing:
                    await _backup_existing_file(file_path)
                
                # Check if content has changed (idempotent write)
                if await _content_unchanged(file_path, content):
                    logger.info(f"File {filename} unchanged, skipping write")
                    written_files.append({
                        "filename": filename,
                        "status": "unchanged",
                        "path": str(file_path)
                    })
                    continue
            
            # Write new content
            try:
                async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
                    await f.write(content)
                
                logger.info(f"Successfully wrote {filename}",
                           file_size=len(content),
                           file_path=str(file_path))
                
                written_files.append({
                    "filename": filename,
                    "status": "written",
                    "path": str(file_path),
                    "size": len(content)
                })
                
            except Exception as e:
                error_msg = f"Failed to write {filename}: {str(e)}"
                logger.error(error_msg)
                
                written_files.append({
                    "filename": filename,
                    "status": "failed",
                    "error": str(e),
                    "path": str(file_path)
                })
        
        # Check if any files were successfully written
        success_count = len([f for f in written_files if f["status"] in ["written", "unchanged"]])
        
        if success_count == 0:
            return {
                "status": ProcessingStatus.FAILED,
                "error_message": "Failed to write any output files",
                "current_step": "write_outputs",
                "written_files": written_files
            }
        
        logger.info("Write outputs completed",
                   processing_id=processing_id,
                   written_files=success_count,
                   total_files=len(written_files))
        
        return {
            "status": ProcessingStatus.SUCCESS,
            "current_step": "write_outputs",
            "written_files": written_files
        }
        
    except Exception as e:
        error_msg = f"Error in write_outputs: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "status": ProcessingStatus.FAILED,
            "error_message": error_msg,
            "current_step": "write_outputs",
            "errors": (state.get("errors", []) if isinstance(state, dict) else getattr(state, "errors", [])) + [error_msg]
        }


async def _backup_existing_file(file_path: Path) -> bool:
    """Create backup of existing file."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{file_path.stem}.{timestamp}.backup{file_path.suffix}"
        backup_path = file_path.parent / backup_name
        
        # Copy file to backup
        shutil.copy2(file_path, backup_path)
        
        logger.info(f"Created backup: {backup_name}")
        return True
        
    except Exception as e:
        logger.warning(f"Failed to create backup for {file_path.name}: {str(e)}")
        return False


async def _content_unchanged(file_path: Path, new_content: str) -> bool:
    """Check if file content is unchanged."""
    try:
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            existing_content = await f.read()
        
        # Compare content hashes for efficiency
        existing_hash = hashlib.sha256(existing_content.encode('utf-8')).hexdigest()
        new_hash = hashlib.sha256(new_content.encode('utf-8')).hexdigest()
        
        return existing_hash == new_hash
        
    except Exception:
        # If we can't read the existing file, assume content changed
        return False


def _preserve_manual_sections(existing_content: str, new_content: str) -> str:
    """Preserve manually edited sections from existing content."""
    # Simple implementation - look for manual comment blocks
    # This could be enhanced with more sophisticated parsing
    
    manual_start_marker = "<!-- MANUAL_EDIT_START -->"
    manual_end_marker = "<!-- MANUAL_EDIT_END -->"
    
    # Extract manual sections from existing content
    manual_sections = []
    
    start_idx = 0
    while True:
        start_pos = existing_content.find(manual_start_marker, start_idx)
        if start_pos == -1:
            break
        
        end_pos = existing_content.find(manual_end_marker, start_pos)
        if end_pos == -1:
            break
        
        # Include the markers in the preserved section
        manual_section = existing_content[start_pos:end_pos + len(manual_end_marker)]
        manual_sections.append(manual_section)
        
        start_idx = end_pos + len(manual_end_marker)
    
    # If no manual sections found, return new content as-is
    if not manual_sections:
        return new_content
    
    # Simple approach: append manual sections at the end
    result = new_content
    if manual_sections:
        result += "\n\n## 手动编辑内容\n\n"
        for section in manual_sections:
            result += section + "\n\n"
    
    return result