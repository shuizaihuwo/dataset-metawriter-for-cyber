"""目录监控模块 - V1.2版本新功能

基于watchdog实现文件系统监控，自动检测新增数据集并触发处理流程。
支持模式匹配、并发处理队列和失败重试机制。
"""

import asyncio
import re
from pathlib import Path
from typing import Dict, List, Optional, Set
from datetime import datetime
import structlog
from asyncio import Queue
from concurrent.futures import ThreadPoolExecutor
import time

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent, DirCreatedEvent

from .config import Config
from .graph import process_dataset
from .models import ProcessingStatus

logger = structlog.get_logger(__name__)


class DatasetHandler(FileSystemEventHandler):
    """数据集文件系统事件处理器"""
    
    def __init__(self, monitor: 'DatasetMonitor'):
        self.monitor = monitor
        self.processed_paths: Set[str] = set()
        self.cooldown_time = 5  # 冷却时间（秒），避免重复处理
        self.last_processed: Dict[str, float] = {}
    
    def on_created(self, event: FileSystemEvent):
        """处理目录创建事件"""
        if not event.is_directory:
            return
            
        path = Path(event.src_path)
        
        # 检查是否匹配监控模式
        if not self._matches_pattern(path):
            return
            
        # 防止重复处理
        path_str = str(path)
        current_time = time.time()
        
        if (path_str in self.last_processed and 
            current_time - self.last_processed[path_str] < self.cooldown_time):
            logger.debug("跳过重复处理", path=path_str, cooldown_remaining=self.cooldown_time - (current_time - self.last_processed[path_str]))
            return
            
        self.last_processed[path_str] = current_time
        
        logger.info("检测到新数据集目录", path=path_str)
        
        # 添加到处理队列
        try:
            self.monitor.task_queue.put_nowait(path_str)
        except asyncio.QueueFull:
            logger.warning("任务队列已满，跳过处理", path=path_str)
    
    def _matches_pattern(self, path: Path) -> bool:
        """检查路径是否匹配监控模式"""
        path_str = str(path)
        
        for pattern in self.monitor.config.monitoring.patterns:
            # 转换glob模式为正则表达式
            regex_pattern = pattern.replace('*', '.*').replace('?', '.')
            if re.search(regex_pattern, path_str):
                return True
        return False


class TaskExecutor:
    """任务执行器 - 处理数据集标注任务"""
    
    def __init__(self, config: Config, max_concurrent: int = 4):
        self.config = config
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_tasks: Dict[str, asyncio.Task] = {}
        self.retry_queue: Queue = Queue(maxsize=1000)
        self.failed_tasks: Dict[str, int] = {}  # 失败次数统计
        self.max_retries = 3
    
    async def process_dataset_task(self, dataset_path: str) -> bool:
        """处理单个数据集任务"""
        async with self.semaphore:
            try:
                logger.info("开始处理数据集", dataset_path=dataset_path)
                
                # 调用核心处理流程
                result = await process_dataset(dataset_path, self.config)
                
                if result.get("status") == ProcessingStatus.SUCCESS:
                    logger.info("数据集处理成功", dataset_path=dataset_path, 
                              duration=result.get("processing_time", 0))
                    return True
                else:
                    logger.error("数据集处理失败", dataset_path=dataset_path,
                               error=result.get("error"))
                    return False
                    
            except Exception as e:
                logger.error("处理数据集时出现异常", dataset_path=dataset_path, 
                           exception=str(e), exc_info=True)
                return False
    
    async def handle_failed_task(self, dataset_path: str):
        """处理失败的任务"""
        retry_count = self.failed_tasks.get(dataset_path, 0)
        
        if retry_count < self.max_retries:
            self.failed_tasks[dataset_path] = retry_count + 1
            # 指数退避延迟
            delay = 2 ** retry_count * 60  # 1分钟、2分钟、4分钟
            
            logger.warning("任务失败，将重试", dataset_path=dataset_path,
                         retry_count=retry_count + 1, delay_seconds=delay)
            
            await asyncio.sleep(delay)
            await self.retry_queue.put(dataset_path)
        else:
            logger.error("任务失败次数过多，放弃重试", dataset_path=dataset_path,
                       max_retries=self.max_retries)


class DatasetMonitor:
    """数据集监控主类"""
    
    def __init__(self, config: Config):
        self.config = config
        self.observer = Observer()
        self.task_queue: Queue = Queue(maxsize=1000)
        self.executor = TaskExecutor(config, max_concurrent=self.config.monitoring.max_concurrent_tasks)
        self.handler = DatasetHandler(self)
        self._running = False
    
    def setup_monitoring(self):
        """设置目录监控"""
        for directory in self.config.monitoring.directories:
            dir_path = Path(directory)
            if not dir_path.exists():
                logger.warning("监控目录不存在", directory=directory)
                continue
                
            logger.info("开始监控目录", directory=directory, 
                       patterns=self.config.monitoring.patterns,
                       recursive=self.config.monitoring.recursive)
            
            self.observer.schedule(
                self.handler,
                str(dir_path),
                recursive=self.config.monitoring.recursive
            )
    
    async def start_processing_loop(self):
        """启动任务处理循环"""
        logger.info("启动任务处理循环", max_concurrent=self.executor.max_concurrent)
        
        async def process_queue():
            """处理任务队列"""
            while self._running:
                try:
                    # 等待新任务
                    dataset_path = await asyncio.wait_for(
                        self.task_queue.get(), 
                        timeout=1.0
                    )
                    
                    # 启动处理任务
                    task = asyncio.create_task(
                        self.executor.process_dataset_task(dataset_path)
                    )
                    
                    self.executor.active_tasks[dataset_path] = task
                    
                    # 处理完成后清理
                    async def cleanup_task(path: str, task: asyncio.Task):
                        try:
                            success = await task
                            if not success:
                                await self.executor.handle_failed_task(path)
                        except Exception as e:
                            logger.error("任务执行异常", dataset_path=path, exception=str(e))
                            await self.executor.handle_failed_task(path)
                        finally:
                            self.executor.active_tasks.pop(path, None)
                    
                    asyncio.create_task(cleanup_task(dataset_path, task))
                    
                except asyncio.TimeoutError:
                    continue  # 队列为空，继续等待
                except Exception as e:
                    logger.error("处理任务队列时出现异常", exception=str(e))
        
        async def process_retry_queue():
            """处理重试队列"""
            while self._running:
                try:
                    dataset_path = await asyncio.wait_for(
                        self.executor.retry_queue.get(),
                        timeout=1.0
                    )
                    await self.task_queue.put(dataset_path)
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error("处理重试队列时出现异常", exception=str(e))
        
        # 启动处理循环
        await asyncio.gather(
            process_queue(),
            process_retry_queue()
        )
    
    async def start(self):
        """启动监控服务"""
        if self._running:
            logger.warning("监控服务已经在运行中")
            return
            
        logger.info("启动数据集目录监控服务", version="1.2.0",
                   directories=self.config.monitoring.directories)
        
        try:
            self.setup_monitoring()
            self.observer.start()
            self._running = True
            
            logger.info("文件监控已启动，开始处理任务队列")
            
            await self.start_processing_loop()
            
        except KeyboardInterrupt:
            logger.info("收到中断信号，正在停止监控服务")
        except Exception as e:
            logger.error("监控服务出现异常", exception=str(e), exc_info=True)
        finally:
            await self.stop()
    
    async def stop(self):
        """停止监控服务"""
        if not self._running:
            return
            
        logger.info("正在停止数据集监控服务")
        self._running = False
        
        # 停止文件监控
        if self.observer.is_alive():
            self.observer.stop()
            self.observer.join()
        
        # 等待活动任务完成
        if self.executor.active_tasks:
            logger.info("等待活动任务完成", active_count=len(self.executor.active_tasks))
            await asyncio.gather(*self.executor.active_tasks.values(), return_exceptions=True)
        
        logger.info("数据集监控服务已停止")
    
    def get_status(self) -> Dict:
        """获取监控状态"""
        return {
            "running": self._running,
            "monitored_directories": len(self.config.monitoring.directories),
            "active_tasks": len(self.executor.active_tasks),
            "failed_tasks": len(self.executor.failed_tasks),
            "queue_size": self.task_queue.qsize(),
            "retry_queue_size": self.executor.retry_queue.qsize()
        }


# 监控服务单例
_monitor_instance: Optional[DatasetMonitor] = None


def get_monitor(config: Config) -> DatasetMonitor:
    """获取监控服务实例"""
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = DatasetMonitor(config)
    return _monitor_instance


async def start_monitoring(config: Config):
    """启动监控服务的便捷函数"""
    monitor = get_monitor(config)
    await monitor.start()


def stop_monitoring():
    """停止监控服务的便捷函数"""
    global _monitor_instance
    if _monitor_instance:
        asyncio.create_task(_monitor_instance.stop())