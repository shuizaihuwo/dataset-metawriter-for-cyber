"""Command line interface for dataset annotation tool."""

import asyncio
import os
import sys
from pathlib import Path
from typing import Optional

import click
import structlog
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich import print as rprint

from .config import load_config, Config
from .graph import process_dataset

# Setup console for rich output
console = Console()

# Setup structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger(__name__)


@click.group()
@click.version_option(version="1.2.0")
@click.option("--debug", is_flag=True, help="Enable debug mode")
@click.option("--config", "-c", type=click.Path(exists=True), help="Configuration file path")
@click.pass_context
def main(ctx: click.Context, debug: bool, config: Optional[str]):
    """Dataset Metadata Annotation Tool using LangGraph."""
    
    # Setup logging level
    level = "DEBUG" if debug else "INFO"
    
    # Load configuration
    try:
        ctx.obj = load_config(config)
        if debug:
            ctx.obj.debug = True
            
        # Setup file logging if specified
        if ctx.obj.logging.file_path:
            import logging
            logging.basicConfig(filename=ctx.obj.logging.file_path, level=level)
        
        logger.info("Configuration loaded", config_file=config, debug=debug)
        
    except Exception as e:
        console.print(f"[red]Error loading configuration: {e}[/red]")
        sys.exit(1)


@main.command()
@click.argument("dataset_path", type=click.Path(exists=True, file_okay=False))
@click.option("--template", "-t", default="default.md.j2", help="Template name")
@click.option("--force", "-f", is_flag=True, help="Force overwrite existing files")
@click.pass_context
def run(ctx: click.Context, dataset_path: str, template: str, force: bool):
    """Process a single dataset directory."""
    
    config: Config = ctx.obj
    
    # Resolve absolute path
    dataset_path = str(Path(dataset_path).resolve())
    
    console.print(f"[blue]Processing dataset:[/blue] {dataset_path}")
    
    if force:
        console.print("[yellow]Force mode enabled - will overwrite existing files[/yellow]")
        config.output.backup_existing = False
    
    # Run the processing workflow
    try:
        result = asyncio.run(_run_single_dataset(dataset_path, config, template))
        
        if result["success"]:
            console.print(f"[green]✓ Successfully processed dataset:[/green] {result['dataset_name']}")
            
            # Show written files
            written_files = result.get("written_files", [])
            if written_files:
                table = Table(title="Generated Files")
                table.add_column("Filename", style="cyan")
                table.add_column("Status", style="green")
                table.add_column("Size", style="yellow")
                
                for file_info in written_files:
                    status = file_info["status"]
                    size = file_info.get("size", "-")
                    if isinstance(size, int):
                        size = f"{size:,} chars"
                    
                    table.add_row(
                        file_info["filename"],
                        status.title(),
                        str(size)
                    )
                
                console.print(table)
        else:
            console.print(f"[red]✗ Failed to process dataset:[/red] {result['error_message']}")
            sys.exit(1)
            
    except KeyboardInterrupt:
        console.print("\n[yellow]Processing interrupted by user[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]Error: {e}[/red]")
        if config.debug:
            raise
        sys.exit(1)


@main.command()
@click.argument("dataset_path", type=click.Path(exists=True, file_okay=False))
@click.pass_context  
def validate(ctx: click.Context, dataset_path: str):
    """Validate existing metadata for a dataset."""
    
    config: Config = ctx.obj
    dataset_path = Path(dataset_path)
    
    console.print(f"[blue]Validating metadata for:[/blue] {dataset_path}")
    
    # Check for existing metadata files
    meta_files = {
        "meta.md": dataset_path / "meta.md",
        "meta.json": dataset_path / "meta.json",
        "meta.yaml": dataset_path / "meta.yaml"
    }
    
    table = Table(title="Metadata Files")
    table.add_column("File", style="cyan")
    table.add_column("Status", style="green")
    table.add_column("Size", style="yellow")
    
    for name, path in meta_files.items():
        if path.exists():
            size = path.stat().st_size
            status = "✓ Exists"
            
            # Basic validation
            try:
                if name.endswith(".json"):
                    import json
                    with open(path, 'r', encoding='utf-8') as f:
                        json.load(f)
                elif name.endswith(".yaml"):
                    import yaml
                    with open(path, 'r', encoding='utf-8') as f:
                        yaml.safe_load(f)
                
                status += " (Valid)"
            except Exception as e:
                status = f"✗ Invalid ({e})"
                
        else:
            status = "✗ Missing"
            size = 0
        
        table.add_row(name, status, f"{size:,} bytes" if size > 0 else "-")
    
    console.print(table)


@main.command()
@click.pass_context
def config_show(ctx: click.Context):
    """Show current configuration."""
    
    config: Config = ctx.obj
    
    console.print("[blue]Current Configuration:[/blue]")
    
    # Create configuration table
    table = Table()
    table.add_column("Setting", style="cyan")
    table.add_column("Value", style="green")
    
    table.add_row("App Name", config.app_name)
    table.add_row("Version", config.version)
    table.add_row("Debug Mode", str(config.debug))
    table.add_row("LLM Model", config.llm.model)
    table.add_row("LLM Provider", config.llm.provider)
    table.add_row("Template Dir", config.output.template_dir)
    table.add_row("Output Formats", ", ".join(config.output.output_formats))
    table.add_row("Min Confidence", str(config.quality_control.min_confidence_score))
    
    console.print(table)


@main.command()
@click.option("--output", "-o", default="config.yaml", help="Output configuration file")
@click.pass_context
def config_init(ctx: click.Context, output: str):
    """Initialize a new configuration file."""
    
    output_path = Path(output)
    
    if output_path.exists():
        if not click.confirm(f"Configuration file {output} already exists. Overwrite?"):
            return
    
    try:
        # Create default config
        config = Config.create_default()
        config.save_to_file(output)
        
        console.print(f"[green]✓ Configuration file created:[/green] {output}")
        console.print("[yellow]Don't forget to set your API keys in environment variables:[/yellow]")
        console.print("  export SILICONFLOW_API_KEY=your_key_here")
        console.print("  export TAVILY_API_KEY=your_key_here (optional)")
        
    except Exception as e:
        console.print(f"[red]Error creating configuration: {e}[/red]")


@main.command()
@click.option("--directories", "-d", multiple=True, help="监控目录（可多个）")
@click.option("--patterns", "-p", multiple=True, default=["**/qiaoyu-*"], help="匹配模式（可多个）")
@click.option("--max-concurrent", "-c", type=int, default=4, help="最大并发任务数")
@click.pass_context
def watch(ctx: click.Context, directories, patterns, max_concurrent):
    """监控指定目录，自动处理新增的数据集."""
    
    config: Config = ctx.obj
    
    # 更新监控配置
    if directories:
        config.monitoring.directories = list(directories)
    if patterns:
        config.monitoring.patterns = list(patterns)
    config.monitoring.max_concurrent_tasks = max_concurrent
    
    if not config.monitoring.directories:
        console.print("[red]错误: 需要指定至少一个监控目录[/red]")
        console.print("使用 -d 参数指定目录，例如:")
        console.print("  dsmeta watch -d /path/to/datasets")
        sys.exit(1)
    
    console.print(f"[green]🔍 开始监控数据集目录[/green]")
    console.print(f"监控目录: {', '.join(config.monitoring.directories)}")
    console.print(f"匹配模式: {', '.join(config.monitoring.patterns)}")
    console.print(f"最大并发: {max_concurrent} 个任务")
    console.print(f"按 [yellow]Ctrl+C[/yellow] 停止监控")
    
    try:
        from .monitor import start_monitoring
        asyncio.run(start_monitoring(config))
    except ImportError as e:
        console.print(f"[red]错误: 监控功能需要安装 watchdog: pip install watchdog[/red]")
        sys.exit(1)
    except KeyboardInterrupt:
        console.print("\n[yellow]监控已停止[/yellow]")
    except Exception as e:
        console.print(f"[red]监控出错: {e}[/red]")
        if config.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


@main.command()
@click.argument("root_directory", type=click.Path(exists=True, file_okay=False))
@click.option("--output", "-o", default="datasets_summary.csv", help="输出CSV文件名")
@click.option("--summary", "-s", is_flag=True, help="生成统计摘要")
@click.pass_context
def export_csv(ctx: click.Context, root_directory: str, output: str, summary: bool):
    """导出所有数据集的元数据到CSV文件."""
    
    config: Config = ctx.obj
    
    console.print(f"[green]📊 开始导出数据集信息[/green]")
    console.print(f"扫描目录: {root_directory}")
    console.print(f"输出文件: {output}")
    
    try:
        from .export import export_datasets_csv
        
        with console.status("正在扫描和导出数据集信息..."):
            result = export_datasets_csv(
                root_directory, 
                output, 
                include_summary=summary
            )
        
        if result["success"]:
            console.print(f"[green]✓ CSV导出完成![/green]")
            
            # 显示导出结果表格
            table = Table(title="导出结果")
            table.add_column("项目", style="cyan")
            table.add_column("数值", style="green")
            
            table.add_row("输出文件", result["output_file"])
            table.add_row("扫描文件数", str(result["total_files"]))
            table.add_row("成功导出", str(result["exported_rows"]))
            table.add_row("失败文件", str(len(result["failed_files"])))
            
            console.print(table)
            
            if result["failed_files"]:
                console.print("[yellow]失败文件:[/yellow]")
                for failed in result["failed_files"]:
                    console.print(f"  - {failed}")
            
            # 显示统计摘要
            if summary and "summary" in result:
                summary_data = result["summary"]
                if "error" not in summary_data:
                    console.print("\n[blue]📈 数据集统计摘要:[/blue]")
                    
                    stats_table = Table()
                    stats_table.add_column("统计项", style="cyan")
                    stats_table.add_column("数值", style="yellow")
                    
                    stats_table.add_row("总数据集数", str(summary_data["total_datasets"]))
                    stats_table.add_row("平均文件数", f"{summary_data['file_count_stats']['mean']:.1f}")
                    stats_table.add_row("最大文件数", str(summary_data['file_count_stats']['max']))
                    
                    console.print(stats_table)
                    
                    # 显示分布情况
                    if summary_data["modality_distribution"]:
                        console.print("\n[blue]数据模态分布:[/blue]")
                        for modality, count in summary_data["modality_distribution"].items():
                            console.print(f"  • {modality}: {count}")
        else:
            console.print(f"[red]✗ 导出失败:[/red] {result['error']}")
            sys.exit(1)
            
    except ImportError as e:
        console.print(f"[red]错误: 缺少必要依赖[/red]")
        console.print("请运行: pip install pandas")
        sys.exit(1)
    except Exception as e:
        console.print(f"[red]导出出错: {e}[/red]")
        if config.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


async def _run_single_dataset(dataset_path: str, config: Config, template: str) -> dict:
    """Run processing for a single dataset with progress indication."""
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        
        # Add progress task
        task = progress.add_task("Processing dataset...", total=None)
        
        try:
            # Update progress through different stages
            progress.update(task, description="Scanning files...")
            
            result = await process_dataset(dataset_path, config)
            
            progress.update(task, description="Complete!", completed=True)
            
            return result
            
        except Exception as e:
            progress.update(task, description=f"Failed: {str(e)}")
            raise


if __name__ == "__main__":
    main()