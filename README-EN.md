# Dataset Metadata Annotation Tool (dsmeta) v1.2.0

A powerful AI-driven tool for automatically generating structured metadata documentation for datasets, with intelligent document parsing and multi-select field support. Designed specifically for cybersecurity datasets with advanced LangGraph workflows.

## 🚀 Key Features

- 🤖 **AI-Powered Analysis**: Uses LLMs to intelligently analyze dataset contents and context
- 📄 **Automatic Document Parsing**: Identifies and parses existing README/documentation files as context
- 🎯 **Intelligent Expansion**: Expands simple document information into comprehensive multi-tag metadata
- 🏷️ **Multi-Select Fields**: Supports multiple business directions and business points per dataset
- 📊 **Structured Metadata**: Generates comprehensive metadata following cybersecurity domain standards
- 🔄 **LangGraph Workflow**: Robust 9-node processing pipeline with conditional branching
- 📝 **Multi-format Output**: Generates Markdown, JSON, and YAML documentation
- 📈 **CSV Export**: Batch export of all dataset metadata with statistical summaries
- 💾 **Idempotent Operations**: Smart caching and incremental updates
- 🎨 **Customizable Templates**: Jinja2-based templating system

## Installation

```bash
# Clone the repository
git clone <repository-url>
cd dsmeta

# Install dependencies
pip install -e .

# Or install requirements directly
pip install -r requirements.txt
```

## Configuration

### Environment Variables

Set up your API keys:

```bash
export SILICONFLOW_API_KEY="your_siliconflow_api_key"
export TAVILY_API_KEY="your_tavily_api_key"  # Optional, for web search
```

### Create Configuration File

```bash
# Initialize default configuration
dsmeta config-init

# Or create custom config.yaml
cp config.yaml my-config.yaml
# Edit my-config.yaml as needed
```

## Usage

### Process Single Dataset

```bash
# Basic usage - automatically parses internal documents
python -m dsmeta.cli run /path/to/dataset

# With custom template
python -m dsmeta.cli run /path/to/dataset --template custom.md.j2

# Force overwrite existing files
python -m dsmeta.cli run /path/to/dataset --force

# With custom configuration
python -m dsmeta.cli -c my-config.yaml run /path/to/dataset
```

### Export All Dataset Metadata

```bash
# Export all datasets to CSV with statistical summary
python -m dsmeta.cli export-csv /path/to/datasets/root --summary

# Export with custom output filename
python -m dsmeta.cli export-csv /path/to/datasets/root -o my_datasets.csv
```

### Validate Existing Metadata

```bash
python -m dsmeta.cli validate /path/to/dataset
```

### Configuration Management

```bash
# Show current configuration
python -m dsmeta.cli config-show

# Initialize new configuration file
python -m dsmeta.cli config-init
```

### Watch Mode (Continuous Processing)

```bash
# Monitor directory for new datasets
python -m dsmeta.cli watch /path/to/datasets/root
```

## Dataset Naming Convention

The tool expects dataset folders to follow the naming pattern:
```
{creator}-{YYYYMMDD}-{dataset_name}
```

Example: `qiaoyu-20250414-CyberBench`

This allows automatic parsing of:
- Creator/submitter name
- Creation date
- Dataset name

## Output Files

The tool generates the following files in the dataset directory:

- **`meta.md`**: Human-readable Markdown documentation
- **`meta.json`**: Structured JSON metadata
- **`meta.yaml`**: YAML format metadata (if configured)

## Metadata Fields

The tool generates comprehensive metadata including:

### Basic Information
- Dataset ID, name, description
- Creator, creation date, version
- Size, file count, estimated record count
- Detected languages

### Technical Attributes  
- Data modality (text, code, images, etc.)
- File format statistics
- Use case classification
- Task types and schemas

### Business Attributes (Cybersecurity Focus)
- Professional domain
- Business direction and points
- Professional rating

### Quality & Security
- PII risk assessment
- Quality notes
- Data checksums

## 🔄 Workflow Architecture

The tool uses a sophisticated LangGraph-based workflow with 9 nodes:

1. **`scan_and_parse`**: File system analysis, path parsing, and automatic document discovery
2. **`read_and_sample`**: Intelligent content sampling with PII masking
3. **`preliminary_analysis`**: Initial LLM-based analysis and classification
4. **`decide_need_search`**: Intelligent decision on whether web search is needed
5. **`web_search`**: Optional web search for additional dataset information (conditional)
6. **`synthesize_and_populate`**: Advanced synthesis using document context for intelligent expansion
7. **`validate_and_postprocess`**: Comprehensive validation and quality control
8. **`generate_markdown`**: Template-based documentation generation
9. **`write_outputs`**: Idempotent file writing with backups

### 🧠 Intelligent Document Processing

The tool automatically:
- Identifies README.md, dataset.md, and similar documentation files
- Parses real information from these documents
- Uses parsed information as **context** for LLM analysis (not direct overwrite)
- Performs intelligent expansion: single tags → comprehensive multi-tag metadata
- Example: "脆弱性分析" → ["代码分析", "漏洞挖掘", "脆弱性分析"]

## Templates

Customize output format using Jinja2 templates:

```bash
# Templates are stored in templates/ directory
ls templates/
# default.md.j2

# Create custom template
cp templates/default.md.j2 templates/custom.md.j2
# Edit custom.md.j2

# Use custom template
dsmeta run /path/to/dataset --template custom.md.j2
```

## Development

### Project Structure

```
dsmeta/
├── src/dsmeta/           # Main package
│   ├── models.py         # Pydantic data models
│   ├── config.py         # Configuration management
│   ├── graph.py          # LangGraph workflow
│   ├── cli.py           # Command line interface
│   └── nodes/           # Workflow nodes
├── templates/           # Jinja2 templates
├── config.yaml         # Default configuration
└── requirements.txt    # Dependencies
```

### Running Tests

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Run with coverage
pytest --cov=dsmeta
```

### Code Quality

```bash
# Format code
black src/

# Lint code  
ruff check src/

# Type checking
mypy src/
```

## Configuration Options

Key configuration sections in `config.yaml`:

```yaml
# LLM Configuration
llm:
  model: "THUDM/GLM-4-9B-0414"
  base_url: "https://api.siliconflow.cn/v1"
  api_key: "${SILICONFLOW_API_KEY}"
  temperature: 0.1

# File Processing
file_processing:
  max_file_size: "50MB"
  sample_head_lines: 1000
  sample_tail_lines: 100

# Quality Control
quality_control:
  min_confidence_score: 0.7
  required_fields: ["name", "description", "modality"]

# Output Options
output:
  template_dir: "./templates"
  default_template: "default.md.j2"
  output_formats: ["markdown", "json"]
  backup_existing: true
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Run code quality checks
6. Submit a pull request

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- Check the documentation
- Search existing issues
- Create a new issue with detailed information
