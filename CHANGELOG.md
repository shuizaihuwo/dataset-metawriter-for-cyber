# Changelog

All notable changes to the Dataset Metadata Annotation Tool (dsmeta) will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2025-08-27

### 🚀 Major Features Added

#### Automatic Document Parsing and Intelligent Expansion
- **New Document Parser**: Automatically identifies and parses README.md, dataset.md, and other documentation files in dataset directories
- **Context-Aware LLM Integration**: Uses parsed document information as context for LLM analysis rather than direct metadata overwriting
- **Intelligent Tag Expansion**: Automatically expands simple single-tag information into comprehensive multi-tag metadata
  - Example: Single "脆弱性分析" → Multiple ["代码分析", "漏洞挖掘", "脆弱性分析"]
- **Domain Knowledge Application**: Simulates LLM reasoning based on cybersecurity domain expertise

#### Multi-Select Field Support
- **Enhanced Data Model**: `business_direction` and `business_point` fields now support multiple selections
- **Automatic Conversion**: Seamlessly converts single values to multi-value lists
- **Fuzzy Matching**: Intelligent enum value matching and validation

#### CSV Export System  
- **Batch Processing**: Export metadata from multiple datasets to consolidated CSV
- **Statistical Summaries**: Automatic generation of dataset statistics and distributions
- **Comprehensive Fields**: 29-field export covering all metadata aspects
- **CLI Integration**: Simple command-line interface for bulk operations

### 🔧 Technical Improvements

#### Enhanced Workflow Pipeline
- **9-Node LangGraph Architecture**: Complete redesign with robust conditional branching
- **Better Error Handling**: Comprehensive error recovery and fallback mechanisms
- **Pipeline Validation**: End-to-end validation ensuring complete processing
- **Mock Mode Support**: Testing capabilities with simulated LLM responses

#### Data Model Enhancements
- **Artifacts Support**: Added artifacts field to DatasetState for proper pipeline completion
- **Required Field Validation**: Automatic supplementation of missing required fields
- **Enum Value Validation**: Comprehensive validation and correction of enumeration values
- **Quality Control**: Multi-tier quality assessment and issue detection

### 🛠️ Bug Fixes
- **Fixed "No artifacts to write" Error**: Resolved pipeline completion issues by adding missing artifacts field
- **LLM Response Parsing**: Improved parsing and error handling for LLM responses
- **Field Validation**: Fixed Pydantic validation errors for required fields
- **Metadata Generation**: Ensured complete metadata generation with proper fallbacks

### 📚 Documentation Updates
- **Comprehensive README**: Updated with new features, workflow architecture, and usage examples
- **API Documentation**: Enhanced inline documentation and type hints
- **Configuration Guide**: Detailed configuration options and examples
- **Usage Examples**: Practical examples for all major use cases

### 🔄 Workflow Changes
1. **scan_and_parse**: Now includes automatic document discovery and parsing
2. **synthesize_and_populate**: Enhanced with document context integration and intelligent expansion
3. **validate_and_postprocess**: Improved validation with multi-select field support
4. **generate_markdown**: Better error handling and fallback strategies

### ⚡ Performance Improvements
- **Parallel Processing**: Optimized file scanning and content sampling
- **Smart Caching**: Improved caching mechanisms for faster reprocessing
- **Memory Efficiency**: Better memory management for large datasets

### 🎯 Cybersecurity Domain Enhancements
- **Domain-Specific Intelligence**: Enhanced cybersecurity domain knowledge integration
- **Business Logic Expansion**: Intelligent expansion based on cybersecurity business patterns
- **Quality Assessment**: Specialized quality metrics for cybersecurity datasets

---

## [1.1.0] - 2025-04-14

### Added
- Initial LangGraph-based workflow implementation
- Multi-format output support (Markdown, JSON, YAML)
- Basic LLM integration with SiliconFlow
- Template system with Jinja2
- Configuration management system

### Technical Details
- Pydantic data models for type safety
- Structured logging with contextual information
- Modular node-based architecture
- Basic PII detection and masking

---

## [1.0.0] - 2025-03-01

### Added
- Initial release of Dataset Metadata Annotation Tool
- Basic dataset scanning and metadata extraction
- Simple template-based documentation generation
- CLI interface for dataset processing

### Core Features
- Dataset directory scanning
- File format analysis
- Basic metadata field extraction
- Markdown documentation generation