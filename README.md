# Semantic Layer Data Masking System

A comprehensive data masking solution that provides role-based access control and automatic field classification for database queries across multiple PostgreSQL instances.

## 🎯 Overview

This system implements:
- **Role-based data masking**: Different user roles see different levels of data sensitivity
- **Cross-database querying**: Query multiple PostgreSQL databases simultaneously
- **AI-powered field classification**: Automatic sensitivity tagging using OpenAI GPT-4
- **Tag-based architecture**: Flexible classification system for data sensitivity
- **Streamlit web interface**: User-friendly interface with role selection
- **Automatic catalog updates**: Seamless integration with database schema changes

## 🏗️ Architecture

### Core Components

1. **Data Masking Engine** (`app/data_masking.py`)
   - Star-based anonymization (variable length based on data type)
   - Role-based access control
   - Blacklist approach (define what roles cannot see)

2. **Tag Management** (`app/tag_loader.py`)
   - Load and manage field sensitivity mappings
   - Automatic regeneration triggers
   - File timestamp-based update detection

3. **AI Field Classifier** (`app/auto_tag_generator.py`)
   - OpenAI GPT-4 powered field classification
   - Uses configuration examples for consistent tagging
   - Supports batch processing of database schemas

4. **Database Introspection** (`app/introspect_to_catalog.py`)
   - Multi-database schema discovery
   - Automatic catalog generation
   - Integration with field classification

5. **Query Execution** (`app/exec_sql.py`)
   - Cross-database SQL execution
   - DuckDB integration for result aggregation
   - Secure connection management

6. **Web Interface** (`app/ui_streamlit.py`)
   - Role-based UI
   - Real-time masking preview
   - Automatic field tag generation integration

## 📁 Configuration Files

### `data_masking_config.json`
Defines roles, tag definitions, and anonymization methods:

```json
{
  "roles": {
    "admin": {
      "description": "Full access to all data",
      "cannot_access_tags": []
    },
    "analyst": {
      "description": "Business analyst with limited PII access",
      "cannot_access_tags": ["highly_sensitive", "financial"]
    }
  },
  "tag_definitions": {
    "highly_sensitive": {
      "description": "Highly sensitive personal information",
      "examples": ["SSN", "passport", "driver_license", "tax_id"]
    }
  },
  "anonymization_methods": {
    "star_mask": {
      "description": "Replace with asterisks",
      "implementation": "variable_length_stars"
    }
  }
}
```

### `dsns.json`
Database connection configurations:

```json
{
  "Billing_core": {
    "host": "localhost",
    "port": 5432,
    "database": "billing_core",
    "username": "your_username",
    "password": "your_password"
  }
}
```

### `field_tag_mappings.json`
AI-generated field sensitivity mappings:

```json
{
  "metadata": {
    "generated_at": "2024-01-15T10:30:00Z",
    "generated_by": "auto_tag_generator"
  },
  "table_mappings": {
    "billing_core.customers": {
      "column_tags": {
        "email": "personal_info",
        "ssn": "highly_sensitive"
      }
    }
  }
}
```

## 🚀 Quick Start

### 1. Environment Setup

Create a `.env` file in the project root:

```env
OPENAI_API_KEY=your_openai_api_key_here
```

### 2. Install Dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure Databases

Edit `dsns.json` with your database connections.

### 4. Update Database Catalog

```bash
python update_catalog.py
```

This will:
- Generate `data/catalog_live.json` with database schema
- Automatically create `field_tag_mappings.json` using AI classification

### 5. Start the Web Interface

```bash
streamlit run app/ui_streamlit.py
```

### 6. Test the System

```bash
python test_system.py
```

## 🔐 Security Features

### Role-Based Access Control
- **Blacklist approach**: Define what roles cannot access rather than what they can
- **Granular control**: Tag-based sensitivity levels
- **Display-level masking**: Shows column names but masks sensitive values

### Data Anonymization
- **Variable-length stars**: `****` to `**********` based on data type
- **Type-aware masking**: Different patterns for emails, phones, numbers
- **Preserves structure**: Maintains data format while hiding content

## 🤖 AI-Powered Classification

The system uses OpenAI GPT-4 to automatically classify database fields:

### Classification Process
1. **Schema Analysis**: Reads database schema from catalog
2. **Context Building**: Uses table names, column names, and data types
3. **AI Classification**: GPT-4 analyzes and assigns sensitivity tags
4. **Configuration Guided**: Uses `data_masking_config.json` examples for consistency

### Automatic Triggers
- Runs automatically when catalog is updated
- Triggered by file modification timestamps
- No manual intervention required

## 📊 Supported Data Types

- **Text fields**: Variable-length star masking
- **Email addresses**: Format-preserving masking
- **Phone numbers**: Pattern-aware anonymization
- **Numeric data**: Length-based star replacement
- **Dates**: Configurable date masking

## 🔧 Customization

### Adding New Roles
Edit `data_masking_config.json`:

```json
{
  "roles": {
    "new_role": {
      "description": "Role description",
      "cannot_access_tags": ["tag1", "tag2"]
    }
  }
}
```

### Creating Custom Tags
Add to `tag_definitions` in `data_masking_config.json`:

```json
{
  "tag_definitions": {
    "custom_tag": {
      "description": "Custom sensitivity level",
      "examples": ["example1", "example2"]
    }
  }
}
```

### Manual Field Mapping
Edit `field_tag_mappings.json` directly:

```json
{
  "table_mappings": {
    "database.table": {
      "column_tags": {
        "column_name": "sensitivity_tag"
      }
    }
  }
}
```

## 🛠️ Maintenance

### Updating Database Catalog
When database schema changes:
```bash
python update_catalog.py
```

### Regenerating Field Mappings
Force regeneration of AI classifications:
```bash
python -c "from app.auto_tag_generator import auto_generate_field_tag_mappings; auto_generate_field_tag_mappings(force_regenerate=True)"
```

### Testing System Health
```bash
python test_system.py
```

## 📈 Monitoring

### Log Files
- System logs in terminal output
- Streamlit app logs in browser console
- Database connection status in UI

### Health Checks
- Configuration file validation
- Database connectivity tests
- AI service availability checks

## 🔍 Troubleshooting

### Common Issues

1. **Missing field_tag_mappings.json**
   - Ensure OPENAI_API_KEY is set in .env
   - Run `python update_catalog.py`

2. **Database Connection Errors**
   - Verify dsns.json configuration
   - Check database server availability
   - Validate credentials

3. **AI Classification Errors**
   - Check OpenAI API key validity
   - Verify internet connection
   - Review API usage limits

4. **Import Errors**
   - Ensure all dependencies are installed
   - Check Python path configuration
   - Verify app directory structure

### Debug Mode
Add debugging to any script:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contributing

When extending the system:

1. **Follow the tag-based architecture**
2. **Update configuration schemas**
3. **Add appropriate tests**
4. **Document new features**
5. **Maintain backward compatibility**

## 📋 System Requirements

- Python 3.8+
- PostgreSQL databases
- OpenAI API access (optional, for auto-classification)
- Network access to databases
- 500MB+ available disk space

## 🎨 UI Features

- **Role Selection**: Dropdown for different user roles
- **Real-time Masking**: Immediate preview of masked data
- **Multi-Database Support**: Query across multiple databases
- **Export Options**: Download results as CSV/JSON
- **Status Indicators**: Visual feedback for system status

---

*This system provides a comprehensive solution for role-based data masking with AI-powered automation, ensuring sensitive data protection while maintaining usability across different user roles.*