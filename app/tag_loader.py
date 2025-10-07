# app/tag_loader.py
import json
import pathlib
from typing import Dict, List, Any, Optional

def load_json_with_encoding(file_path: str) -> Dict[str, Any]:
    """Load JSON file with multiple encoding attempts"""
    p = pathlib.Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    data = p.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            return json.loads(data.decode(enc))
        except Exception:
            continue
    raise ValueError(f"Failed to parse JSON '{file_path}'.")

def load_tag_mappings(mappings_file: str = "field_tag_mappings.json") -> Dict[str, Any]:
    """Load field tag mappings from JSON file"""
    try:
        return load_json_with_encoding(mappings_file)
    except FileNotFoundError:
        print(f"Warning: Tag mappings file '{mappings_file}' not found. Using empty mappings.")
        return {"table_mappings": {}}

def load_masking_config(config_file: str = "data_masking_config.json") -> Dict[str, Any]:
    """Load data masking configuration from JSON file"""
    try:
        return load_json_with_encoding(config_file)
    except FileNotFoundError:
        print(f"Warning: Masking config file '{config_file}' not found. Using default config.")
        return {
            "tag_definitions": {},
            "roles": {"admin": {"blocked_tags": [], "anonymize_tags": [], "allowed_exceptions": []}},
            "anonymization_methods": {},
            "field_exceptions": {}
        }

def get_table_key(db_id: str, table_name: str) -> str:
    """Generate consistent table key for lookups"""
    # Handle both 'schema.table' and 'table' formats
    if '.' not in table_name:
        table_name = f"public.{table_name}"
    return f"{db_id}.{table_name}"

def get_field_tags(db_id: str, table_name: str, column_name: str, tag_mappings: Dict[str, Any]) -> List[str]:
    """Get tags for a specific field from tag mappings"""
    table_key = get_table_key(db_id, table_name)
    table_mappings = tag_mappings.get("table_mappings", {})
    
    if table_key not in table_mappings:
        return []
    
    table_data = table_mappings[table_key]
    
    # Get column-specific tags
    column_tags = table_data.get("column_tags", {}).get(column_name, [])
    
    # Get table-level tags (inherited by all columns)
    table_tags = table_data.get("table_tags", [])
    
    # Combine and deduplicate
    all_tags = list(set(column_tags + table_tags))
    return all_tags

def get_table_tags(db_id: str, table_name: str, tag_mappings: Dict[str, Any]) -> List[str]:
    """Get tags for a specific table"""
    table_key = get_table_key(db_id, table_name)
    table_mappings = tag_mappings.get("table_mappings", {})
    
    if table_key not in table_mappings:
        return []
    
    return table_mappings[table_key].get("table_tags", [])

def merge_catalog_with_tags(catalog: Dict[str, Any], tag_mappings: Dict[str, Any]) -> Dict[str, Any]:
    """Add tag information to catalog structure"""
    enhanced_catalog = {}
    
    for db_id, db_data in catalog.items():
        if not isinstance(db_data, dict) or "tables" not in db_data:
            enhanced_catalog[db_id] = db_data
            continue
            
        enhanced_db = dict(db_data)
        enhanced_tables = {}
        
        for table_fqn, table_data in db_data["tables"].items():
            table_name = table_data.get("name", table_fqn)
            enhanced_table = dict(table_data)
            
            # Add table-level tags
            enhanced_table["table_tags"] = get_table_tags(db_id, table_name, tag_mappings)
            
            # Add column-level tags
            enhanced_columns = []
            for column in table_data.get("columns", []):
                enhanced_column = dict(column)
                column_name = column.get("name", "")
                enhanced_column["tags"] = get_field_tags(db_id, table_name, column_name, tag_mappings)
                enhanced_columns.append(enhanced_column)
            
            enhanced_table["columns"] = enhanced_columns
            enhanced_tables[table_fqn] = enhanced_table
        
        enhanced_db["tables"] = enhanced_tables
        enhanced_catalog[db_id] = enhanced_db
    
    return enhanced_catalog

def get_all_tags_for_table(db_id: str, table_name: str, tag_mappings: Dict[str, Any]) -> Dict[str, List[str]]:
    """Get all field tags for a table - useful for debugging"""
    table_key = get_table_key(db_id, table_name)
    table_mappings = tag_mappings.get("table_mappings", {})
    
    if table_key not in table_mappings:
        return {}
    
    table_data = table_mappings[table_key]
    return {
        "table_tags": table_data.get("table_tags", []),
        "column_tags": table_data.get("column_tags", {})
    }

def list_all_tags(tag_mappings: Dict[str, Any]) -> List[str]:
    """Get list of all unique tags used in mappings"""
    all_tags = set()
    
    for table_key, table_data in tag_mappings.get("table_mappings", {}).items():
        # Add table tags
        all_tags.update(table_data.get("table_tags", []))
        
        # Add column tags
        for column_tags in table_data.get("column_tags", {}).values():
            all_tags.update(column_tags)
    
    return sorted(list(all_tags))

def validate_tag_mappings(tag_mappings: Dict[str, Any], config: Dict[str, Any]) -> List[str]:
    """Validate that all tags in mappings are defined in config"""
    errors = []
    
    defined_tags = set(config.get("tag_definitions", {}).keys())
    used_tags = set(list_all_tags(tag_mappings))
    
    undefined_tags = used_tags - defined_tags
    if undefined_tags:
        errors.append(f"Tags used in mappings but not defined in config: {sorted(undefined_tags)}")
    
    return errors