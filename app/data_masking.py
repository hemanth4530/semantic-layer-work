# app/data_masking.py
import pandas as pd
import hashlib
import re
from typing import Dict, List, Any, Optional, Tuple
from tag_loader import load_tag_mappings, load_masking_config, get_field_tags

def anonymize_value(value: Any, method: str = "star_mask") -> Any:
    """Apply star masking to a value based on data type/length"""
    if pd.isna(value) or value is None:
        return value
    
    str_value = str(value).strip()
    
    # For empty strings, return as is
    if not str_value:
        return value
    
    # Star masking varies by data type/length
    if method == "star_mask" or True:  # Always use star masking now
        # Email pattern
        if '@' in str_value and '.' in str_value:
            return '*' * len(str_value)
        
        # Phone number pattern  
        elif re.match(r'^[\d\s\-\(\)\+\.]+$', str_value) and len(str_value) >= 7:
            return '*' * len(str_value)
        
        # Numeric values (financial, IDs, etc.)
        elif str_value.replace('.', '').replace('-', '').replace(',', '').isdigit():
            return '*' * len(str_value)
        
        # Short text (names, codes) - preserve length
        elif len(str_value) <= 10:
            return '*' * len(str_value)
        
        # Long text (descriptions, notes) - use fixed length stars
        elif len(str_value) > 10:
            return '****'
        
        # Default fallback
        else:
            return '*' * min(len(str_value), 10)
    
    # This shouldn't be reached, but keeping as fallback
    return '****'

def check_role_access(role: str, field_tags: List[str], config: Dict[str, Any]) -> Tuple[bool, bool]:
    """
    Check if role has access to field with given tags
    Returns: (can_access, should_mask_with_stars)
    """
    if not field_tags:
        return True, False  # No tags = full access
    
    roles = config.get("roles", {})
    if role not in roles:
        return True, True  # Unknown role = show column but mask data
    
    role_config = roles[role]
    blocked_tags = set(role_config.get("blocked_tags", []))
    anonymize_tags = set(role_config.get("anonymize_tags", []))
    
    # Check if any field tag is sensitive (blocked OR anonymized)
    field_tag_set = set(field_tags)
    
    # If any field tag is in blocked OR anonymize list, mask with stars
    if (field_tag_set & blocked_tags) or (field_tag_set & anonymize_tags):
        return True, True  # Show column but mask data with stars
    
    # Otherwise, full access
    return True, False

def should_mask_field_with_stars(role: str, field_tags: List[str], config: Dict[str, Any]) -> bool:
    """Check if field should be masked for this role"""
    can_access, should_mask = check_role_access(role, field_tags, config)
    return should_mask

def get_anonymization_method(field_tags: List[str], config: Dict[str, Any]) -> str:
    """Get the anonymization method for field based on its tags"""
    anonymization_methods = config.get("anonymization_methods", {})
    
    # Check each tag and return first matching method
    for tag in field_tags:
        if tag in anonymization_methods:
            return anonymization_methods[tag]
    
    # Default method
    return "redact"

def infer_table_name_from_columns(columns: List[str], tag_mappings: Dict[str, Any]) -> Optional[str]:
    """Try to infer table name from column names - useful for final results"""
    table_mappings = tag_mappings.get("table_mappings", {})
    
    column_set = set(columns)
    best_match = None
    max_matches = 0
    
    for table_key, table_data in table_mappings.items():
        table_columns = set(table_data.get("column_tags", {}).keys())
        matches = len(column_set & table_columns)
        
        if matches > max_matches:
            max_matches = matches
            best_match = table_key
    
    return best_match

def mask_dataframe_for_display(
    df: pd.DataFrame, 
    db_id: str, 
    table_name: Optional[str], 
    role: str, 
    tag_mappings: Dict[str, Any], 
    config: Dict[str, Any]
) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """
    Apply role-based star masking to a pandas DataFrame for display
    All columns are kept visible, sensitive data is replaced with stars
    Returns: (masked_dataframe, masking_indicators)
    """
    if role == "admin":
        return df.copy(), {}
    
    if df.empty:
        return df.copy(), {}
    
    # If table_name is not provided, try to infer it
    if not table_name:
        inferred_table = infer_table_name_from_columns(df.columns.tolist(), tag_mappings)
        if inferred_table:
            # Extract table name from "db.schema.table" format
            parts = inferred_table.split('.')
            if len(parts) >= 2:
                table_name = parts[-1]  # Get just the table name
                if not db_id:
                    db_id = parts[0]  # Use inferred db_id if not provided
    
    masked_df = df.copy()
    masking_indicators = {}
    
    # Process each column - keep all columns visible but mask sensitive values
    for column in df.columns:
        field_tags = get_field_tags(db_id or "unknown", table_name or "unknown", column, tag_mappings)
        
        if should_mask_field_with_stars(role, field_tags, config):
            # Replace sensitive values with stars
            masked_df[column] = masked_df[column].apply(lambda x: anonymize_value(x, "star_mask"))
            masking_indicators[column] = "Masked"
    
    return masked_df, masking_indicators

def get_role_permissions_summary(role: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """Get a summary of what the role can access"""
    roles = config.get("roles", {})
    if role not in roles:
        return {"error": f"Role '{role}' not found"}
    
    role_config = roles[role]
    tag_definitions = config.get("tag_definitions", {})
    
    # Get descriptions for blocked and anonymized tags
    blocked_descriptions = []
    for tag in role_config.get("blocked_tags", []):
        desc = tag_definitions.get(tag, {}).get("description", tag)
        blocked_descriptions.append(f"{tag}: {desc}")
    
    anonymize_descriptions = []
    for tag in role_config.get("anonymize_tags", []):
        desc = tag_definitions.get(tag, {}).get("description", tag)
        method = config.get("anonymization_methods", {}).get(tag, "default")
        anonymize_descriptions.append(f"{tag}: {desc} ({method})")
    
    return {
        "role": role,
        "description": role_config.get("description", ""),
        "max_sensitivity_level": role_config.get("max_sensitivity_level", 0),
        "blocked_tags": role_config.get("blocked_tags", []),
        "blocked_descriptions": blocked_descriptions,
        "anonymize_tags": role_config.get("anonymize_tags", []),
        "anonymize_descriptions": anonymize_descriptions,
        "allowed_exceptions": role_config.get("allowed_exceptions", [])
    }


def get_masking_summary(
    original_columns: List[str],
    masked_columns: List[str], 
    indicators: Dict[str, str]
) -> Dict[str, Any]:
    """Get a summary of what masking was applied"""
    masked_count = len([col for col, ind in indicators.items() if "Masked" in ind])
    
    return {
        "original_column_count": len(original_columns),
        "visible_column_count": len(masked_columns),  # All columns should be visible now
        "masked_columns": masked_count,
        "blocked_columns": 0,  # No columns are blocked anymore, just masked
        "anonymized_columns": 0,  # Everything is now star-masked
        "star_masked_columns": masked_count,
        "columns_removed": [],  # No columns are removed anymore
        "masking_details": indicators
    }
