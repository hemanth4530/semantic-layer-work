#!/usr/bin/env python3
"""
Field descriptor - AI-powered field description generation for database catalogs
"""
import json
import os
import requests
from typing import Dict, Any, List
from pathlib import Path

def load_json_with_encoding(file_path: str) -> Dict[str, Any]:
    """Load JSON file with multiple encoding attempts"""
    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    data = p.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            return json.loads(data.decode(enc))
        except Exception:
            continue
    raise ValueError(f"Failed to parse JSON '{file_path}'.")

def get_llm_credentials():
    """Get LLM API credentials from environment"""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-4o").strip()
    endpoint = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()
    
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set for field description generation")
    
    return api_key, model, endpoint

def build_description_prompt(catalog: Dict[str, Any]) -> str:
    """Build prompt for AI to generate field descriptions"""
    prompt = """You are a database field description expert. Analyze the following database schema and generate concise, professional descriptions for each field.

For each field, provide a brief description that explains:
- What the field represents
- Its business purpose or meaning
- Any relevant context from field names and table relationships

Keep descriptions concise (1-2 sentences max) and professional.

Database Schema:
"""
    
    for db_id, db_data in catalog.items():
        if not isinstance(db_data, dict) or "tables" not in db_data:
            continue
            
        prompt += f"\nDatabase: {db_id}\n"
        
        for table_fqn, table_data in db_data["tables"].items():
            table_name = table_data.get("name", table_fqn.split(".")[-1])
            schema = table_data.get("schema", "public")
            
            prompt += f"  Table: {schema}.{table_name}\n"
            prompt += f"  Business Context: {table_name} table in {db_id} database\n"
            
            for column in table_data.get("columns", []):
                col_name = column.get("name", "")
                col_type = column.get("type", "")
                prompt += f"    - {col_name} ({col_type})\n"
    
    prompt += """
Output format (JSON):
{
  "field_descriptions": {
    "database.schema.table.column": "Description text",
    ...
  }
}

Guidelines:
- Use format: "database_id.schema.table_name.column_name": "description"
- Keep descriptions professional and concise
- Focus on business meaning, not technical implementation
- For ID fields, mention what they identify
- For foreign keys, mention what they reference
- For status/code fields, mention they represent categories or states
- For amount/quantity fields, mention the business context

Only return the JSON, no other text."""

    return prompt

def call_llm_for_descriptions(prompt: str) -> Dict[str, Any]:
    """Call LLM API to generate field descriptions"""
    api_key, model, endpoint = get_llm_credentials()
    
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    
    payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {
                "role": "system", 
                "content": "You are a database documentation expert. Generate concise, professional field descriptions that explain the business purpose and meaning of database columns. Always return valid JSON."
            },
            {
                "role": "user", 
                "content": prompt
            }
        ],
        "temperature": 0.1,
        "max_tokens": 4000
    }
    
    print("Calling LLM for field descriptions...")
    response = requests.post(endpoint, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    content = data["choices"][0]["message"]["content"].strip()
    
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"❌ LLM returned invalid JSON: {content}")
        raise ValueError(f"LLM returned invalid JSON: {e}")

def add_field_descriptions_to_catalog(
    catalog_file: str = "data/catalog_live.json",
    output_file: str = None,
    force_regenerate: bool = False
) -> Dict[str, Any]:
    """
    Add AI-generated field descriptions to catalog
    
    Args:
        catalog_file: Path to input catalog file
        output_file: Path to output catalog file (defaults to same as input)
        force_regenerate: If True, regenerate descriptions even if they exist
    
    Returns:
        Enhanced catalog with descriptions
    """
    if output_file is None:
        output_file = catalog_file
    
    print(f"Adding field descriptions to catalog...")
    print(f"Reading catalog from: {catalog_file}")
    
    # Load catalog
    try:
        catalog = load_json_with_encoding(catalog_file)
    except Exception as e:
        raise RuntimeError(f"Failed to load catalog: {e}")
    
    # Check if descriptions already exist and we're not forcing regeneration
    if not force_regenerate:
        has_descriptions = any(
            column.get("description") 
            for db_data in catalog.values() 
            if isinstance(db_data, dict) and "tables" in db_data
            for table_data in db_data["tables"].values()
            for column in table_data.get("columns", [])
        )
        
        if has_descriptions:
            print("📝 Catalog already has field descriptions. Use force_regenerate=True to overwrite.")
            return catalog
    
    # Generate descriptions
    prompt = build_description_prompt(catalog)
    
    try:
        result = call_llm_for_descriptions(prompt)
        field_descriptions = result.get("field_descriptions", {})
    except Exception as e:
        raise RuntimeError(f"Field description generation failed: {e}")
    
    # Add descriptions to catalog
    enhanced_catalog = add_descriptions_to_catalog_structure(catalog, field_descriptions)
    
    # Save enhanced catalog
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(enhanced_catalog, f, indent=2, ensure_ascii=False)
        print(f"Enhanced catalog with descriptions saved to: {output_file}")
    except Exception as e:
        raise RuntimeError(f"Failed to save enhanced catalog: {e}")
    
    # Show summary
    desc_count = len(field_descriptions)
    total_fields = sum(
        len(table_data.get("columns", []))
        for db_data in catalog.values()
        if isinstance(db_data, dict) and "tables" in db_data
        for table_data in db_data["tables"].values()
    )
    
    print(f"Description Summary:")
    print(f"   - Descriptions generated: {desc_count}")
    print(f"   - Total fields: {total_fields}")
    print(f"   - Coverage: {(desc_count/total_fields)*100:.1f}%" if total_fields > 0 else "   - Coverage: 0%")
    
    return enhanced_catalog

def add_descriptions_to_catalog_structure(catalog: Dict[str, Any], field_descriptions: Dict[str, str]) -> Dict[str, Any]:
    """Add field descriptions to catalog structure"""
    enhanced_catalog = {}
    
    for db_id, db_data in catalog.items():
        if not isinstance(db_data, dict):
            enhanced_catalog[db_id] = db_data
            continue
            
        enhanced_db = dict(db_data)
        
        if "tables" not in db_data:
            enhanced_catalog[db_id] = enhanced_db
            continue
            
        enhanced_tables = {}
        
        for table_fqn, table_data in db_data["tables"].items():
            enhanced_table = dict(table_data)
            table_name = table_data.get("name", table_fqn.split(".")[-1])
            schema = table_data.get("schema", "public")
            
            # Add descriptions to columns
            enhanced_columns = []
            for column in table_data.get("columns", []):
                enhanced_column = dict(column)
                col_name = column.get("name", "")
                
                # Look for field description
                field_key = f"{db_id}.{schema}.{table_name}.{col_name}"
                if field_key in field_descriptions:
                    enhanced_column["description"] = field_descriptions[field_key]
                
                enhanced_columns.append(enhanced_column)
            
            enhanced_table["columns"] = enhanced_columns
            enhanced_tables[table_fqn] = enhanced_table
        
        enhanced_db["tables"] = enhanced_tables
        enhanced_catalog[db_id] = enhanced_db
    
    return enhanced_catalog

def main():
    """Command line interface for field description generation"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Add AI-generated field descriptions to catalog")
    parser.add_argument("--catalog", default="data/catalog_live.json", help="Catalog file path")
    parser.add_argument("--output", help="Output file path (defaults to same as input)")
    parser.add_argument("--force", action="store_true", help="Force regeneration of descriptions")
    
    args = parser.parse_args()
    
    try:
        enhanced_catalog = add_field_descriptions_to_catalog(
            catalog_file=args.catalog,
            output_file=args.output,
            force_regenerate=args.force
        )
        print("🎉 Field description generation completed successfully!")
        return enhanced_catalog
    except Exception as e:
        print(f"❌ Field description generation failed: {e}")
        return None

if __name__ == "__main__":
    main()