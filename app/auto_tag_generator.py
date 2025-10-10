# app/auto_tag_generator.py
import os
import json
import requests
from typing import Dict, Any, List
from dotenv import load_dotenv
from tag_loader import load_masking_config, load_json_with_encoding

# Load environment variables from .env file
load_dotenv()

def get_llm_credentials():
    """Get LLM API credentials from environment"""
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    model = os.getenv("OPENAI_MODEL", "gpt-4o").strip()
    endpoint = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()
    
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set for auto-generation")
    
    return api_key, model, endpoint

def build_classification_prompt(catalog: Dict[str, Any], config: Dict[str, Any]) -> str:
    """Build prompt for LLM to classify database fields into tags"""
    
    tag_definitions = config.get("tag_definitions", {})
    
    # Build tag reference guide
    tag_guide = "AVAILABLE TAGS AND THEIR MEANINGS:\n"
    for tag_name, tag_info in tag_definitions.items():
        description = tag_info.get("description", "")
        sensitivity = tag_info.get("sensitivity_level", 0)
        compliance = tag_info.get("compliance", [])
        
        tag_guide += f"\n- **{tag_name}** (Sensitivity: {sensitivity})\n"
        tag_guide += f"  Description: {description}\n"
        if compliance:
            tag_guide += f"  Compliance: {', '.join(compliance)}\n"
    
    # Build schema information
    schema_info = "\nDATABASE SCHEMA TO CLASSIFY:\n"
    for db_id, db_data in catalog.items():
        schema_info += f"\n## Database: {db_id}\n"
        
        if not isinstance(db_data, dict) or "tables" not in db_data:
            continue
            
        for table_fqn, table_data in db_data["tables"].items():
            # Generate the full table path as DatabaseName.schema.table_name
            schema_name = table_data.get("schema", "public")
            table_name = table_data.get("name", table_fqn.split(".")[-1])
            full_table_path = f"{db_id}.{schema_name}.{table_name}"
            
            schema_info += f"\n### Table: {full_table_path}\n"
            schema_info += f"Business Context: {table_name} table in {db_id} database\n"
            
            for column in table_data.get("columns", []):
                col_name = column.get("name", "")
                col_type = column.get("type", "")
                schema_info += f"- {col_name} ({col_type})\n"
    
    # Build dynamic classification examples from config
    classification_examples = ""
    if tag_definitions:
        classification_examples = "\nCLASSIFICATION GUIDANCE (based on your configuration):\n"
        for tag_name, tag_info in tag_definitions.items():
            examples = tag_info.get("examples", [])
            if examples:
                example_text = ", ".join(examples)
                classification_examples += f"- **{tag_name}**: {example_text}\n"
    
    # Classification instructions
    instructions = """
TASK: Classify each database table and field with appropriate tags from the available tags above.

RULES:
1. Use ONLY the tags defined above - do not invent new tags
2. A field can have multiple tags if appropriate
3. Consider field name, data type, table context, and business meaning
4. Be conservative - only assign tags you're confident about
5. Assign table-level tags based on the overall purpose/sensitivity of the table
6. Use the examples provided in tag definitions as guidance

""" + classification_examples + """

OUTPUT FORMAT: Return ONLY a valid JSON object with this exact structure:
{
  "table_mappings": {
    "DatabaseName.schema.table_name": {
      "table_tags": ["tag1", "tag2"],
      "column_tags": {
        "column_name": ["tag1", "tag2"],
        "another_column": ["tag3"]
      }
    }
  }
}

IMPORTANT: 
- Use the EXACT table names as shown above (DatabaseName.schema.table_name)
- Always include both table_tags and column_tags arrays
- Empty arrays are acceptable if no tags apply
- DO NOT include any markdown formatting, explanations, or text outside the JSON

"""

    return tag_guide + schema_info + instructions

def call_llm_for_classification(prompt: str) -> Dict[str, Any]:
    """Call LLM API to classify database fields"""
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
                "content": "You are a data classification expert. Analyze database schemas and assign appropriate sensitivity tags to fields based on their names, types, and business context. Always return valid JSON."
            },
            {
                "role": "user", 
                "content": prompt
            }
        ],
        "temperature": 0.1,
        "max_tokens": 4000
    }
    
    print("🤖 Calling LLM for field classification...")
    response = requests.post(endpoint, headers=headers, json=payload, timeout=60)
    response.raise_for_status()
    
    data = response.json()
    content = data["choices"][0]["message"]["content"].strip()
    
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        print(f"❌ LLM returned invalid JSON: {content}")
        raise ValueError(f"LLM returned invalid JSON: {e}")

def validate_generated_mappings(mappings: Dict[str, Any], config: Dict[str, Any]) -> List[str]:
    """Validate that generated mappings use only defined tags"""
    errors = []
    defined_tags = set(config.get("tag_definitions", {}).keys())
    
    table_mappings = mappings.get("table_mappings", {}) 
    
    for table_key, table_data in table_mappings.items():
        # Validate table tags
        table_tags = table_data.get("table_tags", [])
        invalid_table_tags = set(table_tags) - defined_tags
        if invalid_table_tags:
            errors.append(f"Table {table_key} has undefined table tags: {invalid_table_tags}")
        
        # Validate column tags
        column_tags = table_data.get("column_tags", {})
        for col_name, col_tags in column_tags.items():
            invalid_col_tags = set(col_tags) - defined_tags
            if invalid_col_tags:
                errors.append(f"Column {table_key}.{col_name} has undefined tags: {invalid_col_tags}")
    
    return errors

def auto_generate_field_tag_mappings(
    catalog_file: str = "data/catalog_live.json",
    config_file: str = "data_masking_config.json",
    output_file: str = "field_tag_mappings.json",
    force_regenerate: bool = False
) -> Dict[str, Any]:
    """
    Auto-generate field tag mappings using AI classification
    
    Args:
        catalog_file: Path to catalog file
        config_file: Path to masking config file  
        output_file: Path to output mappings file
        force_regenerate: If True, regenerate even if output file exists
    
    Returns:
        Generated mappings dictionary
    """
    
    # Check if output already exists
    if not force_regenerate and os.path.exists(output_file):
        print(f"📋 {output_file} already exists. Use force_regenerate=True to overwrite.")
        return load_json_with_encoding(output_file)
    
    print(f"🚀 Starting auto-generation of field tag mappings...")
    print(f"📖 Reading catalog from: {catalog_file}")
    print(f"⚙️  Reading config from: {config_file}")
    
    # Load input files
    try:
        catalog = load_json_with_encoding(catalog_file)
        config = load_masking_config(config_file)
    except Exception as e:
        raise RuntimeError(f"Failed to load input files: {e}")
    
    # Build prompt and call LLM
    prompt = build_classification_prompt(catalog, config)
    
    try:
        generated_mappings = call_llm_for_classification(prompt)
    except Exception as e:
        raise RuntimeError(f"LLM classification failed: {e}")
    
    # Validate generated mappings
    validation_errors = validate_generated_mappings(generated_mappings, config)
    if validation_errors:
        print("⚠️  Validation warnings:")
        for error in validation_errors:
            print(f"   - {error}")
    
    # Save generated mappings
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(generated_mappings, f, indent=2, ensure_ascii=False)
        print(f"✅ Generated field tag mappings saved to: {output_file}")
    except Exception as e:
        raise RuntimeError(f"Failed to save mappings: {e}")
    
    # Show summary
    table_count = len(generated_mappings.get("table_mappings", {}))
    total_columns = sum(
        len(table_data.get("column_tags", {})) 
        for table_data in generated_mappings.get("table_mappings", {}).values()
    )
    
    print(f"📊 Generation Summary:")
    print(f"   - Tables processed: {table_count}")
    print(f"   - Columns classified: {total_columns}")
    print(f"   - Available tags: {len(config.get('tag_definitions', {}))}")
    
    return generated_mappings

def regenerate_mappings_cli():
    """Command line interface for regenerating mappings"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Auto-generate field tag mappings using AI")
    parser.add_argument("--catalog", default="data/catalog_live.json", help="Catalog file path")
    parser.add_argument("--config", default="data_masking_config.json", help="Config file path") 
    parser.add_argument("--output", default="field_tag_mappings.json", help="Output file path")
    parser.add_argument("--force", action="store_true", help="Force regeneration")
    
    args = parser.parse_args()
    
    try:
        mappings = auto_generate_field_tag_mappings(
            catalog_file=args.catalog,
            config_file=args.config,
            output_file=args.output,
            force_regenerate=args.force
        )
        print("🎉 Auto-generation completed successfully!")
        return mappings
    except Exception as e:
        print(f"❌ Auto-generation failed: {e}")
        return None

if __name__ == "__main__":
    regenerate_mappings_cli()