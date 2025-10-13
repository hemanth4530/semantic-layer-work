# introspect_to_catalog.py

import json, sys, os
from sqlalchemy import create_engine, text
from app.tag_loader import load_masking_config, load_json_with_encoding

from dotenv import load_dotenv

load_dotenv()

DSNS_FILE = "dsns.json"

def list_tables_cols(engine):
    q = text("""
        select table_schema, table_name, column_name, data_type
        from information_schema.columns
        where table_schema not in ('pg_catalog','information_schema')
        order by table_schema, table_name, ordinal_position
    """)
    out = {}
    with engine.connect() as c:
        for sch, tbl, col, typ in c.execute(q):
            dbtbl = f"{sch}.{tbl}"
            t = out.setdefault(dbtbl, {"schema": sch, "name": tbl, "columns": []})
            t["columns"].append({"name": col, "type": typ})
    return out


def load_dsns():
    # handle BOM
    with open(DSNS_FILE, "r", encoding="utf-8-sig") as f:
        return json.load(f)

def main():
    dsns = load_dsns()
    catalog = {}
    for db_id, dsn in dsns.items():
        try:
            eng = create_engine(dsn, pool_pre_ping=True, future=True)
            tables = list_tables_cols(eng)
            catalog[db_id] = {"db_id": db_id, "tables": tables}
        except Exception as e:
            catalog[db_id] = {"db_id": db_id, "error": str(e), "tables": {}}
    
    # Save catalog to file first
    with open("data/catalog_live.json", "w", encoding="utf-8") as f:
        json.dump(catalog, f, indent=2, ensure_ascii=False)
    print("Basic catalog saved to data/catalog_live.json", file=sys.stderr)
    
    # Enhance with descriptions and generate tag mappings
    enhance_catalog_with_descriptions()
    generate_field_tag_mappings()
    
    # Also write to stdout for backward compatibility
    catalog_json = json.dumps(catalog, ensure_ascii=False, indent=2)
    sys.stdout.write(catalog_json)


def enhance_catalog_with_descriptions():
    """Add AI-generated field descriptions to catalog_live.json"""
    try:
        if not os.path.exists("data/catalog_live.json"):
            print("Catalog file not found, skipping description enhancement", file=sys.stderr)
            return
            
        if not os.getenv("OPENAI_API_KEY"):
            print("Set OPENAI_API_KEY to enable field description generation", file=sys.stderr)
            return
            
        print("Adding field descriptions to catalog...", file=sys.stderr)
        from app.field_descriptor import add_field_descriptions_to_catalog
        add_field_descriptions_to_catalog(
            catalog_file="data/catalog_live.json",
            force_regenerate=True
        )
        print("Field descriptions added to catalog!", file=sys.stderr)
    except Exception as e:
        print(f"Field description enhancement failed: {e}", file=sys.stderr)

def generate_field_tag_mappings():
    """Generate field tag mappings from enhanced catalog"""
    try:
        if not os.path.exists("data/catalog_live.json"):
            print("Catalog file not found, skipping tag mapping generation", file=sys.stderr)
            return
            
        if not os.getenv("OPENAI_API_KEY"):
            print("Set OPENAI_API_KEY to enable field tag mapping generation", file=sys.stderr)
            return
            
        print("Auto-generating field tag mappings...", file=sys.stderr)
        
        from app.auto_tag_generator import auto_generate_field_tag_mappings
        auto_generate_field_tag_mappings(
            catalog_file="data/catalog_live.json",
            output_file="data/field_tag_mappings.json",
            force_regenerate=True
        )
        print("Field tag mappings generated!", file=sys.stderr)
    except Exception as e:
        print(f"Field tag mapping generation failed: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
