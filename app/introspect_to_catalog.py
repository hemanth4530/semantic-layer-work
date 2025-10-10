# introspect_to_catalog.py

import json, sys, os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

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
    
    # write to stdout; caller can redirect
    catalog_json = json.dumps(catalog, ensure_ascii=False, indent=2)
    sys.stdout.write(catalog_json)
    
    # Auto-generate field tag mappings after catalog creation
    # Only if output is being redirected to data/catalog_live.json
    if len(sys.argv) > 1 and "catalog_live.json" in " ".join(sys.argv):
        try_auto_generate_field_tags()

def try_auto_generate_field_tags():
    """Attempt to auto-generate field tag mappings after catalog update"""
    try:
        import os
        # Check if we should auto-generate (catalog exists and has API key)
        if os.path.exists("data/catalog_live.json") and os.getenv("OPENAI_API_KEY"):
            print("🤖 Auto-generating field tag mappings...", file=sys.stderr)
            
            from auto_tag_generator import auto_generate_field_tag_mappings
            auto_generate_field_tag_mappings(
                catalog_file="data/catalog_live.json",
                force_regenerate=True
            )
            print("✅ Field tag mappings updated!", file=sys.stderr)
        else:
            if not os.getenv("OPENAI_API_KEY"):
                print("💡 Set OPENAI_API_KEY to enable auto field tag generation", file=sys.stderr)
    except Exception as e:
        print(f"⚠️  Auto field tag generation failed: {e}", file=sys.stderr)

if __name__ == "__main__":
    main()
