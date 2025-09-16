# introspect_to_catalog.py

import json, sys, os
from sqlalchemy import create_engine, text
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
    # write to stdout; caller can redirect
    sys.stdout.write(json.dumps(catalog, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
