# ask.py

import argparse, json, os, sys, pathlib
from typing import Dict, Any, List, Tuple
from app.exec_sql import exec_sql
from typing import Dict, Any
from app.llm_planner import plan as llm_plan, llm_generate_final_sql
from dotenv import load_dotenv

load_dotenv()


def handle_query(nl_query: str, catalog_by_db: Dict[str, Any]) -> Dict[str, Any]:
    """Thin wrapper so ask.py / UI both call the same entrypoint."""
    return llm_plan(nl_query, catalog_by_db)

# ---------- Robust JSON loader (BOM-proof) ----------
def load_json(fp: str) -> Dict[str, Any]:
    p = pathlib.Path(fp)
    data = p.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            return json.loads(data.decode(enc))
        except Exception:
            pass
    raise ValueError(f"Failed to parse JSON '{fp}'. Tried utf-8/utf-8-sig/utf-16 variants.")


def load_dsns(fp: str="dsns.json") -> Dict[str, str]:
    p = pathlib.Path(fp)
    data = p.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            j = json.loads(data.decode(enc))
            return {k.lower(): v for k,v in j.items()}
        except Exception:
            pass
    raise ValueError(f"Failed to parse DSNs '{fp}'.")



# ---------- Pretty printing ----------
def print_rows(rows: List[Dict[str, Any]], limit=10):
    for r in rows[:limit]:
        print("  ", r)

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--catalog", default="data/catalog_live.json")
    args = ap.parse_args()

    try:
        catalog = load_json(args.catalog)
        # print(f"[info] loaded catalog from '{catalog}'")
    except Exception as e:
        print(f"[error] catalog load failed: {e}")
        sys.exit(1)

    try:
        dsns = load_dsns("dsns.json")
    except Exception as e:
        print(f"[error] DSNs load failed: {e}")
        sys.exit(1)

    print("Type a phrase (empty to exit). Example: top 3 clients by total invoice amount")
    while True:
        nl = input("> ").strip()
        if not nl:
            break

        # 1) LLM plan
        try:
            logical = handle_query(nl, catalog)
        except Exception as e:
            print(f"[error] planner failed: {e}")
            continue

        per = logical.get("per_db_sql", [])
        final_sql = logical.get("final_sql", "")

        # 2) Execute per-DB SQL
        print("\n=== Per-DB SQL ===")
        exec_results = []
        for item in per:
            db_id = (item.get("db_id") or "").strip()
            sql = (item.get("sql") or "").strip()
            purpose = item.get("purpose") or "query"
            if not db_id or not sql:
                continue
            print(f"[{db_id}] {sql}")
            dsn = dsns.get(db_id.lower())
            if not dsn:
                print(f"[{db_id}] ERROR: DSN not found in dsns.json")
                continue
            res = exec_sql(dsn, sql)
            exec_results.append({"db_id": db_id, "purpose": purpose,
                                 "columns": res["columns"], "rows": res["rows"]})
        # 3) Show first rows per DB
        print("\n=== Results (first rows) ===")
        for item, src in zip(per, exec_results):
            db_id = item["db_id"]
            sql   = (item.get("sql") or "").strip()
            rows  = src["rows"]
            print(f"[{db_id}] {len(rows)} rows | SQL: {sql}")
            print_rows(rows, limit=50)

       
        # 5) Final Rows (LLM-generated SQL over in-memory tables)
        print("\n=== Final Rows (LLM-generated SQL over in-memory tables) ===")
        import pandas as pd
        per_db_dfs = {}
        for item, src in zip(per, exec_results):
            db_id = item["db_id"]
            df = pd.DataFrame(src["rows"], columns=src["columns"])
            per_db_dfs[db_id] = df
        try:
            final_result = llm_generate_final_sql(nl, per_db_dfs)
            sql_used = final_result["sql"]
            df_result = final_result["result"]
            print(f"[LLM Final SQL] {sql_used}")
            print(df_result.head(50).to_string(index=False))
        except Exception as e:
            print(f"[error] LLM final SQL failed: {e}")

        print("\nReady. Ask another question or press Enter to exit.")

if __name__ == "__main__":
    main()
