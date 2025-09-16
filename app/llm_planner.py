import pandas as pd
import duckdb
def llm_generate_final_sql(nl_query: str, per_db_results: dict, openai_api_key: str = None, openai_model: str = None, llm_endpoint: str = None) -> dict:
    """
    Given a natural language query and a dict of per-DB results (db_id -> DataFrame),
    use the LLM to generate a SQL statement that joins/aggregates these tables,
    then execute it in DuckDB and return the result as a DataFrame and the SQL used.
    """
    import json
    import pandas as pd
    import duckdb
    import os
    import requests
    import re

    # Use provided or default env vars
    openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY", "").strip()
    openai_model = openai_model or os.getenv("OPENAI_MODEL", "gpt-4o").strip()
    llm_endpoint = llm_endpoint or os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()
    if not openai_api_key:
        raise RuntimeError("OPENAI_API_KEY not set.")

    # Prepare table schemas for the LLM
    table_schemas = {}
    for db_id, df in per_db_results.items():
        cols = [f"{c} {str(df[c].dtype)}" for c in df.columns]
        table_schemas[db_id] = {"columns": cols}

    # Compose LLM prompt
    SYSTEM = """You are a SQL assistant. Given a set of in-memory tables (from different DBs, now as DataFrames in DuckDB), and a natural language request, generate a single SQL statement to answer the request. Only use the provided table names and columns. Use DuckDB/Postgres SQL syntax. Do not invent columns. Assume all tables are loaded in DuckDB with table names as db_id. Return only the SQL string, nothing else."""
    USER = f"""Natural language request:\n{nl_query}\n\nAvailable tables and columns:\n{json.dumps(table_schemas, indent=2)}\n\nWrite a SQL query that answers the request using these tables. Table names are the db_id keys above. Return only the SQL string."""

    headers = {
        "Authorization": f"Bearer {openai_api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": openai_model,
        "response_format": {"type": "text"},
        "messages": [
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": USER}
        ],
        "temperature": 0.1,
    }
    r = requests.post(llm_endpoint, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    sql = data["choices"][0]["message"]["content"].strip()
    # Clean up SQL (remove markdown, etc)
    sql = re.sub(r"^```sql|```$", "", sql, flags=re.I).strip()

    # Load tables into DuckDB
    con = duckdb.connect()
    for db_id, df in per_db_results.items():
        con.register(db_id, df)
    try:
        result_df = con.execute(sql).df()
    except Exception as e:
        raise RuntimeError(f"DuckDB SQL execution failed: {e}\nSQL: {sql}")
    return {"result": result_df, "sql": sql}
# app/llm_planner.py
import os, json, re, requests
from typing import Dict, Any, List

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o").strip()
LLM_ENDPOINT   = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()

SYSTEM = """You are a SQL planning assistant for a multi-DB semantic layer.

Input: a natural language request and a JSON catalog (DBs → tables → columns).
Output: ONE JSON object:
{
  "per_db_sql": [ { "db_id":"...", "sql":"...", "purpose":"..." } ],
  "final_sql": "..."
}

Rules:
- For each per_db_sql item, the SQL must reference ONLY tables present in that DB's catalog (no cross-DB references).
- Push all filters (dates, thresholds, NOT/exclude) into the relevant per_db_sql.
- final_sql is DISPLAY-ONLY to show the logical join across the chosen per-DB results; do NOT cross-execute. It may alias subqueries.
- When combining results logically, infer join keys and filter columns by analyzing the catalog metadata and the user's request. Prefer keys and columns that are common between relevant tables and are most likely to represent entity relationships (such as customer, project, or invoice identifiers), based on the catalog structure and column names.
- Never invent tables or columns. Use only columns present in the catalog.
- Use Postgres syntax; keep it conservative and append LIMIT 10000 to each per-db query.
- For NOT/EXCEPT logic, prefer <> for inequality and anti-joins (LEFT JOIN ... WHERE right.key IS NULL) where appropriate.
"""

USER_TEMPLATE = """Natural language:
{nl}

Catalog (JSON):
{catalog_json}

Return exactly one JSON object with:
{{
  "per_db_sql": [
    {{"db_id": "...", "sql": "...", "purpose": "..." }}
  ],
  "final_sql": "..."
}}
"""

def _slim_catalog(catalog_by_db: Dict[str, Any], max_tables=150, max_cols=80) -> Dict[str, Any]:
    slim: Dict[str, Any] = {}
    for db_id, db in catalog_by_db.items():
        tmap = db.get("tables") or {}
        stables: Dict[str, Any] = {}
        for fq, t in list(tmap.items())[:max_tables]:
            cols = t.get("columns") or []
            skinny = []
            for c in cols[:max_cols]:
                if isinstance(c, dict):
                    skinny.append({"name": c.get("name"), "type": c.get("type")})
                else:
                    skinny.append({"name": str(c)})
            stables[fq] = {"columns": skinny}
        slim[db_id] = {"tables": stables}
    return slim

def _coerce_json(text: str) -> Dict[str, Any]:
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        m = re.search(r"\{.*\}\s*$", text, re.S)
        if m:
            return json.loads(m.group(0))
        raise ValueError("Planner LLM did not return valid JSON.")

def _llm_complete(system: str, user: str) -> str:
    if not OPENAI_API_KEY:
        raise RuntimeError("OPENAI_API_KEY not set.")
    headers = {
        "Authorization": f"Bearer {OPENAI_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": OPENAI_MODEL,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role":"system","content":system},
            {"role":"user","content":user}
        ],
        "temperature": 0.1,
    }
    r = requests.post(LLM_ENDPOINT, headers=headers, json=payload, timeout=60)
    r.raise_for_status()
    data = r.json()
    return data["choices"][0]["message"]["content"]

def _db_scope_check(db_id: str, sql: str, catalog: Dict[str, Any]) -> bool:
    """Ensure all FROM/JOIN refs exist in this DB's catalog."""
    tables = set((catalog[db_id].get("tables") or {}).keys())  # like 'public.invoices'
    def norm(t: str) -> str:
        t = t.replace('"','')
        return t if "." in t else f"public.{t}"
    refs = set(m.group(1).strip() for m in re.finditer(r'\b(?:from|join)\s+([a-zA-Z0-9_\."]+)', sql, re.I))
    refs = {norm(r).lower() for r in refs}
    return refs.issubset({t.lower() for t in tables})

def plan(nl_query: str, catalog_by_db: Dict[str, Any]) -> Dict[str, Any]:
    # 1) build slim catalog for the LLM
    slim = _slim_catalog(catalog_by_db)
    user = USER_TEMPLATE.format(nl=nl_query, catalog_json=json.dumps(slim, ensure_ascii=False))
    raw = _llm_complete(SYSTEM, user)
    data = _coerce_json(raw)

    # 2) validate & scope-check each per-db SQL; if out of scope, ask LLM to rewrite once
    per_db = data.get("per_db_sql", [])
    if not isinstance(per_db, list):
        raise ValueError("per_db_sql must be a list")

    out_list: List[Dict[str, str]] = []
    seen_db_ids = set()
    for item in per_db:
        if not isinstance(item, dict):
            continue
        db_id = item.get("db_id")
        if not db_id or db_id in seen_db_ids:
            continue  # skip if already processed this DB
        sql   = (item.get("sql") or "").strip()
        purp  = item.get("purpose") or "query"
        if not sql:
            continue
        if db_id not in slim:
            continue
        if not _db_scope_check(db_id, sql, slim):
            fix_user = f"""The following SQL wrongly referenced tables not present in DB '{db_id}'.

DB '{db_id}' tables: {list((slim[db_id]['tables'] or {}).keys())}

Rewrite this SQL to use only tables from DB '{db_id}', keep semantics, and keep LIMIT 10000:
SQL:
{sql}

Return JSON:
{{"db_id":"{db_id}","sql":"...","purpose":"{purp}"}}"""
            fixed_raw = _llm_complete(SYSTEM, fix_user)
            try:
                fixed = _coerce_json(fixed_raw)
                sql2 = (fixed.get("sql") or "").strip()
                if sql2 and _db_scope_check(db_id, sql2, slim):
                    out_list.append({"db_id": db_id, "sql": sql2, "purpose": purp})
                    seen_db_ids.add(db_id)
                    continue
            except Exception:
                pass
            continue
        out_list.append({"db_id": db_id, "sql": sql, "purpose": purp})
        seen_db_ids.add(db_id)

    final_sql = data.get("final_sql", "")
    if not isinstance(final_sql, str):
        final_sql = ""

    return {"per_db_sql": out_list, "final_sql": final_sql.strip()}
