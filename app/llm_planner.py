def llm_generate_final_sql(nl_query: str, per_db_metadata: dict, openai_api_key: str = None, openai_model: str = None, llm_endpoint: str = None) -> dict:
    """
    Given a natural language query and per-DB metadata (db_id -> {"columns": ["col type", ...]}),
    use the LLM to generate a single SQL statement that answers the request. This function
    will NOT receive or send any row-level data (DataFrames) to the LLM — only metadata.

    Returns: {"sql": "..."}
    """
    import json
    import os
    import requests
    import re

    # Use provided or default env vars
    openai_api_key = openai_api_key or os.getenv("OPENAI_API_KEY", "").strip()
    openai_model = openai_model or os.getenv("OPENAI_MODEL", "gpt-4o").strip()
    llm_endpoint = llm_endpoint or os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()
    if not openai_api_key:
        raise RuntimeError("OPENAI_API_KEY not set.")

    # Build table schemas for the LLM from the metadata only
    table_schemas = {}
    for db_id, meta in (per_db_metadata or {}).items():
        if isinstance(meta, dict) and "columns" in meta:
            cols = meta["columns"]
        else:
            cols = []
        table_schemas[db_id] = {"columns": cols}

    # Compose LLM prompt (metadata-only, type-aware)
    SYSTEM = """You are a SQL assistant that MUST be type-aware. You will receive only table metadata (no row-level data).

Before generating SQL, perform the following steps:
1) Scan the provided metadata for every referenced table and column. Use the declared column types (name and type) to decide how to reference or convert columns.
2) NEVER emit blind casts that assume textual values can be cast to numeric types. Instead use guarded patterns:
   - Prefer TRY_CAST(col AS BIGINT) if available, otherwise
   - Use CASE/regex guard: CASE WHEN col ~ '^\\d+$' THEN col::BIGINT ELSE NULL END
   - Use NULLIF(col, '') to protect against empty strings before casting.
3) If a join requires equating a textual key to a numeric key, prefer one of:
   - Prefer textual equality if both sides are textual.
   - Use a guarded cast on the textual side with regex guard.
   - Use LEFT JOIN and preserve rows even when cast fails.
4) When you do include any cast, include a short inline SQL comment explaining why and how it is safe (e.g. -- SAFE CAST: regex-guarded numeric conversion).

Output rules:
- Use only the table names and column names given in metadata. Do not invent columns.
- Use DuckDB/Postgres-compatible SQL constructs. Avoid vendor-specific functions beyond standard Postgres/DuckDB.
- Return only the SQL string as the assistant response (no surrounding markdown or explanation). If you must explain a casting decision, include a single inline SQL comment near the cast.
- If a required mapping is ambiguous or unsafe, return a short SQL snippet that raises no errors (e.g., SELECT NULL WHERE FALSE) and include a one-line JSON-style error in a comment, or prefer to return an explicit JSON-error object instead of SQL.

-- Column inclusion guarantee:
- ALWAYS ensure the final SELECT explicitly includes every column the user requested in the natural-language request. If the requested column name is a natural-language phrase, map it to the exact column from the provided metadata and include it. If the column may not exist or is optional, project an explicit typed NULL (for example, CAST(NULL AS numeric) AS amount) so the output schema still contains the requested column name. Do not silently omit requested columns.
"""

    table_schemas_json = json.dumps(table_schemas, indent=2)

    USER = f"""
Natural language request:
{nl_query}

Available tables and columns (metadata only):
{table_schemas_json}

Instructions to the model:
- Consider every column's declared type.
- Prefer guarded casts (TRY_CAST or regex-guarded CASE) where conversions may be needed.
- Preserve NULLs; do not fabricate rows.
- Append LIMIT 10000 where appropriate for per-db extraction queries.

Column-inclusion requirement:
- The generated SQL MUST explicitly SELECT all columns that the user requested in the natural-language request. Do not omit any requested column even if it may be NULL for some rows.
- If the user referenced a column by a natural-language phrase, map it to the exact column name from the provided metadata and include that column in the SELECT list.
- If you must compute or rename a column to satisfy the user's requested label, alias the expression exactly to the requested label, e.g. "some_expr AS \"some_new_col_name\"" so the output column name matches the user's expectation.
- If one or more requested columns are not present in the provided metadata/catalog, do NOT invent columns. Instead return a short JSON-like error object (as plain text) listing the missing column names and an explanation of why they are missing.

Intersection semantics (default — common rows):
- By default, when combining per-source results, RETURN ONLY rows that appear in every provided per-db table (the intersection of results). Treat intersection as the default merge behavior unless the user explicitly asks for "all rows", "union", "concat", "union all", or equivalent phrasing that requests rows from any source.
- Prefer expressing the intersection via INNER JOINs on natural join keys (columns with identical names/meaning across sources) or, when schemas are identical, via INTERSECT of the per-source SELECTs. Do not use UNION/UNION ALL/LEFT JOIN if intersection is intended.

Return a single SQL statement that answers the request using only the provided tables and columns. Return ONLY the SQL string; if you include any casts add a short inline comment explaining the cast. If you cannot safely produce SQL, return an explicit JSON-like error object (as plain text) explaining the type mismatch or missing columns.
"""

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
    # Keep the response text for debugging but do not include any row-level data in the request.
    print("LLM response status:", r.status_code)
    r.raise_for_status()
    data = r.json()
    sql = data["choices"][0]["message"]["content"].strip()
    # Clean up SQL (remove markdown, etc)
    sql = re.sub(r"^```sql|```$", "", sql, flags=re.I).strip()

    return {"sql": sql}
# app/llm_planner.py
import os, json, re, requests
from typing import Dict, Any, List

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o").strip()
LLM_ENDPOINT   = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1/chat/completions").strip()

SYSTEM = """
You are a SQL planning assistant for a multi-DB semantic layer.

Input:
- A natural language request.
- A JSON catalog: { db_id -> { tables -> { columns } } }.

Output: ONE JSON object:
{
  "per_db_sql": [ { "db_id":"...", "sql":"...", "purpose":"..." } ],
  "final_sql": "..."   # display-only, gives LLM context, never executed
}

GLOBAL CONSTRAINTS
- Use only tables and columns that exist in the provided catalog. Never invent names.
- Each per_db_sql must query inside a single DB only (no cross-DB joins).
- Use conservative Postgres SQL syntax.
- Returning zero rows is allowed; do not fabricate rows in SQL.
- Do not COALESCE NULLs unless explicitly asked; show absence as NULL.
- Never filter out rows with NULL values unless the user explicitly asks.

PER-DB SQL (SOURCE QUERIES)
- Push all filters (dates, thresholds, equality/inequality, NOT/exclude) into the relevant per_db_sql.
- Separate driving entities from measures (SUM/COUNT/AVG).
- Compute measures inside the per_db_sql and final_sql.
- Project all columns needed later (keys, labels, measures) even if they return NULL.
- Prefer simple SELECTs with explicit column lists and append LIMIT 10000.
- Never cross-reference other DBs in a per_db_sql.

NEGATIVE / ABSENCE LOGIC
- Interpret negative phrases (NOT, NO, DOES NOT EXIST, DO NOT HAVE, WITHOUT, MISSING, INVALID) as:
  • “no related rows” → anti-join:
      LEFT JOIN child ON keys …
      WHERE child.key IS NULL
    or: WHERE NOT EXISTS (SELECT 1 FROM child WHERE …)
  • “field is missing” → column IS NULL
  • “not equal to X” → col <> 'X'
      If “not equal OR missing” is clearly implied → (col IS NULL OR col <> 'X')
  • “not in list” → col NOT IN (…)
      If “not in OR missing” is implied → (col IS NULL OR col NOT IN (…))

SCHEMA COMPLETENESS FOR NEGATIVES
- If the user requests fields from an entity that may be absent, still include those fields in output by projecting them from the RIGHT side of a LEFT JOIN so they show up as NULL when unmatched.
- If a requested field can only be shown as absent, project an explicit NULL of the correct type, e.g. CAST(NULL AS numeric) AS amount_paid or CAST(NULL AS text) AS po_number.
- Never drop NULL rows by accident. Always preserve NULL unless explicitly told otherwise.
- If a filter must apply in final_sql, ensure that column is selected upstream in per_db_sql.
- Never reference columns in final_sql that were not selected upstream.

JOIN KEYS & MERGE LOGIC
- Infer join keys only from catalog metadata and column names common across tables. Never invent keys.
- Always preserve rows from the driving entity when merging (use LEFT JOINs so non-matches remain visible as NULL).

FUZZY / SEMANTIC COLUMN & TABLE MATCHING
- If the user provides a name or phrase, and no exact match exists in the catalog, map it to the most semantically similar column/table name available in the catalog.
- If no highly similar relation can be determined, do not guess. Instead, generate an explicit error message in the output indicating the missing or unmatched field.

QUALITY
- Keep syntax conservative and portable (avoid vendor-specific functions beyond standard Postgres).
- When matching literal text and case-insensitive intent is implied, prefer LOWER(col) = LOWER('value').

If something cannot be satisfied due to missing tables/columns in the catalog, still return a best-effort plan that respects all rules above and avoids invented schema.
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
