# ui_streamlit.py

import os, json, pathlib, streamlit as st
from typing import Dict, Any, List
from exec_sql import exec_sql
from typing import Dict, Any
from llm_planner import plan as llm_plan, llm_generate_final_sql

def handle_query(nl_query: str, catalog_by_db: Dict[str, Any]) -> Dict[str, Any]:
    """Thin wrapper so ask.py / UI both call the same entrypoint."""
    return llm_plan(nl_query, catalog_by_db)



st.set_page_config(page_title="Semantic Layer (LLM-only)", layout="wide")

def load_json(fp: str) -> Dict[str, Any]:
    p = pathlib.Path(fp)
    data = p.read_bytes()
    for enc in ("utf-8", "utf-8-sig", "utf-16", "utf-16-le", "utf-16-be"):
        try:
            return json.loads(data.decode(enc))
        except Exception:
            pass
    raise ValueError(f"Failed to parse JSON '{fp}'.")


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



st.title("Semantic Layer (LLM-only)")

catalog_path = st.sidebar.text_input("Catalog JSON", "data/catalog_live.json")
query        = st.text_input("Prompt", "top 3 clients by total invoice amount")

if st.button("Run"):
    try:
        catalog = load_json(catalog_path)
        dsns    = load_dsns("dsns.json")
    except Exception as e:
        st.error(f"Load error: {e}")
    else:
        logical = handle_query(query, catalog)
        per     = logical.get("per_db_sql", [])
        final   = logical.get("final_sql", "")

        st.subheader("Per-DB SQL")
        for item in per:
            db_id = item.get("db_id")
            sql   = item.get("sql", "").strip()
            st.markdown(f"**[{db_id}]**")
            st.code(sql, language="sql")

        st.subheader("Results (per DB)")
        exec_results=[]
        for item in per:
            db_id = item.get("db_id")
            sql   = item.get("sql", "").strip()
            dsn   = dsns.get((db_id or "").lower())
            if not dsn:
                st.error(f"DSN not found for '{db_id}'")
                continue
            res = exec_sql(dsn, sql)
            if res["error"]:
                st.error(f"[{db_id}] {res['error']}")
            else:
                st.caption(f"[{db_id}] {len(res['rows'])} rows")
                st.dataframe(res["rows"], use_container_width=True)
                exec_results.append({"db_id":db_id, "columns":res["columns"], "rows":res["rows"]})

        st.subheader("Final Output (LLM-generated SQL over in-memory tables)")
        import pandas as pd
        per_db_dfs = {}
        for item in exec_results:
            db_id = item["db_id"]
            df = pd.DataFrame(item["rows"], columns=item["columns"])
            per_db_dfs[db_id] = df
        try:
            final_result = llm_generate_final_sql(query, per_db_dfs)
            sql_used = final_result["sql"]
            df_result = final_result["result"]
            st.markdown("**LLM Final SQL Statement:**")
            st.code(sql_used, language="sql")
            st.dataframe(df_result.head(200), use_container_width=True)
        except Exception as e:
            st.error(f"[error] LLM final SQL failed: {e}")
