# ui_streamlit.py

import os, json, pathlib, streamlit as st
from typing import Dict, Any, List
from exec_sql import exec_sql
from typing import Dict, Any
from llm_planner import plan, llm_generate_final_sql
import duckdb, pandas as pd
def handle_query(nl_query: str, catalog_by_db: Dict[str, Any]) -> Dict[str, Any]:
    """Thin wrapper so ask.py / UI both call the same entrypoint."""
    return plan(nl_query, catalog_by_db)



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
                # Always build a DataFrame with explicit columns so headers are preserved even when there are 0 rows
                df = pd.DataFrame(res["rows"], columns=res["columns"])
                st.caption(f"[{db_id}] {len(df)} rows")
                st.dataframe(df, use_container_width=True)
                exec_results.append({"db_id":db_id, "columns":res["columns"], "rows":res["rows"]})

        st.subheader("Final Output (LLM-generated SQL over in-memory tables)")
        per_db_dfs = {}

        # Build and sanitize DataFrames for each DB
        for item in exec_results:
            db_id = item["db_id"]
            df = pd.DataFrame(item["rows"], columns=item["columns"])

            # Sanitize common textual NULL sentinels
            # df = df.replace({"NULL": None, "null": None, "(NULL)": None})

            # Try to coerce object columns that look numeric into numeric dtype
            # for c in df.columns:
            #     if df[c].dtype == object:
            #         coerced = pd.to_numeric(df[c], errors="coerce")
            #         if coerced.notna().sum() > 0:
            #             df[c] = coerced

            per_db_dfs[db_id] = df
        print(f"PER DB DFS: {per_db_dfs}")
        print("END PER DB DFS\n")

        if not per_db_dfs:
            st.info("No per-DB results available to generate a final SQL.")

        # If only one DB produced results, use that DataFrame as the final result
        elif len(per_db_dfs) == 1:
            st.info("Only one DB returned results — using that source as the final output.")
            # display the single DataFrame as the final result
            single_db_id, single_df = next(iter(per_db_dfs.items()))
            st.subheader(f"Final Output (from single source: {single_db_id})")
            st.caption(f"[{single_db_id}] {len(single_df)} rows")
            st.dataframe(single_df.head(200), use_container_width=True)

        else:
            # Register DataFrames into DuckDB
            con = duckdb.connect()
            for db_id, df in per_db_dfs.items():
                con.register(db_id, df)

            # Derive metadata from DuckDB (preferred) with fallback to DataFrame dtypes
            per_db_metadata = {}
            for db_id in per_db_dfs.keys():
                cols = []
                try:
                    info_df = con.execute(f"PRAGMA table_info('{db_id}')").df()
                    if 'name' in info_df.columns and 'type' in info_df.columns:
                        cols = [f"{row['name']} {row['type']}" for _, row in info_df.iterrows()]
                    elif 'column_name' in info_df.columns and 'column_type' in info_df.columns:
                        cols = [f"{row['column_name']} {row['column_type']}" for _, row in info_df.iterrows()]
                    else:
                        cols = [f"{c} {str(per_db_dfs[db_id][c].dtype)}" for c in per_db_dfs[db_id].columns]
                except Exception:
                    cols = [f"{c} {str(per_db_dfs[db_id][c].dtype)}" for c in per_db_dfs[db_id].columns]
                per_db_metadata[db_id] = {"columns": cols}

            # Call LLM with metadata-only and execute returned SQL locally
            try:
                final_plan = llm_generate_final_sql(query, per_db_metadata)
                sql_used = final_plan.get("sql", "")
                st.markdown("**LLM Final SQL Statement (generated using metadata only):**")
                st.code(sql_used, language="sql")

                try:
                    df_result = con.execute(sql_used).df()
                    print(f"FINAL RESULT DF: {df_result}")
                    # If DuckDB returned no columns, derive column names from per-db DataFrame metadata
                    if df_result is None:
                        df_result = pd.DataFrame()
                    if len(df_result.columns) == 0:
                        # Try intersection of column names across per-db dfs (intersection is default merge behavior)
                        try:
                            all_cols = [list(d.columns) for d in per_db_dfs.values()]
                            if all_cols:
                                common = set(all_cols[0])
                                for cols in all_cols[1:]:
                                    common &= set(cols)
                                if common:
                                    df_result = pd.DataFrame(columns=list(common))
                                else:
                                    # fallback: union of all columns
                                    union = []
                                    for cols in all_cols:
                                        for c in cols:
                                            if c not in union:
                                                union.append(c)
                                    df_result = pd.DataFrame(columns=union)
                            else:
                                df_result = pd.DataFrame()
                        except Exception:
                            df_result = pd.DataFrame()
                    st.dataframe(df_result.head(200), use_container_width=True)
                except Exception as e:
                    st.error(f"DuckDB execution failed: {e}")
            except Exception as e:
                st.error(f"Failed to generate final SQL from metadata: {e}")
