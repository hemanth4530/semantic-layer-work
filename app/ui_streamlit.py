# ui_streamlit.py

import os, json, pathlib, streamlit as st
from typing import Dict, Any, List
from dotenv import load_dotenv
from exec_sql import exec_sql
from llm_planner import plan, llm_generate_final_sql
import duckdb, pandas as pd
from data_masking import (
    mask_dataframe_for_display, 
    get_role_permissions_summary,
    get_masking_summary
)
from tag_loader import load_tag_mappings, load_masking_config

# Load environment variables from .env file
load_dotenv()
def handle_query(nl_query: str, catalog_by_db: Dict[str, Any]) -> Dict[str, Any]:
    """Thin wrapper so ask.py / UI both call the same entrypoint."""
    return plan(nl_query, catalog_by_db)



st.set_page_config(
    page_title="Solix - Federated Querying with Data Masking", 
    page_icon="⚡",  # Lightning bolt icon as fallback
    layout="wide",
    initial_sidebar_state="expanded"
)

# Force Light Theme - Override Streamlit's dark theme
st.markdown("""
<style>
    /* Only use CSS selectors that actually work */
    
    /* Button styling - this definitely works */
    .stButton > button {
        background-color: #007BFF !important;
        color: #FFFFFF !important;
        border: none;
        border-radius: 4px;
        font-weight: 500;
    }
    
    .stButton > button:hover {
        background-color: #0056B3 !important;
    }
    
    /* Text input styling - this works */
    .stTextInput > div > div > input {
        background-color: #FFFFFF !important;
        color: #000000 !important;
        border: 1px solid #CED4DA !important;
    }
    
    /* Selectbox styling - this works */
    .stSelectbox > div > div {
        background-color: #FFFFFF !important;
        color: #000000 !important;
    }

</style>
""", unsafe_allow_html=True)

# Custom favicon injection
st.markdown("""
<script>
    // Function to update favicon
    function updateFavicon() {
        // Remove existing favicon
        var existingFavicon = document.querySelector("link[rel*='icon']");
        if (existingFavicon) {
            existingFavicon.remove();
        }
        
        // Create new favicon matching your exact red lightning bolt image
        var newFavicon = document.createElement('link');
        newFavicon.rel = 'icon';
        newFavicon.type = 'image/svg+xml';
        newFavicon.href = 'data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMzIiIGhlaWdodD0iMzIiIHZpZXdCb3g9IjAgMCAzMiAzMiIgZmlsbD0ibm9uZSIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj4KPGNpcmNsZSBjeD0iMTYiIGN5PSIxNiIgcj0iMTYiIGZpbGw9IiNEQzM1NDUiLz4KPHN2ZyB4PSI5IiB5PSI3IiB3aWR0aD0iMTQiIGhlaWdodD0iMTgiIHZpZXdCb3g9IjAgMCAxNCAxOCIgZmlsbD0ibm9uZSI+CjxwYXRoIGQ9Ik03LjUgMEwyIDlINS41TDUgMThMMTAuNSA5SDdMNy41IDBaIiBmaWxsPSJ3aGl0ZSIvPgo8L3N2Zz4KPC9zdmc+';
        
        // Add to head
        document.head.appendChild(newFavicon);
    }
    
    // Update favicon when page loads
    document.addEventListener('DOMContentLoaded', updateFavicon);
    
    // Also update after a short delay to ensure it takes effect
    setTimeout(updateFavicon, 1000);
</script>
""", unsafe_allow_html=True)

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



# Header with logo and title - absolute top positioning
st.markdown("""
<div style="display: flex; align-items: center; margin: -2rem 0 1rem 0; padding: 0.3rem 0 0.8rem 0; border-bottom: 2px solid #E5E7EB; position: relative; top: -10px;">
    <img src="https://192.168.1.116:3001/static/media/solix-logo-black.abebcfd796dd81ecc2f0.png" 
         alt="Solix Logo" 
         style="height: 15px; margin-right: 12px; object-fit: contain;">
    <h1 style="color: #2E3440; font-weight: 600; margin: 0; font-size: 1.8rem; line-height: 1.2;">Federated Querying with Data Masking</h1>
</div>
""", unsafe_allow_html=True)

# Sidebar: Configuration and Role Selection
st.sidebar.header("Configuration")

# Catalog viewer button instead of text input
catalog_path = "data/catalog_live.json"  # Fixed path
if st.sidebar.button("📋 View Catalog File"):
    try:
        catalog_data = load_json(catalog_path)
        with st.sidebar.expander("📊 Catalog Contents", expanded=True):
            st.json(catalog_data)
    except Exception as e:
        st.sidebar.error(f"Error loading catalog: {e}")

st.sidebar.write(f"**Using:** `{catalog_path}`")



st.sidebar.header("Role & Permissions")
role = st.sidebar.selectbox(
    "Select User Role", 
    ["admin", "manager", "employee", "intern"],
    index=0
)

# Load masking configuration and show role permissions
try:
    config = load_masking_config()
    
    # Auto-generate field tag mappings if needed (when catalog is newer)
    with st.spinner("🔍 Checking field tag mappings..."):
        tag_mappings = load_tag_mappings(catalog_file=catalog_path, auto_generate=True)
    
    if role != "admin":
        permissions = get_role_permissions_summary(role, config)
        st.sidebar.write(f"**{role.title()} Access:**")
        st.sidebar.write(f"*{permissions.get('description', '')}*")
        
        sensitive_tags = set(permissions.get('blocked_tags', []) + permissions.get('anonymize_tags', []))
        
        if sensitive_tags:
            st.sidebar.write("⭐ **Masked Data:**")
            st.sidebar.write("*Columns visible, sensitive values shown as stars*")
            
            # Combine descriptions from both blocked and anonymized tags
            all_descriptions = permissions.get('blocked_descriptions', []) + permissions.get('anonymize_descriptions', [])
            # Remove duplicates while preserving order
            seen = set()
            unique_descriptions = []
            for desc in all_descriptions:
                tag_name = desc.split(':')[0]
                if tag_name not in seen:
                    seen.add(tag_name)
                    unique_descriptions.append(desc.split(':')[0] + ': ' + desc.split(':')[1].split('(')[0].strip())
            
            for desc in unique_descriptions:
                st.sidebar.write(f"- {desc}")
        else:
            st.sidebar.write("✅ **Full access to all data**")
    else:
        st.sidebar.write("**👑 Administrator - Full Access**")
        st.sidebar.write("*Can see all data without restrictions*")

except Exception as e:
    st.sidebar.error(f"Error loading masking config: {e}")
    config = None
    tag_mappings = None

# Main query input
query = st.text_input("Natural Language Query", "top 3 clients by total invoice amount")

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
                
                # Apply role-based masking to per-DB results
                if config and tag_mappings and role != "admin":
                    masked_df, masking_indicators = mask_dataframe_for_display(
                        df, db_id, None, role, tag_mappings, config
                    )
                    
                    # Display with masking indicators
                    st.caption(f"[{db_id}] {len(masked_df)} rows (Role: {role})")
                    if masking_indicators:
                        # Show masking summary
                        summary = get_masking_summary(df.columns.tolist(), masked_df.columns.tolist(), masking_indicators)
                        if summary['star_masked_columns'] > 0:
                            st.caption(f"⭐ Data Masking Applied: {summary['star_masked_columns']} columns masked with stars")
                    
                    st.dataframe(masked_df, use_container_width=True)
                    
                    # Show masking details in expander
                    if masking_indicators:
                        with st.expander(f"Masking Details for {db_id}"):
                            for col, indicator in masking_indicators.items():
                                st.write(f"**{col}**: {indicator}")
                else:
                    # Admin or no masking config - show full data
                    st.caption(f"[{db_id}] {len(df)} rows")
                    st.dataframe(df, use_container_width=True)
                
                exec_results.append({"db_id":db_id, "columns":res["columns"], "rows":res["rows"]})

        st.subheader("Final Output (LLM-generated SQL over in-memory tables)")
        per_db_dfs = {}

        # Build and sanitize DataFrames for each DB
        per_db_masking_info = {}  # Track masking applied to each DB
        
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

            # Apply masking to per-DB DataFrames BEFORE DuckDB processing
            # This ensures final results inherit the masking automatically
            if config and tag_mappings and role != "admin":
                masked_df, masking_indicators = mask_dataframe_for_display(
                    df, db_id, None, role, tag_mappings, config
                )
                per_db_dfs[db_id] = masked_df
                per_db_masking_info[db_id] = masking_indicators
                print(f"Applied masking to {db_id}: {len(masking_indicators)} columns masked")
            else:
                per_db_dfs[db_id] = df
                per_db_masking_info[db_id] = {}
                
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
            
            # DataFrame is already masked from per-DB processing, just display it
            st.caption(f"[{single_db_id}] {len(single_df)} rows")
            if role != "admin" and single_db_id in per_db_masking_info and per_db_masking_info[single_db_id]:
                masking_indicators = per_db_masking_info[single_db_id]
                summary = get_masking_summary([], single_df.columns.tolist(), masking_indicators)
                if summary['star_masked_columns'] > 0:
                    st.caption(f"⭐ Final Result Masking: {summary['star_masked_columns']} columns masked with stars (Role: {role})")
                
                with st.expander("Final Result Masking Details"):
                    for col, indicator in masking_indicators.items():
                        st.write(f"**{col}**: {indicator}")
            
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
                    
                    # Data is already masked from per-DB processing, just display it
                    if role != "admin" and any(per_db_masking_info.values()):
                        # Show masking summary from per-DB processing
                        total_masked_columns = sum(len(indicators) for indicators in per_db_masking_info.values())
                        if total_masked_columns > 0:
                            st.caption(f"⭐ Final Combined Result: Data inherits masking from source databases (Role: {role})")
                            
                            with st.expander("Source Database Masking Details"):
                                for db_id, indicators in per_db_masking_info.items():
                                    if indicators:
                                        st.write(f"**{db_id}:**")
                                        for col, indicator in indicators.items():
                                            st.write(f"  - {col}: {indicator}")
                    
                    st.dataframe(df_result.head(200), use_container_width=True)
                except Exception as e:
                    st.error(f"DuckDB execution failed: {e}")
            except Exception as e:
                st.error(f"Failed to generate final SQL from metadata: {e}")
