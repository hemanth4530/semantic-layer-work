# app/exec_sql.py
from typing import Dict, Any, List, Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

def _mk_engine(dsn: str) -> Engine:
    return create_engine(dsn, pool_pre_ping=True, future=True)

def exec_sql(dsn: str, sql: str) -> Dict[str, Any]:
    """Run a single SQL text on a DSN. Return {columns:[...], rows:[{...}], error:Optional[str]}"""
    try:
        eng = _mk_engine(dsn)
        with eng.connect() as cx:
            rs = cx.execute(text(sql))
            cols = [c if isinstance(c, str) else str(c) for c in rs.keys()]
            out_rows: List[Dict[str, Any]] = []
            for r in rs.mappings():
                out_rows.append(dict(r))
            return {"columns": cols, "rows": out_rows, "error": None}
    except SQLAlchemyError as e:
        return {"columns": [], "rows": [], "error": str(e)}
