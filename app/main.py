from __future__ import annotations

import io
import re
from datetime import datetime
from typing import Any

import pandas as pd
from fastapi import FastAPI, File, HTTPException, UploadFile

app = FastAPI(title="Timesheet Auditor Backend", version="0.1.0")


def _to_float(x: Any) -> float | None:
    if x is None:
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _norm(s: Any) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip()).lower()


def audit_timesheet(df: pd.DataFrame) -> dict:
    # Heuristic column mapping
    cols = {c: _norm(c) for c in df.columns}

    def find_col(*cands: str) -> str | None:
        for c, n in cols.items():
            for cand in cands:
                if cand in n:
                    return c
        return None

    col_hours = find_col("hours", "duration")
    col_date = find_col("date", "work date")
    col_client = find_col("client", "customer")
    col_project = find_col("project", "engagement")
    col_task = find_col("task", "service")
    col_employee = find_col("employee", "user", "staff")
    col_notes = find_col("notes", "description", "memo")

    flags: list[dict] = []

    # Row-level checks
    for i, row in df.iterrows():
        row_num = int(i) + 1

        hours = _to_float(row.get(col_hours)) if col_hours else None
        if col_hours and (hours is None):
            flags.append(
                {
                    "row": row_num,
                    "issue": "invalid_hours",
                    "why": f"Hours value is missing or not a number (column: {col_hours}).",
                    "fix": "Provide a numeric hours value (e.g., 0.5, 1, 2.25).",
                }
            )
        if hours is not None:
            if hours < 0:
                flags.append(
                    {
                        "row": row_num,
                        "issue": "negative_hours",
                        "why": "Hours is negative.",
                        "fix": "Hours should be >= 0. Use an adjustment entry if needed.",
                    }
                )
            if hours > 24:
                flags.append(
                    {
                        "row": row_num,
                        "issue": "impossible_hours",
                        "why": "Hours exceeds 24 in a single day entry.",
                        "fix": "Verify the entry; it may be minutes entered as hours or a duplicated import.",
                    }
                )

        # Missing key fields
        for col, name in [
            (col_client, "client"),
            (col_project, "project"),
            (col_task, "task"),
            (col_employee, "employee"),
        ]:
            if col and not str(row.get(col) or "").strip():
                flags.append(
                    {
                        "row": row_num,
                        "issue": f"missing_{name}",
                        "why": f"{name.title()} is blank (column: {col}).",
                        "fix": f"Fill the {name} field for accurate billing/reporting.",
                    }
                )

        # Date parse
        if col_date:
            v = str(row.get(col_date) or "").strip()
            if not v:
                flags.append(
                    {
                        "row": row_num,
                        "issue": "missing_date",
                        "why": f"Date is blank (column: {col_date}).",
                        "fix": "Provide a work date.",
                    }
                )
            else:
                try:
                    pd.to_datetime(v)
                except Exception:
                    flags.append(
                        {
                            "row": row_num,
                            "issue": "invalid_date",
                            "why": f"Date value '{v}' is not parseable.",
                            "fix": "Use ISO format (YYYY-MM-DD) or a consistent date format.",
                        }
                    )

        # Low-quality notes (optional)
        if col_notes:
            note = str(row.get(col_notes) or "").strip()
            if hours is not None and hours >= 1 and len(note) < 6:
                flags.append(
                    {
                        "row": row_num,
                        "issue": "weak_description",
                        "why": "Description is very short for a >= 1h entry.",
                        "fix": "Add a brief, client-friendly summary of work performed.",
                    }
                )

    # Duplicate-ish detection (same day/client/project/task/employee/hours)
    key_cols = [c for c in [col_date, col_client, col_project, col_task, col_employee, col_hours] if c]
    if key_cols:
        dup = df[key_cols].copy()
        # normalize strings
        for c in dup.columns:
            if dup[c].dtype == "object":
                dup[c] = dup[c].astype(str).str.strip().str.lower()
        dup_counts = dup.value_counts(dropna=False)
        dup_keys = dup_counts[dup_counts > 1]
        if len(dup_keys) > 0:
            # mark rows that match any duplicated key
            dup_mask = dup.apply(tuple, axis=1).isin(set(dup_keys.index.tolist()))
            for i in df[dup_mask].index.tolist():
                flags.append(
                    {
                        "row": int(i) + 1,
                        "issue": "possible_duplicate",
                        "why": f"This row matches another row on {', '.join(key_cols)}.",
                        "fix": "Check for duplicate import or repeated entry.",
                    }
                )

    summary = {
        "rows": int(len(df)),
        "flags": int(len(flags)),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "detected_columns": {
            "hours": col_hours,
            "date": col_date,
            "client": col_client,
            "project": col_project,
            "task": col_task,
            "employee": col_employee,
            "notes": col_notes,
        },
    }

    return {"summary": summary, "flags": flags}


@app.get("/health")
def health():
    return {"ok": True}


@app.post("/audit")
async def audit(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="file must be a .csv")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="empty file")

    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"could not parse csv: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="csv has no rows")

    return audit_timesheet(df)
