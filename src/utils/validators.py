"""
Data-quality validation helpers.
Each function returns a dict with counts and issue details.
No data is silently dropped – all problems are reported.
"""
import pandas as pd
from typing import Dict, Any
from src.utils.logger import get_logger

log = get_logger(__name__)


def check_missing(df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
    """Report missing values per column."""
    missing = df.isnull().sum()
    missing_pct = (missing / len(df) * 100).round(2)
    report = missing[missing > 0].to_dict()
    pct_report = missing_pct[missing > 0].to_dict()
    if report:
        log.warning("[%s] Missing values found: %s", dataset_name, report)
    else:
        log.info("[%s] No missing values.", dataset_name)
    return {"missing_counts": report, "missing_pct": pct_report}


def check_duplicates(df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
    """Report exact duplicate rows."""
    n_dups = int(df.duplicated().sum())
    if n_dups:
        log.warning("[%s] %d duplicate rows detected.", dataset_name, n_dups)
    else:
        log.info("[%s] No duplicate rows.", dataset_name)
    return {"duplicate_rows": n_dups}


def check_dtypes(df: pd.DataFrame, expected: Dict[str, str],
                 dataset_name: str) -> Dict[str, Any]:
    """
    Compare actual dtypes against expected.
    expected = {"column_name": "expected_dtype_string"}
    """
    issues = {}
    for col, exp_dtype in expected.items():
        if col not in df.columns:
            issues[col] = f"COLUMN MISSING (expected dtype {exp_dtype})"
        elif str(df[col].dtype) != exp_dtype:
            issues[col] = {"expected": exp_dtype, "actual": str(df[col].dtype)}
    if issues:
        log.warning("[%s] dtype mismatches: %s", dataset_name, issues)
    else:
        log.info("[%s] All dtypes match expectations.", dataset_name)
    return {"dtype_issues": issues}


def check_date_column(df: pd.DataFrame, col: str, fmt: str,
                      dataset_name: str) -> Dict[str, Any]:
    """Try parsing a date column; report unparseable rows."""
    if col not in df.columns:
        log.warning("[%s] Date column '%s' not found.", dataset_name, col)
        return {"date_issues": f"column '{col}' missing"}
    parsed = pd.to_datetime(df[col], format=fmt, errors="coerce")
    n_bad = int(parsed.isnull().sum())
    if n_bad:
        bad_samples = df.loc[parsed.isnull(), col].head(5).tolist()
        log.warning("[%s] %d rows have unparseable dates in '%s'. Samples: %s",
                    dataset_name, n_bad, col, bad_samples)
    else:
        log.info("[%s] All dates in '%s' parsed successfully.", dataset_name, col)
    return {"unparseable_dates": n_bad}


def check_numeric_range(df: pd.DataFrame, col: str,
                         min_val: float, max_val: float,
                         dataset_name: str) -> Dict[str, Any]:
    """Report rows where a numeric column falls outside [min_val, max_val]."""
    if col not in df.columns:
        log.warning("[%s] Numeric column '%s' not found.", dataset_name, col)
        return {"range_issues": f"column '{col}' missing"}
    out = df[(df[col] < min_val) | (df[col] > max_val)]
    if len(out):
        log.warning("[%s] %d rows outside [%s, %s] in '%s'.",
                    dataset_name, len(out), min_val, max_val, col)
    else:
        log.info("[%s] All values in '%s' within expected range.", dataset_name, col)
    return {"out_of_range_rows": len(out)}


def full_quality_report(df: pd.DataFrame, dataset_name: str) -> Dict[str, Any]:
    """Run generic checks (missing + duplicates) and return combined report."""
    report = {
        "dataset": dataset_name,
        "total_rows": len(df),
        "columns": list(df.columns),
    }
    report.update(check_missing(df, dataset_name))
    report.update(check_duplicates(df, dataset_name))
    return report
