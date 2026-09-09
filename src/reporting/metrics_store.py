# -*- coding: utf-8 -*-
"""Metrics-repository helpers.

The normalisation logic here is deliberately pure pandas so it can be unit
tested without a JVM. Both bugs that broke the pipeline during development
lived in exactly this code:

  1. Deequ expands ResultKey tags into their own columns and does NOT return a
     `tags` dict, so a defensive `if "tags" in columns` fallback silently
     overwrote the real `month` column with None.
  2. saveOrAppendResult appends by design, so re-running an analysis leaves
     several rows per month and reindexing raises on duplicate labels.

Both are now covered by tests/test_metrics_store.py.
"""
from __future__ import annotations

from typing import Iterable, List, Optional

import pandas as pd

# Columns Deequ always emits from getSuccessMetricsAsDataFrame.
BASE_COLUMNS = ("entity", "instance", "name", "value")
DATASET_DATE = "dataset_date"


def normalise_metrics(
    metrics: pd.DataFrame,
    tag_column: str = "month",
) -> pd.DataFrame:
    """Return one row per (name, instance, tag), keeping the most recent.

    Raises KeyError if the tag column is absent, rather than fabricating it —
    a missing tag means the ResultKey was built without it, and silently
    substituting None is what caused the original defect.
    """
    if tag_column not in metrics.columns:
        raise KeyError(
            f"No {tag_column!r} column in the metrics frame. Deequ derives it "
            f"from the ResultKey tags. Columns present: {list(metrics.columns)}"
        )

    frame = metrics.copy()
    if DATASET_DATE in frame.columns:
        frame = frame.sort_values(DATASET_DATE)

    return frame.drop_duplicates(
        subset=["name", "instance", tag_column], keep="last"
    ).reset_index(drop=True)


def metric_series(
    metrics: pd.DataFrame,
    name: str,
    instance: str,
    order: Iterable[str],
    tag_column: str = "month",
) -> pd.Series:
    """One metric's values in `order`, NaN where a period is missing."""
    subset = metrics[(metrics["name"] == name) & (metrics["instance"] == instance)]
    return subset.set_index(tag_column)["value"].reindex(list(order))


def coverage(
    metrics: pd.DataFrame,
    expected: Iterable[str],
    tag_column: str = "month",
) -> tuple[List[str], List[str]]:
    """(missing, unexpected) periods, so a stale store is visible rather than
    producing an all-NaN series and a report full of 'nan'."""
    expected = list(expected)
    present = sorted(metrics[tag_column].dropna().unique())
    missing = [p for p in expected if p not in present]
    unexpected = [p for p in present if p not in expected]
    return missing, unexpected


def relative_change(series: pd.Series) -> Optional[float]:
    """Percent change from the first to the last non-null value."""
    clean = series.dropna()
    if len(clean) < 2:
        return None
    first, last = float(clean.iloc[0]), float(clean.iloc[-1])
    if first == 0:
        return None
    return (last - first) / first * 100.0


def build_repository(spark, path: str):
    """FileSystemMetricsRepository at `path`.

    Swap the path for an s3:// URI in production; nothing else changes.
    pyspark/pydeequ are imported lazily so this module stays importable — and
    testable — without a JVM.
    """
    from pydeequ.repository import FileSystemMetricsRepository

    return FileSystemMetricsRepository(spark, str(path))
