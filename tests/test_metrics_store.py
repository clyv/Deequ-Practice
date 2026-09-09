# -*- coding: utf-8 -*-
"""Regression tests for the metrics-store normalisation.

Both cases below are bugs that actually broke this pipeline in development, not
hypotheticals. They are cheap to test because the logic is pure pandas.
"""
import numpy as np
import pandas as pd
import pytest

from src.reporting import metrics_store as ms


def frame(rows):
    return pd.DataFrame(
        rows, columns=["entity", "instance", "name", "value", "month", "dataset_date"]
    )


def test_missing_tag_column_raises_rather_than_fabricating():
    """Deequ expands ResultKey tags into columns and returns no `tags` dict.

    The original code checked for a `tags` column, did not find one, and its
    fallback assigned month = None -- destroying the real column and making
    every row share an index label. Failing loudly is the correct behaviour.
    """
    untagged = pd.DataFrame(
        {"entity": ["Column"], "instance": ["fare_amount"], "name": ["Mean"], "value": [1.0]}
    )
    with pytest.raises(KeyError, match="month"):
        ms.normalise_metrics(untagged)


def test_repeated_runs_collapse_to_the_latest():
    """saveOrAppendResult appends by design, so re-running an analysis leaves
    several rows per month; reindexing them raises on duplicate labels."""
    df = frame([
        ("Column", "fare_amount", "Mean", 19.20, "2025-09", 1_000),
        ("Column", "fare_amount", "Mean", 19.99, "2025-09", 2_000),  # a later re-run
        ("Column", "fare_amount", "Mean", 17.12, "2025-11", 1_500),
    ])
    out = ms.normalise_metrics(df)
    assert len(out) == 2
    september = out.loc[out["month"] == "2025-09", "value"].iloc[0]
    assert september == pytest.approx(19.99), "should keep the most recent run"


def test_metric_series_orders_and_pads():
    df = frame([
        ("Column", "fare_amount", "Mean", 17.12, "2025-11", 3),
        ("Column", "fare_amount", "Mean", 19.20, "2025-09", 1),
    ])
    series = ms.metric_series(
        ms.normalise_metrics(df), "Mean", "fare_amount",
        ["2025-09", "2025-10", "2025-11"],
    )
    assert list(series.index) == ["2025-09", "2025-10", "2025-11"]
    assert series.iloc[0] == pytest.approx(19.20)
    assert np.isnan(series.iloc[1]), "an absent month must be NaN, not dropped"
    assert series.iloc[2] == pytest.approx(17.12)


def test_metric_series_does_not_raise_on_duplicates_after_normalising():
    df = frame([
        ("Column", "fare_amount", "Mean", 1.0, "2025-09", 1),
        ("Column", "fare_amount", "Mean", 2.0, "2025-09", 2),
    ])
    series = ms.metric_series(ms.normalise_metrics(df), "Mean", "fare_amount", ["2025-09"])
    assert series.iloc[0] == pytest.approx(2.0)


def test_coverage_reports_missing_and_stale_periods():
    df = frame([
        ("Column", "fare_amount", "Mean", 1.0, "2024-01", 1),
        ("Column", "fare_amount", "Mean", 2.0, "2025-09", 2),
    ])
    missing, unexpected = ms.coverage(df, ["2025-09", "2025-10"])
    assert missing == ["2025-10"]
    assert unexpected == ["2024-01"], "a stale month left in the store must surface"


@pytest.mark.parametrize(
    "values,expected",
    [([19.20, 17.12], pytest.approx(-10.833, rel=1e-3)), ([10.0, 15.0], pytest.approx(50.0))],
)
def test_relative_change(values, expected):
    assert ms.relative_change(pd.Series(values)) == expected


def test_relative_change_guards_degenerate_input():
    assert ms.relative_change(pd.Series([5.0])) is None
    assert ms.relative_change(pd.Series([0.0, 5.0])) is None
    assert ms.relative_change(pd.Series([np.nan, np.nan])) is None
