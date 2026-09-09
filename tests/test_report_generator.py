# -*- coding: utf-8 -*-
"""Tests for report generation, using real Deequ constraint strings."""
import json

import pandas as pd
import pytest

from src.reporting import report_generator as rg

# Verbatim from checkResultsAsDataFrame — the nesting is why naive truncation
# produced unreadable report rows.
CONSTRAINTS = [
    "CompletenessConstraint(Completeness(fare_amount,None))",
    "ComplianceConstraint(Compliance(Trip distance must be positive,trip_distance > 0,None))",
    "ComplianceConstraint(Compliance(fare_amount is non-negative,"
    "COALESCE(CAST(fare_amount AS DECIMAL(20,10)), 0.0) >= 0,None))",
    "ComplianceConstraint(Compliance(Passenger count must be 1-6,"
    "passenger_count >= 1 AND passenger_count <= 6,None))",
]


def results(statuses, messages=None):
    messages = messages or [""] * len(statuses)
    return pd.DataFrame({
        "constraint": CONSTRAINTS[: len(statuses)],
        "constraint_status": statuses,
        "constraint_message": messages,
    })


def test_readable_names_unwrap_the_nesting():
    names = rg.readable_names(pd.Series(CONSTRAINTS)).tolist()
    assert names == [
        "fare_amount",
        "Trip distance must be positive",
        "fare_amount is non-negative",
        "Passenger count must be 1-6",
    ]


def test_readable_names_fall_back_to_the_raw_string():
    odd = pd.Series(["SomethingUnrecognised"])
    assert rg.readable_names(odd).iloc[0] == "SomethingUnrecognised"


def test_compliance_extracts_the_ratio_and_treats_success_as_one():
    df = results(
        ["Success", "Failure"],
        ["", "Value: 0.9720 does not meet the constraint requirement!"],
    )
    out = rg.compliance(df)
    assert out.loc[0, "compliance"] == pytest.approx(1.0)
    assert out.loc[1, "compliance"] == pytest.approx(0.9720)


def test_summarise_counts_and_names_failures():
    df = results(["Success", "Failure", "Failure"], ["", "Value: 0.5", "Value: 0.1"])
    summary = rg.summarise(df)
    assert (summary["total"], summary["passed"], summary["failed"]) == (3, 1, 2)
    assert summary["pass_rate"] == pytest.approx(1 / 3)
    assert "Trip distance must be positive" in summary["failed_constraints"]


def test_summarise_handles_an_empty_frame():
    summary = rg.summarise(results([]))
    assert summary["total"] == 0 and summary["pass_rate"] == 0.0


def test_compare_labels_fixed_and_still_failing():
    before = results(
        ["Failure", "Failure"],
        ["Value: 0.9720 does not meet", "Value: 0.7561 does not meet"],
    )
    after = results(["Success", "Failure"], ["", "Value: 0.7561 does not meet"])
    out = rg.compare(before, after)

    fixed = out[out["constraint"] == "fare_amount"].iloc[0]
    assert fixed["outcome"] == "fixed"
    assert fixed["delta"] == pytest.approx(1.0 - 0.9720)

    unchanged = out[out["constraint"] == "Trip distance must be positive"].iloc[0]
    assert unchanged["outcome"] == "still failing"
    assert unchanged["delta"] == pytest.approx(0.0)


def test_to_markdown_renders_a_pasteable_block():
    summary = rg.summarise(results(["Success", "Failure"], ["", "Value: 0.5"]))
    md = rg.to_markdown(summary, {"Negative fare amount": 969_118}, total_rows=12_861_158)
    assert "| Total rows validated | 12,861,158 |" in md
    assert "| Negative fare amount | 969,118 | 7.54% |" in md


def test_to_markdown_survives_zero_rows():
    summary = rg.summarise(results(["Success"]))
    assert "0.00%" in rg.to_markdown(summary, {"anything": 0}, total_rows=0)


def test_write_reports_emits_csv_and_json(tmp_path):
    df = results(["Success", "Failure"], ["", "Value: 0.5 does not meet"])
    written = rg.write_reports(df, tmp_path, slug="202509_202511")

    names = {p.name for p in written}
    assert "constraint_results_202509_202511.csv" in names
    assert "quality_summary_202509_202511.json" in names

    payload = json.loads((tmp_path / "quality_summary_202509_202511.json").read_text())
    assert payload["failed"] == 1
    assert payload["failed_constraints"] == ["Trip distance must be positive"]
