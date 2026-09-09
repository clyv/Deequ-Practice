# -*- coding: utf-8 -*-
"""Turn Deequ verification output into reports.

Pure pandas, so it is unit testable without a JVM. The Spark-side call that
produces the input frame is VerificationResult.checkResultsAsDataFrame.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd

# Deequ nests constraint names, e.g.
#   ComplianceConstraint(Compliance(Trip distance must be positive,...))
# so pull out the readable first argument rather than truncating the wrapper.
_ANALYZERS = (
    "Completeness|Compliance|Uniqueness|Distinctness|Size|Maximum|Minimum|Mean"
    "|ApproxQuantile|Correlation|DataType|MaxLength|MinLength|StandardDeviation"
    "|Sum|Entropy|UniqueValueRatio|PatternMatch"
)
_NAME_PATTERN = rf"\((?:{_ANALYZERS})\(([^,)]+)"

# Deequ reports the achieved ratio inside constraint_message on failure.
_VALUE_PATTERN = r"Value:\s*([0-9.eE+-]+)"

SUCCESS = "Success"


def readable_names(constraints: pd.Series) -> pd.Series:
    """Human-readable constraint names, falling back to the raw string.

    Cast first: an empty frame yields a float64 series, and pandas' .str
    accessor rejects it outright rather than returning empty.
    """
    constraints = constraints.astype("string")
    extracted = constraints.str.extract(_NAME_PATTERN)[0]
    return extracted.fillna(constraints)


def compliance(results: pd.DataFrame) -> pd.DataFrame:
    """Add `name` and `compliance` columns.

    A Success means the assertion held; for the suites here that is a ratio of
    1.0. Failures carry the achieved ratio in the message.
    """
    out = results.copy()
    out["name"] = readable_names(out["constraint"])
    values = out["constraint_message"].fillna("").str.extract(_VALUE_PATTERN)[0]
    out["compliance"] = pd.to_numeric(values, errors="coerce")
    out.loc[out["constraint_status"] == SUCCESS, "compliance"] = 1.0
    return out


def summarise(results: pd.DataFrame) -> Dict[str, object]:
    """Headline counts for a verification run."""
    total = len(results)
    passed = int((results["constraint_status"] == SUCCESS).sum())
    return {
        "total": total,
        "passed": passed,
        "failed": total - passed,
        "pass_rate": (passed / total) if total else 0.0,
        "failed_constraints": readable_names(
            results.loc[results["constraint_status"] != SUCCESS, "constraint"]
        ).tolist(),
    }


def compare(before: pd.DataFrame, after: pd.DataFrame) -> pd.DataFrame:
    """Per-constraint before/after compliance, for the clean-and-revalidate step."""
    left, right = compliance(before), compliance(after)
    merged = left.merge(right, on="constraint", suffixes=("_before", "_after"))
    merged["delta"] = merged["compliance_after"] - merged["compliance_before"]
    merged["outcome"] = "was passing"
    newly_fixed = (merged["constraint_status_before"] != SUCCESS) & (
        merged["constraint_status_after"] == SUCCESS
    )
    still_failing = merged["constraint_status_after"] != SUCCESS
    merged.loc[still_failing, "outcome"] = "still failing"
    merged.loc[newly_fixed, "outcome"] = "fixed"
    return merged[
        ["name_before", "compliance_before", "compliance_after", "delta", "outcome"]
    ].rename(columns={"name_before": "constraint"})


def to_markdown(summary: Dict[str, object], issues: Dict[str, int], total_rows: int) -> str:
    """A Results block that can be pasted straight into the README."""
    lines = [
        "| Metric | Value |",
        "|---|---|",
        f"| Total rows validated | {total_rows:,} |",
        f"| Constraint checks run | {summary['total']} |",
        f"| Checks passed | {summary['passed']} |",
        f"| Checks failed | {summary['failed']} |",
        "",
        "| Issue | Rows Affected | % of Data |",
        "|---|---|---|",
    ]
    for issue, count in issues.items():
        pct = (count / total_rows * 100) if total_rows else 0.0
        lines.append(f"| {issue} | {count:,} | {pct:.2f}% |")
    return "\n".join(lines)


def write_reports(
    results: pd.DataFrame,
    destination: Path,
    slug: str,
    extra: Iterable[tuple] = (),
) -> List[Path]:
    """Emit CSV and JSON artefacts; return what was written."""
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    written: List[Path] = []

    csv_path = destination / f"constraint_results_{slug}.csv"
    results.to_csv(csv_path, index=False)
    written.append(csv_path)

    # JSON so downstream tooling (CloudWatch, an alerting lambda) can consume it
    # without parsing a report meant for humans.
    json_path = destination / f"quality_summary_{slug}.json"
    json_path.write_text(
        json.dumps(summarise(results), indent=2), encoding="utf-8"
    )
    written.append(json_path)

    for name, frame in extra:
        path = destination / f"{name}_{slug}.csv"
        frame.to_csv(path, index=False)
        written.append(path)

    return written
