# -*- coding: utf-8 -*-
"""Presence constraints and completeness budgets.

The columns in CO_MISSING_CLUSTER go NULL together in exactly the same rows --
identical masks, never missing individually -- which is the signature of one
upstream feed rather than five independent collection problems. Checking them
as a group is what makes that visible in a report.
"""
from __future__ import annotations

# Always required: without these a trip record means nothing.
REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
]

# Vendor-supplied fields that the broken feed leaves unset as a block.
CO_MISSING_CLUSTER = [
    "passenger_count",
    "congestion_surcharge",
    "RatecodeID",
    "store_and_fwd_flag",
    "Airport_fee",
]

# Advisory floor. Deliberately below 1.0: these fields are not worth failing a
# load over, but a drop below this is worth someone's attention.
COMPLETENESS_FLOOR = 0.95


def build(spark, level=None):
    from pydeequ.checks import Check, CheckLevel

    level = level or CheckLevel.Warning
    check = Check(spark, level, "Completeness")
    for column in REQUIRED_COLUMNS:
        check = check.isComplete(column)
    for column in CO_MISSING_CLUSTER:
        check = check.hasCompleteness(
            column,
            lambda c: c >= COMPLETENESS_FLOOR,
            hint=f"{column} is part of the co-missing vendor field cluster",
        )
    return check
