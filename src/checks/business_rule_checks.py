# -*- coding: utf-8 -*-
"""Business rules: trip validity, revenue integrity and cross-column sanity.

Constraint names tie each rule to its consequence rather than restating the
predicate, so a failure in a report says what it costs.
"""
from __future__ import annotations

# A metered fare is essentially a function of distance, so anything near zero
# means the column is contaminated. Observed: 0.0012 raw, 0.8770 once 0.0028%
# of rows are bounded out.
MIN_DISTANCE_FARE_CORRELATION = 0.80

# Composite key for double-ingestion detection; observed distinctness 0.9999.
TRIP_KEY = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "fare_amount",
]
MIN_KEY_DISTINCTNESS = 0.99


def build(spark, level=None):
    from pydeequ.checks import Check, CheckLevel

    level = level or CheckLevel.Error
    check = Check(spark, level, "Business rules")
    return (
        check
        .satisfies("fare_amount > 0",
                   "Revenue integrity: fare must be positive (billing impact)")
        .satisfies("trip_distance > 0",
                   "Trip validity: a metered trip must cover distance")
        .satisfies("tpep_dropoff_datetime > tpep_pickup_datetime",
                   "Trip validity: dropoff cannot precede pickup")
        .satisfies("passenger_count >= 1 AND passenger_count <= 6",
                   "Occupancy must be within legal vehicle capacity")
        .isNonNegative("tip_amount")
        .hasCorrelation("trip_distance", "fare_amount",
                        lambda c: c >= MIN_DISTANCE_FARE_CORRELATION,
                        hint="fare should track distance on a metered trip")
        .hasDistinctness(TRIP_KEY, lambda d: d >= MIN_KEY_DISTINCTNESS,
                         hint="a re-ingested file would collapse this")
    )
