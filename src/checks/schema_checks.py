# -*- coding: utf-8 -*-
"""Value-domain and type constraints, from the TLC data dictionary.

These are the rules that come from the SPECIFICATION rather than from the data.
That distinction matters: Deequ's ConstraintSuggestionRunner derives allowed
values from what it observes, so on this dataset it proposes a payment_type
range that includes 0 -- the invalid code emitted by the broken upstream feed.
Only a human reading the data dictionary writes the rule below.
"""
from __future__ import annotations

# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
PAYMENT_TYPES = ["1", "2", "3", "4", "5", "6"]
RATE_CODES = ["1", "2", "3", "4", "5", "6"]
VENDOR_IDS = ["1", "2", "6", "7"]
STORE_AND_FWD = ["Y", "N"]

# NYC taxi zones are numbered 1-265.
MIN_ZONE_ID = 1
MAX_ZONE_ID = 265

# Bounds a metered yellow cab cannot credibly exceed. These are sanity limits,
# not business rules: the observed maxima are $323,800 and 318,608 miles.
MAX_CREDIBLE_FARE = 1_000.0
MAX_CREDIBLE_DISTANCE = 500.0


def domain_catalog() -> dict:
    """The value domains as data, so they can be asserted on without Spark."""
    return {
        "payment_type": PAYMENT_TYPES,
        "RatecodeID": RATE_CODES,
        "VendorID": VENDOR_IDS,
        "store_and_fwd_flag": STORE_AND_FWD,
    }


def build(spark, level=None):
    """A blocking Check covering spec violations and impossible magnitudes."""
    from pydeequ.checks import Check, CheckLevel

    level = level or CheckLevel.Error
    check = Check(spark, level, "Schema & domain")
    return (
        check
        .isContainedIn("payment_type", PAYMENT_TYPES,
                       hint="payment_type outside the TLC-defined 1-6")
        .isContainedIn("RatecodeID", RATE_CODES,
                       hint="RatecodeID outside the TLC-defined 1-6")
        .isContainedIn("store_and_fwd_flag", STORE_AND_FWD)
        .satisfies(f"PULocationID >= {MIN_ZONE_ID} AND PULocationID <= {MAX_ZONE_ID}",
                   "Pickup zone must be a valid NYC zone")
        .satisfies(f"DOLocationID >= {MIN_ZONE_ID} AND DOLocationID <= {MAX_ZONE_ID}",
                   "Dropoff zone must be a valid NYC zone")
        .hasMax("fare_amount", lambda v: v <= MAX_CREDIBLE_FARE,
                hint="a metered fare above $1,000 is not credible")
        .hasMax("trip_distance", lambda v: v <= MAX_CREDIBLE_DISTANCE,
                hint="a yellow-cab trip over 500 miles is not credible")
    )
