# -*- coding: utf-8 -*-
"""The check suites are importable without a JVM, and encode the SPEC.

The value domains are the part a suggestion engine cannot produce: Deequ's
ConstraintSuggestionRunner derives allowed values from observed data, so on this
dataset it proposes a payment_type range including 0 -- the invalid code from the
broken upstream feed. These tests pin the specification instead.
"""
import pytest

from src.checks import business_rule_checks, completeness_checks, schema_checks


def test_payment_type_domain_excludes_the_broken_feeds_sentinel():
    assert "0" not in schema_checks.PAYMENT_TYPES
    assert schema_checks.PAYMENT_TYPES == ["1", "2", "3", "4", "5", "6"]


def test_ratecode_domain_excludes_99():
    """RatecodeID 99 appears in 297,873 rows and is not in the data dictionary."""
    assert "99" not in schema_checks.RATE_CODES


def test_zone_ids_cover_the_documented_range():
    assert (schema_checks.MIN_ZONE_ID, schema_checks.MAX_ZONE_ID) == (1, 265)


def test_credible_bounds_reject_the_observed_outliers():
    """Observed maxima are $323,800.27 and 318,608.57 miles."""
    assert schema_checks.MAX_CREDIBLE_FARE < 323_800.27
    assert schema_checks.MAX_CREDIBLE_DISTANCE < 318_608.57


def test_domain_catalog_is_complete():
    catalog = schema_checks.domain_catalog()
    assert set(catalog) == {"payment_type", "RatecodeID", "VendorID", "store_and_fwd_flag"}
    assert all(isinstance(v, list) and v for v in catalog.values())


def test_co_missing_cluster_matches_the_observed_group():
    """These five go NULL in exactly the same rows -- identical masks."""
    assert set(completeness_checks.CO_MISSING_CLUSTER) == {
        "passenger_count", "congestion_surcharge", "RatecodeID",
        "store_and_fwd_flag", "Airport_fee",
    }


def test_required_columns_are_a_strict_subset_of_a_trip_record():
    assert "fare_amount" in completeness_checks.REQUIRED_COLUMNS
    assert not set(completeness_checks.REQUIRED_COLUMNS) & set(
        completeness_checks.CO_MISSING_CLUSTER
    ), "a hard requirement cannot also be an advisory budget"


def test_correlation_floor_sits_between_observed_raw_and_cleaned():
    """0.0012 raw, 0.8770 once outliers are bounded out."""
    assert 0.0012 < business_rule_checks.MIN_DISTANCE_FARE_CORRELATION < 0.8770


def test_trip_key_is_a_composite():
    assert len(business_rule_checks.TRIP_KEY) >= 3
    assert "tpep_pickup_datetime" in business_rule_checks.TRIP_KEY


@pytest.mark.parametrize("module", [schema_checks, completeness_checks, business_rule_checks])
def test_every_suite_exposes_a_builder(module):
    assert callable(module.build)
