"""Constraint suites, grouped by what they assert."""
from src.checks import business_rule_checks, completeness_checks, schema_checks

__all__ = ["schema_checks", "completeness_checks", "business_rule_checks"]


def all_checks(spark):
    """Every suite, ready to hand to a VerificationSuite."""
    return [
        schema_checks.build(spark),
        completeness_checks.build(spark),
        business_rule_checks.build(spark),
    ]
