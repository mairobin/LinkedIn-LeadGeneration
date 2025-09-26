from __future__ import annotations

from pipelines.steps.validate_data import DataValidator


def test_validate_all_companies_filters_empty():
    v = DataValidator()
    companies = [
        {},
        {"Company": "Acme"},
        {"Company_Website": "https://acme.com"},
        {"Company_Domain": "acme.com"},
    ]
    valid = v.validate_all_companies(companies)
    # Expect 3 valid (all but empty)
    assert len(valid) == 3


def test_remove_company_duplicates_prefers_domain():
    v = DataValidator()
    companies = [
        {"Company": "Acme", "Company_Domain": "acme.com"},
        {"Company": "ACME GmbH", "Company_Domain": "acme.com"},
        {"Company": "Acme Berlin", "address": "Berlin"},
        {"Company": "Acme Berlin", "address": "Berlin"},
    ]
    unique = v.remove_company_duplicates(companies)
    # domain-based dedupe reduces first two into one; name+address reduces last two into one
    assert len(unique) == 2


def test_clean_company_data_derives_domain():
    v = DataValidator()
    c = {"Company": "Beta", "Company_Website": "https://www.beta.io/contact"}
    cleaned = v.clean_company_data(c)
    assert cleaned.get("Company_Domain") == "beta.io"



