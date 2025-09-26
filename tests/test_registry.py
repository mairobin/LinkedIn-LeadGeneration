from __future__ import annotations

import importlib
import pytest


def test_builtin_sources_registered():
    # Import package to trigger registration
    import sources  # noqa: F401
    from sources.registry import get_source, available_sources

    names = available_sources().keys()
    assert "linkedin_people_google" in names
    assert "google_maps_companies" in names
    assert "linkedin_companies_google" in names

    src = get_source("linkedin_people_google")
    assert getattr(src, 'entity_type', None) == 'person'


def test_unknown_source_raises():
    from sources.registry import get_source
    with pytest.raises(KeyError):
        get_source("does_not_exist")



