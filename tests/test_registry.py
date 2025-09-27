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



def test_linkedin_people_requires_ai_keys(monkeypatch):
    # Ensure missing keys cause a friendly RuntimeError when AI is enabled
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setenv("AI_ENABLED", "true")
    # Avoid real Google calls by setting dummy keys and small limits
    monkeypatch.setenv("GOOGLE_API_KEY", "dummy")
    monkeypatch.setenv("GOOGLE_CSE_ID", "dummy")
    import sources.linkedin_people as lp
    src = lp.LinkedInPeopleSource()
    raised = False
    try:
        src.run(["Engineer"], 1)
    except RuntimeError as e:
        raised = True
        assert "AI extraction is required" in str(e)
    except Exception:
        raised = True
    assert raised is True

