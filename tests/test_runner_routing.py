from __future__ import annotations

import os
import sys
from types import SimpleNamespace

import importlib


def _run_main_with_args(args_list):
    # Simulate argv and import main fresh
    argv_backup = sys.argv[:]
    try:
        sys.argv = ["main.py"] + args_list
        # Reload main each time to re-parse args
        if "main" in sys.modules:
            del sys.modules["main"]
        import main  # noqa: F401
    finally:
        sys.argv = argv_backup


def test_default_runs_linkedin_people(monkeypatch):
    # Make run fast and deterministic
    monkeypatch.setenv("AI_ENABLED", "false")
    monkeypatch.setenv("RUN_ENV", "test")
    # Monkeypatch LinkedInPeopleSource to return a small person list
    import sources
    from sources.registry import get_source
    src = get_source("linkedin_people_google")

    def _fake_run(terms, max_results):
        return [{"name": "Alice", "profile_url": "https://linkedin.com/in/alice"}]

    src.run = _fake_run  # type: ignore

    # Run main
    _run_main_with_args(["--query", "Engineer Berlin", "--max-results", "1"])  # Should not crash


def test_maps_scaffold_demo_path(monkeypatch):
    monkeypatch.setenv("DEMO", "true")
    # Run companies-only source; should not crash
    _run_main_with_args(["--source", "google_maps_companies", "--terms", "AI", "Berlin"])



