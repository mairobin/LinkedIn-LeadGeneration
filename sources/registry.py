from __future__ import annotations

from typing import Dict, Type, Any


_REGISTRY: Dict[str, Any] = {}


def register(name: str, factory) -> None:
    _REGISTRY[name] = factory


def get_source(name: str):
    if name not in _REGISTRY:
        raise KeyError(f"Unknown source: {name}")
    return _REGISTRY[name]()


def available_sources() -> Dict[str, Any]:
    return dict(_REGISTRY)



