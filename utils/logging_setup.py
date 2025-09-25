from __future__ import annotations

import logging
import sys
from typing import Any

from config.settings import get_settings


_INITIALIZED: bool = False


class SafeExtraFormatter(logging.Formatter):
    """Formatter that tolerates missing extra fields by injecting defaults."""

    DEFAULTS: dict[str, Any] = {
        "step": "-",
        "status": "-",
        "duration_ms": "-",
        "provider": "-",
        "error": "-",
        "run_id": "-",
    }

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        for key, value in self.DEFAULTS.items():
            if not hasattr(record, key):
                setattr(record, key, value)
        return super().format(record)


def init_logging(level: str | None = None) -> None:
    global _INITIALIZED
    if _INITIALIZED:
        return

    settings = get_settings()
    log_level_str = (level or settings.log_level).upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    if not root_logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)
        formatter = SafeExtraFormatter(
            fmt=(
                "%(asctime)s %(levelname)s %(name)s %(message)s "
                "step=%(step)s status=%(status)s duration_ms=%(duration_ms)s "
                "provider=%(provider)s error=%(error)s run_id=%(run_id)s"
            )
        )
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    _INITIALIZED = True


# Backwards compatibility alias
def setup_logging(level: str | None = None) -> None:
    init_logging(level)


