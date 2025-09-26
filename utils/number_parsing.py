from __future__ import annotations

from typing import Optional


def _parse_int_shorthand(value) -> Optional[int]:
    """Parse strings like '1.2K', '3M', '4500', '500+' into an integer.

    Returns None for unparsable inputs.
    """
    if value is None:
        return None
    try:
        s = str(value).strip().upper()
        if not s:
            return None
        if s.endswith('+'):
            s = s[:-1]
        import re as _re
        m = _re.match(r"^([0-9]+(?:\.[0-9]+)?)([KMB]?)$", s)
        if m:
            num = float(m.group(1))
            suf = m.group(2)
            factor = 1
            if suf == 'K':
                factor = 1000
            elif suf == 'M':
                factor = 1000000
            elif suf == 'B':
                factor = 1000000000
            return int(round(num * factor))
        digits = ''.join(ch for ch in s if ch.isdigit())
        return int(digits) if digits else None
    except Exception:
        return None


def _parse_connections(value) -> Optional[int]:
    parsed = _parse_int_shorthand(value)
    if parsed is None:
        return None
    return min(parsed, 500)



