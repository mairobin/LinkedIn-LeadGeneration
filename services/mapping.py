from __future__ import annotations

from typing import Any, Dict, List, Optional


def _parse_int_shorthand(value: Any) -> Optional[int]:
    """Parse strings like '1.2K', '3M', '4500', '500+' into an integer."""
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


def _parse_connections(value: Any) -> Optional[int]:
    parsed = _parse_int_shorthand(value)
    if parsed is None:
        return None
    return min(parsed, 500)


def map_to_person_schema(profiles: List[Dict[str, Any]], lookup_date: Optional[str]) -> List[Dict[str, Any]]:
    """Map extracted profiles to the Person schema (inline, simple)."""
    mapped: List[Dict[str, Any]] = []
    for p in profiles:
        mapped.append({
            'Contact_Name': p.get('name') or '',
            'LinkedIn_Profile': p.get('profile_url') or None,
            'Company': p.get('company') or None,
            'Company_Website': None,
            'Company_Domain': p.get('company_domain') or None,
            'Location': p.get('location') or None,
            'Position': p.get('current_position') or None,
            'Connections_LinkedIn': _parse_connections(p.get('connection_count')) if p.get('connection_count') is not None else None,
            'Followers_LinkedIn': _parse_int_shorthand(p.get('follower_count')) if p.get('follower_count') is not None else None,
            'Website_Info': None,
            'Phone_Info': p.get('phone') or '',
            'Info_raw': p.get('summary') or '',
            'Insights': p.get('summary_other') if isinstance(p.get('summary_other'), list) else [],
            'Email': p.get('email') or None,
            'Lookup_Date': lookup_date or None,
            'Hot': False,
            'Last_Interaction_Date': None,
            'Status': None,
            'Notes': [],
        })
    return mapped




