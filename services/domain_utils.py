from __future__ import annotations

from typing import Optional
import unicodedata


def extract_apex_domain(url_or_domain: Optional[str]) -> Optional[str]:
    if not url_or_domain:
        return None
    try:
        import tldextract
        text = str(url_or_domain).strip().lower()
        if not text.startswith('http://') and not text.startswith('https://'):
            text = f"http://{text}"
        ext = tldextract.extract(text)
        if ext.domain and ext.suffix:
            return f"{ext.domain}.{ext.suffix}"
        return None
    except Exception:
        return None


def normalize_linkedin_profile_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    try:
        from urllib.parse import urlparse, unquote
        u = urlparse(url)
        host = (u.netloc or '').lower().replace('www.', '').replace('de.linkedin.com', 'linkedin.com')
        path = (u.path or '').rstrip('/')
        if not host:
            return None
        if 'linkedin.com' not in host or not path.startswith('/in/'):
            return None
        # Keep only /in/{slug} and drop trailing locale/segments (e.g., /de, /en)
        parts = [p for p in path.split('/') if p]
        if len(parts) >= 2 and parts[0] == 'in':
            slug = parts[1]
            # Decode percent-encoding and normalize Unicode; canonicalize to lowercase
            slug = unquote(slug)
            slug = unicodedata.normalize('NFKC', slug).strip().lower()
            # Remove invisible characters occasionally present
            slug = slug.replace('\u200b', '').replace('\u200c', '').replace('\u200d', '')
            return f"https://linkedin.com/in/{slug}"
        return f"https://linkedin.com{path}"
    except Exception:
        return None


