import re
import logging
import json
import html
from datetime import datetime
from typing import Dict, List, Optional
from urllib.parse import urlparse
from config.settings import get_settings
try:
    import openai
    AI_AVAILABLE = True
except ImportError:
    AI_AVAILABLE = False

class AIProfileExtractor:
    """Uses OpenAI to extract structured data from LinkedIn profiles."""

    def __init__(self, api_key: str, model: str = None):
        self.client = openai.OpenAI(api_key=api_key)
        # Per-operation model selection (cost-efficient defaults)
        self.model_chat = "gpt-4o-mini"
        self.model_responses = "gpt-4o-mini"
        logging.info(
            f"AI extractor initialized with models: chat={self.model_chat}, responses={self.model_responses}"
        )
        self.extraction_stats = {
            'ai_extractions_attempted': 0,
            'ai_extractions_successful': 0,
            'ai_extractions_failed': 0,
            'api_calls_made': 0
        }

    def extract_structured_data(self, profile_name: str, profile_title: str, profile_summary: str) -> Dict[str, Optional[str]]:
        """Extract structured profile data using OpenAI."""
        self.extraction_stats['ai_extractions_attempted'] += 1

        prompt = self._create_extraction_prompt(profile_name, profile_title, profile_summary)

        try:
            completion_params = {
                "model": self.model_chat,
                "messages": [
                    {"role": "system", "content": "You are a professional data extraction assistant. Extract structured information from LinkedIn profiles and return only valid JSON."},
                    {"role": "user", "content": prompt}
                ]
            }

            if "gpt-5" not in self.model_chat:
                completion_params["temperature"] = 0

            if (
                "gpt-4o" in self.model_chat
                or "gpt-4" in self.model_chat
                or "gpt-5" in self.model_chat
            ):
                completion_params["max_completion_tokens"] = 500
            else:
                completion_params["max_tokens"] = 500

            response = self.client.chat.completions.create(**completion_params)
            self.extraction_stats['api_calls_made'] += 1

            content = response.choices[0].message.content.strip()
            json_start = content.find('{')
            json_end = content.rfind('}') + 1

            if json_start >= 0 and json_end > json_start:
                json_content = content[json_start:json_end]
                extracted_data = json.loads(json_content)
                cleaned_data = self._validate_extracted_data(extracted_data)
                self.extraction_stats['ai_extractions_successful'] += 1
                logging.debug(f"AI extraction successful for {profile_name}")
                return cleaned_data
            else:
                logging.warning(f"Could not find valid JSON in AI response for {profile_name}")

        except json.JSONDecodeError as e:
            logging.warning(f"Failed to parse AI response JSON for {profile_name}: {e}")
        except Exception as e:
            logging.error(f"AI extraction failed for {profile_name}: {e}")

        self.extraction_stats['ai_extractions_failed'] += 1
        return {}

    def _create_extraction_prompt(self, name: str, title: str, summary: str) -> str:
        """Create a prompt for structured data extraction."""
        return f"""Extract structured information from this LinkedIn profile and return ONLY a valid JSON object:

Name: {name}
Title: {title}
Summary: {summary}

Extract the following fields (return null for any field you cannot determine confidently):

1. current_position: Person's current job title/role
2. company: Current company name
3. location: Geographic location (city, country)
4. follower_count: LinkedIn follower count (if mentioned)
5. connection_count: LinkedIn connection count (if mentioned)

Return format (JSON only, no other text):
{{
    "current_position": "...",
    "company": "...",
    "location": "...",
    "follower_count": "...",
    "connection_count": "..."
}}

COMPANY EXTRACTION RULES (CRITICAL - THESE ARE OBVIOUS PATTERNS):

STOP BEING CONSERVATIVE! These patterns are 100% reliable for current companies:

1. EXPERIENCE/BERUFSERFAHRUNG PATTERNS (HIGHEST CONFIDENCE):
- "Experience: [COMPANY]" → ALWAYS extract COMPANY
- "Berufserfahrung: [COMPANY]" → ALWAYS extract COMPANY ("Berufserfahrung" = German "Experience")
- "· Experience: [COMPANY] ·" → ALWAYS extract COMPANY
- "· Berufserfahrung: [COMPANY] ·" → ALWAYS extract COMPANY

2. JOB TITLE + PREPOSITION PATTERNS:
- "[Title] at [COMPANY]" → extract COMPANY
- "[Title] bei [COMPANY]" → extract COMPANY ("bei" = German "at")
- "[Title] of [COMPANY]" → extract COMPANY
- "[Title] von [COMPANY]" → extract COMPANY ("von" = German "of")
- "[Title] @ [COMPANY]" → extract COMPANY
- "[Title] | [COMPANY]" → extract COMPANY

3. OBVIOUS EXAMPLES YOU MUST EXTRACT:
- "Experience: ElringKlinger" → company: "ElringKlinger"
- "Experience: IPETRONIK GmbH & Co. KG" → company: "IPETRONIK GmbH & Co. KG"
- "Experience: Daimler Truck AG" → company: "Daimler Truck AG"
- "Berufserfahrung: MAN Truck & Bus SE" → company: "MAN Truck & Bus SE"
- "Berufserfahrung: Boerse Stuttgart Group" → company: "Boerse Stuttgart Group"
- "Experience: Eviden" → company: "Eviden"
- "Head of Sales at Daimler Buses" → company: "Daimler Buses"
- "CEO at Germany Trade & Invest" → company: "Trade & Invest"

4. CONFIDENCE INDICATORS (EXTRACT IMMEDIATELY):
✓ Text after "Experience:" or "Berufserfahrung:"
✓ Text after: at, bei, of, von, @, |
✓ Company suffixes: GmbH, AG, Inc, LLC, Ltd, Corp, Group, SE
✓ Proper nouns after job titles

5. ONLY AVOID IF:
✗ Explicitly marked as past: "former", "previously", "ex-"
✗ Educational institutions: "University", "School", "Institut" (unless they work there)
✗ Geographic locations without company context

CRITICAL: If you see "Experience:" or "Berufserfahrung:" followed by a company name, ALWAYS extract it. These are current employer indicators with 99% confidence.

OTHER RULES:
- For follower_count: look for patterns like "1K followers", "4540 Follower", "2.5K followers"
- For connection_count: look for patterns like "500+ connections", "1K+ Kontakte", "5000 connections"
- For location: prefer current location over past locations
- Extract exact numbers/text as shown (e.g. "1K", "500+", "2.5K")
- Use null only if you really cannot find the company with confidence
- Return only the JSON object, no explanations"""

    def _validate_extracted_data(self, data: Dict) -> Dict[str, Optional[str]]:
        """Validate and clean AI-extracted data."""
        cleaned = {}
        fields = ['current_position', 'company', 'location', 'follower_count', 'connection_count']

        for field in fields:
            value = data.get(field)
            if value and isinstance(value, str) and value.strip().lower() not in ['null', 'none', 'n/a', '']:
                cleaned_value = value.strip()
                if len(cleaned_value) > 1 and len(cleaned_value) < 200:
                    cleaned[field] = cleaned_value

        return cleaned

    def get_company_website(self, company_name: str, location: str = None) -> Optional[str]:
        """Find company website using three-tier cost optimization: domain prediction → AI knowledge → web search."""
        if not company_name or len(company_name.strip()) < 2:
            return None

        # First try: Domain prediction (FREE - fastest)
        logging.info(f"Trying domain prediction for {company_name}")
        website_url = self._predict_company_domain(company_name)
        
        if website_url:
            logging.info(f"Found website using domain prediction: {website_url}")
            return website_url

        # Second try: Knowledge-based AI (CHEAP)
        logging.info(f"Domain prediction failed, trying knowledge-based lookup for {company_name}")
        website_url = self._get_company_website_fallback(company_name, location)
        
        if website_url:
            logging.info(f"Found website using knowledge-based approach: {website_url}")
            return website_url

        # Third try: Web search AI (EXPENSIVE - last resort)
        logging.info(f"Knowledge-based lookup failed, trying web search for {company_name}")
        
        try:
            # Use OpenAI Responses API with web search tool
            response = self.client.responses.create(
                model=self.model_responses,
                tools=[{"type": "web_search"}],
                input=f"Find the official website URL of the company '{company_name.strip()}'. Return only the URL."
            )

            # Extract the model output - find the message in the output
            content = None
            for item in response.output:
                if hasattr(item, 'type') and item.type == 'message':
                    if hasattr(item, 'content') and len(item.content) > 0:
                        content = item.content[0].text.strip()
                        break
            
            if not content:
                logging.warning(f"Could not extract content from web search response for {company_name}")
                return None

            # If the response indicates uncertainty, return None
            if any(word in content.lower() for word in ['unknown', 'not sure', 'cannot', "don't know", 'unclear', 'not found', 'unable to find']):
                logging.info(f"Web search API couldn't find confident result for {company_name}")
                return None

            website_url = self._extract_and_validate_url(content)
            if website_url:
                logging.info(f"Found website using web search API: {website_url}")
            else:
                logging.info(f"Web search API found response but couldn't extract valid URL for {company_name}")
            
            return website_url

        except AttributeError:
            logging.warning(f"Responses API not available for {company_name}")
            return None
        except Exception as e:
            logging.warning(f"Failed to find website for {company_name} using web search: {e}")
            return None

    def _predict_company_domain(self, company_name: str) -> Optional[str]:
        """Predict company domain using common patterns (FREE approach)."""
        try:
            import socket
            import requests
        except ImportError:
            logging.warning("Required libraries for domain prediction not available")
            return None
        
        clean_name = self._clean_company_name(company_name)
        if not clean_name:
            return None
        
        # Generate domain candidates
        candidates = [
            f"{clean_name}.com",
            f"{clean_name}.de",  # German companies
            f"{clean_name}.io",  # Tech companies
            f"{clean_name}.ai",  # AI companies
            f"{clean_name}.org",
            f"{clean_name.replace(' ', '')}.com",
            f"{clean_name.replace(' ', '-')}.com",
            f"{clean_name.replace(' ', '')}.de",
            f"{clean_name.replace(' ', '')}.io",
            f"{clean_name.replace(' ', '')}.ai"
        ]
        
        # Test each candidate
        for domain in candidates:
            if self._validate_domain(domain):
                return f"https://{domain}"
        
        return None
    
    def _clean_company_name(self, name: str) -> Optional[str]:
        """Clean company name for domain prediction."""
        if not name:
            return None
        
        name = name.lower().strip()
        
        # Handle German umlauts
        name = name.replace('ä', 'ae').replace('ö', 'oe').replace('ü', 'ue')
        name = name.replace('ß', 'ss')
        
        # Remove legal suffixes
        suffixes = ['gmbh', 'ltd', 'inc', 'corp', 'ag', 'co', 'kg', '& co', 'se', 'llc']
        for suffix in suffixes:
            name = name.replace(f' {suffix}', '').replace(f' & {suffix}', '')
        
        # Remove special characters
        import re
        name = re.sub(r'[^a-z0-9\s-]', '', name)
        name = name.strip()
        
        # Must be reasonable length
        if len(name) < 2 or len(name) > 50:
            return None
            
        return name
    
    def _validate_domain(self, domain: str) -> bool:
        """Validate if domain exists and is accessible."""
        try:
            import socket
            import requests
            
            # Quick DNS check
            socket.gethostbyname(domain)
            
            # HTTP accessibility check with short timeout
            response = requests.head(f"https://{domain}", timeout=2, allow_redirects=True)
            return 200 <= response.status_code < 400
            
        except Exception:
            # Try HTTP instead of HTTPS
            try:
                response = requests.head(f"http://{domain}", timeout=2, allow_redirects=True)
                return 200 <= response.status_code < 400
            except Exception:
                return False

    def _get_company_website_fallback(self, company_name: str, location: str = None) -> Optional[str]:
        """Fallback method using knowledge base when web search is unavailable."""
        try:
            company_desc = f"{company_name.strip()}"
            if location:
                company_desc += f" based in {location.strip()}"

            response = self.client.chat.completions.create(
                model=self.model_chat,
                messages=[
                    {"role": "system", "content": "You are a company information assistant. Based on your knowledge, provide official company websites for well-known companies. Only return websites you are confident about."},
                    {"role": "user", "content": f"What is the official website URL for {company_desc}? Return only the main website URL (format: https://example.com) or say 'unknown' if you're not confident. Do not return Wikipedia, LinkedIn, or social media URLs."}
                ],
                max_completion_tokens=50,
                temperature=0
            )

            content = response.choices[0].message.content.strip()

            # If the response indicates uncertainty, return None
            if any(word in content.lower() for word in ['unknown', 'not sure', 'cannot', "don't know", 'unclear', 'not found', 'confident']):
                return None

            return self._extract_and_validate_url(content)

        except Exception as e:
            logging.warning(f"Fallback website lookup failed for {company_name}: {e}")
            return None

    def _extract_and_validate_url(self, content: str) -> Optional[str]:
        """Extract and validate URL from response."""
        import re

        # Look for URLs in the response
        url_pattern = r'https?://(?:www\.)?([a-zA-Z0-9-]+\.[a-zA-Z]{2,})'
        urls = re.findall(url_pattern, content)

        # Filter out unwanted sites
        excluded = ['wikipedia', 'linkedin', 'facebook', 'twitter', 'crunchbase', 'xing']

        for url in urls:
            if not any(excluded_site in url.lower() for excluded_site in excluded):
                return f"https://{url}"

        return None

    def get_extraction_stats(self) -> Dict:
        """Return AI extraction statistics."""
        return self.extraction_stats.copy()


class LinkedInDataExtractor:
    def __init__(self, use_ai: bool = False, openai_api_key: str = None, openai_model: str = "gpt-3.5-turbo"):
        self.extraction_stats = {
            'successful_extractions': 0,
            'failed_extractions': 0,
            'duplicate_profiles_removed': 0
        }
        self.seen_urls = set()
        self.use_ai = use_ai and AI_AVAILABLE

        if self.use_ai and openai_api_key:
            self.ai_extractor = AIProfileExtractor(openai_api_key, openai_model)
            logging.info("AI-powered extraction enabled")
        else:
            self.ai_extractor = None
            if use_ai:
                logging.warning("AI extraction requested but OpenAI not available or no API key provided")

    def clean_linkedin_url(self, url: str) -> Optional[str]:
        """Clean and validate LinkedIn URL."""
        if not url:
            return None

        # Remove any query parameters and fragments
        parsed = urlparse(url)
        clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"

        # Ensure it's a LinkedIn profile URL
        settings = get_settings()
        if any(pattern in clean_url.lower() for pattern in settings.linkedin_url_patterns):
            # Normalize to https://linkedin.com format
            clean_url = clean_url.replace('de.linkedin.com', 'linkedin.com')
            clean_url = clean_url.replace('www.linkedin.com', 'linkedin.com')
            if not clean_url.startswith('https://'):
                clean_url = clean_url.replace('http://', 'https://')
            # Drop trailing locale or extra segments after slug
            try:
                parsed2 = urlparse(clean_url)
                path = (parsed2.path or '').rstrip('/')
                parts = [p for p in path.split('/') if p]
                if len(parts) >= 2 and parts[0] == 'in':
                    slug = parts[1]
                    return f"https://linkedin.com/in/{slug}"
            except Exception:
                pass
            return clean_url

        return None

    def extract_name_from_metatags(self, metatags: List[Dict]) -> Optional[str]:
        """Extract person's name from LinkedIn metatags (more reliable)."""
        if not metatags:
            return None
            
        for tag in metatags:
            # Try to get first and last name from profile metatags
            first_name = tag.get('profile:first_name', '').strip()
            last_name = tag.get('profile:last_name', '').strip()
            
            if first_name and last_name:
                return f"{first_name} {last_name}"
                
            # Fallback to og:title which usually has the full title
            og_title = tag.get('og:title', '').strip()
            if og_title:
                # Remove " | LinkedIn" suffix and extract name
                name = re.sub(r'\s*\|\s*LinkedIn.*$', '', og_title, flags=re.IGNORECASE)
                # Split on dash and take the first part (name)
                if ' - ' in name:
                    name = name.split(' - ')[0].strip()
                if len(name) > 2 and len(name) < 200:
                    return name
        
        return None

    def extract_name_from_title(self, title: str) -> Optional[str]:
        """Extract person's name from Google search result title (fallback method)."""
        if not title:
            return None

        # Common patterns for LinkedIn titles
        # "John Doe - LinkedIn"
        # "John Doe | LinkedIn"
        # "John Doe (@username) - LinkedIn"
        # "John Doe – Software Engineer..."

        # Remove " - LinkedIn" or " | LinkedIn" suffix
        name = re.sub(r'\s*[-|]\s*LinkedIn.*$', '', title, flags=re.IGNORECASE)

        # Remove any parenthetical content like (@username)
        name = re.sub(r'\s*\([^)]*\)\s*', ' ', name)

        # For long titles that include job descriptions, try to extract just the name part
        # Look for patterns like "Name – Job Title" or "Name - Job Title"
        if '–' in name or '—' in name:
            # Split on em dash or en dash and take the first part
            name_parts = re.split(r'[–—]', name)
            if name_parts:
                name = name_parts[0].strip()

        # Clean up extra whitespace
        name = ' '.join(name.split())

        # Basic validation - should be reasonable length
        if len(name) > 2 and len(name) < 200:
            return name

        return None

    def extract_headline_from_title(self, title: str, name: str) -> Optional[str]:
        """Extract professional headline from title by removing the name prefix."""
        if not title or not name:
            return None

        # Remove " - LinkedIn" or " | LinkedIn" suffix first
        clean_title = re.sub(r'\s*[-|]\s*LinkedIn.*$', '', title, flags=re.IGNORECASE)

        # Try to remove the name from the beginning of the title
        # Handle various separators: dash, em dash, en dash, pipe
        if name in clean_title:
            # Find the name and remove it along with following separators
            pattern = re.escape(name) + r'\s*[–—\-|]\s*'
            headline = re.sub(pattern, '', clean_title, flags=re.IGNORECASE).strip()

            # If we got a reasonable headline, return it
            if len(headline) > 3 and len(headline) < 200:
                return headline

        # Fallback: if title has separators, take everything after the first one
        separators = ['–', '—', '-', '|']
        for sep in separators:
            if sep in clean_title:
                parts = clean_title.split(sep, 1)
                if len(parts) > 1:
                    headline = parts[1].strip()
                    if len(headline) > 3:
                        return headline

        return None

    def _remove_linkedin_boilerplate(self, text: Optional[str]) -> Optional[str]:
        """Remove LinkedIn boilerplate sentences like
        "View John Doe’s profile on LinkedIn, a professional community of 1 billion members."
        and localized variants from provided text.

        The function preserves other content and normalizes whitespace.
        """
        if not text or not isinstance(text, str):
            return text

        cleaned = text

        patterns = [
            # English variants with straight or curly apostrophes
            r"View [^\n\.!?]{1,200}?’s profile on LinkedIn[^\.!?\n]*[\.!?]",
            r"View [^\n\.!?]{1,200}?'s profile on LinkedIn[^\.!?\n]*[\.!?]",
            r"View [^\n\.!?]{1,200}? profile on LinkedIn, a professional community of [^\.!?\n]*[\.!?]",
            # German common snippet variant
            r"Sehen Sie sich das Profil von [^\n\.!?]{1,200}? auf LinkedIn[^\.!?\n]*[\.!?]",
        ]

        for pattern in patterns:
            try:
                cleaned = re.sub(pattern, " ", cleaned, flags=re.IGNORECASE)
            except re.error:
                # In case of a regex error, skip that pattern
                continue

        # Collapse excessive whitespace and normalize newlines
        cleaned = re.sub(r"[ \t\u00A0]{2,}", " ", cleaned)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
        cleaned = cleaned.strip()

        return cleaned

    def _normalize_text_basic(self, text: Optional[str]) -> Optional[str]:
        """Decode HTML entities, remove list bullets, normalize whitespace and tabs."""
        if not text or not isinstance(text, str):
            return text

        # Decode HTML entities like &amp;
        normalized = html.unescape(text)

        # Replace tabs and non-breaking spaces
        normalized = normalized.replace('\t', ' ').replace('\u00A0', ' ')

        # Remove common bullet characters when they are used as prefixes
        normalized = re.sub(r"^[\s]*[\-\*•·–—›>\u2022\u2023\u25E6\u2043\u2219]+\s*", "", normalized, flags=re.MULTILINE)

        # Collapse multiple spaces
        normalized = re.sub(r"[ ]{2,}", " ", normalized)

        return normalized.strip()

    def extract_basic_info_from_snippet(self, snippet: str) -> Dict[str, Optional[str]]:
        """Extract basic information from the Google search snippet."""
        info = {
            'title': None,
            'company': None,
            'location': None,
            'description': None
        }

        if not snippet:
            return info

        # Common patterns in LinkedIn snippets:
        # "Location · Title · Company Description..."
        # "Title at Company · Location Description..."

        # Try to extract location (often contains country/state patterns)
        location_patterns = [
            r'([A-Z][a-zA-Z\s,-]+(?:Deutschland|Germany|Austria|Switzerland|USA|United States))',
            r'([A-Z][a-zA-Z\s,-]+,\s*[A-Z]{2,})',  # City, State/Country
            r'([A-Z][a-zA-Z\s]+,\s*[A-Z][a-zA-Z\s]+)'  # City, Region
        ]

        for pattern in location_patterns:
            match = re.search(pattern, snippet)
            if match:
                info['location'] = match.group(1).strip()
                break

        # Try to extract job title patterns
        title_patterns = [
            r'(Senior\s+[A-Z][a-zA-Z\s]+Engineer)',
            r'(Software\s+Engineer)',
            r'(Lead\s+[A-Z][a-zA-Z\s]+)',
            r'([A-Z][a-zA-Z\s]*Engineer)',
            r'([A-Z][a-zA-Z\s]*Developer)',
            r'([A-Z][a-zA-Z\s]*Manager)',
            r'([A-Z][a-zA-Z\s]*Director)'
        ]

        for pattern in title_patterns:
            match = re.search(pattern, snippet)
            if match:
                info['title'] = match.group(1).strip()
                break

        # Store the full snippet as description for future AI processing
        info['description'] = snippet.strip()

        return info

    def extract_profile_info_from_metatags(self, metatags: List[Dict]) -> Dict[str, Optional[str]]:
        """Extract profile information using robust, generalizable patterns."""
        info = {
            'current_position': None,
            'company': None,
            'location': None,
            'description': None
        }

        if not metatags:
            return info

        for tag in metatags:
            # Get the full title and description
            og_title = tag.get('og:title', '').strip()
            og_description = tag.get('og:description', '').strip()

            if og_description:
                info['description'] = og_description

            # Extract position from title using robust approach
            if og_title and not info['current_position']:
                # Remove " | LinkedIn" suffix first
                title_clean = re.sub(r'\s*\|\s*LinkedIn.*$', '', og_title, flags=re.IGNORECASE)

                # Simple approach: everything after the first separator is likely the position
                separators = ['–', '—', '-', '|']
                for sep in separators:
                    if sep in title_clean:
                        parts = title_clean.split(sep, 1)
                        if len(parts) > 1:
                            position_part = parts[1].strip()
                            # Take first meaningful chunk as position (before next separator)
                            position_parts = re.split(r'[|·•]', position_part)
                            if position_parts and len(position_parts[0].strip()) > 3:
                                info['current_position'] = position_parts[0].strip()
                            break

            # Extract company using broad patterns - be permissive rather than restrictive
            if og_description and not info['company']:
                # Look for common company indicators (cast a wide net)
                company_indicators = [
                    r'Experience:\s*([^·\n•|]+)',
                    r'(?:Currently|Currently working|Working)\s+(?:at|for|with)\s+([^·\n•|.,]+)',
                    r'(?:Engineer|Developer|Manager|Director|Analyst|Consultant|Scientist)\s+(?:at|@)\s+([^·\n•|.,]+)',
                    r'(?:^|\n)([A-Z][A-Za-z\s&.,\'-]+?)(?:\s*[·•]|\s*\n|\s*$)',  # Company names at start of lines
                ]

                for pattern in company_indicators:
                    matches = re.finditer(pattern, og_description, re.MULTILINE | re.IGNORECASE)
                    for match in matches:
                        potential_company = match.group(1).strip()

                        # Basic quality filters (permissive)
                        if (len(potential_company) > 2 and
                            len(potential_company) < 100 and
                            not potential_company.lower().startswith(('the ', 'a ', 'an ')) and
                            not re.match(r'^[0-9+\s]+$', potential_company)):  # Not just numbers

                            # Clean up
                            potential_company = re.sub(r'\s*[·•,].*$', '', potential_company)
                            potential_company = potential_company.strip()

                            if len(potential_company) > 2:
                                info['company'] = potential_company
                                break

                    if info['company']:
                        break

            # Extract location using broad geographic patterns
            if og_description and not info['location']:
                location_patterns = [
                    r'Location:\s*([^·\n•|]+)',
                    r'Based in\s+([^·\n•|.,]+)',
                    r'Located in\s+([^·\n•|.,]+)',
                    # Geographic patterns - major cities and countries
                    r'\b((?:New York|London|Berlin|Munich|Hamburg|Stuttgart|Frankfurt|Paris|Tokyo|Singapore|Sydney|Toronto|Chicago|Boston|Seattle|San Francisco|Los Angeles|Amsterdam|Zurich|Vienna|Barcelona|Madrid|Rome|Milan|Stockholm|Copenhagen|Helsinki|Oslo|Dublin|Edinburgh|Manchester|Birmingham|Leeds|Glasgow|Cardiff|Belfast)[^·\n•|.,]*)',
                    r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*(?:Germany|UK|USA|Canada|France|Italy|Spain|Netherlands|Sweden|Norway|Denmark|Switzerland|Austria|Australia|Japan|Singapore))',
                ]

                for pattern in location_patterns:
                    match = re.search(pattern, og_description, re.IGNORECASE)
                    if match:
                        location = match.group(1).strip()
                        if len(location) > 2 and len(location) < 100:
                            info['location'] = location
                            break

        return info

    def extract_raw_profile_data(self, search_result_item: Dict) -> Optional[Dict]:
        """Extract raw profile data from a single Google search result."""
        google_result = search_result_item.get('google_result', {})
        search_metadata = search_result_item.get('search_metadata', {})

        # Extract basic fields from Google result
        title = google_result.get('title', '')
        link = google_result.get('link', '')
        snippet = google_result.get('snippet', '')
        # Clean LinkedIn boilerplate from snippet early
        snippet = self._remove_linkedin_boilerplate(snippet)

        # Clean and validate LinkedIn URL
        linkedin_url = self.clean_linkedin_url(link)
        if not linkedin_url:
            logging.warning(f"Invalid LinkedIn URL: {link}")
            self.extraction_stats['failed_extractions'] += 1
            return None

        # Check for duplicates
        if linkedin_url in self.seen_urls:
            logging.info(f"Duplicate profile found: {linkedin_url}")
            self.extraction_stats['duplicate_profiles_removed'] += 1
            return None

        self.seen_urls.add(linkedin_url)

        # Try to extract name from metatags first (more reliable), then fallback to title
        metatags = google_result.get('pagemap', {}).get('metatags', [])
        name = self.extract_name_from_metatags(metatags)
        if not name:
            name = self.extract_name_from_title(title)
        
        if not name:
            logging.warning(f"Could not extract name from title or metatags: {title}")
            self.extraction_stats['failed_extractions'] += 1
            return None

        # Extract headline from title (remove name prefix)
        headline = self.extract_headline_from_title(title, name)

        # Get best summary (prefer metatags description over snippet)
        metatags_info = self.extract_profile_info_from_metatags(metatags)
        snippet_info = self.extract_basic_info_from_snippet(snippet)
        summary = metatags_info.get('description') or snippet_info.get('description') or snippet
        # Clean LinkedIn boilerplate and normalize summary
        summary = self._normalize_text_basic(self._remove_linkedin_boilerplate(summary))

        # Use AI extraction if available, otherwise preserve raw data
        if self.use_ai and self.ai_extractor:
            # Include snippet for better follower/connection data extraction
            enhanced_summary = f"{summary}\n\nGoogle Snippet: {snippet}"
            ai_extracted = self.ai_extractor.extract_structured_data(name, title, enhanced_summary)
            current_position = ai_extracted.get('current_position') or headline
            company = ai_extracted.get('company')
            location = ai_extracted.get('location')
            follower_count = ai_extracted.get('follower_count')
            connection_count = ai_extracted.get('connection_count')

            # If AI failed to extract follower/connection data, try regex fallback
            if not follower_count or not connection_count:
                follower_connection_data = self.extract_follower_and_connection_data(google_result)
                follower_count = follower_count or follower_connection_data.get('follower_count')
                connection_count = connection_count or follower_connection_data.get('connection_count')
        else:
            # Fallback: preserve all available raw data instead of forcing structure
            current_position = headline  # Simple headline extraction
            company = None  # Don't force company extraction
            location = None  # Don't force location extraction

        # Build profile data structure
        if self.use_ai and self.ai_extractor:
            # AI extraction: structured format
            profile_data = {
                'name': name,
                'profile_url': linkedin_url,
                'summary': summary
            }

            # Add AI-extracted fields only if meaningful
            if current_position and len(current_position.strip()) > 3:
                profile_data['current_position'] = current_position.strip()
            if company and len(company.strip()) > 2:
                profile_data['company'] = company.strip()
            if location and len(location.strip()) > 2:
                profile_data['location'] = location.strip()
            if follower_count and len(follower_count.strip()) > 0:
                profile_data['follower_count'] = follower_count.strip()
            if connection_count and len(connection_count.strip()) > 0:
                profile_data['connection_count'] = connection_count.strip()
            # Parse summary-derived fields (email, website, phone, experience_years, summary_other)
            parsed_summary = self._extract_from_summary(summary)
            # Normalize summary_other if present
            if 'summary_other' in parsed_summary and isinstance(parsed_summary['summary_other'], list):
                parsed_summary['summary_other'] = self._clean_summary_other(parsed_summary['summary_other'])
            profile_data.update(parsed_summary)

        else:
            # Fallback: preserve essential raw data with smart deduplication
            profile_data = {
                'name': name,
                'profile_url': linkedin_url,
                'summary': summary
            }

            # Add headline as current_position if meaningful and different from summary
            if headline and len(headline.strip()) > 3:
                profile_data['current_position'] = headline.strip()

            # Create a smart raw_data section that preserves unique information
            raw_data = {}


            # Only include title if it's different from headline/summary
            if title and title != headline and not self._is_similar_text(title, summary[:100]):
                raw_data['title'] = title

            # Only include snippet if it's meaningfully different from summary
            if snippet and not self._is_similar_text(snippet, summary[:200]):
                raw_data['snippet'] = snippet

            # Extract any unique structured data from metatags/snippets
            unique_structured_data = self._extract_unique_structured_data(metatags_info, snippet_info)

            # Extract follower and connection data
            follower_data = self.extract_follower_and_connection_data(google_result)
            if follower_data:
                unique_structured_data.update(follower_data)

            if unique_structured_data:
                raw_data['structured_data'] = unique_structured_data

            # Only add raw_data if it contains unique information
            if raw_data:
                profile_data['raw_data'] = raw_data

            # Parse summary-derived fields (email, website, phone, experience_years, summary_other)
            parsed_summary = self._extract_from_summary(summary)
            # Normalize summary_other if present
            if 'summary_other' in parsed_summary and isinstance(parsed_summary['summary_other'], list):
                parsed_summary['summary_other'] = self._clean_summary_other(parsed_summary['summary_other'])
            profile_data.update(parsed_summary)

        self.extraction_stats['successful_extractions'] += 1
        logging.debug(f"Successfully extracted profile: {name}")

        return profile_data

    def _clean_summary_other(self, items: List[str]) -> List[str]:
        """Clean list of summary_other sentences: remove boilerplate, bullets, html entities, and empties."""
        cleaned_items: List[str] = []
        seen = set()
        for s in items:
            if not s or not isinstance(s, str):
                continue
            txt = self._normalize_text_basic(self._remove_linkedin_boilerplate(s))
            # Drop trivial single bullet markers or dangling punctuation
            if not txt or len(txt) < 2:
                continue
            # Deduplicate preserving order
            if txt not in seen:
                seen.add(txt)
                cleaned_items.append(txt)
        return cleaned_items[:5]

    def _extract_from_summary(self, summary_text: str) -> Dict[str, Optional[str]]:
        """Extract email, website, phone number, years of experience, and summary_other from summary text.

        - email: first valid email found
        - website: first non-linkedin, non-social top-level URL found
        - phone: first valid-looking phone number (E.164 or common international)
        - experience_years: numeric years detected from patterns like "10+ years", "over 7 years"
        - summary_other: list of valuable sentences not captured elsewhere (max 5)
        """
        result: Dict[str, Optional[str]] = {}
        if not summary_text or not isinstance(summary_text, str):
            return result

        text = summary_text.strip()

        # Email extraction
        try:
            email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
            if email_match:
                result['email'] = email_match.group(0)
        except Exception:
            pass

        # URL extraction (prefer personal websites; skip linkedin and common socials)
        try:
            url_pattern = r"https?://[\w.-]+\.[A-Za-z]{2,}(?:/[\w\-./?%&=]*)?"
            urls = re.findall(url_pattern, text)
            excluded_domains = ['linkedin.com', 'twitter.com', 'x.com', 'facebook.com', 'instagram.com', 'youtube.com', 'medium.com']
            website = None
            for url in urls:
                lowered = url.lower()
                if not any(domain in lowered for domain in excluded_domains):
                    website = url
                    break
            if not website:
                # Also consider bare domains without protocol (e.g., example.com)
                bare_pattern = r"\b(?:[a-zA-Z0-9-]+\.)+[A-Za-z]{2,}\b"
                for bare in re.findall(bare_pattern, text):
                    lowered = bare.lower()
                    if not any(domain in lowered for domain in excluded_domains + ['wikipedia.org']):
                        website = f"https://{bare}"
                        break
            if website:
                result['website'] = website
        except Exception:
            pass

        # Phone extraction (simple but robust; allows +country and spaces)
        try:
            phone_pattern = r"(?:(?:\+|00)\d{1,3}[\s-]?)?(?:\(?\d{2,4}\)?[\s-]?)?\d{3,4}[\s-]?\d{3,4}"
            phone_match = re.search(phone_pattern, text)
            if phone_match:
                phone = phone_match.group(0)
                # Basic filter to avoid catching years or counts; require at least 7 digits total
                digits = re.sub(r"\D", "", phone)
                if len(digits) >= 7:
                    result['phone'] = phone.strip()
        except Exception:
            pass

        # Years of experience extraction
        try:
            exp_patterns = [
                r"(\d{1,2})\s*\+?\s*years?\s+of\s+experience",
                r"over\s+(\d{1,2})\s+years",
                r"(\d{1,2})\s*\+\s*years",
                r"(\d{1,2})\s*years",  # last resort
            ]
            experience_years = None
            for pattern in exp_patterns:
                m = re.search(pattern, text, re.IGNORECASE)
                if m:
                    try:
                        experience_years = int(m.group(1))
                        break
                    except Exception:
                        continue
            if experience_years is not None:
                result['experience_years'] = experience_years
        except Exception:
            pass

        # summary_other: pick top up to 5 sentences with signals (numbers, named entities-like)
        try:
            sentences = re.split(r"(?<=[.!?])\s+|\n+", text)
            candidates: List[str] = []
            for s in sentences:
                s_clean = s.strip()
                if not s_clean:
                    continue
                # Skip if it mostly duplicates extracted items
                if 'email' in result and result['email'] and result['email'] in s_clean:
                    continue
                if 'website' in result and result['website'] and str(result['website']).replace('https://','').replace('http://','') in s_clean.lower():
                    continue
                if 'phone' in result and result['phone'] and result['phone'] in s_clean:
                    continue
                # Heuristics: sentences with numbers, capitalized words (proper nouns), or action verbs
                has_number = bool(re.search(r"\d", s_clean))
                has_proper = bool(re.search(r"\b[A-Z][a-z]{2,}\b(?:\s+[A-Z][a-z]{2,})?", s_clean))
                if has_number or has_proper:
                    candidates.append(s_clean)
            if candidates:
                # Deduplicate while preserving order
                seen = set()
                uniq = []
                for c in candidates:
                    if c not in seen:
                        seen.add(c)
                        uniq.append(c)
                result['summary_other'] = uniq[:5]
        except Exception:
            pass

        return result

    def _is_similar_text(self, text1: str, text2: str, threshold: float = 0.8) -> bool:
        """Check if two texts are similar enough to be considered duplicates."""
        if not text1 or not text2:
            return False

        # Simple similarity check: if one text contains most of the other
        shorter = min(text1, text2, key=len)
        longer = max(text1, text2, key=len)

        if len(shorter) < 10:  # Too short to compare meaningfully
            return text1.strip() == text2.strip()

        # If shorter text is mostly contained in longer text, consider them similar
        words_shorter = set(shorter.lower().split())
        words_longer = set(longer.lower().split())

        if len(words_shorter) == 0:
            return False

        overlap = len(words_shorter.intersection(words_longer))
        similarity = overlap / len(words_shorter)

        return similarity >= threshold

    def _extract_unique_structured_data(self, metatags_info: Dict, snippet_info: Dict) -> Dict:
        """Extract unique structured data points that aren't already captured."""
        unique_data = {}

        # Combine all structured data
        all_data = {}
        if metatags_info:
            all_data.update(metatags_info)
        if snippet_info:
            # Only add snippet info if it's different from metatag info
            for key, value in snippet_info.items():
                if key not in all_data or (value and not self._is_similar_text(str(value), str(all_data[key]))):
                    all_data[f"snippet_{key}"] = value

        # Filter to only meaningful, unique data points
        for key, value in all_data.items():
            if value and isinstance(value, str) and len(value.strip()) > 2:
                # Skip description fields as they're likely in summary already
                if 'description' not in key.lower():
                    unique_data[key] = value.strip()

        return unique_data

    def extract_follower_and_connection_data(self, google_result: Dict) -> Dict[str, Optional[str]]:
        """Extract follower count and connection data from Google search results."""
        follower_data = {}

        # Check snippet for follower count
        snippet = google_result.get('snippet', '')
        html_snippet = google_result.get('htmlSnippet', '')

        # Look for follower patterns in snippet
        follower_patterns = [
            r'(\d+(?:\.\d+)?[KMB]?)\s+followers?',  # "1K followers", "4540 followers"
            r'Ca\.\s+(\d+(?:\.\d+)?[KMB]?)\s+Follower',  # German "Ca. 4540 Follower"
        ]

        for pattern in follower_patterns:
            for text in [snippet, html_snippet]:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    follower_data['follower_count'] = match.group(1)
                    break
            if 'follower_count' in follower_data:
                break

        # Check og:description for connections
        pagemap = google_result.get('pagemap', {})
        metatags = pagemap.get('metatags', [])

        for tag in metatags:
            og_description = tag.get('og:description', '')
            if og_description:
                # Look for connection patterns
                connection_patterns = [
                    r'(\d+\+?)\s+connections?',  # "500+ connections"
                    r'(\d+(?:\.\d+)?[KMB]?)\s+Kontakte',  # German "500 Kontakte"
                ]

                for pattern in connection_patterns:
                    match = re.search(pattern, og_description, re.IGNORECASE)
                    if match:
                        follower_data['connection_count'] = match.group(1)
                        break

        return follower_data

    def extract_all_profiles(self, search_results: List[Dict]) -> List[Dict]:
        """Extract profile data from all search results."""
        profiles = []

        logging.info(f"Starting extraction from {len(search_results)} search results")

        for result in search_results:
            profile = self.extract_raw_profile_data(result)
            if profile:
                profiles.append(profile)

        logging.info(f"Extraction completed. Successful: {self.extraction_stats['successful_extractions']}, "
                    f"Failed: {self.extraction_stats['failed_extractions']}, "
                    f"Duplicates removed: {self.extraction_stats['duplicate_profiles_removed']}")

        return profiles

    def enhance_profiles_with_websites(self, profiles: List[Dict]) -> List[Dict]:
        """Enhance profiles with company domain information (derive apex domain)."""
        if not self.use_ai or not self.ai_extractor:
            logging.warning("AI extractor not available for domain enhancement")
            return profiles

        def _extract_apex_domain(url_or_domain: Optional[str]) -> Optional[str]:
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

        enhanced_profiles = []
        for profile in profiles:
            enhanced_profile = profile.copy()

            company = profile.get('company')
            location = profile.get('location')

            if company:
                website = self.ai_extractor.get_company_website(company, location)
                if website:
                    domain = _extract_apex_domain(website)
                    if domain:
                        enhanced_profile['company_domain'] = domain
                        logging.debug(f"Derived apex domain for {company}: {domain}")

            enhanced_profiles.append(enhanced_profile)

        return enhanced_profiles

    def get_extraction_stats(self) -> Dict:
        """Return extraction statistics."""
        return self.extraction_stats.copy()
