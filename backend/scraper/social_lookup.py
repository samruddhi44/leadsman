from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import parse_qs, quote, unquote, urlparse
import json
import re

from backend.state import (
    add_log,
    add_result,
    increment_current,
    is_stopped,
    set_running,
    set_total,
)
from backend.scraper.utils import (
    clean_email,
    clean_phone,
    clean_text,
    close_browser,
    extract_emails_from_text,
    extract_phones_from_text,
    goto_and_wait,
    goto_with_retry,
    get_domain,
    sleep_small,
    start_browser,
    wait_for_any_selector,
)

PLATFORM_DOMAINS = {
    "facebook": "facebook.com",
    "instagram": "instagram.com",
    "linkedin": "linkedin.com",
    "youtube": "youtube.com",
}
SUPPORTED_PLATFORMS = tuple(PLATFORM_DOMAINS.keys())

CITY_HINTS = [
    "mumbai",
    "pune",
    "kolhapur",
    "delhi",
    "bangalore",
    "hyderabad",
    "nagpur",
    "nashik",
    "chennai",
    "patna",
    "kodoli",
    "satara",
    "sangli",
    "karad",
]

BAD_PATTERNS = {
    "instagram": ["/accounts/", "/explore/", "/p/", "/reel/", "/stories/"],
    "facebook": ["/watch", "/reel/", "/groups/", "/events/", "/share/", "/marketplace/"],
    "linkedin": ["/jobs/", "/feed/", "/posts/", "/pulse/", "/authwall", "/learning/"],
    "youtube": ["/playlist", "/results", "/shorts", "/watch", "/feed", "/hashtag/"],
}

DIRECT_SEARCH_CONFIG = {
    "facebook": [
        {
            "label": "Facebook page search",
            "url_template": "https://www.facebook.com/search/pages?q={query}",
            "selectors": ['a[href*="facebook.com"]', "a[href]", "body"],
            "scroll_multiplier": 2,
        },
        {
            "label": "Facebook public directory",
            "url_template": "https://www.facebook.com/public/{query}",
            "selectors": ['a[href*="facebook.com"]', "a[href]", "body"],
            "scroll_multiplier": 1,
        },
    ],
    "instagram": [
        {
            "label": "Instagram search",
            "url_template": "https://www.instagram.com/explore/search/keyword/?q={query}",
            "selectors": ['a[href^="/"]', "a[href]", "body"],
            "scroll_multiplier": 1,
        }
    ],
    "linkedin": [
        {
            "label": "LinkedIn public search",
            "url_template": "https://www.linkedin.com/search/results/companies/?keywords={query}",
            "selectors": ['a[href*="linkedin.com"]', "a[href]", "body"],
            "scroll_multiplier": 1,
        }
    ],
    "youtube": [
        {
            "label": "YouTube channel search",
            "url_template": "https://www.youtube.com/results?search_query={query}",
            "selectors": ['a[href^="/@"]', 'a[href^="/channel/"]', 'a[href^="/c/"]', "a[href]", "body"],
            "scroll_multiplier": 2,
        }
    ],
}

TRACKING_QUERY_KEYS = ("q", "url", "u", "continue", "dest", "next", "target", "redirect")
BLOCKED_TEXT_HINTS = (
    "sign in",
    "signin",
    "log in",
    "login",
    "sign up",
    "signup",
    "join now",
    "create account",
    "continue with google",
    "continue with email",
    "use the app",
    "download the app",
    "authentication",
    "verify your identity",
)
GENERIC_TITLE_HINTS = {
    "facebook",
    "instagram",
    "linkedin",
    "youtube",
    "sign in",
    "login",
}
META_DESCRIPTION_SELECTORS = (
    'meta[name="description"]',
    'meta[property="og:description"]',
    'meta[name="twitter:description"]',
)
META_TITLE_SELECTORS = (
    'meta[property="og:title"]',
    'meta[name="twitter:title"]',
)
SOCIAL_MAX_RESULTS_PER_PLATFORM = 6
SOCIAL_MAX_FINAL_CANDIDATES = 8
SOCIAL_MAX_ANCHOR_SNAPSHOTS = 350
SOCIAL_DIRECT_SEARCH_TIMEOUT_MS = 6000
SOCIAL_DISCOVERY_TIMEOUT_MS = 7000
SOCIAL_DETAIL_TIMEOUT_MS = 4500
SOCIAL_SETTLE_SECONDS = 0.02
SOCIAL_SEARCH_BODY_CHARS = 1200
SOCIAL_PAGE_BODY_CHARS = 3200
SOCIAL_SCROLL_PAUSE_SECONDS = 0.2
PROFILE_PRIORITY_HINTS = ("verified", "official", "business", "company", "brand", "founder")
FOLLOWER_PATTERNS = (
    r"([\d.,]+(?:\s?[KMB])?)\s+followers",
    r"followers\s*[:\-]?\s*([\d.,]+(?:\s?[KMB])?)",
)


def normalize_text(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def tokenize(value: str):
    return [x for x in normalize_text(value).split() if x]


def build_platform_query_text(keyword: str, location: str) -> str:
    return clean_text(f"{keyword} {location}".strip())


def build_search_query(platform: str, keyword: str, location: str) -> str:
    domain = PLATFORM_DOMAINS.get(platform, platform)
    parts = [f"site:{domain}"]

    cleaned_keyword = clean_text(keyword)
    cleaned_location = clean_text(location)

    if cleaned_keyword:
        parts.append(f'"{cleaned_keyword}"')
    if cleaned_location:
        parts.append(f'"{cleaned_location}"')

    parts.extend(["-login", "-signin", "-signup", "-auth"])
    return " ".join(parts)


def build_platform_search_targets(platform: str, keyword: str, location: str, max_pages: int):
    text = build_platform_query_text(keyword, location) or clean_text(keyword)
    encoded = quote(text)
    targets = []

    for config in DIRECT_SEARCH_CONFIG.get(platform, []):
        targets.append(
            {
                "label": config["label"],
                "url": config["url_template"].format(query=encoded),
                "selectors": config["selectors"],
                "scroll_passes": max(1, min(max_pages, 3) * config["scroll_multiplier"]),
            }
        )

    return targets


def host_matches_domain(host: str, domain: str) -> bool:
    host = clean_text(host).lower()
    domain = clean_text(domain).lower()
    return host == domain or host.endswith(f".{domain}")


def normalize_candidate_url(href: str) -> str:
    current = clean_text(href)

    for _ in range(4):
        if not current.lower().startswith("http"):
            return current

        parsed = urlparse(current)
        query = parse_qs(parsed.query)
        replacement = None

        if "google." in parsed.netloc.lower():
            for key in TRACKING_QUERY_KEYS:
                values = query.get(key)
                if not values:
                    continue

                candidate = clean_text(unquote(values[0]))
                if candidate.startswith("http") and candidate != current:
                    replacement = candidate
                    break

        if not replacement:
            break

        current = replacement

    return current


def canonicalize_profile_url(url: str, platform: str) -> str:
    current = normalize_candidate_url(url)
    if not current.lower().startswith("http"):
        return current

    parsed = urlparse(current)
    netloc = parsed.netloc.lower()
    path = parsed.path.rstrip("/") or "/"
    suffixes = []
    query = ""

    if platform in {"facebook", "youtube"}:
        suffixes = ["/about"]
    elif platform == "linkedin":
        suffixes = ["/about", "/details/about"]

    for suffix in suffixes:
        if path.endswith(suffix):
            path = path[: -len(suffix)].rstrip("/") or "/"
            break

    if platform == "youtube":
        for suffix in ("/videos", "/playlists", "/featured", "/streams", "/shorts", "/releases"):
            if path.endswith(suffix):
                path = path[: -len(suffix)].rstrip("/") or "/"
                break

    if platform == "facebook" and path == "/profile.php":
        profile_id = parse_qs(parsed.query).get("id", [""])[0]
        if profile_id:
            query = f"id={profile_id}"

    return parsed._replace(netloc=netloc, path=path, query=query, fragment="").geturl()


def is_platform_profile_path(url: str, platform: str) -> bool:
    path = urlparse(url).path.lower().rstrip("/")

    if not path:
        return False

    if platform == "instagram":
        return bool(re.fullmatch(r"/[a-z0-9._]+", path))

    if platform == "facebook":
        return path.startswith(("/profile.php", "/pages/", "/people/")) or bool(
            re.fullmatch(r"/[a-z0-9.\-]+", path)
        )

    if platform == "linkedin":
        return path.startswith(("/company/", "/in/", "/school/", "/showcase/"))

    if platform == "youtube":
        return path.startswith(("/@", "/channel/", "/c/", "/user/"))

    return True


def is_utility_or_login_page(title: str, href: str, description: str = "") -> bool:
    combined = normalize_text(f"{title} {href} {description}")
    return any(hint in combined for hint in BLOCKED_TEXT_HINTS)


def is_candidate_link(href: str, platform: str) -> bool:
    if not href:
        return False

    resolved = canonicalize_profile_url(href, platform)
    if not resolved.lower().startswith("http"):
        return False

    parsed = urlparse(resolved)
    full = resolved.lower()
    domain = PLATFORM_DOMAINS.get(platform, "")

    if not host_matches_domain(parsed.netloc, domain):
        return False

    if not is_platform_profile_path(resolved, platform):
        return False

    if is_utility_or_login_page("", resolved):
        return False

    blocked = BAD_PATTERNS.get(platform, [])
    for item in blocked:
        if item in full:
            return False

    return True


def clean_display_title(value: str) -> str:
    title = clean_text(value)

    suffixes = (
        " | LinkedIn",
        " | Instagram",
        " | Facebook",
        " | Meta",
        " - Facebook",
        " - LinkedIn",
        " - YouTube",
        " - Instagram",
        " | YouTube",
    )

    for suffix in suffixes:
        if title.endswith(suffix):
            title = title[: -len(suffix)]

    return clean_text(title)


def derive_title_from_url(href: str, platform: str) -> str:
    parsed = urlparse(clean_text(href))
    parts = [part for part in parsed.path.split("/") if part]

    if not parts:
        return clean_text(href)

    if platform == "youtube":
        slug = parts[0]
    elif platform in {"linkedin", "facebook"} and len(parts) >= 2:
        slug = parts[-1]
    else:
        slug = parts[0]

    slug = slug.lstrip("@")
    slug = re.sub(r"[-_.]+", " ", slug)
    return clean_display_title(slug.title())


def get_match_details(text: str, query: str):
    normalized_text = normalize_text(text)
    normalized_query = normalize_text(query)
    query_tokens = tokenize(query)
    text_tokens = tokenize(text)

    if not normalized_query or not query_tokens:
        return {
            "exact": False,
            "ratio": 0.0,
            "matched_tokens": [],
            "tokens": query_tokens,
        }

    matched_tokens = [token for token in query_tokens if token in text_tokens]
    ratio = len(matched_tokens) / len(query_tokens)

    return {
        "exact": normalized_query in normalized_text,
        "ratio": ratio,
        "matched_tokens": matched_tokens,
        "tokens": query_tokens,
    }


def get_relevance_metrics(title: str, href: str, description: str, keyword: str, location: str):
    text = normalize_text(f"{title} {href} {description}")
    keyword_match = get_match_details(text, keyword)
    location_match = get_match_details(text, location)

    return {
        "keyword": keyword_match,
        "location": location_match,
    }


def is_strong_relevance_match(title: str, href: str, description: str, keyword: str, location: str):
    metrics = get_relevance_metrics(title, href, description, keyword, location)
    keyword_match = metrics["keyword"]
    location_match = metrics["location"]

    keyword_ok = True
    if keyword_match["tokens"]:
        keyword_ok = keyword_match["exact"] or keyword_match["ratio"] >= 1.0

    location_ok = True
    if location_match["tokens"]:
        location_ok = location_match["exact"] or location_match["ratio"] >= 1.0

    return keyword_ok and location_ok


def candidate_score(title: str, href: str, keyword: str, location: str, description: str = "") -> int:
    metrics = get_relevance_metrics(title, href, description, keyword, location)
    score = 0
    priority_text = normalize_text(f"{title} {description}")

    keyword_match = metrics["keyword"]
    location_match = metrics["location"]

    if keyword_match["tokens"]:
        score += 12 if keyword_match["exact"] else int(keyword_match["ratio"] * 10)

    if location_match["tokens"]:
        score += 10 if location_match["exact"] else int(location_match["ratio"] * 8)

    if title and normalize_text(title) not in GENERIC_TITLE_HINTS:
        score += 1

    if any(hint in priority_text for hint in PROFILE_PRIORITY_HINTS):
        score += 2

    return score


def get_search_result_items(page):
    selectors = [
        'div.compTitle a',
        '#search a[href]:has(h3)',
        'a[href]:has(h3)',
    ]

    for selector in selectors:
        try:
            locator = page.locator(selector)
            if locator.count() > 0:
                return locator.all()
        except Exception:
            continue

    return []


def wait_for_search_results(page):
    wait_for_any_selector(
        page,
        ['div.compTitle a', '#search a[href]:has(h3)', 'a[href]:has(h3)', "#search", "#web"],
        timeout_ms=3500,
        poll_interval=0.15,
    )


def extract_search_result_description(result_link, title: str) -> str:
    try:
        raw_text = result_link.evaluate(
            """
            node => {
                const card =
                    node.closest('.algo') ||
                    node.closest('li') ||
                    node.closest('div[data-snc]') ||
                    node.closest('div.g') ||
                    node.closest('[data-hveid]') ||
                    node.parentElement;
                return card ? (card.innerText || '') : (node.innerText || '');
            }
            """
        )
    except Exception:
        try:
            raw_text = result_link.inner_text() or ""
        except Exception:
            raw_text = ""

    description = clean_text(raw_text)
    cleaned_title = clean_text(title)

    if cleaned_title and description.lower().startswith(cleaned_title.lower()):
        description = clean_text(description[len(cleaned_title) :])

    if len(description) > 220:
        description = description[:220].rsplit(" ", 1)[0].rstrip(" ,.;:") + "..."

    return description


def safe_body_text(page, max_chars: int = SOCIAL_PAGE_BODY_CHARS) -> str:
    try:
        text = clean_text(page.locator("body").inner_text(timeout=2500))
    except Exception:
        text = ""

    if len(text) > max_chars:
        text = text[:max_chars]

    return text


def collect_anchor_snapshots(page, limit: int = SOCIAL_MAX_ANCHOR_SNAPSHOTS):
    try:
        return page.locator("a[href]").evaluate_all(
            f"""
            els => els.slice(0, {limit}).map(el => {{
                const container =
                    el.closest('ytd-channel-renderer, ytd-video-renderer, ytd-grid-video-renderer, ytd-rich-item-renderer, [role="article"], article, li, [data-pagelet], [data-testid], section, div') ||
                    el.parentElement ||
                    el;

                return {{
                    href: el.href || el.getAttribute('href') || '',
                    text: (el.innerText || el.textContent || '').trim(),
                    title: (el.getAttribute('title') || '').trim(),
                    aria: (el.getAttribute('aria-label') || '').trim(),
                    context: (container.innerText || container.textContent || '').trim(),
                }};
            }})
            """
        )
    except Exception:
        return []


def trim_description(value: str) -> str:
    description = clean_text(value)
    if len(description) > 220:
        description = description[:220].rsplit(" ", 1)[0].rstrip(" ,.;:") + "..."
    return description


def choose_anchor_title(anchor: dict, href: str, platform: str) -> str:
    candidates = [
        clean_text(anchor.get("text")),
        clean_text(anchor.get("title")),
        clean_text(anchor.get("aria")),
    ]

    for value in candidates:
        if not value:
            continue
        value = clean_display_title(value.split("  ")[0].split(" | ")[0].strip())
        if value:
            return value

    return derive_title_from_url(href, platform)


def build_candidate_from_anchor(anchor: dict, platform: str, keyword: str, location: str):
    href = normalize_candidate_url(clean_text(anchor.get("href")))
    if not is_candidate_link(href, platform):
        return None

    title = choose_anchor_title(anchor, href, platform)
    description = trim_description(anchor.get("context") or anchor.get("text"))

    if not title or is_utility_or_login_page(title, href, description):
        return None

    if not is_strong_relevance_match(title, href, description, keyword, location):
        return None

    score = candidate_score(title, href, keyword, location, description)
    if score < 1 and tokenize(keyword):
        return None

    return {
        "platform": platform,
        "title": title,
        "description": description,
        "href": canonicalize_profile_url(href, platform),
        "score": score,
    }


def scroll_results_page(page):
    try:
        page.evaluate("window.scrollBy(0, Math.max(window.innerHeight * 1.8, 1600))")
    except Exception:
        pass
    sleep_small(SOCIAL_SCROLL_PAUSE_SECONDS)


def is_fast_enough_social_row(row: dict, keyword: str, location: str) -> bool:
    if not row:
        return False

    if not row_matches_priority(row, keyword, location):
        return False

    has_contacts = bool(clean_text(row.get("phones")) or clean_text(row.get("emails")))
    has_description = bool(clean_text(row.get("description")))
    has_location = bool(clean_text(row.get("location")))
    return has_contacts or (has_description and has_location)


def collect_candidates_from_direct_search(page, platform: str, keyword: str, location: str, max_pages: int):
    results = []
    seen = set()
    targets = build_platform_search_targets(platform, keyword, location, max_pages)

    for target in targets:
        try:
            goto_with_retry(
                page,
                target["url"],
                selectors=target["selectors"],
                timeout_ms=SOCIAL_DIRECT_SEARCH_TIMEOUT_MS,
                settle_seconds=SOCIAL_SETTLE_SECONDS,
            )
            add_log("social_lookup", f'{platform}: loaded direct search "{target["label"]}"')
        except Exception as exc:
            add_log("social_lookup", f'{platform}: direct search failed for "{target["label"]}": {exc}')
            continue

        page_title = clean_text(page.title())
        page_text = safe_body_text(page, max_chars=SOCIAL_SEARCH_BODY_CHARS)
        if is_utility_or_login_page(page_title, page.url, page_text):
            add_log(
                "social_lookup",
                f'{platform}: direct search "{target["label"]}" looks blocked for anonymous browsing',
            )
            continue

        accepted_before = len(results)

        for scroll_index in range(target["scroll_passes"]):
            anchors = collect_anchor_snapshots(page)

            for anchor in anchors:
                candidate = build_candidate_from_anchor(anchor, platform, keyword, location)
                if not candidate:
                    continue

                if candidate["href"] in seen:
                    continue

                seen.add(candidate["href"])
                results.append(candidate)

                if len(results) >= SOCIAL_MAX_RESULTS_PER_PLATFORM:
                    break

            if len(results) >= SOCIAL_MAX_RESULTS_PER_PLATFORM:
                break

            if scroll_index < target["scroll_passes"] - 1:
                scroll_results_page(page)

        added = len(results) - accepted_before
        add_log(
            "social_lookup",
            f'{platform}: accepted {added} public profile links via "{target["label"]}"',
        )

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:SOCIAL_MAX_RESULTS_PER_PLATFORM]


def collect_candidates_from_google_search(page, platform: str, keyword: str, location: str, max_pages: int):
    results = []
    seen = set()

    for page_num in range(1, max_pages + 1):
        query = quote(build_search_query(platform, keyword, location))
        start = (page_num - 1) * 10 + 1
        url = f"https://search.yahoo.com/search?p={query}&b={start}"

        try:
            goto_with_retry(
                page,
                url,
                selectors=['div.compTitle a', '#search a[href]:has(h3)', 'a[href]:has(h3)', "#web"],
                timeout_ms=SOCIAL_DISCOVERY_TIMEOUT_MS,
                settle_seconds=SOCIAL_SETTLE_SECONDS,
            )
            wait_for_search_results(page)
            add_log("social_lookup", f"{platform}: loaded discovery page {page_num}")
        except Exception as exc:
            add_log("social_lookup", f"{platform}: discovery search failed: {exc}")
            continue

        items = get_search_result_items(page)
        page_count = 0

        for el in items:
            try:
                href = normalize_candidate_url(
                    clean_text(el.evaluate("node => node.href || node.getAttribute('href') || ''") or "")
                )
            except Exception:
                href = ""

            if not href or href in seen:
                continue

            if not is_candidate_link(href, platform):
                continue

            try:
                title = clean_display_title(el.locator("h3").first.inner_text() or href)
            except Exception:
                title = clean_display_title(clean_text(el.inner_text() or href))

            description = extract_search_result_description(el, title)

            if not title or is_utility_or_login_page(title, href, description):
                continue

            if not is_strong_relevance_match(title, href, description, keyword, location):
                continue

            score = candidate_score(title, href, keyword, location, description)
            if score < 1 and tokenize(keyword):
                continue

            seen.add(href)
            results.append(
                {
                    "platform": platform,
                    "title": title,
                    "description": description,
                    "href": canonicalize_profile_url(href, platform),
                    "score": score,
                }
            )
            page_count += 1

            if len(results) >= SOCIAL_MAX_RESULTS_PER_PLATFORM:
                break

        if len(results) >= SOCIAL_MAX_RESULTS_PER_PLATFORM:
            add_log("social_lookup", f"{platform}: reached fast candidate cap")
            break

        add_log("social_lookup", f"{platform}: accepted {page_count} public profile links on page {page_num}")

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:SOCIAL_MAX_RESULTS_PER_PLATFORM]


def collect_candidates(page, platform: str, keyword: str, location: str, max_pages: int):
    direct_results = collect_candidates_from_direct_search(page, platform, keyword, location, max_pages)
    minimum_direct_results = 1

    if len(direct_results) >= minimum_direct_results:
        return direct_results[:SOCIAL_MAX_RESULTS_PER_PLATFORM]

    add_log(
        "social_lookup",
        f"{platform}: switching to fallback discovery because direct search only found {len(direct_results)} result(s)",
    )

    fallback_results = collect_candidates_from_google_search(page, platform, keyword, location, max_pages)
    combined = []
    seen = set()

    for candidate in direct_results + fallback_results:
        href = candidate["href"]
        if href in seen:
            continue
        seen.add(href)
        combined.append(candidate)

    combined.sort(key=lambda x: x["score"], reverse=True)
    return combined[:SOCIAL_MAX_RESULTS_PER_PLATFORM]


def infer_location(text: str) -> str:
    low = text.lower()
    for city in CITY_HINTS:
        if city in low:
            return city.title()
    return ""


def get_meta_content(page, selector: str) -> str:
    try:
        return clean_text(page.locator(selector).first.get_attribute("content") or "")
    except Exception:
        return ""


def get_first_meta_content(page, selectors) -> str:
    for selector in selectors:
        value = get_meta_content(page, selector)
        if value:
            return value
    return ""


def ensure_list(value):
    if isinstance(value, list):
        return value
    if value is None:
        return []
    return [value]


def flatten_json_ld(value):
    items = []

    if isinstance(value, list):
        for item in value:
            items.extend(flatten_json_ld(item))
        return items

    if isinstance(value, dict):
        graph = value.get("@graph")
        if graph:
            items.extend(flatten_json_ld(graph))

        nested_items = value.get("itemListElement")
        if nested_items and nested_items is not graph:
            items.extend(flatten_json_ld(nested_items))

        items.append(value)

    return items


def extract_json_ld_items(page):
    try:
        raw_blocks = page.locator('script[type="application/ld+json"]').evaluate_all(
            "els => els.map(el => el.textContent || '')"
        )
    except Exception:
        return []

    items = []
    for raw in raw_blocks:
        raw = clean_text(raw)
        if not raw:
            continue

        try:
            parsed = json.loads(raw)
        except Exception:
            continue

        items.extend(flatten_json_ld(parsed))

    return items


def append_unique(items: list[str], value: str, cleaner, limit: int):
    cleaned = cleaner(value)
    if cleaned and cleaned not in items:
        items.append(cleaned)
    return items[:limit]


def build_location_value(value) -> str:
    if isinstance(value, dict):
        parts = [
            clean_text(value.get("addressLocality")),
            clean_text(value.get("addressRegion")),
            clean_text(value.get("addressCountry")),
            clean_text(value.get("name")),
        ]
        parts = [part for part in parts if part]
        if parts:
            return clean_text(", ".join(dict.fromkeys(parts)))
        return ""

    return clean_text(value)


def extract_description_from_json_ld(items) -> str:
    for item in items:
        for key in ("description", "headline", "abstract"):
            value = clean_text(item.get(key))
            if value:
                return value
    return ""


def extract_name_from_json_ld(items) -> str:
    for item in items:
        for key in ("name", "headline", "alternateName"):
            value = clean_display_title(item.get(key))
            if value:
                return value
    return ""


def extract_location_from_json_ld(items) -> str:
    for item in items:
        for key in ("address", "location", "contentLocation", "homeLocation"):
            value = build_location_value(item.get(key))
            if value:
                return value
    return ""


def extract_contacts_from_json_ld(items):
    phones = []
    emails = []

    for item in items:
        for key in ("telephone", "phone"):
            for value in ensure_list(item.get(key)):
                append_unique(phones, value, clean_phone, 3)

        for key in ("email", "emails"):
            for value in ensure_list(item.get(key)):
                append_unique(emails, value, clean_email, 2)

        for contact_point in ensure_list(item.get("contactPoint")):
            if not isinstance(contact_point, dict):
                continue
            for value in ensure_list(contact_point.get("telephone")):
                append_unique(phones, value, clean_phone, 3)
            for value in ensure_list(contact_point.get("email")):
                append_unique(emails, value, clean_email, 2)

    return phones[:3], emails[:2]


def collect_page_emails(page, text: str) -> list[str]:
    emails = []

    try:
        mailtos = page.locator('a[href^="mailto:"]').evaluate_all(
            "els => els.map(el => el.getAttribute('href') || '')"
        )
    except Exception:
        mailtos = []

    for href in mailtos:
        email = clean_email(clean_text(str(href)).replace("mailto:", "", 1))
        if email and email not in emails:
            emails.append(email)

    for email in extract_emails_from_text(text):
        if email and email not in emails:
            emails.append(email)

    return emails[:2]


def collect_page_phones(page, text: str) -> list[str]:
    phones = []

    try:
        tels = page.locator('a[href^="tel:"]').evaluate_all(
            "els => els.map(el => el.getAttribute('href') || '')"
        )
    except Exception:
        tels = []

    raw_phones = [clean_text(str(value)).replace("tel:", "", 1) for value in tels]
    raw_phones.extend(extract_phones_from_text(text))

    for value in raw_phones:
        phone = clean_phone(value)
        digits = re.sub(r"\D", "", phone)
        if 10 <= len(digits) <= 15 and phone and phone not in phones:
            phones.append(phone)

    return phones[:3]


def extract_followers(value: str) -> str:
    text = clean_text(value)
    if not text:
        return ""

    for pattern in FOLLOWER_PATTERNS:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return clean_text(match.group(1)).upper().replace(" ", "")

    return ""


def summarize_description(meta_description: str, body_text: str, structured_description: str = "") -> str:
    description = clean_text(meta_description)
    if not description:
        description = clean_text(structured_description)
    if not description:
        description = clean_text(body_text)

    if len(description) > 220:
        description = description[:220].rsplit(" ", 1)[0].rstrip(" ,.;:") + "..."

    return description


def select_display_title(
    search_title: str,
    page_title: str,
    fallback_url: str,
    meta_title: str = "",
    structured_name: str = "",
) -> str:
    for value in (search_title, structured_name, meta_title, page_title):
        cleaned = clean_display_title(value)
        if cleaned and normalize_text(cleaned) not in GENERIC_TITLE_HINTS:
            return cleaned

    return clean_text(fallback_url)


def build_fallback_row(candidate: dict, keyword: str, location: str) -> dict:
    href = clean_text(candidate.get("href"))
    title = clean_display_title(candidate.get("title"))
    description = clean_text(candidate.get("description"))
    platform = clean_text(candidate.get("platform"))
    found_location = ""
    source_text = " ".join(part for part in [title, description, href] if clean_text(part))

    if not description:
        description = f"Public {platform} profile result found for {clean_text(keyword)}."

    if clean_text(location):
        location_match = get_match_details(source_text, location)
        if location_match["exact"] or location_match["ratio"] >= 1.0:
            found_location = clean_text(location)

    if not found_location:
        found_location = infer_location(source_text)

    return {
        "title": title or href,
        "description": description,
        "domain": clean_text(get_domain(href)),
        "phones": "",
        "emails": "",
        "link": href,
        "source": platform,
        "category": clean_text(keyword),
        "location": found_location,
        "followers": "",
    }


def row_matches_priority(row: dict, keyword: str, location: str) -> bool:
    return is_strong_relevance_match(
        row.get("title", ""),
        row.get("link", ""),
        " ".join(
            part
            for part in [row.get("description", ""), row.get("location", "")]
            if clean_text(part)
        ),
        keyword,
        location,
    )


def build_result_signature(row: dict) -> str:
    title = normalize_text(row.get("title", ""))
    location = normalize_text(row.get("location", ""))
    keyword = normalize_text(row.get("category", ""))
    source = normalize_text(row.get("source", ""))
    link = canonicalize_profile_url(clean_text(row.get("link", "")), source)

    if title:
        parts = [title, location, keyword]
        return "|".join(parts)

    return normalize_text(link)


def build_candidate_visit_urls(candidate: dict):
    href = normalize_candidate_url(clean_text(candidate.get("href")))
    platform = clean_text(candidate.get("platform")).lower()

    if not href:
        return []

    urls = [href]

    if platform == "youtube":
        urls.insert(0, href.rstrip("/") + "/about")
    elif platform == "facebook" and "/profile.php" not in href:
        urls.insert(0, href.rstrip("/") + "/about")
    elif platform == "linkedin" and any(marker in href for marker in ("/company/", "/school/")):
        urls.insert(0, href.rstrip("/") + "/about/")

    deduped = []
    seen = set()
    for url in urls:
        cleaned = clean_text(url)
        if cleaned and cleaned not in seen:
            seen.add(cleaned)
            deduped.append(cleaned)

    return deduped


def merge_unique_contacts(values, extra_values, cleaner, limit: int):
    merged = []

    for value in values:
        append_unique(merged, value, cleaner, limit)

    for value in extra_values:
        append_unique(merged, value, cleaner, limit)

    return merged[:limit]


def scrape_candidate(page, candidate: dict, keyword: str, location: str) -> dict:
    href = candidate["href"]
    title = candidate["title"]
    platform = candidate["platform"]
    fallback_row = build_fallback_row(candidate, keyword, location)
    fallback_allowed = row_matches_priority(fallback_row, keyword, location)
    best_row = None

    for target_url in build_candidate_visit_urls(candidate):
        try:
            goto_with_retry(
                page,
                target_url,
                selectors=["body", *META_DESCRIPTION_SELECTORS, 'script[type="application/ld+json"]'],
                timeout_ms=SOCIAL_DETAIL_TIMEOUT_MS,
                settle_seconds=SOCIAL_SETTLE_SECONDS,
            )

            final_url = normalize_candidate_url(page.url)
            canonical_url = canonicalize_profile_url(final_url or target_url, platform) or href
            page_title = clean_text(page.title())
            meta_title = get_first_meta_content(page, META_TITLE_SELECTORS)
            meta_description = get_first_meta_content(page, META_DESCRIPTION_SELECTORS)
            body_text = safe_body_text(page)
            json_ld_items = extract_json_ld_items(page)
            structured_name = extract_name_from_json_ld(json_ld_items)
            structured_description = extract_description_from_json_ld(json_ld_items)
            structured_location = extract_location_from_json_ld(json_ld_items)
            structured_phones, structured_emails = extract_contacts_from_json_ld(json_ld_items)

            combined_text = " ".join(
                part
                for part in [
                    title,
                    page_title,
                    meta_title,
                    meta_description,
                    structured_name,
                    structured_description,
                    structured_location,
                    body_text,
                ]
                if clean_text(part)
            )

            if not is_candidate_link(canonical_url, platform):
                add_log("social_lookup", f"Skipped non-public page: {final_url}")
                continue

            if is_utility_or_login_page(page_title, canonical_url, combined_text):
                add_log("social_lookup", f"Skipped login/utility page: {final_url}")
                continue

            if not is_strong_relevance_match(title, canonical_url, combined_text, keyword, location):
                add_log("social_lookup", f"Skipped weak keyword/location match: {canonical_url}")
                continue

            keyword_hits = sum(1 for token in tokenize(keyword) if token in normalize_text(combined_text))
            location_hits = sum(1 for token in tokenize(location) if token in normalize_text(combined_text))

            phones = merge_unique_contacts(
                structured_phones,
                collect_page_phones(page, combined_text),
                clean_phone,
                3,
            )
            emails = merge_unique_contacts(
                structured_emails,
                collect_page_emails(page, combined_text),
                clean_email,
                2,
            )

            found_location = clean_text(location)
            if not found_location:
                found_location = clean_text(structured_location) or infer_location(combined_text)

            relevance = candidate.get("score", 0) + keyword_hits + location_hits
            relevance += 2 if phones else 0
            relevance += 2 if emails else 0
            relevance += 1 if structured_description or meta_description else 0
            relevance += 2 if any(hint in normalize_text(combined_text) for hint in PROFILE_PRIORITY_HINTS) else 0

            description = summarize_description(meta_description, body_text, structured_description)
            if not description:
                description = fallback_row["description"]

            followers = extract_followers(" ".join([meta_description, body_text, structured_description, page_title]))

            row = {
                "title": select_display_title(title, page_title, canonical_url, meta_title, structured_name),
                "description": description,
                "domain": clean_text(get_domain(canonical_url)) or fallback_row["domain"],
                "phones": clean_text(", ".join(phones)),
                "emails": clean_text(", ".join(emails)),
                "link": clean_text(canonical_url) or fallback_row["link"],
                "source": clean_text(platform),
                "category": clean_text(keyword),
                "location": clean_text(found_location) or fallback_row["location"],
                "followers": followers or fallback_row["followers"],
                "_score": relevance,
            }

            if not row_matches_priority(row, keyword, location):
                add_log("social_lookup", f"Rejected final row for weak keyword/location match: {canonical_url}")
                continue

            if best_row is None or row["_score"] > best_row["_score"]:
                best_row = row

            if is_fast_enough_social_row(best_row, keyword, location):
                break

        except Exception as exc:
            add_log("social_lookup", f"candidate scrape failed: {exc}")

    if best_row:
        return best_row

    if fallback_allowed:
        return fallback_row

    return {}


def collect_platform_candidates_worker(platform: str, keyword: str, location: str, max_pages: int):
    playwright = None
    browser = None
    context = None
    page = None

    try:
        playwright, browser, context, page = start_browser(headless=True, block_images=True)
        add_log("social_lookup", f'Searching {platform} directly for "{keyword}" in "{location}"')
        candidates = collect_candidates(page, platform, keyword, location, max_pages)
        add_log("social_lookup", f"{platform}: total candidates = {len(candidates)}")
        return candidates
    except Exception as exc:
        add_log("social_lookup", f"Unable to start browser: {exc}")
        return []
    finally:
        close_browser(playwright=playwright, browser=browser, context=context, page=page)


def run_social_lookup_scrape(keyword: str, location: str, platforms: list[str], max_pages: int):
    mode = "social_lookup"
    keyword = clean_text(keyword)
    location = clean_text(location)
    platforms = [clean_text(platform).lower() for platform in platforms if clean_text(platform)]

    if not platforms:
        add_log(mode, "No platforms selected")
        set_total(mode, 0)
        set_running(mode, False)
        return

    if not keyword:
        add_log(mode, "No keyword provided")
        set_total(mode, 0)
        set_running(mode, False)
        return

    max_pages = max(1, min(max_pages, 3))

    playwright = None
    browser = None
    context = None
    page = None

    try:
        all_candidates = []

        with ThreadPoolExecutor(max_workers=min(len(platforms), 4)) as executor:
            futures = {
                executor.submit(
                    collect_platform_candidates_worker,
                    platform,
                    keyword,
                    location,
                    max_pages,
                ): platform
                for platform in platforms
            }

            for future in as_completed(futures):
                if is_stopped(mode):
                    add_log(mode, "Social lookup stopped safely")
                    break

                all_candidates.extend(future.result())

        if not all_candidates:
            set_total(mode, 0)
            add_log(mode, "No public candidate links found")
            return

        seen_links = set()
        final_candidates = []
        for candidate in sorted(all_candidates, key=lambda x: x["score"], reverse=True):
            if candidate["href"] in seen_links:
                continue
            seen_links.add(candidate["href"])
            final_candidates.append(candidate)

            if len(final_candidates) >= SOCIAL_MAX_FINAL_CANDIDATES:
                break

        final_candidates = final_candidates[:SOCIAL_MAX_FINAL_CANDIDATES]
        set_total(mode, len(final_candidates))
        seen_result_signatures = set()
        playwright, browser, context, page = start_browser(headless=True, block_images=True)

        for candidate in final_candidates:
            if is_stopped(mode):
                add_log(mode, "Social lookup stopped safely")
                break

            row = scrape_candidate(page, candidate, keyword, location)

            if row and row.get("title") and row.get("domain"):
                result_signature = build_result_signature(row)
                if result_signature and result_signature in seen_result_signatures:
                    add_log(mode, f"Skipped duplicate result: {row.get('title', '')}")
                    increment_current(mode)
                    continue

                if result_signature:
                    seen_result_signatures.add(result_signature)

                row.pop("_score", None)
                add_result(mode, row)

            increment_current(mode)

    except Exception as exc:
        if page is None:
            add_log(mode, f"Unable to start browser: {exc}")
        else:
            add_log(mode, f"Fatal Social Lookup scraper error: {exc}")

    finally:
        close_browser(playwright=playwright, browser=browser, context=context, page=page)
        set_running(mode, False)
        add_log(mode, "Social Lookup scraping finished")
