
import re
from urllib.parse import quote

from backend.state import (
    add_log,
    add_result,
    add_results,
    increment_current,
    increment_total,
    is_stopped,
    set_running,
    set_total,
)
from backend.scraper.utils import (
    clean_email,
    clean_phone,
    clean_text,
    close_browser,
    close_concurrent_pages,
    goto_and_wait,
    goto_with_retry,
    open_concurrent_pages,
    parse_basic_location,
    sleep_small,
    start_browser,
    try_open_and_collect_emails,
)

GOOGLE_MAX_LISTING_LINKS = 24
GOOGLE_MAX_RESULTS_PER_COMBO = 12
GOOGLE_MAX_EMAIL_ENRICHMENTS_PER_COMBO = 4
GOOGLE_RESULTS_SCROLL_LOOPS = 6
GOOGLE_RESULTS_STABLE_ROUNDS = 2
GOOGLE_RESULTS_SCROLL_PAUSE_SECONDS = 0.10
GOOGLE_SEARCH_TIMEOUT_MS = 6000
GOOGLE_DETAIL_TIMEOUT_MS = 4000
GOOGLE_SETTLE_SECONDS = 0.02
GOOGLE_DEFAULT_MAX_PAGES = 3
GOOGLE_MAX_ALLOWED_PAGES = 10
GOOGLE_ALL_PAGES_SCROLL_LOOPS = 120
GOOGLE_ALL_PAGES_STABLE_ROUNDS = 5
GOOGLE_LISTING_LINKS_PER_PAGE = max(1, GOOGLE_MAX_LISTING_LINKS // GOOGLE_DEFAULT_MAX_PAGES)
GOOGLE_RESULTS_PER_PAGE = max(1, GOOGLE_MAX_RESULTS_PER_COMBO // GOOGLE_DEFAULT_MAX_PAGES)
GOOGLE_SCROLL_LOOPS_PER_PAGE = max(1, GOOGLE_RESULTS_SCROLL_LOOPS // GOOGLE_DEFAULT_MAX_PAGES)
LOW_CONFIDENCE_IMAGE_TOKENS = ("gps-cs-s/", "streetviewpixels", "maps.gstatic.com")
NEGATIVE_IMAGE_HINTS = (
    "annual day",
    "celebration",
    "ceremony",
    "crowd",
    "event",
    "function",
    "group photo",
    "people",
    "program",
    "sports",
    "student",
    "students",
)
POSITIVE_IMAGE_HINTS = (
    "building",
    "campus",
    "entrance",
    "front",
    "main gate",
    "photo of",
    "school",
)


def clamp_google_max_pages(max_pages) -> int:
    try:
        value = int(max_pages)
    except (TypeError, ValueError):
        value = GOOGLE_DEFAULT_MAX_PAGES

    return max(0, min(value, GOOGLE_MAX_ALLOWED_PAGES))


def resolve_google_search_limits(max_pages) -> dict:
    max_pages = clamp_google_max_pages(max_pages)
    if max_pages == 0:
        return {
            "all_pages": True,
            "max_pages": 0,
            "listing_limit": None,
            "result_cap": None,
            "email_cap": GOOGLE_MAX_EMAIL_ENRICHMENTS_PER_COMBO,
            "scroll_loops": GOOGLE_ALL_PAGES_SCROLL_LOOPS,
            "stable_round_limit": GOOGLE_ALL_PAGES_STABLE_ROUNDS,
            "page_depth_label": "all available Google pages",
        }

    listing_limit = GOOGLE_LISTING_LINKS_PER_PAGE * max_pages
    result_cap = GOOGLE_RESULTS_PER_PAGE * max_pages
    scroll_loops = GOOGLE_SCROLL_LOOPS_PER_PAGE * max_pages

    return {
        "all_pages": False,
        "max_pages": max_pages,
        "listing_limit": max(GOOGLE_LISTING_LINKS_PER_PAGE, listing_limit),
        "result_cap": max(GOOGLE_RESULTS_PER_PAGE, result_cap),
        "email_cap": min(result_cap, GOOGLE_MAX_EMAIL_ENRICHMENTS_PER_COMBO),
        "scroll_loops": max(GOOGLE_SCROLL_LOOPS_PER_PAGE, scroll_loops),
        "stable_round_limit": GOOGLE_RESULTS_STABLE_ROUNDS,
        "page_depth_label": f"up to {max_pages} Google page{'' if max_pages == 1 else 's'}",
    }


def extract_cid(url):
    match = re.search(r"[?&]cid=([0-9]+)", url or "")
    return match.group(1) if match else ""


def get_text_safe(page, selector):
    try:
        return clean_text(page.locator(selector).first.inner_text())
    except Exception:
        return ""


def get_href_safe(page, selector):
    try:
        return clean_text(page.locator(selector).first.get_attribute("href") or "")
    except Exception:
        return ""


def normalize_match_text(value: str) -> str:
    value = clean_text(value).lower()
    value = re.sub(r"[^a-z0-9\s]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def tokenize_match_text(value: str):
    return [token for token in normalize_match_text(value).split() if token]


def canonicalize_match_token(token: str) -> str:
    token = clean_text(token).lower()
    if not token:
        return ""

    if token.endswith("ies") and len(token) > 4:
        return token[:-3] + "y"

    if token.endswith(("sses", "xes", "zes", "ches", "shes")) and len(token) > 4:
        return token[:-2]

    if token.endswith("s") and len(token) > 3 and not token.endswith("ss"):
        return token[:-1]

    return token


def canonicalize_match_text(value: str) -> str:
    return " ".join(
        canonicalize_match_token(token)
        for token in tokenize_match_text(value)
        if canonicalize_match_token(token)
    )


def clean_preview_text(value) -> str:
    if value is None:
        return ""

    text = str(value).replace("\u00a0", " ")
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\n{2,}", "\n", text)
    return text.strip()


def build_google_relevance_metrics(text: str, keyword: str, location: str):
    normalized_text = normalize_match_text(text)
    canonical_text = canonicalize_match_text(text)
    normalized_keyword = normalize_match_text(keyword)
    normalized_location = normalize_match_text(location)
    keyword_tokens = tokenize_match_text(keyword)
    location_tokens = tokenize_match_text(location)
    canonical_text_tokens = {token for token in canonical_text.split() if token}
    canonical_keyword = canonicalize_match_text(keyword)
    canonical_location = canonicalize_match_text(location)
    canonical_keyword_tokens = [canonicalize_match_token(token) for token in keyword_tokens]
    canonical_location_tokens = [canonicalize_match_token(token) for token in location_tokens]

    return {
        "keyword_hits": sum(
            1 for token in canonical_keyword_tokens if token and token in canonical_text_tokens
        ),
        "location_hits": sum(
            1 for token in canonical_location_tokens if token and token in canonical_text_tokens
        ),
        "has_keyword_phrase": bool(
            (normalized_keyword and normalized_keyword in normalized_text)
            or (canonical_keyword and canonical_keyword in canonical_text)
        ),
        "has_location_phrase": bool(
            (normalized_location and normalized_location in normalized_text)
            or (canonical_location and canonical_location in canonical_text)
        ),
    }


def is_strong_keyword_match(text: str, keyword: str) -> bool:
    keyword_tokens = tokenize_match_text(keyword)
    if not keyword_tokens:
        return True

    metrics = build_google_relevance_metrics(text, keyword, "")
    if metrics["has_keyword_phrase"]:
        return True

    required_hits = 1 if len(keyword_tokens) == 1 else min(len(keyword_tokens), 2)
    return metrics["keyword_hits"] >= required_hits


def is_strong_location_match(text: str, location: str) -> bool:
    location_tokens = tokenize_match_text(location)
    if not location_tokens:
        return True

    metrics = build_google_relevance_metrics(text, "", location)
    if metrics["has_location_phrase"]:
        return True

    return metrics["location_hits"] >= 1


def is_strong_google_match(text: str, keyword: str, location: str) -> bool:
    return is_strong_keyword_match(text, keyword) and is_strong_location_match(text, location)


def score_listing_candidate(text: str, keyword: str, location: str) -> int:
    metrics = build_google_relevance_metrics(text, keyword, location)
    score = (metrics["keyword_hits"] * 5) + (metrics["location_hits"] * 3)

    if metrics["has_keyword_phrase"]:
        score += 6
    if metrics["has_location_phrase"]:
        score += 4

    return score


def build_place_signature(row: dict) -> str:
    cid = clean_text(row.get("cid"))
    if cid:
        return f"cid:{cid}"

    company_name = normalize_match_text(row.get("company_name", ""))
    address = normalize_match_text(row.get("address", ""))
    if company_name or address:
        return f"{company_name}|{address}"

    return normalize_match_text(row.get("map_link", ""))


def scroll_results_panel(
    page,
    loops=GOOGLE_RESULTS_SCROLL_LOOPS,
    pause_seconds=GOOGLE_RESULTS_SCROLL_PAUSE_SECONDS,
    target_count=GOOGLE_MAX_LISTING_LINKS,
    stable_round_limit=GOOGLE_RESULTS_STABLE_ROUNDS,
    on_round=None,
):
    panel_selectors = [
        'div[role="feed"]',
        'div[aria-label][role="feed"]',
        'div.m6QErb[role="feed"]',
    ]

    panel = None
    for sel in panel_selectors:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                panel = loc
                break
        except Exception:
            continue

    if panel is None:
        return

    last_count = page.locator('a[href*="/place/"]').count()
    stable_rounds = 0

    if callable(on_round):
        try:
            on_round(last_count)
        except Exception:
            pass

    for _ in range(max(1, int(loops or GOOGLE_RESULTS_SCROLL_LOOPS))):
        try:
            panel.evaluate("(el) => el.scrollBy(0, el.scrollHeight)")
            sleep_small(pause_seconds)
            new_count = page.locator('a[href*="/place/"]').count()

            if callable(on_round):
                try:
                    on_round(new_count)
                except Exception:
                    pass

            if target_count and new_count >= target_count:
                break

            if new_count == last_count:
                stable_rounds += 1
            else:
                stable_rounds = 0

            last_count = new_count

            if stable_rounds >= max(1, stable_round_limit):
                break
        except Exception:
            break


def scrape_listing_links(page, limit=GOOGLE_MAX_LISTING_LINKS):
    try:
        hrefs = page.locator('a[href*="/place/"]').evaluate_all(
            "els => els.map(el => el.href).filter(Boolean)"
        )
    except Exception:
        hrefs = []

    seen = set()
    links = []

    for href in hrefs:
        if href and href not in seen:
            seen.add(href)
            links.append(href)

    return links[:limit]


def extract_srcset_urls(value) -> list[str]:
    urls = []
    raw = clean_text(value)
    if not raw:
        return urls

    for part in raw.split(","):
        candidate = clean_text(part).split(" ")[0]
        if candidate and candidate not in urls:
            urls.append(candidate)

    return urls


def normalize_featured_image_url(value) -> str:
    raw = clean_text(value)
    if not raw:
        return ""

    if raw.startswith("url(") and raw.endswith(")"):
        raw = clean_text(raw[4:-1]).strip("'\"")

    if raw.startswith("//"):
        raw = f"https:{raw}"

    if raw.startswith("data:"):
        return ""

    if not re.match(r"^https?://", raw, re.IGNORECASE):
        return ""

    blocked = [
        "googlelogo",
        "maps.gstatic",
        "gstatic.com/mapfiles",
        "streetviewpixels",
        "staticmap",
        "maptile",
        "favicon",
        "logo",
        "placeholder",
    ]
    lowered = raw.lower()
    if any(token in lowered for token in blocked):
        return ""

    return raw


def score_featured_image_url(url: str) -> int:
    lowered = url.lower()
    score = 1

    if "googleusercontent.com" in lowered or "lh3.googleusercontent.com" in lowered:
        score += 8
    if "/p/" in lowered or "=s" in lowered or "=w" in lowered:
        score += 2
    if "photo" in lowered or "usercontent" in lowered:
        score += 1

    size_matches = re.findall(r"=(?:s|w)(\d+)", lowered)
    if size_matches:
        score += max(int(size) for size in size_matches)

    return score


def build_featured_image_candidates(values) -> list[dict]:
    candidates = []
    raw_values = values if isinstance(values, (list, tuple)) else [values]

    for index, value in enumerate(raw_values):
        if isinstance(value, dict):
            base = {
                "label": clean_text(value.get("label")),
                "context": clean_text(value.get("context")),
                "width": int(value.get("width") or 0),
                "height": int(value.get("height") or 0),
                "hero": bool(value.get("hero")),
                "background": bool(value.get("background")),
                "index": int(value.get("index") or index),
            }
            raw_url = value.get("url") or value.get("src") or value.get("value")
        else:
            base = {
                "label": "",
                "context": "",
                "width": 0,
                "height": 0,
                "hero": False,
                "background": False,
                "index": index,
            }
            raw_url = value

        for candidate_url in extract_srcset_urls(raw_url):
            normalized = normalize_featured_image_url(candidate_url)
            if normalized:
                candidates.append({**base, "url": normalized})

        normalized = normalize_featured_image_url(raw_url)
        if normalized:
            candidates.append({**base, "url": normalized})

    unique_candidates = []
    seen_urls = set()
    for candidate in candidates:
        url = candidate["url"]
        if url in seen_urls:
            continue
        seen_urls.add(url)
        unique_candidates.append(candidate)

    return unique_candidates


def score_featured_image_candidate(candidate: dict, company_name: str = "", keyword: str = "") -> int:
    url = clean_text(candidate.get("url"))
    if not url:
        return -1

    score = score_featured_image_url(url)
    label_text = canonicalize_match_text(
        " ".join(
            part
            for part in [candidate.get("label"), candidate.get("context")]
            if clean_text(part)
        )
    )
    label_tokens = {token for token in label_text.split() if token}

    if candidate.get("hero"):
        score += 120
    if candidate.get("background"):
        score += 20

    width = max(0, int(candidate.get("width") or 0))
    height = max(0, int(candidate.get("height") or 0))
    if width and height:
        score += min((width * height) // 10000, 40)
    elif width or height:
        score += min(max(width, height) // 20, 15)

    company_tokens = {
        canonicalize_match_token(token)
        for token in tokenize_match_text(company_name)
        if canonicalize_match_token(token)
    }
    keyword_tokens = {
        canonicalize_match_token(token)
        for token in tokenize_match_text(keyword)
        if canonicalize_match_token(token)
    }

    company_hits = sum(1 for token in company_tokens if token in label_tokens)
    keyword_hits = sum(1 for token in keyword_tokens if token in label_tokens)

    if company_hits:
        score += 20 + (company_hits * 6)
    if keyword_hits:
        score += 10 + (keyword_hits * 4)

    lowered_label = label_text.lower()
    if any(hint in lowered_label for hint in POSITIVE_IMAGE_HINTS):
        score += 15
    if any(hint in lowered_label for hint in NEGATIVE_IMAGE_HINTS):
        score -= 35

    score -= min(max(0, int(candidate.get("index") or 0)), 10)
    return score


def featured_image_is_high_confidence(url: str) -> bool:
    lowered = clean_text(url).lower()
    if not lowered:
        return False

    if any(token in lowered for token in LOW_CONFIDENCE_IMAGE_TOKENS):
        return False

    return (
        "/p/" in lowered
        or "googleusercontent.com" in lowered
        or "lh3.googleusercontent.com" in lowered
    )


def select_featured_image_url(values, company_name: str = "", keyword: str = "") -> str:
    candidates = build_featured_image_candidates(values)
    if not candidates:
        return ""

    candidates.sort(
        key=lambda candidate: score_featured_image_candidate(
            candidate,
            company_name=company_name,
            keyword=keyword,
        ),
        reverse=True,
    )
    return candidates[0]["url"]


def collect_listing_candidates(page, keyword: str, location: str, limit=GOOGLE_MAX_LISTING_LINKS):
    try:
        items = page.locator('a[href*="/place/"]').evaluate_all(
            """
            els => els.map(el => {
                const card =
                    el.closest('div[role="article"], .Nv2PK, .hfpxzc, .bfdHYd') ||
                    el.parentElement ||
                    el;

                const imageUrls = [];
                const pushImage = (value) => {
                    if (!value) return;
                    String(value)
                        .split(',')
                        .map(part => part.trim().split(' ')[0])
                        .filter(Boolean)
                        .forEach(url => {
                            if (!imageUrls.includes(url)) {
                                imageUrls.push(url);
                            }
                        });
                };

                card.querySelectorAll('img').forEach(img => {
                    pushImage(img.currentSrc || '');
                    pushImage(img.src || '');
                    pushImage(img.getAttribute('src') || '');
                    pushImage(img.getAttribute('data-src') || '');
                    pushImage(img.getAttribute('srcset') || '');
                    pushImage(img.getAttribute('data-srcset') || '');
                });

                return {
                    href: el.href || '',
                    name: (el.getAttribute('aria-label') || el.innerText || '').trim(),
                    text: (card.innerText || el.innerText || '').trim(),
                    images: imageUrls,
                };
            }).filter(item => item.href)
            """
        )
    except Exception:
        items = []

    seen = set()
    candidates = []

    for item in items:
        href = clean_text(item.get("href"))
        preview_text = clean_preview_text(item.get("text"))

        if not href or href in seen:
            continue

        seen.add(href)

        if not is_strong_keyword_match(preview_text, keyword):
            continue

        candidates.append(
            {
                "href": href,
                "name": clean_text(item.get("name")),
                "preview_text": preview_text,
                "image": select_featured_image_url(
                    item.get("images"),
                    company_name=item.get("name") or "",
                    keyword=keyword,
                ),
                "score": score_listing_candidate(preview_text, keyword, location),
            }
        )

    candidates.sort(key=lambda item: item["score"], reverse=True)
    return candidates[:limit]


def get_body_text_safe(page, max_chars=2500):
    try:
        text = clean_text(page.locator("body").inner_text(timeout=1400))
    except Exception:
        text = ""

    if len(text) > max_chars:
        return text[:max_chars]

    return text


def split_preview_lines(value: str):
    raw = clean_preview_text(value)
    raw = raw.replace("\u00b7", "\n").replace("\u2022", "\n")
    parts = re.split(r"[\r\n]+", raw)
    lines = []

    for part in parts:
        cleaned = clean_text(part)
        if cleaned and cleaned not in lines:
            lines.append(cleaned)

    return lines


def extract_preview_phone(text: str) -> str:
    for line in reversed(split_preview_lines(text)):
        if "," in line:
            continue
        cleaned = clean_phone(line)
        digits = re.sub(r"\D", "", cleaned)
        if len(digits) >= 10:
            return cleaned

    matches = re.findall(r"(?:\+?\d[\d\s().-]{7,}\d)", text or "")
    for match in reversed(matches):
        cleaned = clean_phone(match)
        digits = re.sub(r"\D", "", cleaned)
        if len(digits) >= 10 and len(digits) != 6:
            return cleaned

    return ""


def is_probable_phone_line(line: str) -> bool:
    cleaned = clean_phone(line)
    digits = re.sub(r"\D", "", cleaned)
    letters = re.sub(r"[^A-Za-z]", "", line or "")
    return len(digits) >= 10 and len(letters) <= 2


def is_probable_preview_address(line: str, location: str) -> bool:
    normalized = normalize_match_text(line)
    if not normalized:
        return False

    if is_probable_phone_line(line):
        return False

    address_hints = (
        "road",
        "rd",
        "street",
        "st",
        "lane",
        "nagar",
        "near",
        "opp",
        "opposite",
        "marg",
        "colony",
        "layout",
        "sector",
        "building",
        "floor",
        "plot",
        "phase",
        "district",
        "maharashtra",
        "india",
    )

    if re.search(r"\b\d{5,6}\b", line):
        return True

    if any(hint in normalized for hint in address_hints):
        return True

    location_tokens = tokenize_match_text(location)
    return bool(location_tokens and any(token in normalized for token in location_tokens))


def choose_preview_address(lines: list[str], location: str) -> str:
    candidates = [line for line in lines if is_probable_preview_address(line, location)]
    if candidates:
        candidates.sort(key=len, reverse=True)
        return candidates[0]
    return ""


def choose_preview_category(lines: list[str], company_name: str, address: str, phone_number: str) -> str:
    for line in lines:
        if line == company_name or line == address or line == phone_number:
            continue
        if re.search(r"\b\d\.\d\b", line):
            continue
        if re.search(r"\b[\d,]+\s+reviews?\b", line, re.IGNORECASE):
            continue
        if is_probable_preview_address(line, ""):
            continue
        return line
    return ""


def build_preview_row(candidate: dict, keyword: str, location: str) -> dict:
    preview_text = str(candidate.get("preview_text") or "")
    lines = split_preview_lines(preview_text)
    company_name = clean_text(candidate.get("name")) or (lines[0] if lines else "")
    address = choose_preview_address(lines, location)
    phone_number = extract_preview_phone(preview_text)
    category = choose_preview_category(lines, company_name, address, phone_number)

    rating_match = re.search(r"\b(\d\.\d)\b", preview_text)
    review_match = re.search(r"([\d,]+)\s+reviews", preview_text, re.IGNORECASE)

    city, state, pincode = parse_basic_location(address)
    row = {
        "company_name": company_name,
        "keyword": clean_text(keyword),
        "location": clean_text(location),
        "category": category,
        "address": address,
        "website": "",
        "phone_number": phone_number,
        "email_1": "",
        "email_2": "",
        "rating": clean_text(rating_match.group(1) if rating_match else ""),
        "reviews_count": clean_text(review_match.group(1) if review_match else ""),
        "map_link": clean_text(candidate.get("href")),
        "cid": extract_cid(candidate.get("href")),
        "opening_hours": "",
        "featured_image": clean_text(candidate.get("image")),
        "city": clean_text(city),
        "state": clean_text(state),
        "pincode": clean_text(pincode),
        "country_code": "",
    }

    if row["phone_number"].startswith("+"):
        row["country_code"] = clean_text(row["phone_number"].split()[0])

    row["_search_text"] = clean_text(
        " ".join(part for part in [company_name, category, address, preview_text] if clean_text(part))
    )
    return row


def preview_row_has_business_data(row: dict) -> bool:
    return bool(
        clean_text(row.get("company_name"))
        and (
            clean_text(row.get("address"))
            or clean_text(row.get("category"))
            or clean_text(row.get("phone_number"))
            or clean_text(row.get("rating"))
            or clean_text(row.get("reviews_count"))
        )
    )


def preview_row_is_usable(row: dict, keyword: str, location: str) -> bool:
    if not preview_row_has_business_data(row):
        return False

    search_text = row.get("_search_text", "")
    if not is_strong_keyword_match(search_text, keyword):
        return False

    if not location:
        return True

    if row.get("address"):
        return is_strong_location_match(search_text, location)

    # Google result cards sometimes omit the city line even though the search is already location-scoped.
    return bool(
        clean_text(row.get("category"))
        or clean_text(row.get("phone_number"))
        or clean_text(row.get("rating"))
        or clean_text(row.get("reviews_count"))
    )


def preview_row_needs_detail_enrichment(row: dict) -> bool:
    return not (
        clean_text(row.get("company_name"))
        and clean_text(row.get("address"))
        and clean_text(row.get("city") or row.get("location"))
        and clean_text(row.get("pincode"))
        and featured_image_is_high_confidence(row.get("featured_image"))
    )


def preview_row_can_skip_detail(row: dict) -> bool:
    return not preview_row_needs_detail_enrichment(row)


def merge_google_rows(target: dict, source: dict):
    for key, value in source.items():
        if key.startswith("_"):
            continue
        cleaned_value = clean_text(value)
        existing_value = clean_text(target.get(key))
        if key == "featured_image":
            if not cleaned_value:
                continue

            existing_score = score_featured_image_url(existing_value) if existing_value else -1
            candidate_score = score_featured_image_url(cleaned_value)
            existing_high_confidence = featured_image_is_high_confidence(existing_value)
            candidate_high_confidence = featured_image_is_high_confidence(cleaned_value)
            if (
                (candidate_high_confidence and not existing_high_confidence)
                or candidate_score > existing_score
            ):
                target[key] = value
            continue
        if cleaned_value and (not existing_value or len(cleaned_value) > len(existing_value)):
            target[key] = value
    return target


def extract_featured_image(page, company_name: str = "", keyword: str = ""):
    try:
        image_sources = page.locator('img, [style*="background-image"]').evaluate_all(
            """
            els => els.flatMap((el, index) => {
                const candidates = [];
                const heroSelector =
                    'button[jsaction*="heroHeaderImage"],' +
                    'button[jsaction*="pane.heroHeaderImage"],' +
                    '[aria-label^="Photo of"],' +
                    '[data-photo-index="0"]';
                const closestHero = el.closest(heroSelector);
                const closestLabeled = el.closest('button,[aria-label],[title]');
                const style = window.getComputedStyle ? window.getComputedStyle(el) : null;
                const rect = typeof el.getBoundingClientRect === 'function'
                    ? el.getBoundingClientRect()
                    : { width: 0, height: 0 };
                const label = [
                    el.getAttribute('alt') || '',
                    el.getAttribute('aria-label') || '',
                    el.getAttribute('title') || '',
                    closestLabeled ? (closestLabeled.getAttribute('aria-label') || closestLabeled.getAttribute('title') || '') : '',
                ]
                    .filter(Boolean)
                    .join(' ')
                    .trim();
                const context = [
                    closestHero ? 'hero' : '',
                    el.tagName || '',
                    el.className || '',
                    closestLabeled ? (closestLabeled.className || '') : '',
                ]
                    .filter(Boolean)
                    .join(' ')
                    .trim();

                const pushCandidate = (value, extra = {}) => {
                    if (!value) return;
                    candidates.push({
                        url: value,
                        label,
                        context,
                        width: Math.round(rect.width || 0),
                        height: Math.round(rect.height || 0),
                        hero: Boolean(closestHero),
                        background: false,
                        index,
                        ...extra,
                    });
                };

                [
                    el.currentSrc || '',
                    el.src || '',
                    el.getAttribute('src') || '',
                    el.getAttribute('data-src') || '',
                    el.getAttribute('srcset') || '',
                    el.getAttribute('data-srcset') || '',
                ]
                    .filter(Boolean)
                    .forEach(value => pushCandidate(value));

                const backgroundImage = style && style.backgroundImage ? style.backgroundImage : '';
                const match = backgroundImage.match(/url\\(["']?(.*?)["']?\\)/);
                if (match && match[1]) {
                    pushCandidate(match[1], { background: true });
                }

                return candidates;
            })
            """
        )
    except Exception:
        image_sources = []

    return select_featured_image_url(
        image_sources,
        company_name=company_name,
        keyword=keyword,
    )


def enrich_row_emails(page, row: dict):
    if not row.get("website"):
        return row

    try:
        email_1, email_2 = try_open_and_collect_emails(page, row["website"])
        email_1 = clean_email(email_1)
        email_2 = clean_email(email_2)

        if email_1 and email_2 and email_1 == email_2:
            email_2 = ""

        if "@" in email_1:
            row["email_1"] = email_1
        if "@" in email_2:
            row["email_2"] = email_2
    except Exception:
        pass

    return row


def extract_place_details(page, keyword, location):
    row = {
        "company_name": "",
        "keyword": clean_text(keyword),
        "location": clean_text(location),
        "address": "",
        "website": "",
        "email_1": "",
        "email_2": "",
        "map_link": clean_text(page.url),
        "featured_image": "",
        "city": "",
        "pincode": "",
    }

    body_text = ""

    row["company_name"] = get_text_safe(page, "h1")
    row["address"] = get_text_safe(page, 'button[data-item-id="address"]')
    row["website"] = get_href_safe(page, 'a[data-item-id="authority"]')

    try:
        body_text = get_body_text_safe(page)
    except Exception:
        pass

    try:
        row["featured_image"] = extract_featured_image(
            page,
            company_name=row["company_name"],
            keyword=keyword,
        )
    except Exception:
        row["featured_image"] = ""

    city, state, pincode = parse_basic_location(row["address"])
    row["city"] = clean_text(city)
    row["pincode"] = clean_text(pincode)

    row["_search_text"] = clean_text(
        " ".join(
            part
            for part in [
                row["company_name"],
                row["address"],
                row["city"],
                row["pincode"],
                row["website"],
                body_text,
            ]
            if clean_text(part)
        )
    )

    return row


def open_maps_search(page, keyword, location):
    query = quote(f"{keyword} {location}")
    url = f"https://www.google.com/maps/search/{query}"
    goto_with_retry(
        page,
        url,
        selectors=[
            'div[role="feed"]',
            'div.m6QErb[role="feed"]',
            'a[href*="/place/"]',
            "h1",
        ],
        timeout_ms=GOOGLE_SEARCH_TIMEOUT_MS,
        settle_seconds=GOOGLE_SETTLE_SECONDS,
    )


def run_google_business_scrape(
    keywords,
    locations,
    enable_email_scraping,
    max_pages=GOOGLE_DEFAULT_MAX_PAGES,
):
    mode = "google_business"

    keyword_list = [k.strip() for k in keywords.split(",") if k.strip()] if isinstance(keywords, str) else [k.strip() for k in keywords if k.strip()]
    location_list = [l.strip() for l in locations.split(",") if l.strip()] if isinstance(locations, str) else [l.strip() for l in locations if l.strip()]
    combos = [(k, l) for k in keyword_list for l in location_list]
    search_limits = resolve_google_search_limits(max_pages)
    max_pages = search_limits["max_pages"]
    all_pages = search_limits["all_pages"]
    listing_limit = search_limits["listing_limit"]
    result_cap = search_limits["result_cap"]
    email_cap = search_limits["email_cap"]
    scroll_loops = search_limits["scroll_loops"]
    stable_round_limit = search_limits["stable_round_limit"]
    page_depth_label = search_limits["page_depth_label"]

    set_total(mode, 0)
    if not combos:
        add_log(mode, "No valid keyword/location combinations were provided")
        set_running(mode, False)
        return

    playwright = None
    browser = None
    context = None
    page = None
    seen_result_signatures = set()

    try:
        playwright, browser, context, page = start_browser(headless=True, block_images=True)
        add_log(
            mode,
            (
                f"Google Business depth set to {page_depth_label}"
                if all_pages
                else (
                    f"Google Business depth set to {page_depth_label} "
                    f"with up to {listing_limit} business listings per search"
                )
            ),
        )

        for keyword, location in combos:
            if is_stopped(mode):
                add_log(mode, "Scraping stopped")
                break

            accepted_results = 0
            email_enrichments = 0
            candidate_queue = []
            queued_hrefs = set()

            add_log(mode, f"Searching {keyword} in {location} across {page_depth_label}")

            open_maps_search(page, keyword, location)

            def queue_visible_candidates(_count=None):
                nonlocal accepted_results
                preview_rows_to_add = []
                completed_preview_count = 0

                current_candidates = collect_listing_candidates(
                    page,
                    keyword,
                    location,
                    limit=listing_limit,
                )

                for candidate in current_candidates:
                    href = clean_text(candidate.get("href"))
                    if not href or href in queued_hrefs:
                        continue

                    if result_cap is not None and accepted_results >= result_cap:
                        break

                    queued_hrefs.add(href)
                    increment_total(mode, 1)

                    preview_row = build_preview_row(candidate, keyword, location)
                    if preview_row_is_usable(preview_row, keyword, location):
                        signature = build_place_signature(preview_row)
                        if signature and signature in seen_result_signatures:
                            completed_preview_count += 1
                            continue

                        if signature:
                            seen_result_signatures.add(signature)

                        preview_row.pop("_search_text", None)
                        preview_rows_to_add.append(preview_row)
                        accepted_results += 1

                        needs_detail_enrichment = preview_row_needs_detail_enrichment(preview_row)
                        should_enrich_emails = enable_email_scraping and email_enrichments < email_cap
                        if all_pages and not should_enrich_emails and preview_row_can_skip_detail(preview_row):
                            completed_preview_count += 1
                            continue

                        if not needs_detail_enrichment and not should_enrich_emails:
                            completed_preview_count += 1
                            continue

                        candidate_queue.append(
                            {
                                "candidate": candidate,
                                "active_row": preview_row,
                            }
                        )
                        continue

                    candidate_queue.append(
                        {
                            "candidate": candidate,
                            "active_row": None,
                        }
                    )

                if preview_rows_to_add:
                    add_results(mode, preview_rows_to_add)

                if completed_preview_count:
                    increment_current(mode, completed_preview_count)

            scroll_results_panel(
                page,
                loops=scroll_loops,
                target_count=listing_limit,
                stable_round_limit=stable_round_limit,
                on_round=queue_visible_candidates,
            )

            raw_links = scrape_listing_links(page, limit=listing_limit)
            if queued_hrefs:
                add_log(
                    mode,
                    (
                        f"Queued {len(candidate_queue)} detail checks after saving "
                        f"{accepted_results} fast Google card results from {len(queued_hrefs)} matched listings"
                    ),
                )
            else:
                candidate_queue = [
                    {
                        "candidate": {"href": href, "name": "", "preview_text": ""},
                        "active_row": None,
                    }
                    for href in raw_links
                ]
                increment_total(mode, len(candidate_queue))
                add_log(mode, f"Loaded {len(candidate_queue)} listing links for fast scraping")

            if not candidate_queue and accepted_results == 0:
                increment_total(mode, 1)
                add_log(mode, "No listings found for this search")
                increment_current(mode)
                continue

            # --- Concurrent detail scraping using multiple tabs ---
            detail_tabs = open_concurrent_pages(context, count=5)
            tab_pool = detail_tabs if detail_tabs else [page]
            batch_size = len(tab_pool)

            queue_index = 0
            while queue_index < len(candidate_queue):
                if is_stopped(mode):
                    add_log(mode, "Scraping stopped")
                    break

                # Build current batch
                batch = []
                while len(batch) < batch_size and queue_index < len(candidate_queue):
                    queued_item = candidate_queue[queue_index]
                    queue_index += 1

                    if (
                        result_cap is not None
                        and accepted_results >= result_cap
                        and queued_item["active_row"] is None
                    ):
                        add_log(
                            mode,
                            f"Reached fast result cap of {result_cap} strong matches",
                        )
                        queue_index = len(candidate_queue)  # exit outer loop
                        break

                    batch.append(queued_item)

                if not batch:
                    break

                # Navigate all tabs in the batch concurrently
                for idx, queued_item in enumerate(batch):
                    tab = tab_pool[idx % batch_size]
                    href = queued_item["candidate"]["href"]
                    try:
                        goto_with_retry(
                            tab,
                            href,
                            selectors=[
                                "h1",
                                'button[data-item-id="address"]',
                                'button[data-item-id*="phone"]',
                            ],
                            timeout_ms=GOOGLE_DETAIL_TIMEOUT_MS,
                            settle_seconds=GOOGLE_SETTLE_SECONDS,
                        )
                    except Exception:
                        pass  # handled below during extraction

                # Extract details from each loaded tab
                for idx, queued_item in enumerate(batch):
                    if is_stopped(mode):
                        break

                    tab = tab_pool[idx % batch_size]
                    candidate = queued_item["candidate"]
                    active_row = queued_item["active_row"]

                    try:
                        row = extract_place_details(tab, keyword, location)

                        if row["company_name"] and row["address"]:
                            if not is_strong_google_match(row.get("_search_text", ""), keyword, location):
                                add_log(
                                    mode,
                                    f'Skipped weak match: {row["company_name"]} | {row["address"]}',
                                )
                                increment_current(mode)
                                continue

                            row.pop("_search_text", None)
                            if active_row is not None:
                                merge_google_rows(active_row, row)
                                target_row = active_row
                            else:
                                signature = build_place_signature(row)
                                if signature and signature in seen_result_signatures:
                                    increment_current(mode)
                                    continue

                                if signature:
                                    seen_result_signatures.add(signature)

                                add_result(mode, row)
                                accepted_results += 1
                                target_row = row

                            if enable_email_scraping and email_enrichments < email_cap:
                                enrich_row_emails(tab, target_row)
                                email_enrichments += 1

                    except Exception as e:
                        add_log(mode, f"Error reading place: {e}")

                    increment_current(mode)

            close_concurrent_pages(detail_tabs)

    except Exception as e:
        if page is None:
            add_log(mode, f"Unable to start browser: {e}")
        else:
            add_log(mode, f"Scraper error: {e}")

    finally:
        close_browser(playwright=playwright, browser=browser, context=context, page=page)
        set_running(mode, False)
        add_log(mode, "Google Business scraping finished")
