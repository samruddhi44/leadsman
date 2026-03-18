from urllib.parse import quote

from backend.state import (
    add_log,
    add_result,
    increment_current,
    is_stopped,
    set_running,
    set_total,
)
from backend.scraper.utils import (
    start_browser,
    close_browser,
    sleep_small,
    extract_emails_from_text,
    extract_phones_from_text,
    get_domain,
    clean_text,
)

PLATFORM_DOMAINS = {
    "facebook": "facebook.com",
    "instagram": "instagram.com",
    "linkedin": "linkedin.com",
    "youtube": "youtube.com",
}


def is_valid_platform_link(href: str, domain: str) -> bool:
    if not href:
        return False

    href_low = href.lower()

    blocked = [
        "accounts.google.com",
        "google.com/search",
        "google.com/preferences",
        "google.com/advanced_search",
        "maps.google.com",
        "support.google.com",
        "policies.google.com",
        "webcache.googleusercontent.com",
        "/search?",
        "/imgres?",
        "/aclk?",
        "/policies/",
        "/settings/",
    ]

    for item in blocked:
        if item in href_low:
            return False

    if domain not in href_low:
        return False

    if not href.startswith("http"):
        return False

    return True


def collect_search_results(page, domain):
    results = []
    seen = set()

    try:
        items = page.locator("div.yuRUbf > a").all()

        for el in items:
            href = el.get_attribute("href") or ""

            try:
                title = el.locator("h3").inner_text()
            except Exception:
                title = href

            if not href:
                continue

            if not is_valid_platform_link(href, domain):
                continue

            if href in seen:
                continue

            seen.add(href)
            results.append((clean_text(title), clean_text(href)))

    except Exception as e:
        add_log("social_lookup", f"Search result parsing error: {e}")

    if not results:
        try:
            items = page.locator("a[href]").all()

            for el in items:
                href = el.get_attribute("href") or ""

                try:
                    title = el.inner_text()
                except Exception:
                    title = href

                if not href:
                    continue

                if not is_valid_platform_link(href, domain):
                    continue

                if href in seen:
                    continue

                seen.add(href)
                results.append((clean_text(title), clean_text(href)))
        except Exception:
            pass

    return results[:20]


def scan_profile_page(page, keyword: str, location: str, platform: str, link: str, title: str) -> dict:
    row = {
        "title": clean_text(title),
        "domain": clean_text(get_domain(link)),
        "phones": "",
        "emails": "",
        "link": clean_text(link),
        "source": clean_text(platform),
        "category": clean_text(keyword),
        "location": clean_text(location),
    }

    try:
        page.goto(link, wait_until="domcontentloaded", timeout=15000)
        sleep_small(2)

        html = page.content()
        body_text = ""

        try:
            body_text = page.locator("body").inner_text(timeout=5000)
        except Exception:
            body_text = ""

        combined_text = f"{html} {body_text}"

        phones = extract_phones_from_text(combined_text)
        emails = extract_emails_from_text(combined_text)

        row["phones"] = clean_text(", ".join(phones[:3]))
        row["emails"] = clean_text(", ".join(emails[:2]))

        if not row["location"]:
            lines = [x.strip() for x in body_text.split("\n") if x.strip()]
            for line in lines[:100]:
                low = line.lower()
                if any(city in low for city in [
                    "mumbai", "pune", "kolhapur", "delhi", "bangalore",
                    "hyderabad", "nagpur", "nashik", "chennai", "patna"
                ]):
                    row["location"] = clean_text(line)
                    break

    except Exception:
        pass

    return row


def run_social_lookup_scrape(keyword: str, location: str, platforms: list[str], max_pages: int):
    mode = "social_lookup"

    if not platforms:
        add_log(mode, "No platforms selected")
        set_total(mode, 0)
        set_running(mode, False)
        return

    total_steps = len(platforms) * max_pages
    set_total(mode, total_steps)

    playwright, browser, context, page = start_browser(headless=True)

    try:
        for platform in platforms:
            domain = PLATFORM_DOMAINS.get(platform, platform)

            for page_num in range(1, max_pages + 1):
                if is_stopped(mode):
                    add_log(mode, "Social lookup stopped safely")
                    break

                query_text = f'{keyword} {location}'.strip()
                add_log(mode, f'Navigating to page {page_num}/{max_pages} for "{query_text}" on {domain}')

                query = quote(f'site:{domain} "{query_text}"')
                start = (page_num - 1) * 10
                url = f"https://www.google.com/search?q={query}&start={start}&hl=en"

                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=15000)
                    sleep_small(3)
                except Exception as e:
                    add_log(mode, f"Search page load failed: {e}")
                    increment_current(mode)
                    continue

                results = collect_search_results(page, domain)
                add_log(mode, f"Found {len(results)} candidate links on {domain}")

                for title, href in results:
                    if is_stopped(mode):
                        break

                    row = scan_profile_page(page, keyword, location, platform, href, title)

                    if row["title"] and row["domain"] and domain in row["domain"].lower():
                        add_result(mode, row)

                increment_current(mode)

            if is_stopped(mode):
                break

    except Exception as exc:
        add_log(mode, f"Fatal Social Lookup scraper error: {exc}")

    finally:
        close_browser(playwright, browser)
        set_running(mode, False)
        add_log(mode, "Social Lookup scraping finished")