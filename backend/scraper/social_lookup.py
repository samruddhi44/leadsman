from urllib.parse import quote

from selenium.webdriver.common.by import By

from backend.state import (
    add_log,
    add_result,
    increment_current,
    is_stopped,
    set_running,
    set_total,
)
from backend.scraper.utils import (
    build_driver,
    sleep_small,
    extract_emails_from_text,
    extract_phones_from_text,
    get_domain,
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
    ]

    for item in blocked:
        if item in href_low:
            return False

    if domain not in href_low:
        return False

    return href.startswith("http")


def clean_text(value: str) -> str:
    if not value:
        return ""

    value = str(value)

    replacements = {
        "\u00a0": " ",
        "\n": " ",
        "\r": " ",
        "\t": " ",
        "": "",
        "": "",
        "": "",
        "": "",
    }

    for old, new in replacements.items():
        value = value.replace(old, new)

    return value.strip()


def collect_search_results(driver, domain):
    anchors = driver.find_elements(By.CSS_SELECTOR, "a[href]")
    results = []
    seen = set()

    for a in anchors:
        href = a.get_attribute("href") or ""
        title = (a.text or "").strip()

        if not is_valid_platform_link(href, domain):
            continue

        if href in seen:
            continue

        if not title or len(title.strip()) < 3:
            title = href

        seen.add(href)
        results.append((title, href))

    return results[:20]


def scan_profile_page(driver, keyword, platform, link, title):
    row = {
        "title": clean_text(title),
        "domain": clean_text(get_domain(link)),
        "phones": "",
        "emails": "",
        "link": clean_text(link),
        "source": clean_text(platform),
        "category": clean_text(keyword),
        "location": "",
    }

    try:
        driver.get(link)
        sleep_small(2.5)

        text = driver.page_source.replace("\n", " ")
        body_text = driver.find_element(By.TAG_NAME, "body").text

        phones = extract_phones_from_text(text)
        emails = extract_emails_from_text(text)

        row["phones"] = clean_text(", ".join(phones))
        row["emails"] = clean_text(", ".join(emails))

        lines = [x.strip() for x in body_text.split("\n") if x.strip()]

        for line in lines[:80]:
            low = line.lower()
            if any(city_word in low for city_word in [
                "mumbai", "delhi", "pune", "kolkata",
                "bangalore", "hyderabad", "patna", "kolhapur",
                "chennai", "nagpur", "nashik"
            ]):
                row["location"] = clean_text(line)
                break

    except Exception:
        pass

    return row


def run_social_lookup_scrape(keyword, platforms, max_pages):
    mode = "social_lookup"
    total_steps = len(platforms) * max_pages if platforms else 0
    set_total(mode, total_steps)

    driver = build_driver()

    try:
        for platform in platforms:
            domain = PLATFORM_DOMAINS.get(platform, platform)

            for page_num in range(1, max_pages + 1):
                if is_stopped(mode):
                    add_log(mode, "Social lookup stopped safely")
                    break

                add_log(mode, f'Navigating to page {page_num}/{max_pages} for "{keyword}" on {domain}')

                query = quote(f'site:{domain} "{keyword}"')
                start = (page_num - 1) * 10
                url = f"https://www.google.com/search?q={query}&start={start}"

                driver.get(url)
                sleep_small(2.5)

                results = collect_search_results(driver, domain)

                add_log(mode, f"Found {len(results)} filtered links on {domain}")

                for title, href in results:
                    if is_stopped(mode):
                        break

                    row = scan_profile_page(driver, keyword, platform, href, title)

                    if (
                        row["domain"]
                        and domain in row["domain"].lower()
                        and (
                            row["title"]
                            or row["phones"]
                            or row["emails"]
                            or row["location"]
                        )
                    ):
                        add_result(mode, row)

                increment_current(mode)

            if is_stopped(mode):
                break

    except Exception as exc:
        add_log(mode, f"Fatal Social Lookup scraper error: {exc}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

        set_running(mode, False)
        add_log(mode, "Social Lookup scraping finished")