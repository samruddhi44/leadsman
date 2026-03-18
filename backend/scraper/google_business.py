

import re
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
    clean_text,
    clean_phone,
    clean_email,
    sleep_small,
    parse_basic_location,
    try_open_and_collect_emails,
)


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


def scroll_results_panel(page, loops=20):
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

    last_count = 0
    stable_rounds = 0

    for _ in range(loops):
        try:
            card_count = page.locator('a[href*="/place/"]').count()
            panel.evaluate("(el) => el.scrollBy(0, el.scrollHeight)")
            sleep_small(1.2)
            new_count = page.locator('a[href*="/place/"]').count()

            if new_count == last_count:
                stable_rounds += 1
            else:
                stable_rounds = 0

            last_count = new_count

            if stable_rounds >= 4:
                break
        except Exception:
            break


def scrape_listing_links(page, limit=80):
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


def extract_featured_image(page):
    try:
        srcs = page.locator("img").evaluate_all(
            "els => els.map(el => el.src).filter(Boolean)"
        )
    except Exception:
        srcs = []

    for src in srcs:
        low = src.lower()
        blocked = [
            "googlelogo",
            "maps.gstatic",
            "gstatic.com/mapfiles",
            "streetviewpixels",
            "staticmap",
            "maptile",
            "favicon",
        ]

        if any(x in low for x in blocked):
            continue

        if "googleusercontent.com" in low or "lh3.googleusercontent.com" in low:
            return src

    return ""


def extract_place_details(page, keyword, location, enable_email_scraping):
    row = {
        "company_name": "",
        "keyword": clean_text(keyword),
        "location": clean_text(location),
        "category": "",
        "address": "",
        "website": "",
        "phone_number": "",
        "email_1": "",
        "email_2": "",
        "rating": "",
        "reviews_count": "",
        "map_link": clean_text(page.url),
        "cid": extract_cid(page.url),
        "opening_hours": "",
        "featured_image": "",
        "city": "",
        "state": "",
        "pincode": "",
        "country_code": "",
    }

    row["company_name"] = get_text_safe(page, "h1")
    row["category"] = get_text_safe(page, 'button[jsaction*="pane.rating.category"]')
    row["address"] = get_text_safe(page, 'button[data-item-id="address"]')
    row["website"] = get_href_safe(page, 'a[data-item-id="authority"]')
    row["phone_number"] = clean_phone(get_text_safe(page, 'button[data-item-id*="phone"]'))

    try:
        html = page.content()

        rating_match = re.search(r"(\d\.\d)", html)
        if rating_match:
            row["rating"] = clean_text(rating_match.group(1))

        review_match = re.search(r"([\d,]+)\s+reviews", html, re.IGNORECASE)
        if review_match:
            row["reviews_count"] = clean_text(review_match.group(1))

        hours_match = re.search(r"(\d+\s*hours?[^<]{0,60})", html, re.IGNORECASE)
        if hours_match:
            row["opening_hours"] = clean_text(hours_match.group(1))
    except Exception:
        pass

    try:
        row["featured_image"] = extract_featured_image(page)
    except Exception:
        row["featured_image"] = ""

    city, state, pincode = parse_basic_location(row["address"])
    row["city"] = clean_text(city)
    row["state"] = clean_text(state)
    row["pincode"] = clean_text(pincode)

    if row["phone_number"].startswith("+"):
        row["country_code"] = clean_text(row["phone_number"].split()[0])

    if enable_email_scraping and row["website"]:
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


def open_maps_search(page, keyword, location):
    query = quote(f"{keyword} {location}")
    url = f"https://www.google.com/maps/search/{query}"
    page.goto(url, wait_until="domcontentloaded")
    sleep_small(4)


def run_google_business_scrape(keywords, locations, enable_email_scraping):
    mode = "google_business"

    keyword_list = [k.strip() for k in keywords.split(",") if k.strip()]
    location_list = [l.strip() for l in locations.split(",") if l.strip()]
    combos = [(k, l) for k in keyword_list for l in location_list]

    set_total(mode, len(combos))

    playwright, browser, context, page = start_browser(headless=True)

    try:
        for keyword, location in combos:
            if is_stopped(mode):
                add_log(mode, "Scraping stopped")
                break

            add_log(mode, f"Searching {keyword} in {location}")

            open_maps_search(page, keyword, location)
            scroll_results_panel(page, loops=25)

            links = scrape_listing_links(page, limit=80)
            add_log(mode, f"Loaded {len(links)} listing links")

            if not links:
                add_log(mode, "No listings found for this search")
                increment_current(mode)
                continue

            for href in links:
                if is_stopped(mode):
                    add_log(mode, "Scraping stopped")
                    break

                try:
                    page.goto(href, wait_until="domcontentloaded")
                    sleep_small(2)

                    row = extract_place_details(page, keyword, location, enable_email_scraping)

                    if row["company_name"] and row["address"]:
                        add_result(mode, row)

                except Exception as e:
                    add_log(mode, f"Error reading place: {e}")

            increment_current(mode)

    except Exception as e:
        add_log(mode, f"Scraper error: {e}")

    finally:
        close_browser(playwright, browser)
        set_running(mode, False)
        add_log(mode, "Google Bussiness scraping finished")