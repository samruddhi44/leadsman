import re
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
    safe_text,
    safe_attr,
    sleep_small,
    parse_basic_location,
    try_open_and_collect_emails,
)


def clean_text(value):
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


def clean_phone(value):
    value = clean_text(value)
    if not value:
        return ""

    value = re.sub(r"[^\d+\-\s()]", "", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def clean_email(value):
    value = clean_text(value).lower()
    if not value:
        return ""

    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", value)
    return match.group(0) if match else ""


def extract_cid(url):
    match = re.search(r"[?&]cid=([0-9]+)", url or "")
    if match:
        return match.group(1)
    return ""


def scrape_listing_links(driver):
    cards = driver.find_elements(By.CSS_SELECTOR, 'a[href*="/place/"]')
    seen = set()
    links = []

    for card in cards:
        href = card.get_attribute("href") or ""
        if href and href not in seen:
            seen.add(href)
            links.append(href)

    return links[:25]


def extract_featured_image(driver):
    try:
        meta = driver.find_element(By.CSS_SELECTOR, 'meta[property="og:image"]')
        img = meta.get_attribute("content") or ""
        if img and img.startswith("http") and "googlelogo" not in img.lower():
            return img
    except Exception:
        pass

    try:
        images = driver.find_elements(By.CSS_SELECTOR, "img")
        for img in images:
            src = img.get_attribute("src") or ""
            if not src:
                continue
            low = src.lower()
            if "googlelogo" in low:
                continue
            if src.startswith("http"):
                return src
    except Exception:
        pass

    return ""


def extract_place_details(driver, keyword, location, enable_email_scraping):
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
        "map_link": clean_text(driver.current_url),
        "cid": extract_cid(driver.current_url),
        "opening_hours": "",
        "featured_image": "",
        "city": "",
        "state": "",
        "pincode": "",
        "country_code": "",
    }

    try:
        name = driver.find_element(By.CSS_SELECTOR, "h1")
        row["company_name"] = clean_text(safe_text(name))
    except Exception:
        pass

    try:
        category = driver.find_element(By.CSS_SELECTOR, 'button[jsaction*="pane.rating.category"]')
        row["category"] = clean_text(safe_text(category))
    except Exception:
        pass

    try:
        address = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id="address"]')
        row["address"] = clean_text(safe_text(address))
    except Exception:
        pass

    try:
        website = driver.find_element(By.CSS_SELECTOR, 'a[data-item-id="authority"]')
        row["website"] = clean_text(safe_attr(website, "href"))
    except Exception:
        pass

    try:
        phone = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id*="phone"]')
        row["phone_number"] = clean_phone(safe_text(phone))
    except Exception:
        pass

    try:
        page = driver.page_source

        rating_match = re.search(r"(\d\.\d)", page)
        if rating_match:
            row["rating"] = clean_text(rating_match.group(1))

        review_match = re.search(r"([\d,]+)\s+reviews", page, re.IGNORECASE)
        if review_match:
            row["reviews_count"] = clean_text(review_match.group(1))
    except Exception:
        pass

    try:
        row["featured_image"] = extract_featured_image(driver)
    except Exception:
        pass

    try:
        hours_match = re.search(r"(\d+\s*hours?[^<]{0,60})", driver.page_source, re.IGNORECASE)
        if hours_match:
            row["opening_hours"] = clean_text(hours_match.group(1))
    except Exception:
        pass

    city, state, pincode = parse_basic_location(row["address"])
    row["city"] = clean_text(city)
    row["state"] = clean_text(state)
    row["pincode"] = clean_text(pincode)

    if row["phone_number"].startswith("+"):
        row["country_code"] = clean_text(row["phone_number"].split()[0])

    # safer email scraping
    if enable_email_scraping and row["website"]:
        try:
            email_1, email_2 = try_open_and_collect_emails(driver, row["website"])

            email_1 = clean_email(email_1)
            email_2 = clean_email(email_2)

            # avoid duplicate emails
            if email_1 and email_2 and email_1 == email_2:
                email_2 = ""

            # avoid putting garbage values
            if "@" in email_1:
                row["email_1"] = email_1
            if "@" in email_2:
                row["email_2"] = email_2

        except Exception:
            row["email_1"] = ""
            row["email_2"] = ""

    return row


def run_google_business_scrape(keywords, locations, enable_email_scraping):
    mode = "google_business"

    keyword_list = [k.strip() for k in keywords.split(",") if k.strip()]
    location_list = [l.strip() for l in locations.split(",") if l.strip()]
    combos = [(k, l) for k in keyword_list for l in location_list]

    set_total(mode, len(combos))
    driver = build_driver()

    try:
        for keyword, location in combos:
            if is_stopped(mode):
                add_log(mode, "Scraping stopped")
                break

            add_log(mode, f"Searching {keyword} in {location}")

            search_url = f"https://www.google.com/maps/search/{quote(keyword + ' ' + location)}"
            driver.get(search_url)
            sleep_small(5)

            for _ in range(6):
                driver.execute_script("window.scrollBy(0, 1600);")
                sleep_small(1.5)

            links = scrape_listing_links(driver)
            add_log(mode, f"Found {len(links)} listings")

            for href in links:
                if is_stopped(mode):
                    add_log(mode, "Scraping stopped")
                    break

                if not href:
                    continue

                try:
                    driver.get(href)
                    sleep_small(3)

                    row = extract_place_details(driver, keyword, location, enable_email_scraping)

                    # add only meaningful rows
                    if row["company_name"] and row["address"]:
                        add_result(mode, row)

                except Exception as e:
                    add_log(mode, f"Error reading place: {e}")

            increment_current(mode)

    except Exception as e:
        add_log(mode, f"Scraper error: {e}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

        set_running(mode, False)
        add_log(mode, "Google Business scraping finished")