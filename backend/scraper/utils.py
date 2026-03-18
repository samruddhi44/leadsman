import re
import time
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
PINCODE_REGEX = re.compile(r"\b\d{5,6}\b")


def start_browser(headless=True):
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=headless)
    context = browser.new_context()
    page = context.new_page()
    page.set_default_timeout(15000)
    return playwright, browser, context, page


def close_browser(playwright, browser):
    try:
        browser.close()
    except Exception:
        pass

    try:
        playwright.stop()
    except Exception:
        pass


def sleep_small(seconds=1.2):
    time.sleep(seconds)


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
    match = EMAIL_REGEX.search(value)
    return match.group(0) if match else ""


def extract_emails_from_text(text: str):
    if not text:
        return []

    emails = EMAIL_REGEX.findall(text)
    cleaned = []

    blocked_words = [
        "example.com",
        "your@email",
        "test@test",
        "wixpress.com",
        "sentry.io",
    ]

    for email in emails:
        e = email.strip().lower()
        if any(bad in e for bad in blocked_words):
            continue
        if e not in cleaned:
            cleaned.append(e)

    return cleaned[:2]


def extract_phones_from_text(text: str):
    if not text:
        return []
    found = [x.strip() for x in PHONE_REGEX.findall(text)]
    unique = []
    for item in found:
        item = clean_phone(item)
        if item and item not in unique:
            unique.append(item)
    return unique[:3]


def get_domain(url: str):
    try:
        return urlparse(url).netloc
    except Exception:
        return ""


def parse_basic_location(address: str):
    city, state, pincode = "", "", ""
    if not address:
        return city, state, pincode

    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) >= 2:
        city = parts[-3] if len(parts) >= 3 else parts[-2]
        state = parts[-2] if len(parts) >= 2 else ""

    pin_match = PINCODE_REGEX.search(address)
    if pin_match:
        pincode = pin_match.group(0)

    return city, state, pincode


def try_open_and_collect_emails(page, website_url: str):
    if not website_url:
        return "", ""

    emails = []

    pages_to_try = [website_url]
    for suffix in ["/contact", "/contact-us", "/about", "/about-us"]:
        if website_url.endswith("/"):
            pages_to_try.append(website_url[:-1] + suffix)
        else:
            pages_to_try.append(website_url + suffix)

    for target in pages_to_try:
        try:
            temp = page.context.new_page()
            temp.goto(target, wait_until="domcontentloaded", timeout=15000)
            sleep_small(1.5)

            text = temp.content()
            found = extract_emails_from_text(text)

            for e in found:
                e = clean_email(e)
                if e and e not in emails:
                    emails.append(e)

            temp.close()

            if len(emails) >= 2:
                break
        except Exception:
            try:
                temp.close()
            except Exception:
                pass
            continue

    emails = emails[:2]

    if len(emails) == 1:
        return emails[0], ""
    if len(emails) >= 2:
        return emails[0], emails[1]
    return "", ""