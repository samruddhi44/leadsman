import re
import time
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
PINCODE_REGEX = re.compile(r"\b\d{5,6}\b")
BLOCKED_RESOURCE_TYPES = {"font", "media"}
BLOCKED_RESOURCE_TYPES_WITH_IMAGES = BLOCKED_RESOURCE_TYPES | {"image"}
EMAIL_LOOKUP_TIMEOUT_MS = 3000
EMAIL_LOOKUP_SETTLE_SECONDS = 0.02
EMAIL_BODY_TIMEOUT_MS = 800
EMAIL_LOOKUP_SUFFIXES = ("", "/contact", "/contact-us")
CONCURRENT_TAB_LIMIT = 6


def _install_resource_filter(context, block_images: bool):
    blocked_types = (
        BLOCKED_RESOURCE_TYPES_WITH_IMAGES if block_images else BLOCKED_RESOURCE_TYPES
    )

    def handler(route):
        try:
            if route.request.resource_type in blocked_types:
                route.abort()
                return
        except Exception:
            pass

        route.continue_()

    context.route("**/*", handler)


def start_browser(headless=True, block_images=False):
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(
        headless=headless,
        args=[
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
        ],
    )
    context = browser.new_context(
        viewport={"width": 1440, "height": 900},
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        locale="en-US",
    )
    _install_resource_filter(context, block_images=block_images)
    page = context.new_page()
    page.set_default_timeout(15000)
    return playwright, browser, context, page


def open_concurrent_pages(context, count=CONCURRENT_TAB_LIMIT):
    """Open multiple browser tabs for parallel detail scraping."""
    pages = []
    for _ in range(count):
        try:
            p = context.new_page()
            p.set_default_timeout(15000)
            pages.append(p)
        except Exception:
            break
    return pages


def close_concurrent_pages(pages):
    """Close all concurrent tab pages."""
    for p in pages:
        try:
            p.close()
        except Exception:
            pass


def close_browser(playwright=None, browser=None, context=None, page=None):
    try:
        if page:
            page.close()
    except Exception:
        pass

    try:
        if context:
            context.close()
    except Exception:
        pass

    try:
        if browser:
            browser.close()
    except Exception:
        pass

    try:
        if playwright:
            playwright.stop()
    except Exception:
        pass


def sleep_small(seconds=1.2):
    time.sleep(seconds)


def wait_for_any_selector(page, selectors, timeout_ms=5000, poll_interval=0.2):
    deadline = time.time() + (timeout_ms / 1000)

    while time.time() < deadline:
        for selector in selectors:
            try:
                if page.locator(selector).first.count() > 0:
                    return True
            except Exception:
                continue
        time.sleep(poll_interval)

    return False


def goto_and_wait(
    page,
    url: str,
    selectors=None,
    timeout_ms=20000,
    settle_seconds=0.2,
):
    page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)

    if selectors:
        wait_for_any_selector(page, selectors, timeout_ms=max(1500, timeout_ms // 2))

    if settle_seconds > 0:
        sleep_small(settle_seconds)


def goto_with_retry(
    page,
    url: str,
    selectors=None,
    timeout_ms=20000,
    settle_seconds=0.2,
    attempts: int = 2,
    retry_delay_seconds: float = 0.05,
):
    last_error = None

    for attempt in range(max(1, attempts)):
        try:
            goto_and_wait(
                page,
                url,
                selectors=selectors,
                timeout_ms=timeout_ms,
                settle_seconds=settle_seconds,
            )
            return
        except Exception as exc:
            last_error = exc
            if attempt < attempts - 1 and retry_delay_seconds > 0:
                sleep_small(retry_delay_seconds)

    if last_error is not None:
        raise last_error


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

    pin_match = PINCODE_REGEX.search(address)
    if pin_match:
        pincode = pin_match.group(0)

    relevant_parts = parts
    if len(parts) >= 3 and not PINCODE_REGEX.search(parts[-1]) and len(parts[-1].split()) <= 3:
        relevant_parts = parts[:-1]

    if len(relevant_parts) >= 2:
        city = relevant_parts[-2]
        state = relevant_parts[-1]
    elif relevant_parts:
        city = relevant_parts[-1]

    state = re.sub(PINCODE_REGEX, "", state).strip(" ,;-")

    return clean_text(city), clean_text(state), clean_text(pincode)


def try_open_and_collect_emails(page, website_url: str):
    if not website_url:
        return "", ""

    emails = []
    base_url = website_url[:-1] if website_url.endswith("/") else website_url
    pages_to_try = [base_url + suffix if suffix else website_url for suffix in EMAIL_LOOKUP_SUFFIXES]
    temp = None

    try:
        temp = page.context.new_page()

        for target in pages_to_try:
            try:
                goto_and_wait(
                    temp,
                    target,
                    selectors=["body", 'a[href^="mailto:"]'],
                    timeout_ms=EMAIL_LOOKUP_TIMEOUT_MS,
                    settle_seconds=EMAIL_LOOKUP_SETTLE_SECONDS,
                )

                try:
                    body_text = temp.locator("body").inner_text(timeout=EMAIL_BODY_TIMEOUT_MS)
                except Exception:
                    body_text = ""

                try:
                    mailtos = temp.locator('a[href^="mailto:"]').evaluate_all(
                        "els => els.map(el => el.getAttribute('href') || '')"
                    )
                except Exception:
                    mailtos = []

                found = extract_emails_from_text(body_text)
                for href in mailtos:
                    found.append(clean_text(str(href)).replace("mailto:", "", 1))

                for e in found:
                    e = clean_email(e)
                    if e and e not in emails:
                        emails.append(e)

                if len(emails) >= 2:
                    break
            except Exception:
                continue
    finally:
        try:
            if temp:
                temp.close()
        except Exception:
            pass

    emails = emails[:2]

    if len(emails) == 1:
        return emails[0], ""
    if len(emails) >= 2:
        return emails[0], emails[1]
    return "", ""
