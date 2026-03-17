import re
import time
from urllib.parse import urlparse

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

EMAIL_REGEX = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_REGEX = re.compile(r"(?:\+?\d[\d\s().-]{7,}\d)")
PINCODE_REGEX = re.compile(r"\b\d{5,6}\b")


def build_driver():
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-notifications")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)


def safe_text(element):
    try:
        return element.text.strip()
    except Exception:
        return ""


def safe_attr(element, attr_name):
    try:
        return element.get_attribute(attr_name) or ""
    except Exception:
        return ""


def sleep_small(seconds=1.2):
    time.sleep(seconds)


def extract_emails_from_text(text: str) -> list[str]:
    if not text:
        return []
    return list(dict.fromkeys(EMAIL_REGEX.findall(text)))[:2]


def extract_phones_from_text(text: str) -> list[str]:
    if not text:
        return []
    found = [x.strip() for x in PHONE_REGEX.findall(text)]
    return list(dict.fromkeys(found))[:3]


def get_domain(url: str) -> str:
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


def try_open_and_collect_emails(driver, website_url: str):
    if not website_url:
        return "", ""

    emails = []
    original = driver.current_window_handle

    try:
        driver.execute_script("window.open(arguments[0], '_blank');", website_url)
        driver.switch_to.window(driver.window_handles[-1])
        sleep_small(2)

        pages_to_try = [website_url]
        for suffix in ["/contact", "/contact-us", "/about", "/about-us"]:
            if website_url.endswith("/"):
                pages_to_try.append(website_url[:-1] + suffix)
            else:
                pages_to_try.append(website_url + suffix)

        for page in pages_to_try:
            try:
                driver.get(page)
                sleep_small(2)
                text = driver.page_source
                emails.extend(extract_emails_from_text(text))
                emails = list(dict.fromkeys(emails))
                if len(emails) >= 2:
                    break
            except Exception:
                continue

    except Exception:
        pass

    finally:
        try:
            driver.close()
            driver.switch_to.window(original)
        except Exception:
            pass

    emails = emails[:2]

    if len(emails) == 1:
        return emails[0], ""
    if len(emails) >= 2:
        return emails[0], emails[1]
    return "", ""