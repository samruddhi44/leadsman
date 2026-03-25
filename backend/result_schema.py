from urllib.parse import urlparse

from backend.scraper.utils import clean_email, clean_phone, clean_text, parse_basic_location


def clean_http_url(value: str) -> str:
    url = clean_text(value)
    if not url:
        return ""

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return ""

    return url


def dedupe_clean_values(values, cleaner, limit=None):
    items = []

    for value in values:
        cleaned = cleaner(value)
        if cleaned and cleaned not in items:
            items.append(cleaned)
        if limit and len(items) >= limit:
            break

    return items


def build_google_output_row(row: dict) -> dict:
    full_address = clean_text(row.get("full_address") or row.get("address"))
    city = clean_text(row.get("city"))
    pin_code = clean_text(row.get("pin_code") or row.get("pincode"))
    requested_location = clean_text(row.get("location"))

    if full_address and (not city or not pin_code):
        parsed_city, _, parsed_pin = parse_basic_location(full_address)
        city = city or clean_text(parsed_city)
        pin_code = pin_code or clean_text(parsed_pin)

    if requested_location:
        normalized_city = clean_text(city).lower()
        normalized_location = requested_location.lower()
        normalized_address = full_address.lower()

        if not city or (
            normalized_location in normalized_address and normalized_location not in normalized_city
        ):
            city = requested_location

    emails = dedupe_clean_values(
        [
            row.get("email"),
            row.get("email_1"),
            row.get("email_2"),
        ],
        clean_email,
        limit=2,
    )

    return {
        "business_name": clean_text(row.get("business_name") or row.get("company_name")),
        "city": city,
        "map_link": clean_http_url(row.get("map_link")),
        "full_address": full_address,
        "email": ", ".join(emails),
        "featured_image_url": clean_http_url(
            row.get("featured_image_url") or row.get("featured_image")
        ),
        "pin_code": pin_code,
    }


def google_output_is_valid(row: dict) -> bool:
    return bool(
        clean_text(row.get("business_name"))
        and clean_text(row.get("city"))
        and clean_http_url(row.get("map_link"))
        and clean_text(row.get("full_address"))
    )


def build_social_output_row(row: dict) -> dict:
    contact_info = dedupe_clean_values(
        [
            value
            for key in ("contact_info", "emails", "phones")
            for value in clean_text(row.get(key)).split(",")
        ],
        lambda value: clean_email(value) or clean_phone(value),
        limit=4,
    )

    return {
        "profile_name": clean_text(row.get("profile_name") or row.get("title")),
        "platform": clean_text(row.get("platform") or row.get("source")),
        "profile_link": clean_http_url(row.get("profile_link") or row.get("link")),
        "bio": clean_text(row.get("bio") or row.get("description")),
        "followers": clean_text(row.get("followers")),
        "contact_info": ", ".join(contact_info),
    }


def social_output_is_valid(row: dict) -> bool:
    has_details = bool(clean_text(row.get("bio")) or clean_text(row.get("contact_info")))
    return bool(
        clean_text(row.get("profile_name"))
        and clean_text(row.get("platform"))
        and clean_http_url(row.get("profile_link"))
        and has_details
    )


def project_mode_result(mode: str, row: dict) -> dict:
    if mode == "google_business":
        return build_google_output_row(row)
    if mode == "social_lookup":
        return build_social_output_row(row)
    return {key: clean_text(value) for key, value in row.items()}


def project_results(mode: str, rows: list[dict]) -> list[dict]:
    projected = []

    for row in rows:
        mapped = project_mode_result(mode, row)
        if mode == "google_business" and not google_output_is_valid(mapped):
            continue
        if mode == "social_lookup" and not social_output_is_valid(mapped):
            continue
        projected.append(mapped)

    return projected
