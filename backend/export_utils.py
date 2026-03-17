from pathlib import Path
import pandas as pd

EXPORT_DIR = Path("backend/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def clean_cell(value):
    if value is None:
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


def export_results(rows: list[dict], mode: str, export_format: str) -> Path:
    cleaned_rows = []

    for row in rows:
        cleaned_row = {}
        for key, value in row.items():
            cleaned_row[key] = clean_cell(value)
        cleaned_rows.append(cleaned_row)

    df = pd.DataFrame(cleaned_rows)

    desired_columns = [
        "company_name",
        "keyword",
        "location",
        "category",
        "address",
        "website",
        "phone_number",
        "email_1",
        "email_2",
        "rating",
        "reviews_count",
        "map_link",
        "cid",
        "opening_hours",
        "featured_image",
        "city",
        "state",
        "pincode",
        "country_code",
    ]

    existing_columns = [col for col in desired_columns if col in df.columns]
    if existing_columns:
        df = df[existing_columns]

    file_path = EXPORT_DIR / f"{mode}_export.{export_format}"

    if export_format == "csv":
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
    elif export_format == "xlsx":
        df.to_excel(file_path, index=False)
    else:
        raise ValueError("Unsupported export format")

    return file_path