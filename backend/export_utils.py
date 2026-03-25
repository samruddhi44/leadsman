from pathlib import Path
import pandas as pd

from backend.result_schema import project_results

BASE_DIR = Path(__file__).resolve().parent
EXPORT_DIR = BASE_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
SUPPORTED_EXPORT_FORMATS = ("csv", "xlsx")


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
    export_format = export_format.strip().lower()
    if export_format not in SUPPORTED_EXPORT_FORMATS:
        raise ValueError("Unsupported export format")

    projected_rows = project_results(mode, rows)
    cleaned_rows = []

    for row in projected_rows:
        cleaned_row = {}
        for key, value in row.items():
            cleaned_row[key] = clean_cell(value)
        cleaned_rows.append(cleaned_row)

    if mode == "google_business":
        desired_columns = [
            "business_name",
            "city",
            "map_link",
            "full_address",
            "email",
            "featured_image_url",
            "pin_code",
        ]
    elif mode == "social_lookup":
        desired_columns = [
            "profile_name",
            "platform",
            "profile_link",
            "bio",
            "followers",
            "contact_info",
        ]
    else:
        desired_columns = []

    if cleaned_rows:
        df = pd.DataFrame(cleaned_rows)
    else:
        df = pd.DataFrame(columns=desired_columns)

    if desired_columns:
        for column in desired_columns:
            if column not in df.columns:
                df[column] = ""
        df = df[desired_columns]

    file_path = EXPORT_DIR / f"{mode}_export.{export_format}"

    if export_format == "csv":
        df.to_csv(file_path, index=False, encoding="utf-8-sig")
    else:
        df.to_excel(file_path, index=False)

    return file_path
