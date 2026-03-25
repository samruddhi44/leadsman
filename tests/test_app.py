import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from backend.app import app
from backend.export_utils import export_results
from backend.scraper.google_business import (
    build_google_relevance_metrics,
    build_place_signature,
    build_preview_row,
    collect_listing_candidates,
    is_strong_google_match,
    preview_row_is_usable,
    run_google_business_scrape,
    scrape_listing_links,
)
from backend.scraper.social_lookup import (
    build_candidate_visit_urls,
    build_fallback_row,
    build_platform_search_targets,
    build_result_signature,
    build_search_query,
    canonicalize_profile_url,
    is_candidate_link,
    is_strong_relevance_match,
    normalize_candidate_url,
    scrape_candidate,
    run_social_lookup_scrape,
)
from backend.scraper.utils import parse_basic_location
from backend.state import APP_STATE, get_mode_state, reset_mode, set_running


class LeadsManAppTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(app)
        for mode in APP_STATE:
            reset_mode(mode)

    def tearDown(self):
        self.client.close()
        for mode in APP_STATE:
            reset_mode(mode)

    def test_home_page_loads(self):
        response = self.client.get("/")


        self.assertEqual(response.status_code, 200)
        self.assertIn("LeadsMan", response.text)

    def test_progress_rejects_invalid_mode(self):
        response = self.client.get("/api/progress", params={"mode": 
            
            "bad_mode"})

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported mode", response.json()["detail"])

    def test_export_rejects_invalid_format(self):
        response = self.client.get(
            "/api/export",
            params={"mode": "social_lookup", "format": "pdf"},
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported export format", response.json()["detail"])

    def test_social_lookup_rejects_invalid_platform(self):
        response = self.client.post(
            "/api/social-lookup/start",
           
            json={
                "keyword": "school",
                "location": "Pune",
                "max_pages": 2,
                "platforms": ["tiktok"],
            },
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Unsupported social platform", response.json()["detail"])

    def test_social_lookup_export_uses_current_schema(self):
        file_path = export_results(
            [
                {
                    "title": "Acme School",
                    "description": "Public profile for Acme School in Pune.",
                    "domain": "acmeschool.com",
                    "phones": "+91 12345 67890",
                    "emails": "hello@acmeschool.com",
                    "link": "https://facebook.com/acmeschool",
                    "source": "facebook",
                    "category": "school",
                    "location": "Pune",
                }
            ],
            mode="social_lookup",
            export_format="csv",
        )

        lines = file_path.read_text(encoding="utf-8-sig").splitlines()

        self.assertGreaterEqual(len(lines), 2)
        self.assertEqual(
            lines[0],
            "profile_name,platform,profile_link,bio,followers,contact_info",
        )
        self.assertIn("Acme School", lines[1])
        self.assertIn("Public profile for Acme School in Pune.", lines[1])
        self.assertIn("facebook", lines[1])

    def test_build_fallback_row_keeps_public_candidate_data(self):
        row = build_fallback_row(
            {
                "title": "Acme Studio",
                "description": "Makeup studio in Kolhapur with bridal services.",
                "href": "https://www.instagram.com/acme.studio/",
                "platform": "instagram",
            },
            keyword="makeup artist",
            location="Kolhapur",
        )

        self.assertEqual(row["title"], "Acme Studio")
        self.assertEqual(row["domain"], "www.instagram.com")
        self.assertIn("bridal services", row["description"])
        self.assertEqual(row["location"], "Kolhapur")

    def test_normalize_candidate_url_unwraps_google_redirect(self):
        url = (
            "https://www.google.com/url?q=https%3A%2F%2Fwww.linkedin.com%2Fin%2Facme-studio"
            "&sa=U&ved=123"
        )

        self.assertEqual(
            normalize_candidate_url(url),
            "https://www.linkedin.com/in/acme-studio",
        )

    def test_is_candidate_link_rejects_login_pages(self):
        self.assertFalse(is_candidate_link("https://www.linkedin.com/login", "linkedin"))
        self.assertTrue(
            is_candidate_link("https://www.linkedin.com/in/acme-studio/", "linkedin")
        )

    def test_build_platform_search_targets_uses_direct_platform_urls(self):
        targets = build_platform_search_targets("youtube", "acme studio", "Pune", 2)

        self.assertGreaterEqual(len(targets), 1)
        self.assertTrue(targets[0]["url"].startswith("https://www.youtube.com/results?search_query="))
        self.assertGreaterEqual(targets[0]["scroll_passes"], 2)

    def test_build_search_query_quotes_keyword_and_location_separately(self):
        query = build_search_query("linkedin", "makeup artist", "Kolhapur")

        self.assertIn('site:linkedin.com', query)
        self.assertIn('"makeup artist"', query)
        self.assertIn('"Kolhapur"', query)

    def test_relevance_match_requires_keyword_and_location(self):
        self.assertTrue(
            is_strong_relevance_match(
                "Acme Makeup Artist",
                "https://www.instagram.com/acme.makeup/",
                "Bridal makeup artist in Kolhapur Maharashtra",
                "makeup artist",
                "Kolhapur",
            )
        )
        self.assertFalse(
            is_strong_relevance_match(
                "Acme Studio",
                "https://www.instagram.com/acme.studio/",
                "Creative beauty studio in Pune",
                "makeup artist",
                "Kolhapur",
            )
        )
        self.assertFalse(
            is_strong_relevance_match(
                "Bake My Day",
                "https://www.instagram.com/bakemyday/",
                "Custom cakes and desserts in Kolhapur",
                "cake shop",
                "Kolhapur",
            )
        )

    def test_canonicalize_profile_url_strips_about_subpages(self):
        self.assertEqual(
            canonicalize_profile_url("https://www.youtube.com/@acmestudio/about", "youtube"),
            "https://www.youtube.com/@acmestudio",
        )
        self.assertEqual(
            canonicalize_profile_url("https://www.linkedin.com/company/acme/about/", "linkedin"),
            "https://www.linkedin.com/company/acme",
        )
        self.assertEqual(
            canonicalize_profile_url(
                "https://www.instagram.com/acme.studio/?hl=en&utm_source=abc",
                "instagram",
            ),
            "https://www.instagram.com/acme.studio",
        )

    def test_build_candidate_visit_urls_adds_about_pages_for_supported_platforms(self):
        youtube_urls = build_candidate_visit_urls(
            {"href": "https://www.youtube.com/@acmestudio", "platform": "youtube"}
        )
        linkedin_urls = build_candidate_visit_urls(
            {"href": "https://www.linkedin.com/company/acme", "platform": "linkedin"}
        )
        instagram_urls = build_candidate_visit_urls(
            {"href": "https://www.instagram.com/acme.studio/", "platform": "instagram"}
        )

        self.assertEqual(
            youtube_urls,
            [
                "https://www.youtube.com/@acmestudio/about",
                "https://www.youtube.com/@acmestudio",
            ],
        )
        self.assertEqual(
            linkedin_urls,
            [
                "https://www.linkedin.com/company/acme/about/",
                "https://www.linkedin.com/company/acme",
            ],
        )
        self.assertEqual(instagram_urls, ["https://www.instagram.com/acme.studio/"])

    def test_build_result_signature_dedupes_same_business_across_platforms(self):
        first = build_result_signature(
            {
                "title": "Acme Cake Shop",
                "location": "Kolhapur",
                "category": "cake shop",
                "source": "instagram",
                "link": "https://www.instagram.com/acmecakes/?hl=en",
            }
        )
        second = build_result_signature(
            {
                "title": "Acme Cake Shop",
                "location": "Kolhapur",
                "category": "cake shop",
                "source": "facebook",
                "link": "https://www.facebook.com/acmecakes/about",
            }
        )

        self.assertEqual(first, second)

    def test_scrape_candidate_returns_fallback_row_for_login_redirect(self):
        class DummyLocator:
            def __init__(self, text="", content=None):
                self._text = text
                self._content = content
                self.first = self

            def get_attribute(self, _):
                return self._content

            def inner_text(self, timeout=None):
                return self._text

        page = Mock()
        page.url = "https://www.linkedin.com/authwall?trk=public_profile"
        page.goto.return_value = None
        page.title.return_value = "Sign In | LinkedIn"
        page.locator.side_effect = lambda selector: DummyLocator(
            text="Sign in to continue to LinkedIn",
            content=None,
        )

        row = scrape_candidate(
            page,
            {
                "title": "Acme Studio",
                "description": "Acme Studio makeup artist services in Kolhapur.",
                "href": "https://www.linkedin.com/in/acme-studio/",
                "platform": "linkedin",
                "score": 4,
            },
            keyword="makeup artist",
            location="Kolhapur",
        )

        self.assertEqual(row["title"], "Acme Studio")
        self.assertEqual(row["link"], "https://www.linkedin.com/in/acme-studio/")
        self.assertEqual(row["domain"], "www.linkedin.com")
        self.assertIn("Kolhapur", row["description"])

    def test_parse_basic_location_handles_country_suffix(self):
        city, state, pincode = parse_basic_location(
            "123 Main Street, Pune, Maharashtra 411001, India"
        )

        self.assertEqual(city, "Pune")
        self.assertEqual(state, "Maharashtra")
        self.assertEqual(pincode, "411001")

    def test_scrape_listing_links_dedupes_and_limits_for_fast_mode(self):
        class DummyLocator:
            def evaluate_all(self, _):
                return [
                    "https://www.google.com/maps/place/Acme+School",
                    "https://www.google.com/maps/place/Acme+School",
                    "https://www.google.com/maps/place/Beta+School",
                    "https://www.google.com/maps/place/Gamma+School",
                ]

        page = Mock()
        page.locator.return_value = DummyLocator()

        links = scrape_listing_links(page, limit=2)

        self.assertEqual(
            links,
            [
                "https://www.google.com/maps/place/Acme+School",
                "https://www.google.com/maps/place/Beta+School",
            ],
        )

    def test_collect_listing_candidates_keeps_multiline_preview_text_for_fast_path(self):
        class DummyLocator:
            def evaluate_all(self, _):
                return [
                    {
                        "href": "https://www.google.com/maps/place/Acme+High+School",
                        "name": "Acme High School",
                        "text": "Acme High School\nHigh school\nKolhapur, Maharashtra 416005",
                    }
                ]

        page = Mock()
        page.locator.return_value = DummyLocator()

        candidates = collect_listing_candidates(page, "school", "Kolhapur", limit=5)

        self.assertEqual(len(candidates), 1)
        self.assertIn("\n", candidates[0]["preview_text"])

    def test_google_relevance_match_requires_keyword_and_location(self):
        self.assertTrue(
            is_strong_google_match(
                "Acme High School High school Pune Maharashtra",
                "school",
                "Pune",
            )
        )
        self.assertFalse(
            is_strong_google_match(
                "Acme Bakery Pune Maharashtra",
                "school",
                "Pune",
            )
        )
        self.assertFalse(
            is_strong_google_match(
                "Acme High School Mumbai Maharashtra",
                "school",
                "Pune",
            )
        )

        metrics = build_google_relevance_metrics(
            "Acme High School Pune Maharashtra",
            "high school",
            "Pune",
        )
        self.assertTrue(metrics["has_keyword_phrase"])
        self.assertGreaterEqual(metrics["location_hits"], 1)

    def test_build_place_signature_prefers_cid_and_falls_back_to_name_address(self):
        self.assertEqual(
            build_place_signature(
                {
                    "cid": "12345",
                    "company_name": "Acme High School",
                    "address": "Pune, Maharashtra",
                }
            ),
            "cid:12345",
        )
        self.assertEqual(
            build_place_signature(
                {
                    "cid": "",
                    "company_name": "Acme High School",
                    "address": "Pune, Maharashtra",
                }
            ),
            "acme high school|pune maharashtra",
        )

    def test_build_preview_row_extracts_fast_google_card_fields(self):
        row = build_preview_row(
            {
                "href": "https://www.google.com/maps/place/Acme+High+School?cid=12345",
                "name": "Acme High School",
                "preview_text": (
                    "Acme High School\n"
                    "4.3 120 reviews\n"
                    "High school\n"
                    "Near Market Road, Kolhapur, Maharashtra 416005\n"
                    "060030 00700"
                ),
            },
            keyword="school",
            location="Kolhapur",
        )

        self.assertEqual(row["company_name"], "Acme High School")
        self.assertEqual(row["category"], "High school")
        self.assertEqual(row["city"], "Kolhapur")
        self.assertEqual(row["pincode"], "416005")
        self.assertEqual(row["phone_number"], "060030 00700")
        self.assertEqual(row["cid"], "12345")

    def test_preview_row_is_usable_requires_core_match(self):
        usable = build_preview_row(
            {
                "href": "https://www.google.com/maps/place/Acme+High+School",
                "name": "Acme High School",
                "preview_text": "Acme High School\nHigh school\nKolhapur, Maharashtra 416005",
            },
            keyword="school",
            location="Kolhapur",
        )
        weak = build_preview_row(
            {
                "href": "https://www.google.com/maps/place/Acme+Bakery",
                "name": "Acme Bakery",
                "preview_text": "Acme Bakery\nBakery\nKolhapur, Maharashtra 416005",
            },
            keyword="school",
            location="Kolhapur",
        )

        self.assertTrue(preview_row_is_usable(usable, "school", "Kolhapur"))
        self.assertFalse(preview_row_is_usable(weak, "school", "Kolhapur"))

    @patch("backend.scraper.google_business.close_browser")
    @patch("backend.scraper.google_business.start_browser", side_effect=RuntimeError("boom"))
    def test_google_business_startup_failure_clears_running_state(self, _, mock_close_browser):
        set_running("google_business", True)

        run_google_business_scrape("schools", "Pune", False)
        state = get_mode_state("google_business")

        self.assertFalse(state["running"])
        self.assertTrue(any("Unable to start browser" in log for log in state["logs"]))
        mock_close_browser.assert_called_once()

    @patch("backend.scraper.social_lookup.close_browser")
    @patch("backend.scraper.social_lookup.start_browser", side_effect=RuntimeError("boom"))
    def test_social_lookup_startup_failure_clears_running_state(self, _, mock_close_browser):
        set_running("social_lookup", True)

        run_social_lookup_scrape("schools", "Pune", ["facebook"], 2)
        state = get_mode_state("social_lookup")

        self.assertFalse(state["running"])
        self.assertTrue(any("Unable to start browser" in log for log in state["logs"]))
        self.assertGreaterEqual(mock_close_browser.call_count, 1)


if __name__ == "__main__":
    unittest.main()
