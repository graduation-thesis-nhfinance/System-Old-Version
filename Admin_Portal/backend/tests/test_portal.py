"""
Unit Tests for NHFinance Admin Portal Backend Gateway
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.main import app, healthz


class TestAdminPortal(unittest.TestCase):
    def test_portal_metadata(self):
        self.assertIn("Admin Portal", app.title)
        self.assertIsNotNone(app.version)

    def test_healthz_function(self):
        res = healthz()
        self.assertEqual(res, {"status": "ok", "app": "admin-portal"})

    def test_registered_routes(self):
        routes = [r.path for r in app.routes if hasattr(r, "path")]
        self.assertIn("/healthz", routes)
        self.assertIn("/readyz", routes)
        self.assertIn("/api/v1/system/status", routes)


if __name__ == "__main__":
    unittest.main()
