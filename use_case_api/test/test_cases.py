import unittest
from src import app

class ErrorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def setUp(self):
        self.app = app.test_client()
        self.app.testing = True

    def tearDown(self):
        pass

    def test_pagenotfound_statuscode1(self):
        result = self.app.get("/invalid/url")

        self.assertEqual(result.status_code, 404)

    def test_pagenotfound_statuscode2(self):
        result = self.app.get("/api/q1")

        self.assertEqual(result.status_code, 200)

    def test_pagenotfound_statuscode3(self):
        result = self.app.get("/api/q2")

        self.assertEqual(result.status_code, 200)

    def test_pagenotfound_statuscode4(self):
        result = self.app.get("/api/q3")

        self.assertEqual(result.status_code, 200)

    def test_pagenotfound_statuscode5(self):
        result = self.app.get("/api/query4")

        self.assertEqual(result.status_code, 404)

    def test_pagenotfound_statuscode6(self):
        result = self.app.get("/api/query5")

        self.assertEqual(result.status_code, 404)