import pytest
import requests
from unittest.mock import patch, Mock
from etl.extract import Extractor

class TestJSONPlaceholderExtractor:
    @pytest.fixture
    def config(self):
        # Return a test config dict
        return {
            'API_BASE_URL': 'https://jsonplaceholder.typicode.com/posts',
            'API_USER_ID': None,
        }

    @patch('etl.extract.requests.get')
    def test_fetch_posts_success(self, mock_get, config):
        # Mock API response
        fake_response = Mock()
        fake_response.status_code = 200
        fake_response.json.return_value = [{"userId": 1, "id": 1, "title": "A", "body": "B"}]
        mock_get.return_value = fake_response

        # Instantiate with config
        extractor = Extractor(config=config)
        posts = extractor.fetch_posts()

        assert len(posts) == 1
        assert posts[0]["id"] == 1
        mock_get.assert_called_once()
        # Ensure params are correct
        args, kwargs = mock_get.call_args
        assert kwargs["params"] == {}

    @patch('etl.extract.requests.get')
    def test_fetch_posts_with_userid(self, mock_get, config):
        config['API_USER_ID'] = 5
        fake_response = Mock()
        fake_response.status_code = 200
        fake_response.json.return_value = []
        mock_get.return_value = fake_response

        extractor = Extractor(config=config)
        posts = extractor.fetch_posts()

        assert posts == []
        args, kwargs = mock_get.call_args
        assert kwargs["params"] == {"userId": 5}

    @patch('etl.extract.requests.get')
    def test_fetch_posts_http_error(self, mock_get, config):
        # Simulate network error
        mock_get.side_effect = requests.RequestException("Network down")

        extractor = Extractor(config=config)
        posts = extractor.fetch_posts()

        assert posts == []  # Handles error and returns empty