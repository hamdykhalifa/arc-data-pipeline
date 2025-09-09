import logging
import requests
from config.config import get_config

logger = logging.getLogger(__name__)

class Extractor:
    """Handles extraction of posts from an API endpoint."""

    def __init__(self, config=None):
        self.config = config or get_config()
        self.base_url = self.config.get('API_BASE_URL')
        self.user_id = self.config.get('API_USER_ID')

    def fetch_posts(self) -> list:
        """Fetches posts from the configured API endpoint.

        Returns:
            list: A list of posts retrieved from the API. Returns an empty list if the request fails.
        """
        params = {}
        if self.user_id:
            params['userId'] = self.user_id
        
        try:
            response = requests.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            posts = response.json()
            logger.info(f"Fetched {len(posts)} posts from {self.base_url} with params {params}")
            return posts
        except requests.RequestException as e:
            logger.error(f"Failed to fetch posts: {e}")
            return []
