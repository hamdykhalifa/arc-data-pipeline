import logging
import pandas as pd

logger = logging.getLogger(__name__)

class Transformer:
    def __init__(self, max_title_len=50):
        self.max_title_len = max_title_len

    def clean_title(self, title: str) -> str:
        """
        Strips whitespace, truncates to max_title_len,
        and capitalizes the first character.

        Args:
            title (str): The title string to clean.

        Returns:
            str: The cleaned title string. Returns an empty string if input is not a str.
        """
        if not isinstance(title, str):
            return ''
        title = title.strip()[:self.max_title_len]
        if title:
            title = title[0].upper() + title[1:]
        return title

    def clean_body(self, body: str) -> str:
        """
        Strips whitespace, replaces multiple spaces with a single space.

        Args:
            body (str): The body string to clean.

        Returns:
            str: The cleaned body string. Returns an empty string if input is not a str.
        """
        if not isinstance(body, str):
            return ''
        import re
        body = body.strip()
        body = re.sub(r'\s+', ' ', body)
        return body

    def add_derived_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Add derived fields to the DataFrame: 'title_length' and 'body_word_count'.

        Args:
            df (pd.DataFrame): DataFrame containing at least 'title' and 'body' columns.

        Returns:
            pd.DataFrame: The DataFrame with added derived fields.
        """
        df['title_length'] = df['title'].str.len()
        df['body_word_count'] = df['body'].str.split().apply(len)
        return df

    def filter_valid(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter DataFrame, removing rows with empty titles or bodies and duplicate IDs.

        Args:
            df (pd.DataFrame): DataFrame containing 'title', 'body', and 'id' columns.

        Returns:
            pd.DataFrame: Filtered DataFrame containing only valid and unique records.
        """
        df = df.loc[(df['title'] != '') & (df['body'] != '')]
        dupes = df['id'][df['id'].duplicated(keep=False)].unique()
        df = df[~df['id'].isin(dupes)]
        return df

    def transform(self, records: list[dict]) -> pd.DataFrame:
        """
        Main entrypoint: transforms a list of record dicts into a cleaned and validated DataFrame.

        Args:
            records (list[dict]): List of record dictionaries with keys 'title', 'body', 'id'.

        Returns:
            pd.DataFrame: Transformed and filtered DataFrame.
        """
        logger.info("Starting transformation...")
        df = pd.DataFrame(records)
        df['title'] = df['title'].apply(self.clean_title)
        df['body'] = df['body'].apply(self.clean_body)
        df = self.add_derived_fields(df)
        df = self.filter_valid(df)
        df.reset_index(drop=True, inplace=True)
        logger.info(f"Transformation complete. {len(df)} records remain.")
        return df
