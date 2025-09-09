import os
import pandas as pd
import logging
import shutil
import boto3

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(
        self,
        output_dir: str = "data/output",
        output_format: str = "parquet",
        partition_cols: list[str] | None = None,
        use_s3: bool = False,
        s3_bucket: str | None = None,
        s3_prefix: str = "",
        aws_region: str | None = None,
    ) -> None:
        """
        Initializes a DataLoader instance.

        Args:
            output_dir (str): Directory to write output files.
            output_format (str): File format for output (currently only 'parquet' is used).
            partition_cols (list[str] | None): Columns to partition the output Parquet files.
            use_s3 (bool): If True, upload files to S3 after saving locally.
            s3_bucket (str | None): Name of the S3 bucket to upload files to.
            s3_prefix (str): Prefix/path in the S3 bucket.
            aws_region (str | None): AWS region for the S3 client.
        """
        self.output_dir = output_dir
        self.partition_cols = partition_cols or []
        self.use_s3 = use_s3
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix.strip("/")
        self.aws_region = aws_region

    def save(self, df: pd.DataFrame, filename: str = "posts.parquet") -> None:
        """
        Saves the given DataFrame as a Parquet file.

        If partition_cols are provided, saves DataFrame partitioned by those columns.
        If use_s3 is True, the saved file or folder is uploaded to S3.

        Args:
            df (pd.DataFrame): The DataFrame to save.
            filename (str): Filename for the saved Parquet file (used for non-partitioned, and as folder name for partitioned).
        """
        if self.partition_cols:
            temp_dir = f"/tmp/{filename}_part"
            df.to_parquet(
                temp_dir,
                partition_cols=self.partition_cols,
                index=False,
                engine="pyarrow",
            )
            if self.use_s3:
                self._upload_folder_to_s3(temp_dir)
                logger.info(f"Uploaded Parquet partitions to s3://{self.s3_bucket}/{self.s3_prefix}")
            else:
                out_path = os.path.join(self.output_dir, "partitioned")
                os.makedirs(out_path, exist_ok=True)
                if os.path.exists(out_path):
                    shutil.rmtree(out_path)
                shutil.copytree(temp_dir, out_path)
                logger.info(f"Saved partitioned Parquet files to {out_path}")
        else:
            out_path = os.path.join(self.output_dir, filename)
            os.makedirs(self.output_dir, exist_ok=True)
            df.to_parquet(out_path, index=False)
            if self.use_s3:
                self._upload_file_to_s3(out_path, filename)
                logger.info(f"Uploaded {filename} to s3://{self.s3_bucket}/{self.s3_prefix}/{filename}")
            else:
                logger.info(f"Saved Parquet file to {out_path}")

    def _upload_file_to_s3(self, filepath: str, filename: str) -> None:
        """
        Uploads a single file to S3.

        Args:
            filepath (str): Path to the local file.
            filename (str): Filename to use in the S3 bucket/prefix.
        """
        s3_key = f"{self.s3_prefix}/{filename}".lstrip("/")
        s3 = boto3.client("s3", region_name=self.aws_region)
        s3.upload_file(filepath, self.s3_bucket, s3_key)

    def _upload_folder_to_s3(self, folder: str) -> None:
        """
        Uploads an entire folder and its contents to S3.

        Args:
            folder (str): Path to the local folder to upload.
        """
        s3 = boto3.client("s3", region_name=self.aws_region)
        for root, _, files in os.walk(folder):
            for f in files:
                abs_path = os.path.join(root, f)
                rel_path = os.path.relpath(abs_path, folder)
                s3_key = f"{self.s3_prefix}/{rel_path}".replace("\\", "/").lstrip("/")
                s3.upload_file(abs_path, self.s3_bucket, s3_key)