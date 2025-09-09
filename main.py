import logging, os
from config.logging_config import setup_logging
from etl.extract import Extractor
from etl.transform import Transformer
from etl.load import DataLoader
from etl.quality import generate_data_quality_report, save_quality_report

def main():
    setup_logging()

    # Extract
    extractor = Extractor()
    raw_posts = extractor.fetch_posts()
    if not raw_posts:
        logging.error("No data extracted. Exiting.")
        return

    # Transform
    transformer = Transformer(max_title_len=100)
    df = transformer.transform(raw_posts)
    if df.empty:
        logging.error("No data after transformation. Exiting.")
        return

    # Load
    loader = DataLoader(
        output_dir="data/output",
        partition_cols=["userId"],  # or None for non-partitioned
        use_s3=os.getenv("USE_S3"),                # set to False for local, True for S3
        s3_bucket=os.getenv("AWS_S3_BUCKET"),
        s3_prefix="posts",
        aws_region=os.getenv("AWS_DEFAULT_REGION")
    )
    output_file = loader.save(df)
    logging.info(f"Pipeline complete. Data saved to {output_file}")

    # Quality report
    report = generate_data_quality_report(df)
    save_quality_report(report, output_path="data/output/data_quality_report.json")
    logging.info("Data quality report written to data/output/data_quality_report.json")

if __name__ == "__main__":
    main()