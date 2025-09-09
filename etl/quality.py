import pandas as pd
import json

def generate_data_quality_report(df: pd.DataFrame, key_column="id"):
    report = {}
    report['record_count'] = len(df)
    report['column_nulls'] = {
        col: int(df[col].isnull().sum()) for col in df.columns
    }
    report['column_null_ratios'] = {
        col: float(df[col].isnull().mean()) for col in df.columns
    }
    report['column_types'] = {
        col: str(df[col].dtype) for col in df.columns
    }
    report['duplicate_key_count'] = int(df[key_column].duplicated().sum())
    if "title_length" in df.columns:
        report['title_length_summary'] = df['title_length'].describe().to_dict()
    if "body_word_count" in df.columns:
        report['body_word_count_summary'] = df['body_word_count'].describe().to_dict()
    return report

def save_quality_report(report, output_path="data/output/data_quality_report.json"):
    with open(output_path, "w") as f:
        json.dump(report, f, indent=2)