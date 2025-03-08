from google.cloud import bigquery
import os

def load_csv_to_bigquery(csv_path, table_id):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Header row in CSV
        autodetect=True,      # Let BigQuery infer the schema
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    with open(csv_path, "rb") as csv_file:
        load_job = client.load_table_from_file(csv_file, table_id, job_config=job_config)

    load_job.result()  # Wait for the job to complete.
    print(f"Loaded {load_job.output_rows} rows into {table_id}.")

if __name__ == "__main__":
    csv_path = os.path.join("data", "financial_metrics.csv")
    table_id = "your-project.your_dataset.financial_metrics"  # Replace with your project and dataset
    load_csv_to_bigquery(csv_path, table_id)
