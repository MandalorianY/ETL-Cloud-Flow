import functions_framework
import google.cloud.bigquery as bigquery


@functions_framework.cloud_event
def upload_csv_to_bigquery(cloud_event):
    """
    This function is triggered by a Cloud Storage event, when a new parquet file is uploaded to a GCS bucket.
    The function extracts the GCS bucket name, folder name, and file name from thmessage data,
    and then uploads the file to a BigQuery dataset.
    """

    # Get the GCS bucket name, folder name, and file name from the message data.
    data = cloud_event.data
    bucket_name = data["bucket"]
    folder_name = data["name"].split("/")[0]
    file_name = data["name"].split("/")[1]
    print("file_name", file_name)
    print("bucket_name", bucket_name)
    print("folder_name", folder_name)
    if file_name != "_SUCCESS":
        # Create a BigQuery client.
        client = bigquery.Client()
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True
        job_config.source_format = bigquery.SourceFormat.CSV

        # Get the dataset name from the bucket name.
        dataset_name = "nodale.crime_dataset"

        # Create a BigQuery table from the csv file.
        table_id = f"{dataset_name}.{folder_name}"
        client.load_table_from_uri(
            f"gs://{bucket_name}/{folder_name}/{file_name}",
            table_id, job_config=job_config
        ).result()

        print(
            f"File {file_name} was uploaded to BigQuery table {table_id} under dataset {dataset_name}.")
    else:
        print("File is _SUCCESS, skipping upload")
