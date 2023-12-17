from pyspark.sql.functions import regexp_extract, to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
from google.cloud import bigquery

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def load_dataframe_to_bigquery(df: DataFrame, table_name: str) -> None:
    print(f"Uploading '{table_name}' table to BigQuery.")
    df.write.format('bigquery').option(
        "table", table_name).option("temporaryGcsBucket", "datalake_nodale").mode("overwrite").save()
    print(f"Table uploaded {table_name}.")


def add_3y(df: DataFrame) -> DataFrame:
    date_pattern = "^(\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2})"
    df = df.withColumn("Date", to_timestamp(
        regexp_extract("Date", date_pattern, 1), "dd/MM/yyyy HH:mm"))
    df = df.withColumn("Year", col("Year").cast("int") + 3)
    df = df.withColumn("Date", expr("date + interval 3 years"))
    return df


def rename_columns(df: DataFrame):
    return df.withColumnRenamed("Primary Type", "Primary_type") \
        .withColumnRenamed("Community Area", "Community_Area") \
        .withColumnRenamed("FBI Code", "Fbi_Code") \
        .withColumnRenamed("X Coordinate", "X_Coordinate") \
        .withColumnRenamed("Y Coordinate", "Y_Coordinate") \
        .withColumnRenamed("Updated On", "Updated_On") \
        .withColumnRenamed("Location Description", "Location_Description") \
        .withColumnRenamed("Case Number", "Case_Number")


def run_query(query: str, name: str) -> None:
    table_name_output = f"nodale.crime_dataset_architecture_2.{name}"
    client = bigquery.Client()
    job_config = bigquery.QueryJobConfig(
        destination=table_name_output,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    query_job = client.query(query, job_config=job_config)
    query_job.result()


def total_crimes_per_year(table_name_input: str) -> str:
    return (f"""
        SELECT year, COUNT(*) AS total_crimes
        FROM {table_name_input}
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def top_10_theft_crimes_location_past_3y(table_name_input: str) -> str:
    return (f"""
        SELECT Location_Description , COUNT(*) as count
        FROM {table_name_input}
        WHERE `Year` BETWEEN EXTRACT(YEAR FROM CURRENT_DATE()) - 3
        AND EXTRACT(YEAR FROM CURRENT_DATE()) AND Primary_Type = 'THEFT'
        AND Date IS NOT NULL
        GROUP BY Location_Description
        ORDER BY count DESC
        LIMIT 10
    """)


def total_crimes_past_5y_per_month(table_name_input: str) -> str:
    return (f"""
        SELECT EXTRACT(MONTH FROM TIMESTAMP(Date)) AS month,
               COUNT(Primary_Type) AS total_crimes
        FROM {table_name_input}
        WHERE year >= EXTRACT(YEAR FROM CURRENT_DATE()) - 5
        AND year <= EXTRACT(YEAR FROM CURRENT_DATE()) - 5
        AND Date IS NOT NULL
        GROUP BY month
        ORDER BY month
    """)


def types_of_crimes_most_arrested_2016_to_2019(table_name_input: str) -> str:
    return (f"""
    SELECT Primary_Type,
    COUNT(Primary_Type) AS arrest_count
    FROM  {table_name_input}
    WHERE Year >= 2016 AND Year <= 2019 AND Arrest = TRUE
    GROUP BY Primary_Type
    ORDER BY arrest_count DESC
    LIMIT 15;
    """)


def safest_locations_10pm_to_4am(table_name_input: str) -> str:
    return (f"""SELECT
    Location_Description,
    COUNT(*) AS location_count
    FROM {table_name_input}
    WHERE
        (EXTRACT(HOUR FROM TIMESTAMP(Date)) BETWEEN 22 AND 23
        OR EXTRACT(HOUR FROM TIMESTAMP(Date)) BETWEEN 0 AND 4)
        AND Date IS NOT NULL
    GROUP BY Location_Description
    ORDER BY location_count ASC
    LIMIT 15;
    """)


def main():

    df = spark.read.csv('gs://datalake_nodale/Crimes_-_2001_to_Present.csv',
                        header=True, inferSchema=True)
    df = add_3y(df)
    df = rename_columns(df)
    table_name_input = "nodale.crime_dataset_architecture_2_raw.crime_data"
    load_dataframe_to_bigquery(
        df, table_name_input)
    query_list = [
        top_10_theft_crimes_location_past_3y,
        total_crimes_past_5y_per_month,
        total_crimes_per_year,
        types_of_crimes_most_arrested_2016_to_2019,
        safest_locations_10pm_to_4am
    ]

    for query in query_list:
        run_query(query(table_name_input), query.__name__)


if __name__ == "__main__":
    main()
