from pyspark.sql.functions import regexp_extract, to_timestamp
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import expr, col
import datetime

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")


def top_10_theft_crimes_location_past_3y(df: DataFrame) -> DataFrame:
    current_year = datetime.datetime.now().year
    df.createOrReplaceTempView("crimes")

    return spark.sql(f"""
        SELECT `Location Description` AS Location_Description , COUNT(*) as count
        FROM crimes
        WHERE `Year` BETWEEN {current_year - 3} AND {current_year} AND `Primary Type` = 'THEFT'
        GROUP BY `Location Description`
        ORDER BY count DESC
        LIMIT 10
    """)


def total_crimes_past_5y_per_month(df: DataFrame) -> DataFrame:
    current_year = datetime.datetime.now().year
    df.createOrReplaceTempView("crimes")

    return spark.sql(f"""
        SELECT EXTRACT(MONTH FROM Date) AS month,
               COUNT(`Primary Type`) AS total_crimes
        FROM crimes
        WHERE year >= {current_year - 5}
        AND year <= {current_year - 5}
        GROUP BY month
        ORDER BY month
    """)


def total_crimes_per_year(df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("crime_data")

    return spark.sql("""
        SELECT year, COUNT(*) AS total_crimes
        FROM crime_data
        WHERE year IS NOT NULL
        GROUP BY year
        ORDER BY year
    """)


def types_of_crimes_most_arrested_2016_to_2019(df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("crime_data")

    return spark.sql("""
                     SELECT `Primary Type` AS Primary_Type, COUNT(`Primary Type`) AS arrest_count
    FROM crimes
    WHERE Year >= 2016 AND Year <= 2019 AND Arrest = 'true'
    GROUP BY `Primary Type`
    ORDER BY arrest_count DESC
    LIMIT 15;
    """)


def safest_locations_10pm_to_4am(df: DataFrame) -> DataFrame:
    df.createOrReplaceTempView("crime_data")

    return spark.sql("""SELECT
        `Location Description` AS Location_Description,
        count(*) AS location_count
    FROM crime_data
    WHERE (EXTRACT(HOUR FROM Date) BETWEEN 22 AND 23) OR (EXTRACT(HOUR FROM Date) BETWEEN 0 AND 4)
    GROUP BY `Location Description`
    ORDER BY location_count ASC
    LIMIT 15;
    """)


def load_df_to_gcs_parquet(df: DataFrame, bucket: str, name: str) -> None:
    print(
        f"Uploading csv result of '{name}' processing into gs://{bucket}/{name}'.")
    output_gcs_path = f'gs://{bucket}/{name}'
    df.coalesce(1).write.parquet(output_gcs_path, mode='overwrite')
    print('File uploaded.')


def add_3y(df: DataFrame) -> DataFrame:
    date_pattern = "^(\\d{2}/\\d{2}/\\d{4} \\d{2}:\\d{2})"
    df = df.withColumn("Date", to_timestamp(regexp_extract("Date", date_pattern, 1), "dd/MM/yyyy HH:mm"))
    df = df.withColumn("Year", col("Year").cast("int") + 3)
    df = df.withColumn("Date", expr("date + interval 3 years"))
    return df

def main():
    bucket_name = "crime_processed_data"
    df = spark.read.csv('gs://datalake_nodale/Crimes_-_2001_to_Present.csv',
                        header=True, inferSchema=True)
    df = add_3y(df)
    function_list = [
        top_10_theft_crimes_location_past_3y,
        total_crimes_past_5y_per_month,
        total_crimes_per_year,
        types_of_crimes_most_arrested_2016_to_2019,
        safest_locations_10pm_to_4am
    ]
    for func in function_list:
        # Execute the function and get the result
        result_df = func(df)

        file_name = str(func.__name__)
        load_df_to_gcs_parquet(result_df, bucket_name, file_name)


if __name__ == "__main__":
    main()
