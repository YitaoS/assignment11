from pyspark.sql import SparkSession

def load_and_query(file_path="dbfs:/FileStore/mini_proj11/winequality-red.csv", output_path="dbfs:/FileStore/mini_proj11/query_result.parquet"):
    """
    Loads data from a CSV file, renames columns for compatibility, and queries wine quality data.
    """
    spark = SparkSession.builder.appName("Query Data").getOrCreate()

    # Load CSV file with correct delimiter
    print(f"Loading data from {file_path} into a Spark DataFrame...")
    df = spark.read.csv(file_path, header=True, inferSchema=True, sep=";")
    print(f"Data successfully loaded into a DataFrame with {df.count()} rows.")

    # Rename columns to valid identifiers
    valid_columns = [col_name.replace(" ", "_").lower() for col_name in df.columns]
    df = df.toDF(*valid_columns)
    print("Renamed columns for Delta Lake compatibility.")

    # Persist the DataFrame as a table
    table_name = "winequality_red_data"
    df.createOrReplaceTempView(table_name)
    print(f"Data successfully registered as a table: {table_name}")

    # Query to find the top 10 quality wines with their average alcohol and acidity
    query_result = spark.sql(f"""
        SELECT 
            quality,
            ROUND(AVG(alcohol), 2) AS avg_alcohol,
            ROUND(AVG(fixed_acidity), 2) AS avg_fixed_acidity,
            ROUND(AVG(volatile_acidity), 2) AS avg_volatile_acidity,
            COUNT(*) AS count
        FROM {table_name}
        GROUP BY quality
        ORDER BY quality DESC
        LIMIT 10
    """)

    print("Query completed successfully.")

    print("\nWine Quality Analysis:")
    query_result.show(truncate=False)

    # Save query result to a file
    query_result.write.mode("overwrite").parquet(output_path)
    print(f"\nQuery result saved to {output_path}")

if __name__ == "__main__":
    file_path = "dbfs:/FileStore/mini_proj11/winequality-red.csv"
    output_path = "dbfs:/FileStore/mini_proj11/query_result.parquet"
    load_and_query(file_path, output_path)
