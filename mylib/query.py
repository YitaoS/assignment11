from pyspark.sql import SparkSession

def query(output_path):
    """
    Runs an SQL query to select the top 10 countries by total alcohol consumption,
    saves the result to a file, and displays it.
    """
    spark = SparkSession.builder.appName("Query Data").getOrCreate()

    # SQL query to fetch top 10 countries by total alcohol consumption
    query_result = spark.sql("""
        SELECT 
            country,
            beer_servings,
            spirit_servings,
            wine_servings,
            total_litres_of_pure_alcohol
        FROM alcohol_data
        ORDER BY total_litres_of_pure_alcohol DESC
        LIMIT 10
    """)

    print("Query completed successfully.")

    print("\nTop 10 Countries by Total Alcohol Consumption:")
    query_result.show(truncate=False)

    # Save query result to a file
    query_result.write.mode("overwrite").parquet(output_path)
    print(f"\nQuery result saved to {output_path}")

if __name__ == "__main__":
    output_path = "dbfs:/FileStore/mini_proj11/query_result.parquet"
    query(output_path)
