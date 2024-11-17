import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

def visualize(input_path="dbfs:/FileStore/mini_proj11/query_result.parquet"):
    """
    Visualizes the saved query result by creating:
    1. A bar plot of average alcohol content by wine quality.
    2. A grouped bar plot of fixed acidity and volatile acidity by wine quality.
    """
    spark = SparkSession.builder.appName("Visualize Data").getOrCreate()

    # Load the saved query result
    query_result = spark.read.parquet(input_path)
    df = query_result.toPandas()
    # Bar Plot: Average alcohol content by wine quality
    plt.figure(figsize=(10, 6))
    plt.bar(df['quality'], df['avg_alcohol'], color='skyblue')
    plt.title("Average Alcohol Content by Wine Quality")
    plt.xlabel("Wine Quality")
    plt.ylabel("Average Alcohol Content")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig("avg_alcohol_by_quality.png")
    plt.show()

    # Grouped Bar Plot: Fixed acidity and volatile acidity by wine quality
    x = df['quality']
    width = 0.35  # width of the bars

    plt.figure(figsize=(10, 6))
    plt.bar(x - width / 2, df['avg_fixed_acidity'], width, label="Fixed Acidity", color='gold')
    plt.bar(x + width / 2, df['avg_volatile_acidity'], width, label="Volatile Acidity", color='lightcoral')
    plt.title("Acidity Levels by Wine Quality")
    plt.xlabel("Wine Quality")
    plt.ylabel("Average Acidity")
    plt.xticks(rotation=0)
    plt.legend()
    plt.tight_layout()
    plt.savefig("acidity_by_quality.png")
    plt.show()


if __name__ == "__main__":
    input_path = "dbfs:/FileStore/mini_proj11/query_result.parquet"
    visualize(input_path)
