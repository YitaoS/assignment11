"""
Main cli or app entry point
"""
from mylib.extract import extract
from mylib.load import load
from mylib.query import query
from mylib.visualize import visualize

if __name__ == "__main__":
    extract()
    load()
    query("dbfs:/FileStore/mini_proj11/query_result.parquet")
    visualize("dbfs:/FileStore/mini_proj11/query_result.parquet")