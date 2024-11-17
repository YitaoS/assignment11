import requests
import os

def create_directory(path):
    """
    Creates a directory in DBFS if it doesn't already exist.
    """
    local_path = path.replace("dbfs:/", "/dbfs/")
    if not os.path.exists(local_path):
        os.makedirs(local_path, exist_ok=True)
        print(f"Directory created at {path}")
    else:
        print(f"Directory already exists at {path}")


def extract(url = "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv", file_path="dbfs:/FileStore/mini_proj11/winequality-red.csv"):
    """
    Downloads a file from the specified URL and saves it to the specified DBFS path.
    """
    # Ensure the directory exists
    directory_path = "/".join(file_path.split("/")[:-1])
    create_directory(directory_path)

    print(f"Starting download from {url}")
    local_path = file_path.replace("dbfs:/", "/dbfs/")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"File downloaded and saved to {file_path}")


if __name__ == "__main__":
    url = "https://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
    file_path = "dbfs:/FileStore/mini_proj11/winequality-red.csv"

    extract(url, file_path)
