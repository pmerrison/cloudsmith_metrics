import requests
import csv
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Change to logging.INFO or logging.ERROR for less verbosity
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load environment variables from .env file
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")

BASE_URL = "https://api.cloudsmith.io/v1"

def fetch_entitlement_keys(namespace, repo):
    """
    Fetch entitlement keys for the given repository.
    """
    url = f"{BASE_URL}/entitlements/{namespace}/{repo}/"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}

    logging.debug(f"Fetching entitlement keys from: {url}")
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        logging.debug(f"Entitlement keys fetched successfully.")
    else:
        logging.error(f"Failed to fetch entitlement keys: {response.status_code} {response.text}")
    response.raise_for_status()
    return response.json()

def fetch_usage_metrics(namespace, repo, entitlement_token):
    """
    Fetch usage metrics for a specific entitlement key.
    """
    url = f"{BASE_URL}/entitlements/{namespace}/{repo}/{entitlement_token}/metrics/"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}

    logging.debug(f"Fetching usage metrics for entitlement token: {entitlement_token}")
    response = requests.get(url, headers=headers)
    if response.status_code == 404:
        logging.warning(f"No usage metrics found for entitlement token: {entitlement_token}")
        return []
    elif response.status_code != 200:
        logging.error(f"Failed to fetch usage metrics: {response.status_code} {response.text}")
    response.raise_for_status()
    metrics = response.json()
    logging.debug(f"Usage metrics fetched for {entitlement_token}: {metrics}")
    return metrics

def get_layer_pulls(namespace, repo, months):
    """
    Fetch entitlement keys and count layer pulls per month.
    """
    pulls_data = defaultdict(lambda: defaultdict(int))
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=months * 30)

    # Fetch all entitlement keys
    logging.info(f"Fetching entitlement keys for repository: {namespace}/{repo}")
    entitlements = fetch_entitlement_keys(namespace, repo)

    if not entitlements:
        logging.warning("No entitlement keys found.")
        return pulls_data

    logging.debug(f"Found {len(entitlements)} entitlement keys.")

    for entitlement in entitlements:
        token = entitlement["token"]
        logging.info(f"Processing entitlement token: {token}")

        # Fetch usage metrics for each entitlement key
        usage_metrics = fetch_usage_metrics(namespace, repo, token)

        for metric in usage_metrics:
            pull_date = datetime.fromisoformat(metric["date"][:-1])  # Remove 'Z' for ISO parsing
            if start_date <= pull_date <= end_date:
                month_key = pull_date.strftime("%Y-%m")
                pulls_data[token][month_key] += metric["count"]

    logging.debug(f"Completed processing. Pulls data: {pulls_data}")
    return pulls_data

def write_csv(pulls_data, months, output_file):
    """
    Write layer pull data to a CSV file.
    """
    end_date = datetime.now(timezone.utc)
    months_list = [(end_date - timedelta(days=i * 30)).strftime("%Y-%m") for i in range(months)][::-1]

    logging.info(f"Writing data to CSV file: {output_file}")
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Entitlement Key"] + months_list)

        for key, month_data in pulls_data.items():
            row = [key] + [month_data.get(month, 0) for month in months_list]
            writer.writerow(row)

    logging.info(f"CSV file written successfully: {output_file}")

if __name__ == "__main__":
    # User inputs
    if not API_TOKEN:
        logging.error("API_TOKEN not found in .env file.")
        exit(1)

    REPOSITORY = "tetrate/tid-fips-containers"  # Replace with "namespace/repository"
    MONTHS = 6  # Number of months to analyze
    OUTPUT_FILE = "entitlement_pulls.csv"

    # Extract namespace and repo
    namespace, repo = REPOSITORY.split("/")

    # Fetch data and write to CSV
    try:
        pulls_data = get_layer_pulls(namespace, repo, MONTHS)
        write_csv(pulls_data, MONTHS, OUTPUT_FILE)
    except requests.HTTPError as e:
        logging.error(f"HTTP Error: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
