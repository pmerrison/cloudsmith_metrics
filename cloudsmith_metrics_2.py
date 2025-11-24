import requests
import csv
import logging
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dotenv import load_dotenv
import os

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
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
    response.raise_for_status()
    return response.json()

def fetch_usage_metrics(namespace, repo, token, start, finish):
    """
    Fetch usage metrics for a specific entitlement token.
    Returns the total number of downloads for the given date range.
    """
    url = f"{BASE_URL}/metrics/entitlements/{namespace}/{repo}/"
    headers = {"Authorization": f"Bearer {API_TOKEN}"}
    params = {
        "tokens": token,
        "start": start,
        "finish": finish
    }

    logging.debug(f"Fetching usage metrics from: {url} with params: {params}")
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 404:
        logging.warning(f"No usage metrics found for entitlement token: {token}")
        return 0
    response.raise_for_status()
    metrics = response.json()

    # Extract total downloads from the response structure
    # Response format: {"tokens": {"downloads": {"total": {"value": N}}}}
    total_downloads = metrics.get("tokens", {}).get("downloads", {}).get("total", {}).get("value", 0)
    logging.debug(f"Total downloads for {token} from {start} to {finish}: {total_downloads}")
    return total_downloads

def get_layer_pulls(namespace, repo, months):
    """
    Fetch entitlement keys and count downloads per month.
    Makes separate API calls for each month to get monthly breakdowns.
    """
    pulls_data = defaultdict(lambda: defaultdict(int))
    end_date = datetime.now(timezone.utc)

    # Fetch all entitlement keys
    logging.info(f"Fetching entitlement keys for repository: {namespace}/{repo}")
    entitlements = fetch_entitlement_keys(namespace, repo)

    if not entitlements:
        logging.warning("No entitlement keys found.")
        return pulls_data

    logging.info(f"Found {len(entitlements)} entitlement keys.")

    # Generate list of months to query
    months_list = []
    for i in range(months):
        month_date = end_date - timedelta(days=i * 30)
        # Get the first day of the month
        first_day = month_date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        # Get the last day of the month (first day of next month minus one day)
        if first_day.month == 12:
            last_day = first_day.replace(year=first_day.year + 1, month=1) - timedelta(days=1)
        else:
            last_day = first_day.replace(month=first_day.month + 1) - timedelta(days=1)
        last_day = last_day.replace(hour=23, minute=59, second=59)

        month_key = first_day.strftime("%Y-%m")
        months_list.append({
            "key": month_key,
            "start": first_day.strftime("%Y-%m-%d"),
            "finish": last_day.strftime("%Y-%m-%d")
        })

    months_list.reverse()  # Process in chronological order

    # For each entitlement token, fetch metrics for each month
    for entitlement in entitlements:
        token = entitlement["slug_perm"]
        token_name = entitlement.get("name", token)
        logging.info(f"Processing entitlement: {token_name} ({token})")

        for month_info in months_list:
            month_key = month_info["key"]
            start_str = month_info["start"]
            finish_str = month_info["finish"]

            logging.debug(f"  Fetching metrics for {month_key} (from {start_str} to {finish_str})")

            # Fetch usage metrics for this token and month
            total_downloads = fetch_usage_metrics(namespace, repo, token, start_str, finish_str)
            pulls_data[token][month_key] = total_downloads

            logging.debug(f"    Downloads: {total_downloads}")

    logging.info(f"Completed processing all entitlements.")
    return pulls_data

def write_csv(pulls_data, months, output_file):
    """
    Write download data to a CSV file.
    """
    end_date = datetime.now(timezone.utc)

    # Generate proper list of month keys
    months_list = []
    for i in range(months):
        month_date = end_date - timedelta(days=i * 30)
        first_day = month_date.replace(day=1)
        month_key = first_day.strftime("%Y-%m")
        months_list.append(month_key)

    months_list.reverse()  # Order chronologically

    logging.info(f"Writing data to CSV file: {output_file}")
    with open(output_file, "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Entitlement Token"] + months_list)

        for token, month_data in pulls_data.items():
            row = [token] + [month_data.get(month, 0) for month in months_list]
            writer.writerow(row)

    logging.info(f"CSV file written successfully: {output_file}")

if __name__ == "__main__":
    # User inputs
    if not API_TOKEN:
        logging.error("API_TOKEN not found in .env file.")
        exit(1)

    # Configuration
    REPOSITORY = "tetrate/tid-fips-containers"  # Replace with your namespace/repository
    MONTHS = 6  # Number of months to analyze
    OUTPUT_FILE = "entitlement_downloads.csv"

    # Extract namespace and repo
    namespace, repo = REPOSITORY.split("/")

    # Fetch data and write to CSV
    try:
        logging.info(f"Starting metrics collection for {REPOSITORY}")
        logging.info(f"Analyzing {MONTHS} months of data")
        pulls_data = get_layer_pulls(namespace, repo, MONTHS)
        write_csv(pulls_data, MONTHS, OUTPUT_FILE)
        logging.info(f"Successfully generated {OUTPUT_FILE}")
    except requests.HTTPError as e:
        logging.error(f"HTTP Error: {e}")
        exit(1)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
