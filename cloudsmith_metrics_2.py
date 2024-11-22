import cloudsmith_api
import csv
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")

def get_layer_pulls(api_token, repository, months):
    # Configure the Cloudsmith API client
    configuration = cloudsmith_api.Configuration()
    configuration.api_key['X-Api-Key'] = api_token
    client = cloudsmith_api.ApiClient(configuration)
    api_instance = cloudsmith_api.EntitlementsApi(client)

    # Calculate the start date for the query
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=months * 30)

    try:
        # Fetch entitlement keys
        entitlements = api_instance.entitlements_list(owner=repository.split('/')[0],
                                                       repo=repository.split('/')[1])

        # Dictionary to store pulls grouped by key and month
        pulls_data = defaultdict(lambda: defaultdict(int))

        for entitlement in entitlements:
            key = entitlement.token
            usage = api_instance.entitlements_usage_list(owner=repository.split('/')[0],
                                                          repo=repository.split('/')[1],
                                                          identifier=key)

            for usage_record in usage:
                pull_date = datetime.strptime(usage_record.date_created, '%Y-%m-%dT%H:%M:%S.%fZ')
                if start_date <= pull_date <= end_date:
                    month_key = pull_date.strftime('%Y-%m')
                    pulls_data[key][month_key] += usage_record.docker_pull_count

        return pulls_data

    except Exception as e:
        print(f"An error occurred: {e}")
        return {}

def write_csv(pulls_data, months, output_file):
    # Generate month columns
    end_date = datetime.now(timezone.utc)
    months_list = [(end_date - timedelta(days=i * 30)).strftime('%Y-%m') for i in range(months)][::-1]

    # Write data to CSV
    with open(output_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Entitlement Key'] + months_list)

        for key, month_data in pulls_data.items():
            row = [key] + [month_data.get(month, 0) for month in months_list]
            writer.writerow(row)

if __name__ == "__main__":
    # User inputs
    if not API_TOKEN:
        print("Error: API_TOKEN not found in .env file.")
        exit(1)

    REPOSITORY = "your_namespace/your_repository"  # Replace with "namespace/repository"
    MONTHS = 6  # Number of months to analyze
    OUTPUT_FILE = "entitlement_pulls.csv"

    # Process data
    pulls_data = get_layer_pulls(API_TOKEN, REPOSITORY, MONTHS)
    write_csv(pulls_data, MONTHS, OUTPUT_FILE)

    print(f"Data written to {OUTPUT_FILE}")
