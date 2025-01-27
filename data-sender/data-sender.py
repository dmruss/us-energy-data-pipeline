import pandas as pd
import time
import requests
import argparse
import traceback

def send_records(file_path, api_url, api_key, interval_minutes, num_records, header_line):
    try:
        # Load the Excel file with custom header and record start lines
        data = pd.read_excel(file_path, header=header_line - 1)
        total_records = len(data)

        # Determine how many records to send
        records_to_send = min(num_records, total_records) if num_records > 0 else total_records

        print(f"Total records: {total_records}. Sending {records_to_send} records.")

        # Iterate through the records
        for index, record in data.iterrows():
            if index == 0: continue
            if index >= records_to_send+1:
                break

            print(record['Month'])
            # Parse the "Month" column to extract year and month
            try:
                year = record['Month'].year
                month = record['Month'].month
            except ValueError:
                print(f"Skipping record {index + 1}: Invalid 'Month' format.")
                continue

            # Construct the URL with query parameters
            record_url = f"{api_url}?year={year}&month={month}"
            print(record_url)
            # Convert the record to a dictionary (excluding the "Month" field)
            record_body = record.drop(labels=['Month']).to_dict()

            # Send the POST request
            headers = {
                "Content-Type": "application/json",
                "x-api-key": api_key,
            }
            try:
                response = requests.post(record_url, json=record_body, headers=headers)
                print(f"Record {index + 1}: {response.status_code} - {response.text}")
            except Exception as e:
                print(f"Failed to send record {index + 1}: {e}")

            # Wait for the specified interval
            if index < records_to_send - 1:
                print(f"Waiting for {interval_minutes} minutes before sending the next record...")
                time.sleep(interval_minutes * 60)

    except Exception as e:
        traceback.print_exc()
        print(f"Error reading the file or sending requests: {e}")

# Define and parse command-line arguments
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send records from an Excel file as POST requests.")
    parser.add_argument("--file", type=str, required=True, help="Path to the Excel file.")
    parser.add_argument("--url", type=str, required=True, help="Base API URL for the POST request.")
    parser.add_argument("--key", type=str, required=True, help="API key for authentication.")
    parser.add_argument("--interval", type=float, required=True, help="Interval between requests (in minutes).")
    parser.add_argument("--records", type=int, default=-1, help="Number of records to send (1 to all). Use -1 for all records.")
    parser.add_argument("--header-line", type=int, required=True, help="The line number where the headers are located (1-based index).")

    args = parser.parse_args()

    send_records(
        args.file,
        args.url,
        args.key,
        args.interval,
        args.records,
        args.header_line
    )
