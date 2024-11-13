import os
import json
import tempfile
import boto3
from dateutil import parser
import logging

# S3 Configuration
s3_bucket = 'common-crawl-s3'
s3_input_prefix = 'athena-results/cleaning/'
s3_output_prefix = 'athena-results/analysis/'
s3_client = boto3.client('s3')

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_data(input_paths, output_path):
    filtered_data = []
    
    # Read each input file line by line to handle JSON Lines format
    for input_path in input_paths:
        with open(input_path, 'r') as input_file:
            for line in input_file:
                try:
                    item = json.loads(line)
                    
                    # Remove '$' sign and comma from price and convert to float if applicable
                    if 'price' in item and isinstance(item['price'], str):
                        item['price'] = item['price'].replace('$', '').replace(',', '').strip()
                        if item['price'].isdigit() or '.' in item['price']:
                            try:
                                item['price'] = float(item['price'])
                            except ValueError:
                                logging.warning(f"Unable to convert price to float: {item['price']}")
                                item['price'] = None

                    # Attempt to parse fetch_time if present
                    if 'fetch_time' in item and item['fetch_time']:
                        try:
                            item['fetch_time'] = parser.parse(item['fetch_time']).isoformat()
                        except (ValueError, TypeError):
                            logging.warning(f"Unable to parse fetch_time: {item['fetch_time']}")
                            item['fetch_time'] = None
                    
                    # Filter out unwanted results based on title and price
                    if (item['title'] not in [
                        "301 Moved Permanently", "Robot Check", "Sorry! Something went wrong!", "No Title Found"
                    ] and item['title'].lower() not in [
                        "amazon prime", "amazon prime day", "prime", "amazon best sellers", "best sellers", 
                        "cyber monday", "black friday", "error page"
                    ] and ('price' not in item or (isinstance(item['price'], (int, float)) and item['price'] >= 99 and item['price'] not in [2020, 2021, 2022, 2023, 2024, 1996]))):
                        filtered_data.append(item)
                except json.JSONDecodeError as e:
                    logging.warning(f"Skipping line due to JSON decode error: {e}")

    # Write the filtered data to the output file
    with open(output_path, 'w') as output_file:
        json.dump(filtered_data, output_file, indent=2)

try:
    # Create a temporary directory to hold input and output files
    with tempfile.TemporaryDirectory() as temp_dir:
        # List files in the S3 input prefix
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_input_prefix)
        if 'Contents' not in response or len(response['Contents']) == 0:
            raise FileNotFoundError("No files found in the input S3 path.")
        
        input_paths = []

        # Download each input file from S3
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                local_input_path = os.path.join(temp_dir, os.path.basename(obj['Key']))
                s3_client.download_file(s3_bucket, obj['Key'], local_input_path)
                input_paths.append(local_input_path)

        if not input_paths:
            raise FileNotFoundError("No JSON files found to process.")

        # Define the local output path
        local_output_path = os.path.join(temp_dir, 'output.json')

        # Run the cleaning process for all files
        clean_data(input_paths, local_output_path)

        # Upload the cleaned output file to S3
        s3_output_key = f"{s3_output_prefix}combined_cleaned_data.json"
        s3_client.upload_file(local_output_path, s3_bucket, s3_output_key)

        # Print success message
        logging.info(f"Cleaning.py ran successfully. Output path: s3://{s3_bucket}/{s3_output_key}")

except Exception as e:
    logging.error(f"An error occurred: {e}")
