import os
import logging
import pandas as pd
import boto3
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
import json
from botocore.exceptions import ClientError
import re
from dateutil import parser  # Importing dateutil parser

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

# S3 Configuration
s3_bucket = 'common-crawl-s3'
s3_input_prefix = 'athena-results/data_extraction/'
s3_output_prefix = 'athena-results/cleaning/'
s3_client = boto3.client('s3')

# Extract data from WARC file using Boto3
def extract_data_from_warc(record):
    warc_filename = record['warc_filename']
    offset = int(record['warc_record_offset'])
    length = int(record['warc_record_length'])

    # Extract bucket and key from warc_filename
    bucket_name = "commoncrawl"
    warc_key = warc_filename

    # Use the S3 API to get the WARC record
    try:
        # Fetch the WARC file from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=warc_key, Range=f'bytes={offset}-{offset + length - 1}')
        raw_stream = response['Body']

        # Iterate through the WARC file using Warcio
        for warc_record in ArchiveIterator(raw_stream):
            if warc_record.rec_type == 'response':
                html_content = warc_record.content_stream().read().decode('utf-8', errors='replace')
                return html_content

    except ClientError as e:
        logger.error(f"Error fetching WARC record from {warc_key}: {e}")
        return None

    return None

# Extract product data from HTML content
def extract_product_data(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')

    # Update selectors as needed with fallbacks
    title = ""
    if soup.select_one("#productTitle"):
        title = soup.select_one("#productTitle").get_text().strip()
    elif soup.select_one("#btAsinTitle"):
        title = soup.select_one("#btAsinTitle").get_text().strip()
    elif soup.find("h1", {"class": "title"}):
        title = soup.find("h1", {"class": "title"}).get_text().strip()
    elif soup.find("title"):
        title = soup.find("title").get_text().strip()
    elif soup.find("h1"):
        title = soup.find("h1").get_text().strip()
    else:
        text = soup.get_text()
        title_match = re.search(r'Title:\s*(.*)', text)
        if title_match:
            title = title_match.group(1).strip()

    price = ""
    if soup.select_one("#priceblock_saleprice, #priceblock_ourprice, #priceblock_dealprice"):
        price = soup.select_one("#priceblock_saleprice, #priceblock_ourprice, #priceblock_dealprice").get_text().strip()
    elif soup.find("span", {"class": "a-price-whole"}):
        price = soup.find("span", {"class": "a-price-whole"}).get_text().strip()
    elif soup.find("span", {"class": "a-price"}):
        price = soup.find("span", {"class": "a-price"}).get_text().strip()
    elif soup.find("span", {"id": "price_inside_buybox"}):
        price = soup.find("span", {"id": "price_inside_buybox"}).get_text().strip()
    else:
        text = soup.get_text()
        word_pattern = re.compile(r'\d+(?:\.\d{2})?')
        match = word_pattern.search(text)
        if match:
            price = match.group()

    data = {
        'title': title if title else "No Title Found",
        'price': price if price else "No Price Found",
    }
    return data

# Process Parquet files from S3
def process_parquet_files():
    try:
        # List objects in the specified S3 location
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_input_prefix)
        for obj in response.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                parquet_key = obj['Key']
                logger.info(f"Processing Parquet file from S3: {parquet_key}")
                try:
                    # Download the Parquet file locally
                    local_parquet_path = os.path.join('/tmp', os.path.basename(parquet_key))
                    s3_client.download_file(s3_bucket, parquet_key, local_parquet_path)

                    # Load the Parquet file into a DataFrame
                    df = pd.read_parquet(local_parquet_path)
                    
                    # Skip empty Parquet files
                    if df.empty:
                        logger.warning(f"Skipping empty Parquet file: {parquet_key}")
                        continue
                except Exception as e:
                    logger.error(f"Error reading Parquet file {parquet_key}: {e}")
                    continue

                total_records = len(df)
                extracted_data = []

                for idx, record in df.iterrows():
                    try:
                        logger.info(f"Processing record {idx + 1} of {total_records}")
                        record_dict = record.to_dict()
                        html_content = extract_data_from_warc(record_dict)
                        if html_content:
                            product_data = extract_product_data(html_content)
                            product_data['url'] = record_dict['url']

                            # Attempt to parse fetch_time to handle known format
                            fetch_time = record_dict.get('fetch_time')
                            if fetch_time:
                                try:
                                    # Parse using the known date format
                                    parsed_fetch_time = pd.to_datetime(fetch_time, format="%Y-%m-%d %H:%M:%S")
                                    product_data['fetch_time'] = parsed_fetch_time.isoformat()
                                except ValueError:
                                    logger.warning(f"Unable to parse fetch_time: {fetch_time}")
                                    product_data['fetch_time'] = None

                            if any(product_data.values()):
                                extracted_data.append(product_data)
                                logger.info(f"Successfully extracted data for record {idx + 1}")
                            else:
                                logger.info(f"No product data found for record {idx + 1}")
                        else:
                            logger.info(f"Failed to extract HTML content for record {idx + 1}")
                    except Exception as e:
                        logger.error(f"Error processing record {idx + 1}: {e}")

                # Save extracted data to a new JSON file
                output_filename = f"extracted_data_{os.path.basename(parquet_key)}.json"
                output_file_path = os.path.join('/tmp', output_filename)
                with open(output_file_path, 'w', encoding='utf-8') as output_file:
                    for data in extracted_data:
                        json.dump(data, output_file, ensure_ascii=False)
                        output_file.write('\n')

                logger.info(f"Data extraction completed. File saved to '{output_file_path}'")

                # Upload the JSON file to S3
                s3_output_key = f"{s3_output_prefix}{output_filename}"
                s3_client.upload_file(output_file_path, s3_bucket, s3_output_key)
                logger.info(f"Output file uploaded to S3: {s3_output_key}")

                # Remove the downloaded Parquet file and JSON file after processing
                os.remove(local_parquet_path)
                os.remove(output_file_path)

            else:
                logger.info(f"Skipping non-Parquet file: {obj['Key']}")
    except Exception as e:
        logger.error(f"Error listing or processing Parquet files from S3: {e}")

if __name__ == "__main__":
    process_parquet_files()
