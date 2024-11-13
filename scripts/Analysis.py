import json
import logging
import os
import statistics
import boto3
import tempfile
from collections import defaultdict

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# S3 Configuration
s3_bucket = 'common-crawl-s3'
s3_input_prefix = 'athena-results/analysis/combined_cleaned_data.json'
s3_output_prefix = 'athena-results/final/analysis_summary.json'
s3_client = boto3.client('s3')

# Define thresholds for price segmentation
LOW_PRICE_THRESHOLD = 300
MID_PRICE_THRESHOLD = 700

def calculate_statistics(prices):
    """
    Calculate statistics for the given list of prices.
    """
    if not prices:
        return {'average': None, 'max': None, 'min': None, 'stdev': None}
    return {
        'average': round(statistics.mean(prices), 2),
        'max': round(max(prices), 2),
        'min': round(min(prices), 2),
        'stdev': round(statistics.stdev(prices), 2) if len(prices) > 1 else 0.0
    }

def analyze_data(input_path, output_path):
    """
    Analyze the data by segmenting prices and calculating statistics.
    """
    try:
        with open(input_path, 'r') as file:
            file_content = file.read()
            logging.info(f"File content preview: {file_content[:100]}...")  # Log the first 100 characters

            if not file_content.strip():
                logging.error("Input file is empty.")
                return

            # Load the JSON content
            try:
                records = json.loads(file_content)
            except json.JSONDecodeError as e:
                logging.error(f"Input file is not a valid JSON format. Error: {e}")
                return

    except FileNotFoundError:
        logging.error("Input file not found.")
        return

    # Segment prices by year and into low, mid, and high categories
    yearly_price_segments = defaultdict(lambda: {'low': [], 'mid': [], 'high': []})

    for record in records:
        fetch_time = record.get('fetch_time')
        if not fetch_time:
            logging.warning("Record missing fetch_time, skipping this record.")
            continue

        year = fetch_time[:4]  # Extract year from 'fetch_time'
        price = record.get('price')
        if price is None:
            logging.warning(f"Record missing price, skipping: {record}")
            continue

        # Segmenting the prices
        if 100 <= price <= LOW_PRICE_THRESHOLD:
            yearly_price_segments[year]['low'].append(price)
        elif LOW_PRICE_THRESHOLD < price <= MID_PRICE_THRESHOLD:
            yearly_price_segments[year]['mid'].append(price)
        elif price > MID_PRICE_THRESHOLD:
            yearly_price_segments[year]['high'].append(price)

    # Calculate statistics for each segment in each year
    yearly_segment_summary = {}
    for year, segments in yearly_price_segments.items():
        yearly_segment_summary[year] = {segment: calculate_statistics(prices) for segment, prices in segments.items()}

    # Calculate inflation rates for each year compared to the previous year
    inflation_summary = {}
    inflation_analysis = []
    sorted_years = sorted(yearly_segment_summary.keys())
    for i in range(1, len(sorted_years)):
        previous_year = sorted_years[i - 1]
        current_year = sorted_years[i]
        inflation_summary[current_year] = {}
        for segment in ['low', 'mid', 'high']:
            prev_avg = yearly_segment_summary[previous_year][segment]['average']
            curr_avg = yearly_segment_summary[current_year][segment]['average']
            if prev_avg is not None and curr_avg is not None and prev_avg != 0:
                inflation_rate = ((curr_avg - prev_avg) / prev_avg) * 100
                inflation_summary[current_year][segment] = round(inflation_rate, 2)
                if inflation_rate > 0:
                    inflation_analysis.append(f"The inflation increased from year {previous_year} to {current_year} by {round(inflation_rate, 2)}% in the {segment} segment.")
                elif inflation_rate < 0:
                    inflation_analysis.append(f"The inflation decreased from year {previous_year} to {current_year} by {round(abs(inflation_rate), 2)}% in the {segment} segment.")
            else:
                inflation_summary[current_year][segment] = None

    # Reorganize the output data structure
    output_data = {
        'inflation_analysis': inflation_analysis,
        'inflation_summary': dict(sorted(inflation_summary.items())),  # Sort by year in ascending order
        'yearly_segment_summary': dict(sorted(yearly_segment_summary.items()))  # Sort by year in ascending order
    }

    # Save the summary to the output file
    with open(output_path, 'w', encoding='utf-8') as json_file:
        json.dump(output_data, json_file, ensure_ascii=False, indent=4)

    logging.info(f"Data analysis completed. Summary saved to '{output_path}'")

try:
    # Create a temporary directory to hold input and output files
    with tempfile.TemporaryDirectory() as temp_dir:
        # Define local paths for input and output
        local_input_path = os.path.join(temp_dir, 'input.json')
        local_output_path = os.path.join(temp_dir, 'output.json')

        # Download the input file from S3
        try:
            s3_client.download_file(s3_bucket, s3_input_prefix, local_input_path)
            logging.info("Input file downloaded successfully.")
        except Exception as e:
            logging.error(f"Failed to download input file from S3. Error: {e}")
            raise

        # Run the data analysis
        analyze_data(local_input_path, local_output_path)

        # Upload the analysis output file to S3
        try:
            s3_client.upload_file(local_output_path, s3_bucket, s3_output_prefix)
            logging.info(f"Data analysis completed successfully. Output path: s3://{s3_bucket}/{s3_output_prefix}")
        except Exception as e:
            logging.error(f"Failed to upload output file to S3. Error: {e}")

except Exception as e:
    logging.error(f"An unexpected error occurred: {e}")
