CREATE TABLE "commoncrawl-test"."amazon_q14_test"
WITH (
  format = 'PARQUET',
  external_location = 's3://common-crawl-s3/athena-results/data_extraction/'
) AS
SELECT 
  url, warc_filename, warc_record_offset, warc_record_length, fetch_time
FROM "ccindex"
WHERE crawl IN (
    'CC-MAIN-2020-05',
    'CC-MAIN-2020-29',
    'CC-MAIN-2021-17',
    'CC-MAIN-2021-39',
    'CC-MAIN-2022-05',
    'CC-MAIN-2022-33',
    'CC-MAIN-2023-14',
    'CC-MAIN-2023-40',
    'CC-MAIN-2024-38'
  )
  AND url_host_registered_domain IN ('amazon.com','walmart.com')
  AND lower(url) LIKE '%laptop%'
  AND NOT (
    lower(url) LIKE '%reviews%'
    OR lower(url) LIKE '%store%'
    OR lower(url) LIKE '%blogs%'
    OR lower(url) LIKE '%track%'
    OR lower(url) LIKE '%corporate%'
    OR lower(url) LIKE '%browse%'
    OR lower(url) LIKE '%category%'
  );
