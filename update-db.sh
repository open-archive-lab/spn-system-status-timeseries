#!/bin/bash

# --- Configuration ---
API_URL="https://web.archive.org/save/status/system"
OUTPUT_CSV="db.csv"
BASE_LOG_DIR="data/log"

# --- Error Handling Function ---
# $1: Error message (string)
# $2: Raw API response (string)
log_error() {
    local error_message="$1"
    local raw_response="$2"
    
    # Format: YYYY-MM-DD_HH-mm-SS
    # %Y-%m-%d -> Year-Month-Day
    # %H-%M-%S -> Hour-Minute-Second
    local LOG_DIR="$BASE_LOG_DIR/$(date -u +'%Y-%m-%d_%H-%M-%S')"
    
    echo "Error: $error_message" >&2
    echo "Creating log directory: $LOG_DIR" >&2
    
    # The -p flag ensures parent directories are also created if they don't exist
    mkdir -p "$LOG_DIR"
    if [ $? -ne 0 ]; then
        echo "Fatal Error: Could not create log directory $LOG_DIR. Please check permissions." >&2
        exit 1
    fi
    
    # Write to error log
    echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ'): $error_message" > "$LOG_DIR/error.log"
    
    # Write raw response
    echo "$raw_response" > "$LOG_DIR/raw_response.json"
    
    # Exit script with an error code
    exit 1
}

# --- Dependency Checks ---
# Check if 'curl' is installed
if ! command -v curl &> /dev/null; then
    echo "Error: 'curl' command not found. Please install curl." >&2
    exit 1
fi

# Check if 'jq' is installed
if ! command -v jq &> /dev/null; then
    echo "Error: 'jq' command not found. Please install jq." >&2
    exit 1
fi

# --- 1. Call API ---
# -s means silent mode (no progress bar)
echo "Fetching data from API: $API_URL"
RAW_RESPONSE=$(curl -s "$API_URL")
CURL_EXIT_CODE=$?

# Exception Check 1: Check if curl command executed successfully
if [ $CURL_EXIT_CODE -ne 0 ]; then
    log_error "curl command failed (exit code: $CURL_EXIT_CODE). Check network connection or URL." ""
fi

# Exception Check 2: Check if API response is empty
if [ -z "$RAW_RESPONSE" ]; then
    log_error "API returned an empty response." ""
fi

# --- 2. Validate API Status ---
# Use jq to check JSON format and 'status' field
# -e option: sets a non-zero exit code if the last result is false or null
# -r option: removes quotes from the string output
API_STATUS=$(echo "$RAW_RESPONSE" | jq -e -r '.status')
JQ_EXIT_CODE=$?

# Exception Check 3: Check if JSON is valid and '.status' field exists
if [ $JQ_EXIT_CODE -ne 0 ]; then
    log_error "JSON parsing failed or '.status' field missing/null. (jq exit code: $JQ_EXIT_CODE)" "$RAW_RESPONSE"
fi

# Exception Check 4: Check if API business status is "ok"
if [ "$API_STATUS" != "ok" ]; then
    log_error "API reported status is not 'ok'. Received status: $API_STATUS" "$RAW_RESPONSE"
fi

# --- 3. Extract Data ---
# Generate an ISO 8601 UTC timestamp, the standard for time-series data
TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

# Define CSV header (used when creating the file)
CSV_HEADER="timestamp,recent_captures,queue_spn2_captures,queue_spn2_captures_misc,queue_spn2_outlinks,queue_spn2_outlinks_misc,queue_spn2_api,queue_spn2_api_misc,queue_spn2_api_outlinks,queue_spn2_api_outlinks_misc,queue_spn2_high_fidelity,queue_spn2_vip,queue_spn2_vip_outlinks,queue_spn2_screenshots"

# Use jq to extract all data at once and format as a CSV row
# --arg ts "$TIMESTAMP" passes the bash variable $TIMESTAMP into jq as $ts
# [...] | @csv converts the array into a CSV formatted line
CSV_DATA_LINE=$(echo "$RAW_RESPONSE" | jq -r --arg ts "$TIMESTAMP" '
    [
        $ts,
        .recent_captures,
        .queues."spn2-captures",
        .queues."spn2-captures-misc",
        .queues."spn2-outlinks",
        .queues."spn2-outlinks-misc",
        .queues."spn2-api",
        .queues."spn2-api-misc",
        .queues."spn2-api-outlinks",
        .queues."spn2-api-outlinks-misc",
        .queues."spn2-high-fidelity",
        .queues."spn2-vip",
        .queues."spn2-vip-outlinks",
        .queues."spn2-screenshots"
    ] | @csv
')

# Exception Check 5: Ensure all fields were extracted successfully
if [ $? -ne 0 ] || [ -z "$CSV_DATA_LINE" ]; then
    log_error "jq data extraction failed. JSON structure may have changed or key fields are missing." "$RAW_RESPONSE"
fi

# --- 4. Write to CSV File ---

# Check if CSV file exists and has content (-s: file exists and has a size greater than zero)
# If file doesn't exist or is empty, write the header
if [ ! -s "$OUTPUT_CSV" ]; then
    echo "Creating new CSV file and writing header: $OUTPUT_CSV"
    echo "$CSV_HEADER" > "$OUTPUT_CSV"
    # Exception Check 6: Check if header write was successful
    if [ $? -ne 0 ]; then
        echo "Fatal Error: Could not write header to $OUTPUT_CSV. Please check file permissions." >&2
        exit 1
    fi
fi

# Append new data row to the CSV file
echo "$CSV_DATA_LINE" >> "$OUTPUT_CSV"
# Exception Check 7: Check if data append was successful
if [ $? -ne 0 ]; then
    echo "Fatal Error: Could not append data to $OUTPUT_CSV. Please check file permissions or disk space." >&2
    exit 1
fi

echo "Success: Data appended to $OUTPUT_CSV"
exit 0
