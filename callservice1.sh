#!/bin/bash

BUCKET_NAME="service1csvbucket"
FILE_NAME="data.csv"

aws s3 cp "$FILE_NAME" "s3://$BUCKET_NAME/$FILE_NAME"

if [ $? -eq 0 ]; then
    echo "File successfully uploaded to S3."

    start=$(date +%s.%N)

    LAMBDA_URL="https://yioyhexj3utrpqt463dtdpez5i0utorv.lambda-url.us-east-2.on.aws/"

    JSON={"\"bucket_name\"":"\"$BUCKET_NAME\"","\"key\"":"\"$FILE_NAME\""}
    echo "JSON: $JSON"
    echo "Invoking Transform and Load function"
    time output=`curl -s -H "Content-Type: application/json" -X POST -d $JSON $LAMBDA_URL`
    echo ""
    echo "Lambda function response: $output"
    echo ""

    BUCKET_NAME=$(echo $output | jq -r '.bucket_name')
    FILE_NAME=$(echo $output | jq -r '.key')
    LAMBDA_URL="replace with query lambda url"

    JSON={"\"bucket_name\"":"\"$BUCKET_NAME\"","\"key\"":"\"$FILE_NAME\""}
    echo ""
    echo "JSON: $JSON"
    echo ""

    BUCKET_NAME=$(echo $output | jq -r '.bucket_name')
    FILE_NAME=$(echo $output | jq -r '.key')
    LAMBDA_URL="https://dtnhwksaaej43mvv7t2rkfiwp40ibuqq.lambda-url.us-east-2.on.aws/"

    declare -A JSON
    JSON["bucket_name"]=$BUCKET_NAME
    JSON["key"]=$FILE_NAME
    JSON["Filters"]='{"Region":"Sub-Saharan Africa","ItemType":"Office Supplies","SalesChannel":"Offline","OrderPriority":"Low","Country":"Zambia"}'
    JSON["Group By"]='["Region", "ItemType"]'

    JSON_STRING=$(jq -n \
                  --arg bucket_name "${JSON["bucket_name"]}" \
                  --arg key "${JSON["key"]}" \
                  --argjson filters "${JSON["Filters"]}" \
                  --argjson groupBy "${JSON["Group By"]}" \
                  '{
                     bucket_name: $bucket_name,
                     key: $key,
                     Filters: $filters,
                     "Group By": $groupBy
                   }')

    echo ""
    echo "JSON: $JSON_STRING"
    echo ""
    echo "Invoking Query function"
    echo ""
    time output=$(curl -s -H "Content-Type: application/json" -X POST -d "$JSON_STRING" $LAMBDA_URL)
    echo ""
    echo "Lambda function response: $output"
    echo ""

    end=$(date +%s.%N)
    duration=$(echo "$end - $start" | bc)
    formatted_duration=$(printf "%.3f" $duration)

    echo "Total time taken: $formatted_duration seconds"
else
    echo "Failed to upload file to S3."
fi