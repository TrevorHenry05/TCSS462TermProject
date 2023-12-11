#!/bin/bash

BUCKET_NAME="service1csvbucket"
FILE_NAME="data.csv"

aws s3 cp "$FILE_NAME" "s3://$BUCKET_NAME/$FILE_NAME"

if [ $? -eq 0 ]; then
    echo "File successfully uploaded to S3."

    LAMBDA_URL="https://eohzm62ggjmbg4rtwfpl47rpam0ysqbk.lambda-url.us-east-2.on.aws/"

    JSON={"\"bucket_name\"":"\"$BUCKET_NAME\"","\"key\"":"\"$FILE_NAME\""}
    echo "JSON: $JSON"
    echo "Invoking Transform function"
    time output=`curl -s -H "Content-Type: application/json" -X POST -d $JSON $LAMBDA_URL`
    echo ""
    echo "Lambda function response: $output"
    echo ""

    BUCKET_NAME=$(echo $output | jq -r '.bucket_name')
    FILE_NAME=$(echo $output | jq -r '.key')
    LAMBDA_URL="https://stqsdxuxwne7ircsb2aqcqmfxm0tesai.lambda-url.us-east-2.on.aws/"

    JSON={"\"bucket_name\"":"\"$BUCKET_NAME\"","\"key\"":"\"$FILE_NAME\""}
    echo ""
    echo "JSON: $JSON"
    echo ""
    echo "Invoking Load function"
    time output=`curl -s -H "Content-Type: application/json" -X POST -d $JSON $LAMBDA_URL`
    echo ""
    echo "Lambda function response: $output"
    echo ""
else
    echo "Failed to upload file to S3."
fi
