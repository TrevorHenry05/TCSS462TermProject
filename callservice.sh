#!/bin/bash

# Default number of runs is 10, or use the provided argument
NUM_RUNS=${1:-10}
declare -a RUN_TIMES

for run in $(seq 1 $NUM_RUNS)
do
    echo ""
    echo "Run $run"
    echo ""

    BUCKET_NAME="service1csvbucket"
    FILE_NAME="data.csv"

    aws s3 cp "$FILE_NAME" "s3://$BUCKET_NAME/$FILE_NAME"

    if [ $? -eq 0 ]; then
        echo "File successfully uploaded to S3."

        start=$(date +%s.%N)

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

        RUN_TIMES[$run]=$formatted_duration

        echo "Run $run time: ${RUN_TIMES[$run]} seconds"
        echo ""
    else
        echo "Failed to upload file to S3."
    fi
done

total_time=0
run_number=1

echo ""
echo "All runtimes table:"
echo ""

for time in "${RUN_TIMES[@]}"
do
    echo "Run $run_number time: $time seconds"
    total_time=$(echo "$total_time + $time" | bc)
    run_number=$((run_number + 1))
done

avg_time=$(echo "scale=3; $total_time / $NUM_RUNS" | bc)
echo ""
echo "Average runtime: $avg_time seconds"


