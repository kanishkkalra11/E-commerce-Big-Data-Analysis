#!/bin/bash
gcloud config set account 284934505401-compute@developer.gserviceaccount.com

gcloud auth activate-service-account 284934505401-compute@developer.gserviceaccount.com --key-file="auth.json"

# Define variables
CLUSTER_NAME="utkarshlalcluster"
REGION="us-west1"
PROJECT_ID="data-management-project-452400"
FILES="gs://data-mgmt-bucket/pipelines/authkey.json"
PROPERTIES="spark.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=authkey.json,spark.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=authkey.json"

# Define ETL scripts (to run sequentially)
ETL_SCRIPTS=(
    "gs://data-mgmt-bucket/pipelines/ETL.py"
    "gs://data-mgmt-bucket/pipelines/Aggregation.py"
    "gs://data-mgmt-bucket/pipelines/churn_ETL.py"
    "gs://data-mgmt-bucket/pipelines/vis.py"
    "gs://data-mgmt-bucket/pipelines/churn_prediction.py"
)

# Track job status
declare -a JOB_STATUS

# Function to submit Dataproc job with retries
submit_job() {
    local SCRIPT_PATH=$1
    local RETRIES=2
    local ATTEMPT=1

    echo "🚀 Submitting job: $SCRIPT_PATH"

    while [ $ATTEMPT -le $((RETRIES+1)) ]; do
        gcloud dataproc jobs submit pyspark "$SCRIPT_PATH" \
            --cluster="$CLUSTER_NAME" \
            --region="$REGION" \
            --project="$PROJECT_ID" \
            --files="$FILES" \
            --properties="$PROPERTIES"

        # Capture exit code
        EXIT_CODE=$?

        if [ $EXIT_CODE -eq 0 ]; then
            echo "✅ Job succeeded: $SCRIPT_PATH (Attempt $ATTEMPT)"
	    JOB_STATUS+=("Success")
            return 0
        else
            echo "⚠️ Job failed: $SCRIPT_PATH (Attempt $ATTEMPT)"
            ATTEMPT=$((ATTEMPT+1))
        fi
    done

    echo "❌ Job permanently failed after $((RETRIES+1)) attempts: $SCRIPT_PATH"
    JOB_STATUS+=("Failed")
    return 1
}

# Run ETL jobs sequentially (strictly one after another)
for SCRIPT in "${ETL_SCRIPTS[@]}"; do
    submit_job "$SCRIPT"
done

# Display final job status
echo -e "\n📌 Final Job Status:"
for SCRIPT in "${!JOB_STATUS[@]}"; do
    echo "🔹 ${ETL_SCRIPTS[SCRIPT]} → ${JOB_STATUS[SCRIPT]}"
done

echo -e "\n\nUPDATING REDIS CACHE WITH CURRENT HOTTEST BRANDS, PRODUCTS, CATEGORIES ..."
gcloud compute ssh vm-airflow --command "python3 /mnt/update_cache.py" --project=data-management-project-452400 --zone=us-west1-a
echo ">>> UPDATED TOP 10 BRANDS:"
gcloud compute ssh vm-airflow --command "redis-cli lrange top10_brands 0 -1" --project=data-management-project-452400 --zone=us-west1-a
echo ">>> UPDATED TOP 10 PRODUCTS:"
gcloud compute ssh vm-airflow --command "redis-cli lrange top10_products 0 -1" --project=data-management-project-452400 --zone=us-west1-a
echo ">>> UPDATED TOP 10 CATEGORIES:"
gcloud compute ssh vm-airflow --command "redis-cli lrange top10_productCategories 0 -1" --project=data-management-project-452400 --zone=us-west1-a

echo -e "\n🎉 ETL pipeline execution completed!"
