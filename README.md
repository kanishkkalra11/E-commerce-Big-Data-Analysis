# E-Commerce User Behavior Analysis

We will be analyzing the customer, sales, and product trends for an Indonesian online fashion retail platform ‘Fashion Campus’ for targeted marketing, sales trends analysis, and product recommendations. This repository contains the entire code used to run the analysis pipeline on GCP. Our datasource (in GCS bucket) has data on customers, products, transactions, and clickstreams for a 6 years period. We ingest this data into our compute clusters and run our models and spark jobs to analyze sales trends, find product popularity, and perform churn prediction. The results are stored inside bigquery from where they are used to create tableau dashboard. These insights can be used by multiple stakeholders such as sales, marketing, and product teams to target customers, recommend promotions, etc. Further features and visualizations can be easily added in this framework to extend its utility. For more details, you can view the project_slides in this repo or look at this presentation recording - https://drive.google.com/file/d/15pjxNu0ikjtSZH2wj-qgsuDjKJRdEsXF/view?usp=sharing.

<b>A note on pipeline scheduling:</b>
This pipeline can be run at any time to gather insights using the available data. The idea however is to schedule overnight daily so that the required computations are done incorporating the new data. The the classifier and regression models being used are trained quarterly and the model weights are stored in the bucket for daily predictions. A Redis cache has been created as a standalone endpoint and contains the hottest selling products, brands, and categories, updated everytime the pipeline is run. This can be quickly queried by other services like the recommendation engine as and when needed.



## Steps to run the ETL pipelinlie

1. You will need the gcloud to run the pipeline on GCP. Follow the steps here to install gcloud (make sure gcloud is added to path): https://cloud.google.com/sdk/docs/install
   
2. You will just need the run_etl.sh and auth.json to call the pipeline on GCP. Please download these two files and keep them in the same directory. Alternatively, run the following lines of code (you will need to have 'git' installed):

```bash
    git clone https://github.com/Prof-Rosario-UCLA/team15.git etl_pipeline_repo_temp && cd etl_pipeline_repo_temp
```

3. Execute the pipeline:

```bash
    bash run_etl.sh
```
\<You might need to enter empty passphrase twice in the terminal to update the cache if you are running gcoud cli for the first time. All aggregations and analyses for visualizations will be populated without any internvention.\>

4. To build the dashboard using the updated results, you'll need to refresh/extract your tableau dashboard. Here is a published dashboard on tableau public to see how it will look like: https://public.tableau.com/views/DataManagementDashboard/Dashboard1?:language=en-GB&publish=yes&:sid=&:redirect=auth&:display_count=n&:origin=viz_share_link



## Order of Execution

1. scripts/ETL.py: Fetches and integrates data from the following three tables: clickstream, product, and transactions. Clickstream data did not have product_ids for many click events. These click events were inferred using pyspark window functions. The immediate click, homepage click, and item_detail event preceding every add to cart event were identified and product IDs were inferred for them. A new feature dwell time was computed and aggregated on a product_id level to measure the engagement time for every product. All these click events were  aggregated for every product_id.

2. scripts/Aggregation.py: Calculated popularity scores based on an XGBoost model's feature importance scores for every click event (Code available in jupyter notebooks). Top 10 products, brands, subcategories, and most popular products for every genger and season were calculated and pushed to BigQUery.

3. scripts/churn_ETL.py: Customer, transaction, and clickstream data are retrieved from Google Cloud Storage and preprocessed using PySpark. Monthly features are generated for each customer, with churn labels assigned based on the rule: "churned if no transactions occur in the next three months." This process applies to all months except the last three, forming the training dataset. Both the training dataset and the prediction dataset (last three months) are then stored in BigQuery.

4. scripts/vis.py: Aggregates the data from transaction, product, and customer tables at a transaction level. After that, cetral latitude and longitude are extracted from the city names available in customer table using the geolocation API. The final table is dumped to bigquery for visualizations.

5. scripts/churn_prediction.py: The preprocessed training and prediction datasets are extracted from BigQuery, and a RandomForestClassifier is trained quarterly. The trained model is stored as a pickle file. Monthly predictions are made using the latest features and are pushed to BigQuery for dashboarding.

6. scripts/update_cache.py: This script is run on a GCE instance where redis server is installed and takes the top 10 data from the previous spark jobs and updates the cache.


## To check the analysis

We have added jupyter notebooks here to view the analysis we have performed and the models we have run. These are present inside the 'jupyter' folder. You need to have jupyter, git, and openjdk installed on your system. To run, follow these steps:

1. Clone the repository

    ```bash
    git clone https://github.com/Prof-Rosario-UCLA/team15.git check_ETL_analysis_temp && cd check_ETL_analysis_temp
    ```

2. Create a virtual environment

    For Windows: 

    ```bash
        python3 -m venv py_env
    ```

    Activate the virtual environment using the following command:

    ```bash
        py_env\Scripts\activate
    ```

    For Mac/Linux: 

    ```bash
        python -m venv py_env
    ```

    Activate the virtual environment using the following command:

    ```bash
        source py_env/bin/activate
    ```

3. Install the required packages using the requirements.txt file

```bash
    pip install -r scripts/requirements.txt
```

4. Open the notebooks to see our analyses.


### Contributors
- <a href="https://github.com/UtkarshRedd">Utkarsh Lal</a>
- <a href="https://github.com/kanishkkalra11">Kanishk Kalra</a>
- <a href="https://github.com/aparnas98">Aparna Suresh</a>
- <a href="https://github.com/VarshaBonam">Varsha Bonam</a>
- <a href="https://github.com/keer-09-thi">Keerthi Ravindran</a>
- <a href="https://github.com/palashk9808">Palash Khandelwal</a>


