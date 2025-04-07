import os
from google.cloud import bigquery
import pandas as pd
import numpy as np
from google.cloud import storage
import pickle
from sklearn.ensemble import RandomForestClassifier
from io import StringIO

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "authkey.json"   


client = bigquery.Client()


 
query1 = """
SELECT   customer_id, month, frequency,
    monetary, recency, session_count,
    avg_session_duration, churn
FROM `data-management-project-452400.data_mgmt_project.churn_X_train_full`

"""

X_train_full = client.query(query1).to_dataframe()




#X_pred_full = X_pred_full.fillna(0)
#X_train_full = X_train_full.fillna(0)

features = ['recency', 'frequency', 'monetary', 'session_count', 'avg_session_duration']
#Y_train = X_train_full['churn']
#X_train = X_train_full[['customer_id'] + features ]



# Train a Random Forest model
# model = RandomForestClassifier(n_estimators=100, random_state=42)
# model.fit(X_train, Y_train)

#connect to GCS
client1 = storage.Client()
bucket_name = 'data-mgmt-bucket'  


#Store the model
#file_path1 = 'dataset/trained_model.pkl' 
#bucket = client1.get_bucket(bucket_name)
#blob = bucket.blob(file_path1)


#pickle_buffer = BytesIO()
#pickle.dump(model, pickle_buffer)
#pickle_buffer.seek(0)
#blob.upload_from_file(pickle_buffer, content_type='application/octet-stream')




# Get trained model
#model_data = BytesIO(blob.download_as_bytes())
#loaded_model = pickle.load(model_data)

#Predict
#churn_likelihood = loaded_model.predict_proba(X_pred)[:, 1]  
#X_pred_full['predicted_churn'] = (churn_likelihood > 0.95).astype(int)



file_path = 'dataset/X_pred_full.csv'  # Path to your CSV file in GCS
dtypes = {
    'customer_id': 'Int64',  # or 'datetime64[ns]' if it's a date
    'frequency': 'Int64',
    'monetary': 'Int64',
    'recency': 'Int64',
    'session_count': 'Int64',
    'avg_session_duration': 'float64',
    'predicted_churn': 'int64',
    'PredictedMonthFlag': 'int64'
}

# Get the bucket and blob (file)
bucket = client1.get_bucket(bucket_name)
blob = bucket.blob(file_path)

# Download the CSV file as a string
csv_data = blob.download_as_text()
X_pred_full = pd.read_csv(StringIO(csv_data), dtype=dtypes,  parse_dates=['month'])


#Final Dataset
X_pred_full['PredictedMonthFlag'] = 1
#X_train_full = X_train_full.drop(columns=['date_diff'], errors='ignore')
X_train_full = X_train_full.rename(columns={'churn': 'predicted_churn'})
X_train_full['PredictedMonthFlag'] = 0
latest_month = X_train_full['month'].max()
X_train_full = X_train_full[X_train_full['month'] >= latest_month - pd.DateOffset(months=9)]
Churn_Data_pred = pd.concat([X_train_full, X_pred_full], ignore_index=True)
 


#Push to Bigquery

client = bigquery.Client(project='data-management-project-452400')
table_id = 'data-management-project-452400.data_mgmt_project.Churn_Data_view'
job_config = bigquery.LoadJobConfig(
    write_disposition='WRITE_TRUNCATE'  # Overwrite the table if it exists
)

 
job = client.load_table_from_dataframe(
    Churn_Data_pred,   
    table_id,  
    job_config=job_config   
)

