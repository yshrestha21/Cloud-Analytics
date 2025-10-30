from google.cloud import storage
import pandas as pd
from io import StringIO

RAW_BUCKET = "analytics-raw-yshrestha21"  
PROCESSED_BUCKET = "analytics-processed-yshrestha21"

print("GCP Cloud Analytics Dashboard - ETL Pipeline Test")

try:
    # Initialize client - auto-detect project from gcloud config
    print("\n Initializing GCP client")
    client = storage.Client()  # Remove the project parameter
    print(f"Connected to project: {client.project}")
    
    # List files in raw bucket
    print(f"\nFiles in {RAW_BUCKET}:")
    bucket = client.bucket(RAW_BUCKET)
    blobs = list(bucket.list_blobs())
    
    if len(blobs) == 0:
        print("No files found in bucket")
    else:
        for blob in blobs:
            print(f"   • {blob.name}")
    
    # Download and process sales.csv
    print("\nDownloading sales.csv...")
    blob = bucket.blob('sales.csv')
    
    if not blob.exists():
        raise FileNotFoundError(f"sales.csv not found in {RAW_BUCKET}")
    
    data = blob.download_as_text()
    df = pd.read_csv(StringIO(data))
    
    print("\nOriginal Data:")
    print(df)
    print(f"\nShape: {df.shape[0]} rows × {df.shape[1]} columns")
    
    # Validate required columns
    if 'revenue' not in df.columns:
        raise ValueError(f"Required column 'revenue' not found")
    
    # Transform data
    print("\nTransforming data...")
    df['profit_margin'] = (df['revenue'] * 0.3).round(2)
    
    print("\nTransformed Data:")
    print(df)
    
    # Upload processed data
    print("\nUploading processed data...")
    processed_bucket = client.bucket(PROCESSED_BUCKET)
    processed_blob = processed_bucket.blob('processed_sales.csv')
    processed_blob.upload_from_string(
        df.to_csv(index=False), 
        content_type='text/csv'
    )
    
    print("Pipeline complete")
    print(f"  Processed {df.shape[0]} records")
    print(f"  Output: gs://{PROCESSED_BUCKET}/processed_sales.csv")

except Exception as e:
    print(f"\nERROR: {e}")
    print("\nRun these commands:")
    print("gcloud config set project cogent-splicer-476702-i8")
    print("gcloud auth application-default login")