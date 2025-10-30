from google.cloud import storage
import pandas as pd
from io import StringIO

# CHANGE THESE TO YOUR BUCKET NAMES
RAW_BUCKET = "analytics-raw-yshrestha21"  
PROCESSED_BUCKET = "analytics-processed-yshrestha21"
print("=" * 60)
print("GCP Cloud Analytics - Quick Test")
print("=" * 60)

# Initialize client
client = storage.Client(project='cloud-analytics-dashboard')
bucket = client.bucket(RAW_BUCKET)

# List files
print("\n1. Files in bucket:")
blobs = bucket.list_blobs()
for blob in blobs:
    print(f"   - {blob.name}")

# Downloading and process file
print("\n2. Downloading and processing data...")
blob = bucket.blob('sales.csv')
data = blob.download_as_text()
df = pd.read_csv(StringIO(data))

print("\n3. Original Data:")
print(df)

# Transforming data
print("\n4. Transforming data...")
df['profit_margin'] = (df['revenue'] * 0.3).round(2)
print("\n5. Transformed Data:")
print(df)


print("\n6. Uploading processed data...")
processed_bucket = client.bucket(PROCESSED_BUCKET)
processed_blob = processed_bucket.blob('processed_sales.csv')
processed_blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')

print("\n✓ SUCCESS! Pipeline complete!")
print("=" * 60)