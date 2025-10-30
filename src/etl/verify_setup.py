from google.cloud import storage

print("Checking GCP Setup...")

try:
    client = storage.Client()
    print(f" Project: {client.project}")
    
    # Check both buckets
    for bucket_name in ["analytics-raw-yshrestha21", "analytics-processed-yshrestha21"]:
        bucket = client.bucket(bucket_name)
        if bucket.exists():
            print(f"Bucket exists: {bucket_name}")
            blobs = list(bucket.list_blobs())
            print(f"  Files: {len(blobs)}")
            for blob in blobs:
                print(f"    • {blob.name}")
        else:
            print(f" Bucket NOT found: {bucket_name}")
    
    print("Setup looks good, now Run test.py now.")
    
except Exception as e:
    print(f"\n Error: {e}")
    print("\nRun these commands:")
    print("  gcloud auth application-default login")
    print("  gcloud config set project YOUR_PROJECT_ID")