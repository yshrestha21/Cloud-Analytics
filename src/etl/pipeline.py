"""
Complete ETL Pipeline for Cloud Analytics Dashboard
Author: Yubraj Shrestha
Course: CSE 4333-001
Project: Cloud-Based Data Analytics Dashboard
"""

from google.cloud import storage
import pandas as pd
from io import StringIO
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataExtractor:
    """Handles data extraction from Google Cloud Storage"""
    
    def __init__(self, bucket_name):
        """Initialize extractor with bucket name"""
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        logger.info(f" DataExtractor initialized for bucket: {bucket_name}")
    
    def extract_csv(self, file_name):
        """Extract CSV file from GCS and return as DataFrame"""
        try:
            logger.info(f"Extracting {file_name} from {self.bucket_name}...")
            blob = self.bucket.blob(file_name)
            
            if not blob.exists():
                raise FileNotFoundError(f"{file_name} not found in {self.bucket_name}")
            
            data = blob.download_as_text()
            df = pd.read_csv(StringIO(data))
            logger.info(f"Extracted {len(df)} rows from {file_name}")
            return df
            
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            raise
    
    def list_files(self):
        """List all files in the bucket"""
        try:
            blobs = list(self.bucket.list_blobs())
            logger.info(f"Found {len(blobs)} files in {self.bucket_name}")
            return [blob.name for blob in blobs]
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            raise


class DataTransformer:
    """Handles data transformation logic"""
    
    def __init__(self):
        logger.info("DataTransformer initialized")
    
    def calculate_profit_margin(self, df, revenue_col='revenue', margin_rate=0.3):
        """Calculate profit margin based on revenue"""
        try:
            logger.info(f"Calculating profit margins (rate: {margin_rate})...")
            
            if revenue_col not in df.columns:
                raise ValueError(f"Column '{revenue_col}' not found in data")
            
            df['profit_margin'] = (df[revenue_col] * margin_rate).round(2)
            logger.info(f"Added profit_margin column")
            return df
            
        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            raise
    
    def calculate_statistics(self, df):
        """Add statistical calculations"""
        try:
            logger.info("Calculating additional statistics...")
            
            if 'revenue' in df.columns:
                # Calculate revenue rank
                df['revenue_rank'] = df['revenue'].rank(ascending=False, method='dense').astype(int)
                
                # Calculate percentage of total revenue
                total_revenue = df['revenue'].sum()
                df['revenue_percentage'] = ((df['revenue'] / total_revenue) * 100).round(2)
                
                logger.info("Added statistical columns")
            
            return df
            
        except Exception as e:
            logger.error(f"Statistics calculation failed: {e}")
            raise
    
    def clean_data(self, df):
        """Clean data by removing nulls and duplicates"""
        try:
            logger.info("Cleaning data...")
            original_rows = len(df)
            
            # Remove duplicates
            df = df.drop_duplicates()
            
            # Remove rows with null values in critical columns
            df = df.dropna(subset=['revenue'] if 'revenue' in df.columns else [])
            
            removed_rows = original_rows - len(df)
            if removed_rows > 0:
                logger.info(f"Cleaned data: removed {removed_rows} rows")
            else:
                logger.info("Data already clean")
            
            return df
            
        except Exception as e:
            logger.error(f"Cleaning failed: {e}")
            raise
    
    def add_metadata(self, df):
        """Add processing metadata"""
        try:
            df['processed_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            df['processing_version'] = 'v1.0'
            logger.info("Added metadata columns")
            return df
        except Exception as e:
            logger.error(f"Failed to add metadata: {e}")
            raise


class DataLoader:
    """Handles loading processed data to Google Cloud Storage"""
    
    def __init__(self, bucket_name):
        """Initialize loader with bucket name"""
        self.bucket_name = bucket_name
        self.client = storage.Client()
        self.bucket = self.client.bucket(bucket_name)
        logger.info(f"DataLoader initialized for bucket: {bucket_name}")
    
    def load_csv(self, df, file_name):
        """Load DataFrame to GCS as CSV"""
        try:
            logger.info(f"Loading data to {self.bucket_name}/{file_name}...")
            
            csv_data = df.to_csv(index=False)
            blob = self.bucket.blob(file_name)
            blob.upload_from_string(csv_data, content_type='text/csv')
            
            logger.info(f"Loaded {len(df)} rows to {file_name}")
            return f"gs://{self.bucket_name}/{file_name}"
            
        except Exception as e:
            logger.error(f"Loading failed: {e}")
            raise


class ETLPipeline:
    """Complete ETL pipeline orchestrator"""
    
    def __init__(self, raw_bucket, processed_bucket):
        """Initialize pipeline with GCS buckets"""
        logger.info("=" * 70)
        logger.info("CLOUD ANALYTICS ETL PIPELINE - Yubraj Shrestha")
        logger.info("=" * 70)
        
        self.raw_bucket = raw_bucket
        self.processed_bucket = processed_bucket
        self.extractor = DataExtractor(raw_bucket)
        self.transformer = DataTransformer()
        self.loader = DataLoader(processed_bucket)
        
        logger.info("ETL Pipeline initialized and ready")
    
    def run_pipeline(self, input_file, output_file=None, add_stats=True):
        """
        Run complete ETL pipeline
        
        Args:
            input_file: Name of file in raw bucket
            output_file: Name for processed file (optional)
            add_stats: Whether to add statistical calculations
        
        Returns:
            DataFrame with processed data
        """
        try:
            if output_file is None:
                output_file = f"processed_{input_file}"
            
            logger.info("\n" + "=" * 70)
            logger.info(f"STARTING ETL PIPELINE")
            logger.info(f"Input:  {input_file}")
            logger.info(f"Output: {output_file}")
            logger.info("=" * 70)
            
            # PHASE 1: EXTRACT
            logger.info("\n" + "─" * 70)
            logger.info("[PHASE 1/3] EXTRACT")
            logger.info("─" * 70)
            df = self.extractor.extract_csv(input_file)
            logger.info(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
            logger.info(f"Columns: {', '.join(df.columns)}")
            
            # PHASE 2: TRANSFORM
            logger.info("\n" + "─" * 70)
            logger.info("[PHASE 2/3] TRANSFORM")
            logger.info("─" * 70)
            
            # Clean data
            df = self.transformer.clean_data(df)
            
            # Calculate profit margins
            df = self.transformer.calculate_profit_margin(df)
            
            # Add statistics if requested
            if add_stats:
                df = self.transformer.calculate_statistics(df)
            
            # Add metadata
            df = self.transformer.add_metadata(df)
            
            logger.info(f"Final shape: {df.shape[0]} rows × {df.shape[1]} columns")
            
            # PHASE 3: LOAD
            logger.info("\n" + "─" * 70)
            logger.info("[PHASE 3/3] LOAD")
            logger.info("─" * 70)
            output_path = self.loader.load_csv(df, output_file)
            
            # SUMMARY
            logger.info("\n" + "=" * 70)
            logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
            logger.info("=" * 70)
            logger.info(f"Source: gs://{self.raw_bucket}/{input_file}")
            logger.info(f"Target: {output_path}")
            logger.info(f"Records Processed: {df.shape[0]}")
            logger.info(f"Total Columns: {df.shape[1]}")
            
            if 'revenue' in df.columns and 'profit_margin' in df.columns:
                logger.info(f"\nFINANCIAL SUMMARY:")
                logger.info(f"   Total Revenue:      ${df['revenue'].sum():,.2f}")
                logger.info(f"   Total Profit:       ${df['profit_margin'].sum():,.2f}")
                logger.info(f"   Average Revenue:    ${df['revenue'].mean():,.2f}")
                logger.info(f"   Max Revenue:        ${df['revenue'].max():,.2f}")
                logger.info(f"   Min Revenue:        ${df['revenue'].min():,.2f}")
            
            logger.info("=" * 70 + "\n")
            
            return df
            
        except Exception as e:
            logger.error("\n" + "=" * 70)
            logger.error("ETL PIPELINE FAILED")
            logger.error("=" * 70)
            logger.error(f"Error Type: {type(e).__name__}")
            logger.error(f"Error Message: {e}")
            logger.error("=" * 70)
            raise
    
    def list_raw_files(self):
        """List all files in raw bucket"""
        return self.extractor.list_files()


# Main execution
if __name__ == "__main__":
    # Configuration
    RAW_BUCKET = "analytics-raw-yshrestha21"
    PROCESSED_BUCKET = "analytics-processed-yshrestha21"
    INPUT_FILE = "sales.csv"
    
    try:
        # Initialize pipeline
        pipeline = ETLPipeline(RAW_BUCKET, PROCESSED_BUCKET)
        
        # List available files
        logger.info("\nAvailable files in raw bucket:")
        files = pipeline.list_raw_files()
        for file in files:
            logger.info(f"  • {file}")
        
        # Run pipeline
        logger.info("\n")
        result_df = pipeline.run_pipeline(INPUT_FILE, add_stats=True)
        
        # Display sample of results
        logger.info("Sample of processed data:")
        print(result_df.head())
        
    except Exception as e:
        logger.error(f"\n Pipeline execution failed: {e}")
        exit(1)