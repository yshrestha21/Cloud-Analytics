import pandas as pd
import numpy as np
from datetime import datetime


class DataTransformer:
    """
    Handles all data transformation operations
    """
    
    def __init__(self):
        """Initialize transformer with empty log"""
        self.transformation_log = []
        self.start_time = datetime.now()
    
    def clean_data(self, df):
        """
        Remove duplicates and handle missing values
        
        Args:
            df (pandas.DataFrame): Input dataframe
            
        Returns:
            pandas.DataFrame: Cleaned dataframe
        """
        initial_rows = len(df)
        initial_missing = df.isnull().sum().sum()
        
        # Remove duplicate rows
        df = df.drop_duplicates()
        duplicates_removed = initial_rows - len(df)
        
        # Handle missing values
        # For numeric columns: fill with 0
        numeric_columns = df.select_dtypes(include=[np.number]).columns
        df[numeric_columns] = df[numeric_columns].fillna(0)
        
        # For text columns: fill with 'Unknown'
        text_columns = df.select_dtypes(include=['object']).columns
        df[text_columns] = df[text_columns].fillna('Unknown')
        
        # Log this transformation
        self.transformation_log.append({
            'step': 'clean_data',
            'timestamp': datetime.now().isoformat(),
            'initial_rows': initial_rows,
            'final_rows': len(df),
            'duplicates_removed': duplicates_removed,
            'missing_values_handled': initial_missing
        })
        
        print(f"Data cleaned: {duplicates_removed} duplicates removed, {initial_missing} missing values handled")
        return df
    
    def normalize_dates(self, df, date_columns):
        """
        Convert date columns to standard datetime format
        
        Args:
            df (pandas.DataFrame): Input dataframe
            date_columns (list): List of column names containing dates
            
        Returns:
            pandas.DataFrame: Dataframe with normalized dates
        """
        columns_processed = []
        
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    columns_processed.append(col)
                except Exception as e:
                    print(f"Could not convert {col} to datetime: {e}")
        
        self.transformation_log.append({
            'step': 'normalize_dates',
            'timestamp': datetime.now().isoformat(),
            'columns_processed': columns_processed
        })
        
        print(f"Dates normalized for columns: {', '.join(columns_processed)}")
        return df
    
    def aggregate_data(self, df, group_by, agg_dict):
        """
        Aggregate data based on specified columns
        
        Args:
            df (pandas.DataFrame): Input dataframe
            group_by (list): Columns to group by
            agg_dict (dict): Aggregation operations (e.g., {'revenue': 'sum'})
            
        Returns:
            pandas.DataFrame: Aggregated dataframe
        """
        original_rows = len(df)
        
        aggregated = df.groupby(group_by).agg(agg_dict).reset_index()
        
        self.transformation_log.append({
            'step': 'aggregate_data',
            'timestamp': datetime.now().isoformat(),
            'group_by': group_by,
            'original_rows': original_rows,
            'aggregated_rows': len(aggregated),
            'reduction_percent': round((1 - len(aggregated)/original_rows) * 100, 2)
        })
        
        print(f"Data aggregated: {original_rows} rows → {len(aggregated)} rows")
        return aggregated
    
    def add_calculated_fields(self, df):
        """
        Add calculated fields for analytics
        
        Args:
            df (pandas.DataFrame): Input dataframe
            
        Returns:
            pandas.DataFrame: Dataframe with new calculated fields
        """
        fields_added = []
        
        # Extract date components if date column exists
        if 'date' in df.columns:
            try:
                df['year'] = df['date'].dt.year
                df['month'] = df['date'].dt.month
                df['quarter'] = df['date'].dt.quarter
                df['day_of_week'] = df['date'].dt.dayofweek
                df['week_of_year'] = df['date'].dt.isocalendar().week
                fields_added.extend(['year', 'month', 'quarter', 'day_of_week', 'week_of_year'])
            except Exception as e:
                print(f"Error adding date fields: {e}")
        
        # Calculate profit and margins if revenue and cost exist
        if 'revenue' in df.columns and 'cost' in df.columns:
            df['profit'] = df['revenue'] - df['cost']
            df['profit_margin'] = ((df['profit'] / df['revenue']) * 100).round(2)
            fields_added.extend(['profit', 'profit_margin'])
        
        # Calculate average unit price if revenue and units_sold exist
        if 'revenue' in df.columns and 'units_sold' in df.columns:
            df['avg_unit_price'] = (df['revenue'] / df['units_sold']).round(2)
            fields_added.append('avg_unit_price')
        
        self.transformation_log.append({
            'step': 'add_calculated_fields',
            'timestamp': datetime.now().isoformat(),
            'fields_added': fields_added
        })
        
        print(f"Added {len(fields_added)} calculated fields: {', '.join(fields_added)}")
        return df
    
    def filter_data(self, df, conditions):
        """
        Filter data based on conditions
        
        Args:
            df (pandas.DataFrame): Input dataframe
            conditions (dict): Filter conditions (e.g., {'revenue': '>1000'})
            
        Returns:
            pandas.DataFrame: Filtered dataframe
        """
        original_rows = len(df)
        
        for column, condition in conditions.items():
            if column in df.columns:
                # Parse condition
                if '>' in condition:
                    value = float(condition.replace('>', ''))
                    df = df[df[column] > value]
                elif '<' in condition:
                    value = float(condition.replace('<', ''))
                    df = df[df[column] < value]
                elif '==' in condition:
                    value = condition.replace('==', '').strip()
                    df = df[df[column] == value]
        
        rows_filtered = original_rows - len(df)
        
        self.transformation_log.append({
            'step': 'filter_data',
            'timestamp': datetime.now().isoformat(),
            'conditions': conditions,
            'original_rows': original_rows,
            'final_rows': len(df),
            'rows_filtered': rows_filtered
        })
        
        print(f"Data filtered: {rows_filtered} rows removed")
        return df
    
    def get_transformation_summary(self):
        """
        Get summary of all transformations performed
        
        Returns:
            dict: Summary including all transformation steps and timing
        """
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        summary = {
            'total_transformations': len(self.transformation_log),
            'duration_seconds': round(duration, 2),
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'transformations': self.transformation_log
        }
        
        return summary
    
    def export_to_csv(self, df, output_path):
        """
        Export transformed data to CSV
        
        Args:
            df (pandas.DataFrame): Dataframe to export
            output_path (str): Path where CSV should be saved
            
        Returns:
            bool: True if successful
        """
        try:
            df.to_csv(output_path, index=False)
            print(f"✓ Data exported to {output_path}")
            return True
        except Exception as e:
            print(f"✗ Error exporting data: {e}")
            return False


# Example usage and testing
if __name__ == "__main__":
    print("=" * 60)
    print("Data Transformer - Test Run")
    print("=" * 60)
    
    # Create sample data for testing
    print("\n1. Creating sample data...")
    data = {
        'date': ['2024-01-01', '2024-01-02', '2024-01-01', '2024-01-03', '2024-01-03'],
        'product': ['A', 'B', 'A', 'C', None],
        'category': ['Electronics', 'Clothing', 'Electronics', 'Electronics', 'Home'],
        'region': ['North', 'South', 'North', 'East', 'West'],
        'revenue': [1000, 1500, 1000, 800, None],
        'cost': [600, 900, 600, 500, 400],
        'units_sold': [10, 15, 10, 8, 7]
    }
    df = pd.DataFrame(data)
    print(f"   Created dataframe with {len(df)} rows")
    print("\n   Original data:")
    print(df)
    
    # Initialize transformer
    print("\n2. Initializing transformer...")
    transformer = DataTransformer()
    
    # Test transformations
    print("\n3. Cleaning data...")
    df = transformer.clean_data(df)
    
    print("\n4. Normalizing dates...")
    df = transformer.normalize_dates(df, ['date'])
    
    print("\n5. Adding calculated fields...")
    df = transformer.add_calculated_fields(df)
    
    print("\n6. Filtering data (revenue > 900)...")
    df = transformer.filter_data(df, {'revenue': '>900'})
    
    print("\n   Transformed data:")
    print(df)
    
    print("\n7. Aggregating by category...")
    agg_df = transformer.aggregate_data(
        df.copy(),
        group_by=['category'],
        agg_dict={
            'revenue': 'sum',
            'cost': 'sum',
            'units_sold': 'sum'
        }
    )
    print("\n   Aggregated data:")
    print(agg_df)
    
    # Get transformation summary
    print("\n8. Transformation Summary:")
    summary = transformer.get_transformation_summary()
    print(f"   Total transformations: {summary['total_transformations']}")
    print(f"   Duration: {summary['duration_seconds']} seconds")
    print(f"\n   Detailed log:")
    for i, transform in enumerate(summary['transformations'], 1):
        print(f"   {i}. {transform['step']}")
        for key, value in transform.items():
            if key != 'step' and key != 'timestamp':
                print(f"      {key}: {value}")
    
    print("\n" + "=" * 60)
    print("Test Complete!")
    print("=" * 60)