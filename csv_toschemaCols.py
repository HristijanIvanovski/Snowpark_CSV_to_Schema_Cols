from snowpark import *
from snowpark.functions import *

# Create a Snowpark session
session = Session()

# Load CSV file into DataFrame
df = session.read.format('csv').load('my_file.csv')

# Get column names of CSV file
csv_columns = df.columns

# Get column names of target table
target_table_columns = session.table('my_target_table').columns

# Find common columns
common_columns = list(set(csv_columns) & set(target_table_columns))

# Rearrange columns in DataFrame to match target table column order
df = df.select([col(c) for c in target_table_columns if c in common_columns])

# Write DataFrame to target table
df.write.mode('append').saveAsTable('my_target_table')

# Get column names of staging table
staging_table_columns = session.table('my_staging_table').columns

# Find new columns in CSV file
new_columns = list(set(csv_columns) - set(staging_table_columns))

# Add new columns to staging table
for col_name in new_columns:
    session.sql(f'ALTER TABLE my_staging_table ADD COLUMN {col_name} VARIANT')

# Write DataFrame to staging table
df.write.mode('append').saveAsTable('my_staging_table')
