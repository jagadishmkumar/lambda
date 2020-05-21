
# Introduction :

> This lambda function can be configured to COPY parquet files from S3 event trigger to Redshift

## Usage example

1. Modify the config.py file with the appropriate settings for bucket, role, schema and table
2. Upload the changes to config.py to update function.zip file
3. Upload the function.zip file to lambda service
4. Set up event notification trigger on source s3 bucket
   

