parameters = {
    'schema_name' : 'supplychain',
    'table_name' : 'dim_supplier',
    'bucket_name': 'batchdatafiles' ,
    'iam_role': 'arn:aws:iam::%s:role/RedshiftS3GlueAccess',
    'format':'PARQUET'
	}
