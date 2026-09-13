# Uttarakhand Trek Intelligence
## AWS & Snowflake Project Information

### AWS
Region: ap-south-1
S3 Bucket: ayush-uttarakhand-trek-intelligence

### S3 Paths
Gold Treks: s3://ayush-uttarakhand-trek-intelligence/gold/treks/
Gold Weather: s3://ayush-uttarakhand-trek-intelligence/gold/weather/
Trek Summary: s3://ayush-uttarakhand-trek-intelligence/gold/treks/summary/
Weather Summary: s3://ayush-uttarakhand-trek-intelligence/gold/weather/summary/

### IAM
Role Name: SnowflakeTrekS3Role
Role ARN: arn:aws:iam::282353613738:role/SnowflakeTrekS3Role
Policy Name: SnowflakeTrekS3ReadPolicy

### Data Layers
Bronze → Raw CSV + Weather JSON
Silver → Clean Parquet
Gold → Analytics-ready Parquet

### Snowflake
Purpose: Data Warehouse
Source: AWS S3 Gold Layer