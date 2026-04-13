import os
 
S3_BUCKET = os.getenv("S3_BUCKET", "amzn-s3-meetup-bucket-631957124123-us-east-2-an")
S3_GOLD_PREFIX = f"s3://{S3_BUCKET}/gold"
S3_RAW_PREFIX  = f"s3://{S3_BUCKET}/raw"
