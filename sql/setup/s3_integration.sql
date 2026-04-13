USE ROLE ACCOUNTADMIN;
 
CREATE STORAGE INTEGRATION s3_meetup_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::631957124123:role/snowflake-meetup-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://amzn-s3-meetup-bucket-631957124123-us-east-2-an/');
 
-- Ejecuta esto y copia STORAGE_AWS_IAM_USER_ARN y STORAGE_AWS_EXTERNAL_ID
DESC INTEGRATION s3_meetup_integration;
 
USE ROLE SYSADMIN;
 
CREATE OR REPLACE STAGE MEETUP_DB.BRONZE.S3_STAGE
    STORAGE_INTEGRATION = s3_meetup_integration
    URL                 = 's3://amzn-s3-meetup-bucket-631957124123-us-east-2-an/raw/'
    FILE_FORMAT         = (
        TYPE                          = CSV
        FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
        SKIP_HEADER                   = 1
        NULL_IF                       = ('NULL', 'null', '')
        EMPTY_FIELD_AS_NULL           = TRUE
        ENCODING                      = 'UTF-8'
    );
 
LIST @MEETUP_DB.BRONZE.S3_STAGE;

CREATE OR REPLACE FILE FORMAT MEETUP_DB.GOLD.PARQUET_FORMAT
    TYPE                         = PARQUET
    SNAPPY_COMPRESSION           = TRUE;

CREATE OR REPLACE STAGE MEETUP_DB.GOLD.S3_GOLD_STAGE
    STORAGE_INTEGRATION = s3_meetup_integration
    URL                 = 's3://amzn-s3-meetup-bucket-631957124123-us-east-2-an/gold/'
    FILE_FORMAT         = MEETUP_DB.GOLD.PARQUET_FORMAT
    DIRECTORY           = (ENABLE = TRUE);