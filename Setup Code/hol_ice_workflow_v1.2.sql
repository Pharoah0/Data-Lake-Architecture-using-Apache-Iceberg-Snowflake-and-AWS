/*--------------------------------------------------------------------------------
  Snowflake and AWS Iceberg Hands on Lab - Setup



  Authors:  Andries Engelbrecht
  Updated:  March 25, 2024
  Version:  1.2
--------------------------------------------------------------------------------*/


/* ---------------------------------------------------------------------------
First we set the context for execution
----------------------------------------------------------------------------*/

USE ROLE HOL_ICE_RL;

USE HOL_ICE_DB.PUBLIC;

USE WAREHOUSE HOL_ICE_WH;

/* ---------------------------------------------------------------------------
Configure External Volume and Glue Catalog integration
See Snowflake documentation for details
----------------------------------------------------------------------------*/
--External Volume
-- Create S3 Policy and IAM role in AWS 

/* NOTE TO ENTER YOUR INFORMATION IN THIS SECTION
-- Enter your S3 bucket name that was created in the STORAGE BASE URL
-- Enter your AWS account ID in the STORAGE AWS ROLE ARN
-- Copy your AWS role that was created in the STORAGE AWS ROLE ARN
-------------------------------------------------------------------------*/

CREATE OR REPLACE EXTERNAL VOLUME HOL_ICE_EXT_VOL
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'my-s3-ice-ext-vol'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://<enter your S3 bucket name>/iceberg/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<enter your AWS account ID>:role/<your AWS Role that was created>'

         )
      );

DESC EXTERNAL VOLUME HOL_ICE_EXT_VOL;


-- NOTE: Update the AWS IAM Role Trust Policy with the Snowflake IAM USER ARN and EXTERNAL ID


--Glue Catalog
-- Create Glue Catalog Policy and IAM role in AWS 

/* NOTE TO ENTER YOUR INFORMATION IN THIS SECTION
-- Enter your AWS account ID in the GLUE AWS ROLE ARN
-- Copy your AWS role that was created in the STORAGE AWS ROLE ARN
-- Enter your AWS account ID in the GLUE CATALOG ID
-------------------------------------------------------------------------*/

CREATE or REPLACE CATALOG INTEGRATION HOL_ICE_GLUE_CAT_INT
  CATALOG_SOURCE=GLUE
  CATALOG_NAMESPACE='iceberg_devday'
  TABLE_FORMAT=ICEBERG
  GLUE_AWS_ROLE_ARN='arn:aws:iam::<enter your AWS account ID>:role/<your AWS Role that was created>'
  GLUE_CATALOG_ID='<enter your AWS account ID>'
  ENABLED=TRUE; 

DESC CATALOG INTEGRATION HOL_ICE_GLUE_CAT_INT;

-- NOTE: Update the AWS IAM Role Trust Policy with the Snowflake IAM USER ARN and EXTERNAL ID



/* ---------------------------------------------------------------------------
Create an Unmanaged Iceberg table using Glue Catalog integration
----------------------------------------------------------------------------*/

CREATE OR REPLACE ICEBERG TABLE QUOTES_ICE
  EXTERNAL_VOLUME='HOL_ICE_EXT_VOL'
  CATALOG='HOL_ICE_GLUE_CAT_INT'
  CATALOG_TABLE_NAME='QUOTES'; 

-- Query 1: Read the Quotes data in Iceberg format from S3
SELECT * FROM QUOTES_ICE LIMIT 10;


/*---------------------------------------------------------------------------
We can now combine internal Snowflake tables CUSTOMER and POLICIES with the 
Iceberg table QUOTES_ICE for analysis
-----------------------------------------------------------------------------*/

-- Query 2: Combining the Iceberg table (Quotes data) withe inetrnal Snowflake Customer table data for analysis.
--Join customers with more than 5 quotes with the quotes table 
SELECT C.FULLNAME, C.POSTCODE, C.CUSTID, C.IPID, C.PRODUCTNAME, C.QUOTECOUNT,
Q.POLICYNO, Q.QUOTEDATE, Q.QUOTE_PRODUCT, Q.ORIGINALPREMIUM, Q.TOTALPREMIUMPAYABLE 
FROM CUSTOMER C, QUOTES_ICE Q
WHERE C.FULLNAME = Q.FULLNAME
AND C.POSTCODE = Q.POSTCODE
AND C.QUOTECOUNT > 5
ORDER BY C.QUOTECOUNT DESC;

-- Query 3: Create an aggreagte data set by combining the Quotes data in Iceberg with Customer and Policy data in internal Snowfake tables.
-- Let's add the Policy data to the customer and quote data to get a more complete
-- picture of which policies and customer are doign what quotes for potential 
-- further analysis

WITH CUSTQUOTE AS
(SELECT C.FULLNAME, C.POSTCODE, C.CUSTID, C.IPID, C.PRODUCTNAME, C.QUOTECOUNT,
Q.POLICYNO, Q.QUOTEDATE, Q.QUOTE_PRODUCT, Q.ORIGINALPREMIUM, Q.TOTALPREMIUMPAYABLE 
FROM CUSTOMER C, QUOTES_ICE Q
WHERE C.FULLNAME = Q.FULLNAME
AND C.POSTCODE = Q.POSTCODE
AND C.QUOTECOUNT > 5)
SELECT CQ.FULLNAME, CQ.POSTCODE, CQ.CUSTID, CQ.IPID, CQ.PRODUCTNAME,
CQ.QUOTECOUNT, CQ.POLICYNO, CQ.QUOTEDATE, CQ.QUOTE_PRODUCT,
CQ.ORIGINALPREMIUM, CQ.TOTALPREMIUMPAYABLE, 
P.CREATEDDATE, P.BRAND, P.BRANCHCODE, P.POLICY_STATUS_DESC,
P.TYPEOFCOVER_DESC, P.INSURER_NAME, P.INCEPTIONDATE, P.RENEWALDATE
FROM CUSTQUOTE CQ, POLICIES P
WHERE CQ.CUSTID = P.CUSTID;



/*We can now create a Managed Iceberg table that can be used by
  Snowflake, Glue or any other service/engine that support Iceberg
  and have permissions in the customer managed S3 location */
-- Query 4: Writing data in Iceberg Format back to S3
CREATE OR REPLACE ICEBERG TABLE QUOTE_ANALYSIS_ICE  
  CATALOG='SNOWFLAKE'
  EXTERNAL_VOLUME='HOL_ICE_EXT_VOL'
  BASE_LOCATION='quoteanalysisiceberg'
  AS 
  WITH CUSTQUOTE AS
(SELECT C.FULLNAME, C.POSTCODE, C.CUSTID, C.IPID, C.PRODUCTNAME, C.QUOTECOUNT,
Q.POLICYNO, Q.QUOTEDATE, Q.QUOTE_PRODUCT, Q.ORIGINALPREMIUM, Q.TOTALPREMIUMPAYABLE 
FROM CUSTOMER C, QUOTES_ICE Q
WHERE C.FULLNAME = Q.FULLNAME
AND C.POSTCODE = Q.POSTCODE
AND C.QUOTECOUNT > 5)
SELECT CQ.FULLNAME, CQ.POSTCODE, CQ.CUSTID, CQ.IPID, CQ.PRODUCTNAME,
CQ.QUOTECOUNT, CQ.POLICYNO, CQ.QUOTEDATE, CQ.QUOTE_PRODUCT,
CQ.ORIGINALPREMIUM, CQ.TOTALPREMIUMPAYABLE, 
P.CREATEDDATE, P.BRAND, P.BRANCHCODE, P.POLICY_STATUS_DESC,
P.TYPEOFCOVER_DESC, P.INSURER_NAME, P.INCEPTIONDATE, P.RENEWALDATE
FROM CUSTQUOTE CQ, POLICIES P
WHERE CQ.CUSTID = P.CUSTID;


--Query 5: Using Snowflake to analyze the Iceberg table
SELECT DISTINCT(CUSTID), FULLNAME, POSTCODE,IPID, PRODUCTNAME, QUOTECOUNT,
POLICYNO, QUOTEDATE, QUOTE_PRODUCT, ORIGINALPREMIUM, TOTALPREMIUMPAYABLE,
CREATEDDATE, BRAND, BRANCHCODE, POLICY_STATUS_DESC, TYPEOFCOVER_DESC,
INSURER_NAME, INCEPTIONDATE, RENEWALDATE
FROM QUOTE_ANALYSIS_ICE
WHERE TOTALPREMIUMPAYABLE >100
AND POLICY_STATUS_DESC = 'Renewed' 
ORDER BY CREATEDDATE DESC;

