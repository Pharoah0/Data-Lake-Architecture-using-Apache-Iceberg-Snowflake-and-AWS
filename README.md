# Real-Time Streaming Application with Apache Kafka

Author: Pharoah Evelyn

<p align="center">
    <img src="https://github.com/Pharoah0/Data-Lake-Architecture-using-Apache-Iceberg-Snowflake-and-AWS/blob/main/images/Iceberg%20Snowflake%20AWS.drawio.png" />
</p>

## Overview

This repository outlines how to enhance interoperability between Snowflake and AWS using AWS Glue and Apache Iceberg to support ACID (Atomicity, Consistency, Isolation, Durability) transactions on a data lake, ensuring data integrity and reliability.

## Business Problem

A Financial Services firm is facing a critical business problem. They need to analyze insurance data to identify customers who are likely to churn or be potentially fraudulent. This is particularly urgent due to a high number of recent quote requests, which could indicate a significant change in customer behavior or potential fraud.

## Data Preparation

The Quotes data is collected from systems and stored as parquet on S3, while Customer and Policy data is already available as internal Snowflake tables.

## Methods Used

AWS Glue was employed to create Apache Iceberg tables within a Glue Data Catalog. A Glue Crawler was then used to add existing data within S3 to the Glue Data Catalog.

Snowflake integration is then configured with the Glue Data Catalog via AWS IAM Roles. This involves creating a role in AWS IAM, granting necessary permissions to access the Glue Data Catalog, and then passing the role's credentials to Snowflake via the Snowflake web interface or command line.

Once the process is complete, Snowflake will have streamlined access to the Iceberg Tables stored on S3. Snowflake can seamlessly analyze Iceberg and Internal Tables without the need for code translation, using familiar SQL commands. The results are then written back into S3 in Iceberg format, ready to be utilized by the Glue Data Catalog and other services/engines, ensuring a highly efficient workflow!

## Ways to improve this project

###### Option 1:

It is possible to use Apache Iceberg as an external catalog. Data can be ingested into S3, and then the iceberg metadata will be created by AWS Glue via Glue Crawler.

Snowflake will read the Glue Data Catalog to create or update Iceberg Tables on S3 for direct consumption. Metadata and statistics will be updated in Snowflake for performance optimization.

Iceberg Tables can then be used with Internal Tables within Snowflake for Data collaboration, BI Analytics with Quicksight, or Machine Learning with SageMaker and other tasks. This allows for seamless integration of data from different sources and formats, enabling comprehensive data analysis and insights.

<p align="center">
    <img src="" />
</p>

###### Option 2:

Similarly, Snowflake itself can house the data catalog. Data could be ingested directly into Snowflake, and the Iceberg tables can be created within S3 with a generated Snowflake catalog.

Snowflake Catalog metadata can be written to S3, then read by a Glue Crawler to be stored in a Glue Data Catalog.

Snowflake can use Iceberg Tables for BI and Machine Learning, and AWS services like EMR, Glue, and Athena from the Glue Data Catalog can also use them!

<p align="center">
    <img src="" />
</p>
